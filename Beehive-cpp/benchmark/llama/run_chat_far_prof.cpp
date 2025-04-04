/* Inference for Llama-2 Transformer model in pure C */

#include <ctype.h>
#include <fcntl.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <x86intrin.h>

#include <chrono>
#include <thread>
#include <unordered_map>

#include "cache/accessor.hpp"
#include "data_structure/far_vector.hpp"
#include "rdma/client.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/parallel.hpp"
#include "utils/perf.hpp"
#if defined _WIN32
#include "win.h"
#else
#include <sys/mman.h>
#include <unistd.h>
#endif

// #define STANDALONE
// ----------------------------------------------------------------------------
// Transformer model

static std::unordered_map<std::string, size_t> tus;
static std::unordered_map<std::string, size_t> cnts;
using namespace FarLib;
using namespace FarLib::rdma;
using namespace std::chrono_literals;
static constexpr size_t UTHREAD_FACTOR = FarVector<float>::UTHREAD_FACTOR;
static inline size_t get_thread_count() {
    return uthread::get_worker_count() * UTHREAD_FACTOR;
}

template <typename F>
static void prof(const std::string name, F&& f, size_t count = 1) {
    {
        auto start = std::chrono::high_resolution_clock::now();
        f();
        auto end = std::chrono::high_resolution_clock::now();
        tus[name] +=
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                .count();
        cnts[name] += count;
    }
}

void prof_res_print() {
    for (auto& p : tus) {
        std::cout << "avg " << p.first << ": "
                  << static_cast<double>(p.second) / cnts[p.first] << "us"
                  << std::endl;
    }
}
typedef struct {
    int dim;         // transformer dimension
    int hidden_dim;  // for ffn layers
    int n_layers;    // number of layers
    int n_heads;     // number of query heads
    int n_kv_heads;  // number of key/value heads (can be < query heads because
                     // of multiquery)
    int vocab_size;  // vocabulary size, usually 256 (byte-level)
    int seq_len;     // max sequence length
} Config;

struct TransformerWeights {
    // token embedding table
    FarVector<float> token_embedding_table;  // (vocab_size, dim)
    // weights for rmsnorms
    FarVector<float> rms_att_weight;  // (layer, dim) rmsnorm weights
    FarVector<float> rms_ffn_weight;  // (layer, dim)
    // weights for matmuls. note dim == n_heads * head_size
    FarVector<float> wq;  // (layer, dim, n_heads * head_size)
    FarVector<float> wk;  // (layer, dim, n_kv_heads * head_size)
    FarVector<float> wv;  // (layer, dim, n_kv_heads * head_size)
    FarVector<float> wo;  // (layer, n_heads * head_size, dim)
    // weights for ffn
    FarVector<float> w1;  // (layer, hidden_dim, dim)
    FarVector<float> w2;  // (layer, dim, hidden_dim)
    FarVector<float> w3;  // (layer, hidden_dim, dim)
    // final rmsnorm
    FarVector<float> rms_final_weight;  // (dim,)
    // (optional) classifier weights for the logits, on the last layer
    FarVector<float> wcls;

    void free() {
        token_embedding_table.clear();
        rms_att_weight.clear();
        rms_ffn_weight.clear();
        wq.clear();
        wk.clear();
        wv.clear();
        wo.clear();
        w1.clear();
        w2.clear();
        w3.clear();
        rms_final_weight.clear();
        wcls.clear();
    }
};

typedef struct {
    // current wave of activations
    float* x;       // activation at current time stamp (dim,)
    float* xb;      // same, but inside a residual branch (dim,)
    float* xb2;     // an additional buffer just for convenience (dim,)
    float* hb;      // buffer for hidden dimension in the ffn (hidden_dim,)
    float* hb2;     // buffer for hidden dimension in the ffn (hidden_dim,)
    float* q;       // query (dim,)
    float* att;     // buffer for scores/attention values (n_heads, seq_len)
    float* logits;  // output logits
    // kv cache
    FarVector<float> key_cache;    // (layer, seq_len, dim)
    FarVector<float> value_cache;  // (layer, seq_len, dim)
} RunState;

typedef struct {
    Config config;  // the hyperparameters of the architecture (the blueprint)
    TransformerWeights weights;  // the weights of the model
    RunState
        state;  // buffers for the "wave" of activations in the forward pass
    // some more state needed to properly clean up the memory mapping (sigh)
    int fd;             // file descriptor for memory mapping
    float* data;        // memory mapped data pointer
    ssize_t file_size;  // size of the checkpoint file in bytes
} Transformer;

void malloc_run_state(RunState* s, Config* p) {
    // we calloc instead of malloc to keep valgrind happy
    int kv_dim = (p->dim * p->n_kv_heads) / p->n_heads;
    s->x = static_cast<float*>(
        calloc(p->dim, sizeof(float)));  // 16K for llama-7b-chat
    s->xb = static_cast<float*>(
        calloc(p->dim, sizeof(float)));  // 16K for llama-7b-chat
    s->xb2 = static_cast<float*>(
        calloc(p->dim, sizeof(float)));  // 16K for llama-7b-chat
    s->hb = static_cast<float*>(
        calloc(p->hidden_dim, sizeof(float)));  // 43K for llama-7b-chat
    s->hb2 = static_cast<float*>(
        calloc(p->hidden_dim, sizeof(float)));  // 43K for llama-7b-chat
    s->q = static_cast<float*>(
        calloc(p->dim, sizeof(float)));  // 16K for llama-7b-chat
    const size_t key_cache_size =
        p->n_layers * p->seq_len * kv_dim;  // 1G for llama-7b-chat
    const size_t value_cache_size =
        p->n_layers * p->seq_len * kv_dim;  // 1G for llama-7b-chat
    s->key_cache.resize(key_cache_size);
    s->value_cache.resize(value_cache_size);
    s->att = static_cast<float*>(calloc(
        p->n_heads * p->seq_len, sizeof(float)));  // 256K for llama-7b-chat
    s->logits = static_cast<float*>(
        calloc(p->vocab_size, sizeof(float)));  // 125K for llama-7b-chat
    // ensure all mallocs went fine
    if (!s->x || !s->xb || !s->xb2 || !s->hb || !s->hb2 || !s->q ||
        s->key_cache.size() != key_cache_size ||
        s->value_cache.size() != value_cache_size || !s->att || !s->logits) {
        fprintf(stderr, "malloc failed!\n");
        exit(EXIT_FAILURE);
    }
}

void free_run_state(RunState* s) {
    free(s->x);
    free(s->xb);
    free(s->xb2);
    free(s->hb);
    free(s->hb2);
    free(s->q);
    free(s->att);
    free(s->logits);
    s->key_cache.clear();
    s->value_cache.clear();
}

void memory_map_weights(TransformerWeights* w, Config* p, float* ptr,
                        int shared_weights) {
    int head_size = p->dim / p->n_heads;
    // make sure the multiplications below are done in 64bit to fit the
    // parameter counts of 13B+ models
    unsigned long long n_layers = p->n_layers;
    const size_t token_embedding_table_size =
        p->vocab_size * p->dim;  // 125M for llama-7b-chat
    const size_t rms_att_weight_size =
        n_layers * p->dim;  // 128K for llama-7b-chat
    const size_t wq_size =
        n_layers * p->dim * (p->n_heads * head_size);  // 512M for llama-7b-chat
    const size_t wk_size =
        n_layers * p->dim *
        (p->n_kv_heads * head_size);  // 512M for llama-7b-chat
    const size_t wv_size =
        n_layers * p->dim *
        (p->n_kv_heads * head_size);  // 512M for llama-7b-chat
    const size_t wo_size =
        n_layers * (p->n_heads * head_size) * p->dim;  // 512M for llama-7b-chat
    const size_t rms_ffn_weight_size =
        n_layers * p->dim;  // 128K for llama-7b-chat
    const size_t w1_size =
        n_layers * p->dim * p->hidden_dim;  // 1376M for llama-7b-chat
    const size_t w2_size =
        n_layers * p->hidden_dim * p->dim;  // 1376M for llama-7b-chat
    const size_t w3_size =
        n_layers * p->dim * p->hidden_dim;  // 1376M for llama-7b-chat
    const size_t rms_final_weight_size =
        p->dim + p->seq_len * head_size / 2 +
        p->seq_len * head_size / 2;  // 4K + 128K + 128K for llama-7b-chat
    const size_t wcls_size = p->dim * p->vocab_size;  // 125M for llama-7b-chat
    float* token_embedding_table_ptr = ptr;
    w->token_embedding_table.assign_all(ptr, token_embedding_table_size);
    ptr += token_embedding_table_size;
    w->rms_att_weight.assign_all(ptr, rms_att_weight_size);
    ptr += rms_att_weight_size;
    w->wq.assign_all(ptr, wq_size);
    ptr += wq_size;
    w->wk.assign_all(ptr, wk_size);
    ptr += wk_size;
    w->wv.assign_all(ptr, wv_size);
    ptr += wv_size;
    w->wo.assign_all(ptr, wo_size);
    ptr += wo_size;
    w->rms_ffn_weight.assign_all(ptr, rms_ffn_weight_size);
    ptr += rms_ffn_weight_size;
    w->w1.assign_all(ptr, w1_size);
    ptr += w1_size;
    w->w2.assign_all(ptr, w2_size);
    ptr += w2_size;
    w->w3.assign_all(ptr, w3_size);
    ptr += w3_size;
    w->rms_final_weight.assign_all(ptr, rms_final_weight_size);
    ptr += rms_final_weight_size;
    w->wcls.assign_all(shared_weights ? token_embedding_table_ptr : ptr,
                       wcls_size);
}

void read_checkpoint(const char* checkpoint, Config* config,
                     TransformerWeights* weights, int* fd, float** data,
                     ssize_t* file_size) {
    FILE* file = fopen(checkpoint, "rb");
    if (!file) {
        fprintf(stderr, "Couldn't open file %s\n", checkpoint);
        exit(EXIT_FAILURE);
    }
    // read in the config header
    if (fread(config, sizeof(Config), 1, file) != 1) {
        exit(EXIT_FAILURE);
    }
    // negative vocab size is hacky way of signaling unshared weights. bit
    // yikes.
    int shared_weights = config->vocab_size > 0 ? 1 : 0;
    config->vocab_size = abs(config->vocab_size);
    // figure out the file size
    fseek(file, 0, SEEK_END);  // move file pointer to end of file
    *file_size = ftell(file);  // get the file size, in bytes
    fclose(file);
    // memory map the Transformer weights into the data pointer
    *fd = open(checkpoint, O_RDONLY);  // open in read only mode
    if (*fd == -1) {
        fprintf(stderr, "open failed!\n");
        exit(EXIT_FAILURE);
    }
    *data = static_cast<float*>(
        mmap(NULL, *file_size, PROT_READ, MAP_PRIVATE, *fd, 0));
    if (*data == MAP_FAILED) {
        fprintf(stderr, "mmap failed!\n");
        exit(EXIT_FAILURE);
    }
    float* weights_ptr = *data + sizeof(Config) / sizeof(float);
    memory_map_weights(weights, config, weights_ptr, shared_weights);
}

void build_transformer(Transformer* t, const char* checkpoint_path) {
    // read in the Config and the Weights from the checkpoint
    read_checkpoint(checkpoint_path, &t->config, &t->weights, &t->fd, &t->data,
                    &t->file_size);
    // allocate the RunState buffers
    malloc_run_state(&t->state, &t->config);
}

void free_transformer(Transformer* t) {
    // close the memory mapping
    if (t->data != MAP_FAILED) {
        munmap(t->data, t->file_size);
    }
    t->weights.free();
    if (t->fd != -1) {
        close(t->fd);
    }
    // free the RunState buffers
    free_run_state(&t->state);
}

// ----------------------------------------------------------------------------
// neural net blocks; the dynamics of the Transformer

void rmsnorm(float* o, float* x, float* weight, int size) {
    // calculate sum of squares
    float ss = 0.0f;
    for (int j = 0; j < size; j++) {
        ss += x[j] * x[j];
    }
    ss /= size;
    ss += 1e-5f;
    ss = 1.0f / sqrtf(ss);
    // normalize and scale
    for (int j = 0; j < size; j++) {
        o[j] = weight[j] * (ss * x[j]);
    }
}

void rmsnorm(float* o, float* x, FarVector<float>& weight_fv, size_t start,
             int size) {
    // calculate sum of squares
    float ss = 0.0f;
    for (int j = 0; j < size; j++) {
        ss += x[j] * x[j];
    }
    ss /= size;
    ss += 1e-5f;
    ss = 1.0f / sqrtf(ss);
    // normalize and scale
    const size_t thread_cnt = get_thread_count();
    const size_t block = (size + thread_cnt - 1) / thread_cnt;
    uthread::parallel_for_with_scope<1>(
        thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
            using it_t = decltype(weight_fv.clbegin());
            const size_t o_start = i * block;
            const size_t o_end =
                std::min(o_start + block, static_cast<size_t>(size));
            const size_t idx_start = o_start + start;
            const size_t idx_end = o_end + start;

            if (idx_start >= idx_end) {
                return;
            }
            struct Scope : public DereferenceScope {
                it_t it;

                void pin() const override { it.pin(); }

                void unpin() const override { it.unpin(); }

                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
            } scp(&scope);
            scp.it = weight_fv.get_const_lite_iter(idx_start, scp, idx_start,
                                                   idx_end);
            for (size_t oi = o_start; oi < o_end; oi++, scp.it.next(scp)) {
                o[oi] = *(scp.it) * (ss * x[oi]);
            }
        });
}

void softmax(float* x, int size) {
    // find max value (for numerical stability)
    float max_val = x[0];
    for (int i = 1; i < size; i++) {
        if (x[i] > max_val) {
            max_val = x[i];
        }
    }
    // exp and sum
    float sum = 0.0f;
    for (int i = 0; i < size; i++) {
        x[i] = expf(x[i] - max_val);
        sum += x[i];
    }
    // normalize
    for (int i = 0; i < size; i++) {
        x[i] /= sum;
    }
}

void matmul(float* xout, float* x, float* w, int n, int d) {
    // W (d,n) @ x (n,) -> xout (d,)
    // by far the most amount of time is spent inside this little function
    int i;
#pragma omp parallel for private(i)
    for (i = 0; i < d; i++) {
        float val = 0.0f;
        for (int j = 0; j < n; j++) {
            val += w[i * n + j] * x[j];
        }
        xout[i] = val;
    }
}

void matmul(float* xout, float* x, FarVector<float>& weight_fv, size_t wstart,
            int n, int d) {
    // W (d,n) @ x (n,) -> xout (d,)
    // by far the most amount of time is spent inside this little function
    const size_t thread_cnt = get_thread_count();
    const size_t block = (d + thread_cnt - 1) / thread_cnt;
    uthread::parallel_for_with_scope<1>(
        thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
            const size_t d_start = i * block;
            const size_t d_end =
                std::min(d_start + block, static_cast<size_t>(d));
            const size_t idx_start = wstart + d_start * n;
            const size_t idx_end = wstart + d_end * n;
            if (d_start >= d_end) {
                return;
            }
            using it_t = decltype(weight_fv.clbegin());
            struct Scope : public DereferenceScope {
                it_t it;

                void pin() const override { it.pin(); }

                void unpin() const override { it.unpin(); }

                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
            } scp(&scope);
            scp.it = weight_fv.get_const_lite_iter(idx_start, scp, idx_start,
                                                   idx_end);
            for (size_t dd = d_start; dd < d_end; dd++) {
                float val = 0.0f;
                for (size_t j = 0; j < n; j++, scp.it.next(scp)) {
                    val += *(scp.it) * x[j];
                }
                xout[dd] = val;
            }
        });
}

void matmul(FarVector<float>& xout_fv, size_t xout_start, float* x,
            FarVector<float>& weight_fv, size_t wstart, int n, int d) {
    // W (d,n) @ x (n,) -> xout (d,)
    // by far the most amount of time is spent inside this little function
    const size_t thread_cnt = get_thread_count();
    const size_t block = (d + thread_cnt - 1) / thread_cnt;
    uthread::parallel_for_with_scope<1>(
        thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
            const size_t d_start = i * block;
            const size_t d_end =
                std::min(d_start + block, static_cast<size_t>(d));
            const size_t out_start = xout_start + d_start;
            const size_t out_end = xout_start + d_end;
            if (d_start >= d_end) {
                return;
            }
            using w_it_t = decltype(weight_fv.clbegin());
            using out_it_t = decltype(xout_fv.lbegin());
            struct Scope : public DereferenceScope {
                w_it_t w_it;
                out_it_t out_it;
                void pin() const override {
                    w_it.pin();
                    out_it.pin();
                }

                void unpin() const override {
                    w_it.unpin();
                    out_it.unpin();
                }

                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
            } scp(&scope);
            scp.out_it =
                xout_fv.get_lite_iter(out_start, scp, out_start, out_end);
            for (size_t dd = d_start; dd < d_end; dd++, scp.out_it.next(scp)) {
                float val = 0.0f;
                const size_t idx_start = wstart + dd * n;
                const size_t idx_end = wstart + (dd + 1) * n;
                scp.w_it = weight_fv.get_const_lite_iter(idx_start, scp,
                                                         idx_start, idx_end);
                for (size_t j = 0; j < n; j++, scp.w_it.next(scp)) {
                    val += *(scp.w_it) * x[j];
                }
                *(scp.out_it) = val;
            }
        });
}

float* forward(Transformer* transformer, int token, int pos) {
    // a few convenience variables
    Config* p = &transformer->config;
    TransformerWeights* w = &transformer->weights;
    RunState* s = &transformer->state;
    float* x = s->x;
    int dim = p->dim;
    int kv_dim = (p->dim * p->n_kv_heads) / p->n_heads;
    int kv_mul =
        p->n_heads /
        p->n_kv_heads;  // integer multiplier of the kv sharing in multiquery
    int hidden_dim = p->hidden_dim;
    int head_size = dim / p->n_heads;

    // copy the token embedding into x
    w->token_embedding_table.copy_to_local(x, token * dim, dim);

    // forward all the layers
    for (unsigned long long l = 0; l < p->n_layers; l++) {
        // attention rmsnorm
        prof("rmsnorm1",
             [&] { rmsnorm(s->xb, x, w->rms_att_weight, l * dim, dim); });

        // key and value point to the kv cache
        int loff =
            l * p->seq_len * kv_dim;  // kv cache layer offset for convenience

        // qkv matmuls for this position
        const size_t key_cache_start = loff + pos * kv_dim;
        const size_t value_cache_start = loff + pos * kv_dim;
        prof("matmul1",
             [&] { matmul(s->q, s->xb, w->wq, l * dim * dim, dim, dim); });

        prof(
            "matmul2",
            [&] {
                matmul(s->key_cache, loff + pos * kv_dim, s->xb, w->wk,
                       l * dim * kv_dim, dim, kv_dim);
                matmul(s->value_cache, loff + pos * kv_dim, s->xb, w->wv,
                       l * dim * kv_dim, dim, kv_dim);
            },
            2);

        // RoPE relative positional encoding: complex-valued rotate q and k in
        // each head
        prof("uth1", [&] {
            const int min_dim = std::min(dim, kv_dim);
            const size_t thread_cnt = get_thread_count();
            const size_t block = (min_dim / 2 + thread_cnt - 1) / thread_cnt;
            uthread::parallel_for_with_scope<1>(
                thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
                    const int idx_start = i * block * 2;
                    const int idx_end = std::min(
                        min_dim, static_cast<int>(idx_start + block * 2));
                    if (idx_start >= idx_end) {
                        return;
                    }
                    using it_t = decltype(s->key_cache.lbegin());
                    struct Scope : public DereferenceScope {
                        it_t it;
                        it_t it1;

                        void pin() const override {
                            it.pin();
                            it1.pin();
                        }

                        void unpin() const override {
                            it.unpin();
                            it1.unpin();
                        }

                        void next2() {
                            it.nextn(2, *this);
                            it1.nextn(2, *this);
                        }

                        Scope(DereferenceScope* scope)
                            : DereferenceScope(scope) {}
                    } scp(&scope);
                    scp.it = s->key_cache.get_lite_iter(
                        key_cache_start + idx_start, scp,
                        key_cache_start + idx_start, key_cache_start + idx_end);
                    scp.it1 = s->key_cache.get_lite_iter(
                        key_cache_start + idx_start + 1, scp,
                        key_cache_start + idx_start, key_cache_start + idx_end);
                    for (int i = idx_start; i < idx_end; i += 2, scp.next2()) {
                        int head_dim = i % head_size;
                        float freq =
                            1.0f / powf(10000.0f, head_dim / (float)head_size);
                        float val = pos * freq;
                        float fcr = cosf(val);
                        float fci = sinf(val);

                        int rotn =
                            2;  // how many vectors? 2 = q & k, 1 = q only
                        float v0 = *(scp.it);
                        float v1 = *(scp.it1);
                        *(scp.it) = v0 * fcr - v1 * fci;
                        *(scp.it1) = v0 * fci + v1 * fcr;
                    }
                });
        });

        for (int i = 0; i < dim; i += 2) {
            int head_dim = i % head_size;
            float freq = 1.0f / powf(10000.0f, head_dim / (float)head_size);
            float val = pos * freq;
            float fcr = cosf(val);
            float fci = sinf(val);

            int rotn = 1;       // how many vectors? 2 = q & k, 1 = q only
            float* vec = s->q;  // the vector to rotate (query or key)
            float v0 = vec[i];
            float v1 = vec[i + 1];
            vec[i] = v0 * fcr - v1 * fci;
            vec[i + 1] = v0 * fci + v1 * fcr;
        }

        prof("multihead", [&] {
            // multihead attention. iterate over all heads
            const size_t thread_cnt = get_thread_count();
            const size_t block = (p->n_heads + thread_cnt - 1) / thread_cnt;
            uthread::parallel_for_with_scope<1>(
                thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
                    const size_t h_start = i * block;
                    const size_t h_end = std::min(
                        h_start + block, static_cast<size_t>(p->n_heads));
                    if (h_start >= h_end) {
                        return;
                    }
                    for (size_t h = h_start; h < h_end; h++) {
                        // get the query vector for this head
                        float* q = s->q + h * head_size;
                        // attention scores for this head
                        float* att = s->att + h * p->seq_len;
                        // iterate over all timesteps, including the current one
                        using it_t = decltype(s->key_cache.clbegin());
                        struct Scope : public DereferenceScope {
                            it_t it;

                            void pin() const override { it.pin(); }

                            void unpin() const override { it.unpin(); }

                            Scope(DereferenceScope* scope)
                                : DereferenceScope(scope) {}
                        } scp(&scope);
                        for (int t = 0; t <= pos; t++) {
                            // get the key vector for this head and at this
                            // timestep
                            const size_t key_cache_base =
                                loff + t * kv_dim + (h / kv_mul) * head_size;
                            scp.it = s->key_cache.get_const_lite_iter(
                                key_cache_base, scp, key_cache_base,
                                key_cache_base + head_size);
                            // calculate the attention score as the dot
                            // product of q and k
                            float score = 0.0f;
                            for (int i = 0; i < head_size;
                                 i++, scp.it.next(scp)) {
                                score += q[i] * (*(scp.it));
                            }
                            score /= sqrtf(head_size);
                            // save the score to the attention buffer
                            att[t] = score;
                        }

                        // softmax the scores to get attention weights, from
                        // 0..pos inclusively
                        softmax(att, pos + 1);

                        // weighted sum of the values, store back into xb
                        float* xb = s->xb + h * head_size;
                        memset(xb, 0, head_size * sizeof(float));
                        for (int t = 0; t <= pos; t++) {
                            // get the value vector for this head and at this
                            // timestep
                            const size_t value_cache_base =
                                loff + t * kv_dim + (h / kv_mul) * head_size;
                            scp.it = s->value_cache.get_const_lite_iter(
                                value_cache_base, scp, value_cache_base,
                                value_cache_base + head_size);
                            // get the attention weight for this timestep
                            float a = att[t];
                            // accumulate the weighted value into xb
                            for (int i = 0; i < head_size;
                                 i++, scp.it.next(scp)) {
                                xb[i] += a * (*(scp.it));
                            }
                        }
                    }
                });
        });

        // final matmul to get the output of the attention
        prof("matmul1",
             [&] { matmul(s->xb2, s->xb, w->wo, l * dim * dim, dim, dim); });

        // residual connection back into x
        for (int i = 0; i < dim; i++) {
            x[i] += s->xb2[i];
        }

        // ffn rmsnorm
        rmsnorm(s->xb, x, w->rms_ffn_weight, l * dim, dim);

        // Now for FFN in PyTorch we have: self.w2(F.silu(self.w1(x)) *
        // self.w3(x)) first calculate self.w1(x) and self.w3(x)
        prof(
            "matmul1",
            [&] {
                matmul(s->hb, s->xb, w->w1, l * dim * hidden_dim, dim,
                       hidden_dim);
                matmul(s->hb2, s->xb, w->w3, l * dim * hidden_dim, dim,
                       hidden_dim);
            },
            2);

        // SwiGLU non-linearity
        for (int i = 0; i < hidden_dim; i++) {
            float val = s->hb[i];
            // silu(x)=x*σ(x), where σ(x) is the logistic sigmoid
            val *= (1.0f / (1.0f + expf(-val)));
            // elementwise multiply with w3(x)
            val *= s->hb2[i];
            s->hb[i] = val;
        }

        prof("matmul1", [&] {
            // final matmul to get the output of the ffn
            matmul(s->xb, s->hb, w->w2, l * dim * hidden_dim, hidden_dim, dim);
        });

        // residual connection
        for (int i = 0; i < dim; i++) {
            x[i] += s->xb[i];
        }
    }

    // final rmsnorm
    prof("rmsnorm1", [&] { rmsnorm(x, x, w->rms_final_weight, 0, dim); });
    // classifier into logits
    prof("matmul1", [&] {
        matmul(s->logits, x, w->wcls, 0, p->dim,
               p->vocab_size);  // wcls size = p->dim * p->vocab_size = 125M
    });
    return s->logits;
}

// ----------------------------------------------------------------------------
// The Byte Pair Encoding (BPE) Tokenizer that translates strings <-> tokens

typedef struct {
    const char* str;
    int id;
} TokenIndex;

typedef struct {
    char** vocab;
    float* vocab_scores;
    TokenIndex* sorted_vocab;
    int vocab_size;
    unsigned int max_token_length;
    unsigned char byte_pieces[512];  // stores all single-byte strings
} Tokenizer;

int compare_tokens(const void* a, const void* b) {
    return strcmp(((TokenIndex*)a)->str, ((TokenIndex*)b)->str);
}

void build_tokenizer(Tokenizer* t, const char* tokenizer_path, int vocab_size) {
    // i should have written the vocab_size into the tokenizer file... sigh
    t->vocab_size = vocab_size;
    // malloc space to hold the scores and the strings
    t->vocab =
        (char**)malloc(vocab_size * sizeof(char*));  // 31K for llama-7b-chat
    t->vocab_scores =
        (float*)malloc(vocab_size * sizeof(float));  // 125K for llama-7b-chat
    t->sorted_vocab = NULL;                          // initialized lazily
    for (int i = 0; i < 256; i++) {
        t->byte_pieces[i * 2] = (unsigned char)i;
        t->byte_pieces[i * 2 + 1] = '\0';
    }
    // read in the file
    FILE* file = fopen(tokenizer_path, "rb");
    if (!file) {
        fprintf(stderr, "couldn't load %s\n", tokenizer_path);
        exit(EXIT_FAILURE);
    }
    if (fread(&t->max_token_length, sizeof(int), 1, file) != 1) {
        fprintf(stderr, "failed read\n");
        exit(EXIT_FAILURE);
    }
    int len;
    for (int i = 0; i < vocab_size; i++) {
        if (fread(t->vocab_scores + i, sizeof(float), 1, file) != 1) {
            fprintf(stderr, "failed read\n");
            exit(EXIT_FAILURE);
        }
        if (fread(&len, sizeof(int), 1, file) != 1) {
            fprintf(stderr, "failed read\n");
            exit(EXIT_FAILURE);
        }
        t->vocab[i] = (char*)malloc(len + 1);
        if (fread(t->vocab[i], len, 1, file) != 1) {
            fprintf(stderr, "failed read\n");
            exit(EXIT_FAILURE);
        }
        t->vocab[i][len] = '\0';  // add the string terminating token
    }
    fclose(file);
}

void free_tokenizer(Tokenizer* t) {
    for (int i = 0; i < t->vocab_size; i++) {
        free(t->vocab[i]);
    }
    free(t->vocab);
    free(t->vocab_scores);
    free(t->sorted_vocab);
}

char* decode(Tokenizer* t, int prev_token, int token) {
    char* piece = t->vocab[token];
    // following BOS (1) token, sentencepiece decoder strips any leading
    // whitespace (see PR #89)
    if (prev_token == 1 && piece[0] == ' ') {
        piece++;
    }
    // careful, some tokens designate raw bytes, and look like e.g. '<0x01>'
    // parse this and convert and return the actual byte
    unsigned char byte_val;
    if (sscanf(piece, "<0x%02hhX>", &byte_val) == 1) {
        piece = (char*)t->byte_pieces + byte_val * 2;
    }
    return piece;
}

void safe_printf(char* piece) {
    // piece might be a raw byte token, and we only want to print printable
    // chars or whitespace because some of the other bytes can be various
    // control codes, backspace, etc.
    if (piece == NULL) {
        return;
    }
    if (piece[0] == '\0') {
        return;
    }
    if (piece[1] == '\0') {
        unsigned char byte_val = piece[0];
        if (!(isprint(byte_val) || isspace(byte_val))) {
            return;  // bad byte, don't print it
        }
    }
    printf("%s", piece);
}

int str_lookup(const char* str, TokenIndex* sorted_vocab, int vocab_size) {
    // efficiently find the perfect match for str in vocab, return its index or
    // -1 if not found
    TokenIndex tok = {.str = str};  // acts as the key to search for
    TokenIndex* res = static_cast<TokenIndex*>(bsearch(
        &tok, sorted_vocab, vocab_size, sizeof(TokenIndex), compare_tokens));
    return res != NULL ? res->id : -1;
}

void encode(Tokenizer* t, char* text, int8_t bos, int8_t eos, int* tokens,
            int* n_tokens) {
    // encode the string text (input) into an upper-bound preallocated tokens[]
    // array bos != 0 means prepend the BOS token (=1), eos != 0 means append
    // the EOS token (=2)
    if (text == NULL) {
        fprintf(stderr, "cannot encode NULL text\n");
        exit(EXIT_FAILURE);
    }

    if (t->sorted_vocab == NULL) {
        // lazily malloc and sort the vocabulary
        t->sorted_vocab = static_cast<TokenIndex*>(malloc(
            t->vocab_size * sizeof(TokenIndex)));  // 500K for llama-7b-chat
        for (int i = 0; i < t->vocab_size; i++) {
            t->sorted_vocab[i].str = t->vocab[i];
            t->sorted_vocab[i].id = i;
        }
        qsort(t->sorted_vocab, t->vocab_size, sizeof(TokenIndex),
              compare_tokens);
    }

    // create a temporary buffer that will store merge candidates of always two
    // consecutive tokens *2 for concat, +1 for null terminator +2 for UTF8 (in
    // case max_token_length is 1)
    char* str_buffer = static_cast<char*>(
        malloc((t->max_token_length * 2 + 1 + 2) * sizeof(char)));
    size_t str_len = 0;

    // start at 0 tokens
    *n_tokens = 0;

    // add optional BOS (=1) token, if desired
    if (bos) tokens[(*n_tokens)++] = 1;

    // add_dummy_prefix is true by default
    // so prepend a dummy prefix token to the input string, but only if text !=
    // ""
    // TODO: pretty sure this isn't correct in the general case but I don't have
    // the energy to read more of the sentencepiece code to figure out what it's
    // doing
    if (text[0] != '\0') {
        int dummy_prefix = str_lookup(" ", t->sorted_vocab, t->vocab_size);
        tokens[(*n_tokens)++] = dummy_prefix;
    }

    // Okay UTF-8 time. This will get messy. Here is the reference from
    // Wikipedia: Code point ↔ UTF-8 conversion First code point	Last code
    // point	Byte 1	Byte 2	Byte 3	Byte 4 U+0000	U+007F	    0xxxxxxx
    // U+0080	U+07FF	    110xxxxx	10xxxxxx
    // U+0800	U+FFFF	    1110xxxx	10xxxxxx	10xxxxxx
    // U+10000	U+10FFFF    11110xxx	10xxxxxx	10xxxxxx	10xxxxxx

    // process the raw (UTF-8) byte sequence of the input string
    for (char* c = text; *c != '\0'; c++) {
        // reset buffer if the current byte is ASCII or a leading byte
        // 0xC0 is 11000000, so (*c & 0xC0) keeps the first 2 bits and zeros the
        // rest 0x80 is 10000000 in UTF-8, all continuation bytes start with
        // "10" in first two bits so in English this is: "if this byte is not a
        // continuation byte"
        if ((*c & 0xC0) != 0x80) {
            // this byte must be either a leading byte (11...) or an ASCII char
            // (0x...)
            // => reset our location, as we're starting a new UTF-8 codepoint
            str_len = 0;
        }

        // append the current byte to the buffer
        str_buffer[str_len++] =
            *c;  // ++ is post-increment, incremented after this line
        str_buffer[str_len] = '\0';

        // while the next character is a continuation byte, continue appending
        // but if there are too many of them, just stop to avoid overruning
        // str_buffer size.
        if ((*(c + 1) & 0xC0) == 0x80 && str_len < 4) {
            continue;
        }

        // ok c+1 is not a continuation byte, so we've read in a full codepoint
        int id = str_lookup(str_buffer, t->sorted_vocab, t->vocab_size);

        if (id != -1) {
            // we found this codepoint in vocab, add it as a token
            tokens[(*n_tokens)++] = id;
        } else {
            // byte_fallback encoding: just encode each byte as a token
            // +3 is here because the first 3 vocab elements are <unk>, <s>,
            // </s> so the individual bytes only start at index 3
            for (int i = 0; i < str_len; i++) {
                tokens[(*n_tokens)++] = (unsigned char)str_buffer[i] + 3;
            }
        }
        str_len =
            0;  // protect against a sequence of stray UTF8 continuation bytes
    }

    // merge the best consecutive pair each iteration, according the scores in
    // vocab_scores
    while (1) {
        float best_score = -1e10;
        int best_id = -1;
        int best_idx = -1;

        for (int i = 0; i < (*n_tokens - 1); i++) {
            // check if we can merge the pair (tokens[i], tokens[i+1])
            sprintf(str_buffer, "%s%s", t->vocab[tokens[i]],
                    t->vocab[tokens[i + 1]]);
            int id = str_lookup(str_buffer, t->sorted_vocab, t->vocab_size);
            if (id != -1 && t->vocab_scores[id] > best_score) {
                // this merge pair exists in vocab! record its score and
                // position
                best_score = t->vocab_scores[id];
                best_id = id;
                best_idx = i;
            }
        }

        if (best_idx == -1) {
            break;  // we couldn't find any more pairs to merge, so we're done
        }

        // merge the consecutive pair (best_idx, best_idx+1) into new token
        // best_id
        tokens[best_idx] = best_id;
        // delete token at position best_idx+1, shift the entire sequence back 1
        for (int i = best_idx + 1; i < (*n_tokens - 1); i++) {
            tokens[i] = tokens[i + 1];
        }
        (*n_tokens)--;  // token length decreased
    }

    // add optional EOS (=2) token, if desired
    if (eos) tokens[(*n_tokens)++] = 2;

    free(str_buffer);
}

// ----------------------------------------------------------------------------
// The Sampler, which takes logits and returns a sampled token
// sampling can be done in a few ways: greedy argmax, sampling, top-p sampling

typedef struct {
    float prob;
    int index;
} ProbIndex;  // struct used when sorting probabilities during top-p sampling

typedef struct {
    int vocab_size;
    ProbIndex* probindex;  // buffer used in top-p sampling
    float temperature;
    float topp;
    unsigned long long rng_state;
} Sampler;

int sample_argmax(float* probabilities, int n) {
    // return the index that has the highest probability
    int max_i = 0;
    float max_p = probabilities[0];
    for (int i = 1; i < n; i++) {
        if (probabilities[i] > max_p) {
            max_i = i;
            max_p = probabilities[i];
        }
    }
    return max_i;
}

int sample_mult(float* probabilities, int n, float coin) {
    // sample index from probabilities (they must sum to 1!)
    // coin is a random number in [0, 1), usually from random_f32()
    float cdf = 0.0f;
    for (int i = 0; i < n; i++) {
        cdf += probabilities[i];
        if (coin < cdf) {
            return i;
        }
    }
    return n - 1;  // in case of rounding errors
}

int compare(const void* a, const void* b) {
    ProbIndex* a_ = (ProbIndex*)a;
    ProbIndex* b_ = (ProbIndex*)b;
    if (a_->prob > b_->prob) return -1;
    if (a_->prob < b_->prob) return 1;
    return 0;
}

int sample_topp(float* probabilities, int n, float topp, ProbIndex* probindex,
                float coin) {
    // top-p sampling (or "nucleus sampling") samples from the smallest set of
    // tokens that exceed probability topp. This way we never sample tokens that
    // have very low probabilities and are less likely to go "off the rails".
    // coin is a random number in [0, 1), usually from random_f32()

    int n0 = 0;
    // quicksort indices in descending order of probabilities
    // values smaller than (1 - topp) / (n - 1) cannot be part of the result
    // so for efficiency we crop these out as candidates before sorting
    const float cutoff = (1.0f - topp) / (n - 1);
    for (int i = 0; i < n; i++) {
        if (probabilities[i] >= cutoff) {
            probindex[n0].index = i;
            probindex[n0].prob = probabilities[i];
            n0++;
        }
    }
    qsort(probindex, n0, sizeof(ProbIndex), compare);

    // truncate the list where cumulative probability exceeds topp
    float cumulative_prob = 0.0f;
    int last_idx = n0 - 1;  // in case of rounding errors consider all elements
    for (int i = 0; i < n0; i++) {
        cumulative_prob += probindex[i].prob;
        if (cumulative_prob > topp) {
            last_idx = i;
            break;  // we've exceeded topp by including last_idx
        }
    }

    // sample from the truncated list
    float r = coin * cumulative_prob;
    float cdf = 0.0f;
    for (int i = 0; i <= last_idx; i++) {
        cdf += probindex[i].prob;
        if (r < cdf) {
            return probindex[i].index;
        }
    }
    return probindex[last_idx].index;  // in case of rounding errors
}

void build_sampler(Sampler* sampler, int vocab_size, float temperature,
                   float topp, unsigned long long rng_seed) {
    sampler->vocab_size = vocab_size;
    sampler->temperature = temperature;
    sampler->topp = topp;
    sampler->rng_state = rng_seed;
    // buffer only used with nucleus sampling; may not need but it's ~small
    sampler->probindex = static_cast<ProbIndex*>(malloc(
        sampler->vocab_size * sizeof(ProbIndex)));  // 125K for llama-7b-chat
}

void free_sampler(Sampler* sampler) { free(sampler->probindex); }

unsigned int random_u32(unsigned long long* state) {
    // xorshift rng: https://en.wikipedia.org/wiki/Xorshift#xorshift.2A
    *state ^= *state >> 12;
    *state ^= *state << 25;
    *state ^= *state >> 27;
    return (*state * 0x2545F4914F6CDD1Dull) >> 32;
}
float random_f32(unsigned long long* state) {  // random float32 in [0,1)
    return (random_u32(state) >> 8) / 16777216.0f;
}

int sample(Sampler* sampler, float* logits) {
    // sample the token given the logits and some hyperparameters
    int next;
    if (sampler->temperature == 0.0f) {
        // greedy argmax sampling: take the token with the highest probability
        next = sample_argmax(logits, sampler->vocab_size);
    } else {
        // apply the temperature to the logits
        for (int q = 0; q < sampler->vocab_size; q++) {
            logits[q] /= sampler->temperature;
        }
        // apply softmax to the logits to get the probabilities for next token
        softmax(logits, sampler->vocab_size);
        // flip a (float) coin (this is our source of entropy for sampling)
        float coin = random_f32(&sampler->rng_state);
        // we sample from this distribution to get the next token
        if (sampler->topp <= 0 || sampler->topp >= 1) {
            // simply sample from the predicted probability distribution
            next = sample_mult(logits, sampler->vocab_size, coin);
        } else {
            // top-p (nucleus) sampling, clamping the least likely tokens to
            // zero
            next = sample_topp(logits, sampler->vocab_size, sampler->topp,
                               sampler->probindex, coin);
        }
    }
    return next;
}

// ----------------------------------------------------------------------------
// utilities: time

long time_in_ms() {
    // return time in milliseconds, for benchmarking the model speed
    struct timespec time;
    clock_gettime(CLOCK_REALTIME, &time);
    return time.tv_sec * 1000 + time.tv_nsec / 1000000;
}

// ----------------------------------------------------------------------------
// generation loop

void generate(Transformer* transformer, Tokenizer* tokenizer, Sampler* sampler,
              char* prompt, int steps) {
    char* empty_prompt = "";
    if (prompt == NULL) {
        prompt = empty_prompt;
    }

    // encode the (string) prompt into tokens sequence
    int num_prompt_tokens = 0;
    int* prompt_tokens = (int*)malloc((strlen(prompt) + 3) *
                                      sizeof(int));  // +3 for '\0', ?BOS, ?EOS
    encode(tokenizer, prompt, 1, 0, prompt_tokens, &num_prompt_tokens);
    if (num_prompt_tokens < 1) {
        fprintf(stderr,
                "something is wrong, expected at least 1 prompt token\n");
        exit(EXIT_FAILURE);
    }

    // start the main loop
    long start =
        0;     // used to time our code, only initialized after first iteration
    int next;  // will store the next token in the sequence
    int token =
        prompt_tokens[0];  // kick off with the first token in the prompt
    int pos = 0;           // position in the sequence
    while (pos < steps) {
        // forward the transformer to get logits for the next token
        float* logits = forward(transformer, token, pos);

        // advance the state machine
        if (pos < num_prompt_tokens - 1) {
            // if we are still processing the input prompt, force the next
            // prompt token
            next = prompt_tokens[pos + 1];
        } else {
            // otherwise sample the next token from the logits
            next = sample(sampler, logits);
        }
        pos++;

        // data-dependent terminating condition: the BOS (=1) token delimits
        // sequences
        if (next == 1) {
            break;
        }

        // print the token as string, decode it with the Tokenizer object
        char* piece = decode(tokenizer, token, next);
        safe_printf(
            piece);  // same as printf("%s", piece), but skips "unsafe" bytes
        fflush(stdout);
        token = next;

        // init the timer here because the first iteration can be slower
        if (start == 0) {
            start = time_in_ms();
        }
    }
    printf("\n");

    // report achieved tok/s (pos-1 because the timer starts after first
    // iteration)
    if (pos > 1) {
        long end = time_in_ms();
        fprintf(stderr, "achieved tok/s: %f\n",
                (pos - 1) / (double)(end - start) * 1000);
    }

    free(prompt_tokens);
}

void read_stdin(const char* guide, char* buffer, size_t bufsize) {
    // read a line from stdin, up to but not including \n
    printf("%s", guide);
    if (fgets(buffer, bufsize, stdin) != NULL) {
        size_t len = strlen(buffer);
        if (len > 0 && buffer[len - 1] == '\n') {
            buffer[len - 1] = '\0';  // strip newline
        }
    }
}

// ----------------------------------------------------------------------------
// chat loop
// I manually inspected the tokens for a few chat conversations compared to
// python reference and that seemed ok, but this was not thoroughly tested and
// is not safely implemented, it's more a proof of concept atm.

void chat(Transformer* transformer, Tokenizer* tokenizer, Sampler* sampler,
          char* cli_user_prompt, char* cli_system_prompt, int steps) {
    // buffers for reading the system prompt and user prompt from stdin
    // you'll notice they are soomewhat haphazardly and unsafely set atm
    char system_prompt[512];
    char user_prompt[512];
    char rendered_prompt[1152];
    int num_prompt_tokens = 0;
    int* prompt_tokens = (int*)malloc(1152 * sizeof(int));
    int user_idx;

    // start the main loop
    int8_t user_turn = 1;  // user starts
    int next;              // will store the next token in the sequence
    int token;  // stores the current token to feed into the transformer
    int prev_token;
    int pos = 0;  // position in the sequence
    size_t assistant_t = 0;
    size_t assistant_tokens = 0;
    while (pos < steps) {
        // when it is the user's turn to contribute tokens to the dialog...
        if (user_turn) {
            // get the (optional) system prompt at position 0
            if (pos == 0) {
                // at position 0, the user can also contribute a system prompt
                if (cli_system_prompt == NULL) {
                    // system prompt was not passed in, attempt to get it from
                    // stdin
                    read_stdin("Enter system prompt (optional): ",
                               system_prompt, sizeof(system_prompt));
                } else {
                    // system prompt was passed in, use it
                    strcpy(system_prompt, cli_system_prompt);
                }
            }
            // get the user prompt
            if (pos == 0 && cli_user_prompt != NULL) {
                // user prompt for position 0 was passed in, use it
                strcpy(user_prompt, cli_user_prompt);
            } else {
                // otherwise get user prompt from stdin
                read_stdin("User: ", user_prompt, sizeof(user_prompt));
            }
            if (!strcmp(user_prompt, "<end>")) {
                break;
            }
            // render user/system prompts into the Llama 2 Chat schema
            if (pos == 0 && system_prompt[0] != '\0') {
                char system_template[] =
                    "[INST] <<SYS>>\n%s\n<</SYS>>\n\n%s [/INST]";
                sprintf(rendered_prompt, system_template, system_prompt,
                        user_prompt);
            } else {
                char user_template[] = "[INST] %s [/INST]";
                sprintf(rendered_prompt, user_template, user_prompt);
            }
            auto start = get_cycles();
            // encode the rendered prompt into tokens
            encode(tokenizer, rendered_prompt, 1, 0, prompt_tokens,
                   &num_prompt_tokens);
            auto end = get_cycles();
            // printf("encode: %lu\n", end - start);
            assistant_t += end - start;
            user_idx = 0;  // reset the user index
            user_turn = 0;
            printf("Assistant: ");
        }
        auto start = get_cycles();
        // determine the token to pass into the transformer next
        if (user_idx < num_prompt_tokens) {
            // if we are still processing the input prompt, force the next
            // prompt token
            token = prompt_tokens[user_idx++];
        } else {
            // otherwise use the next token sampled from previous turn
            token = next;
        }
        assistant_tokens++;
        // EOS (=2) token ends the Assistant turn
        if (token == 2) {
            user_turn = 1;
        }

        // forward the transformer to get logits for the next token
        auto fstart = get_cycles();
        float* logits = forward(transformer, token, pos);
        auto fend = get_cycles();
        // printf("forward: %lu\n", fend - fstart);
        auto sstart = get_cycles();
        next = sample(sampler, logits);
        auto send = get_cycles();
        // printf("sample: %lu\n", send - sstart);
        pos++;

        if (user_idx >= num_prompt_tokens && next != 2) {
            // the Assistant is responding, so print its output
            auto dstart = get_cycles();
            char* piece = decode(tokenizer, token, next);
            auto dend = get_cycles();
            // printf("decode: %lu\n", dend - dstart);
            safe_printf(piece);  // same as printf("%s", piece), but skips
                                 // "unsafe" bytes
            fflush(stdout);
        }
        if (next == 2) {
            printf("\n");
        }
        auto end = get_cycles();
        assistant_t += end - start;
    }
    printf("\n");
    printf("achieved tok/s: %lf\n",
           static_cast<double>(assistant_tokens) /
               (static_cast<double>(assistant_t) / 2.8 / 1e9));
    free(prompt_tokens);
}

// ----------------------------------------------------------------------------
// CLI, include only if not testing
#ifndef TESTING

void error_usage() {
    fprintf(stderr, "Usage:   run <checkpoint> [options]\n");
    fprintf(stderr, "Example: run model.bin -n 256 -i \"Once upon a time\"\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  -t <float>  temperature in [0,inf], default 1.0\n");
    fprintf(stderr,
            "  -p <float>  p value in top-p (nucleus) sampling in [0,1] "
            "default 0.9\n");
    fprintf(stderr, "  -s <int>    random seed, default time(NULL)\n");
    fprintf(stderr,
            "  -n <int>    number of steps to run for, default 256. 0 = "
            "max_seq_len\n");
    fprintf(stderr, "  -i <string> input prompt\n");
    fprintf(stderr, "  -z <string> optional path to custom tokenizer\n");
    fprintf(stderr, "  -m <string> mode: generate|chat, default: generate\n");
    fprintf(stderr, "  -y <string> (optional) system prompt in chat mode\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char* argv[]) {
    Configure config;
#ifdef STANDALONE
    constexpr size_t FAR_ARGC = 0;
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.server_buffer_size = 1024L * 1024 * 1024 * 32;
    config.client_buffer_size = 1024L * 1024 * 1024 * 8;
    config.evict_batch_size = 64 * 1024;
    config.max_thread_cnt = 8;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
#else
    constexpr size_t FAR_ARGC = 1;
    config.from_file(argv[1]);
#endif
    // default parameters
    char* checkpoint_path = NULL;  // e.g. out/model.bin
    const char* tokenizer_path = "tokenizer.bin";
    float temperature = 1.0f;  // 0.0 = greedy deterministic. 1.0 =
                               // original. don't set higher
    float topp = 0.9f;    // top-p in nucleus sampling. 1.0 = off. 0.9 works
                          // well, but slower
    int steps = 256;      // number of steps to run for
    char* prompt = NULL;  // prompt string
    unsigned long long rng_seed = 1;  // seed rng with time by default
    const char* mode = "generate";    // generate|chat
    char* system_prompt =
        NULL;  // the (optional) system prompt to use in chat mode

    // poor man's C argparse so we can override the defaults above from the
    // command line
    if (argc >= 2 + FAR_ARGC) {
        checkpoint_path = argv[1 + FAR_ARGC];
    } else {
        error_usage();
    }
    for (int i = 2 + FAR_ARGC; i < argc; i += 2) {
        // do some basic validation
        if (i + 1 >= argc) {
            error_usage();
        }  // must have arg after flag
        if (argv[i][0] != '-') {
            error_usage();
        }  // must start with dash
        if (strlen(argv[i]) != 2) {
            error_usage();
        }  // must be -x (one dash, one letter)
        // read in the args
        if (argv[i][1] == 't') {
            temperature = atof(argv[i + 1]);
        } else if (argv[i][1] == 'p') {
            topp = atof(argv[i + 1]);
        } else if (argv[i][1] == 's') {
            rng_seed = atoi(argv[i + 1]);
        } else if (argv[i][1] == 'n') {
            steps = atoi(argv[i + 1]);
        } else if (argv[i][1] == 'i') {
            prompt = argv[i + 1];
        } else if (argv[i][1] == 'z') {
            tokenizer_path = argv[i + 1];
        } else if (argv[i][1] == 'm') {
            mode = argv[i + 1];
        } else if (argv[i][1] == 'y') {
            system_prompt = argv[i + 1];
        } else if (argv[i][1] == 'b') {
            config.client_buffer_size = std::stoul(argv[i + 1]);
        } else {
            error_usage();
        }
    }

    // parameter validation/overrides
    if (rng_seed <= 0) rng_seed = (unsigned int)time(NULL);
    if (temperature < 0.0) temperature = 0.0;
    if (topp < 0.0 || 1.0 < topp) topp = 0.9;
    if (steps < 0) steps = 0;
    std::cout << "llama init: " << std::endl;
    std::cout << "client buffer size: "
              << static_cast<double>(config.client_buffer_size) / (1 << 30)
              << "G" << std::endl;
    std::cout << "core count: " << config.max_thread_cnt << std::endl;
    FarLib::runtime_init(config);
    // perf_init();
    // perf_profile([&] {
    // build the Transformer via the model .bin file
    Transformer transformer;
    build_transformer(&transformer, checkpoint_path);
    if (steps == 0 || steps > transformer.config.seq_len)
        steps = transformer.config.seq_len;  // override to ~max length

    // build the Tokenizer via the tokenizer .bin file
    Tokenizer tokenizer;
    build_tokenizer(&tokenizer, tokenizer_path, transformer.config.vocab_size);

    // build the Sampler
    Sampler sampler;
    build_sampler(&sampler, transformer.config.vocab_size, temperature, topp,
                  rng_seed);
    profile::reset_all();
    // run!
    if (strcmp(mode, "generate") == 0) {
        generate(&transformer, &tokenizer, &sampler, prompt, steps);
    } else if (strcmp(mode, "chat") == 0) {
        chat(&transformer, &tokenizer, &sampler, prompt, system_prompt, steps);
    } else {
        fprintf(stderr, "unknown mode: %s\n", mode);
        error_usage();
    }

    // memory and file handles cleanup
    free_sampler(&sampler);
    free_tokenizer(&tokenizer);
    free_transformer(&transformer);
    prof_res_print();
    profile::print_profile_data();
    // }).print();
    FarLib::runtime_destroy();
#ifdef STANDALONE
    server_thread.join();
#endif
    return 0;
}
#endif
