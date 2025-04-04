#include <array>
#include <chrono>
#include <vector>

#include "async/scoped_inline_task.hpp"
#include "cache/cache.hpp"
#include "utils/control.hpp"

using namespace FarLib;

namespace {

inline size_t align_up(size_t n, size_t alignment) {
    return (n + (alignment - 1)) / alignment;
}

template <typename Fn>
void time_for(Fn &&fn, size_t n = 1) {
    auto start = std::chrono::high_resolution_clock::now();
    fn();
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::nano> duration_ns = end - start;
    std::cout << "time: " << duration_ns.count() / n << " ns" << std::endl;
}

template <typename Data = uint64_t, typename Index = size_t,
          size_t ChunkSize = (4096 - allocator::BlockHeadSize) / sizeof(Index)>
void test(size_t data_size, size_t index_size, size_t eval_count,
          bool pararoutine = false) {
    using DataChunk = std::array<Data, ChunkSize>;
    using IndexChunk = std::array<Index, ChunkSize>;

    std::vector<UniqueFarPtr<DataChunk>> data(align_up(data_size, ChunkSize));
    std::vector<UniqueFarPtr<DataChunk>> target(
        align_up(index_size, ChunkSize));
    std::vector<UniqueFarPtr<IndexChunk>> index(
        align_up(index_size, ChunkSize));

    RootDereferenceScope scope;

    // prepare data
    // data[x] = x
    for (size_t i = 0; i < data.size(); i++) {
        auto acc = data[i].allocate_lite(scope);
        for (size_t j = 0, k = i * ChunkSize; j < ChunkSize && k < data_size;
             j++, k++) {
            (*acc)[j] = k;
        }
    }
    srand(0);
    for (size_t i = 0; i < index.size(); i++) {
        auto acc = index[i].allocate_lite(scope);
        for (size_t j = 0, k = i * ChunkSize; j < ChunkSize && k < index_size;
             j++, k++) {
            // (*acc)[j] = rand() % data_size;
            (*acc)[j] = std::min(k * data_size / index_size, data_size - 1);
        }
    }

    for (size_t n = 0; n < eval_count; n++) {
        // copy
        time_for(
            [&] {
                if (pararoutine) {
                    struct Frame {
                        size_t index_size;
                        std::vector<UniqueFarPtr<DataChunk>> *data;
                        UniqueFarPtr<IndexChunk> *idx_uptr;
                        UniqueFarPtr<DataChunk> *tgt_uptr;
                        LiteAccessor<IndexChunk> idx_acc;
                        LiteAccessor<DataChunk, true> tgt_acc;
                        LiteAccessor<DataChunk> data_acc;
                        size_t j;
                        size_t k;
                        Index ii;
                        int label = 0;

                        bool fetched() const {
                            return cache::at_local(data_acc) &&
                                   cache::at_local(idx_acc);
                        }
                        void pin() {
                            idx_acc.pin();
                            tgt_acc.pin();
                            data_acc.pin();
                        }
                        void unpin() {
                            idx_acc.unpin();
                            tgt_acc.unpin();
                            data_acc.unpin();
                        }
                        bool run(DereferenceScope &scope) {
                            switch (label) {
                            case 1:
                                goto L1;
                            case 2:
                                goto L2;
                            }
                            tgt_acc = tgt_uptr->allocate_lite(scope);
                            if (!idx_acc.async_fetch(*idx_uptr, scope)) {
                                label = 1;
                                return false;
                            }
                        L1:
                            for (j = 0; j < ChunkSize && k < index_size;
                                 j++, k++) {
                                {
                                    ii = (*idx_acc)[j];
                                    assert(cache::at_local(idx_acc));
                                    bool at_local = data_acc.async_fetch(
                                        (*data)[ii / ChunkSize], scope);
                                    if (!at_local) {
                                        label = 2;
                                        assert(cache::at_local(idx_acc));
                                        return false;
                                    }
                                }
                            L2:
                                (*tgt_acc)[j] = (*data_acc)[ii % ChunkSize];
                            }
                            return true;
                        }
                    };
                    async::for_range(scope, 0, index.size(), [&](size_t i) {
                        return Frame{.index_size = index_size,
                                     .data = &data,
                                     .idx_uptr = &index[i],
                                     .tgt_uptr = &target[i],
                                     .k = i * ChunkSize};
                    });
                } else {
                    for (size_t i = 0; i < index.size(); i++) {
                        struct Scope : DereferenceScope {
                            LiteAccessor<IndexChunk> idx_acc;
                            LiteAccessor<DataChunk, true> tgt_acc;
                            Scope(DereferenceScope *parent)
                                : DereferenceScope(parent) {}
                            void pin() const override {
                                idx_acc.pin();
                                tgt_acc.pin();
                            }
                            void unpin() const override {
                                idx_acc.unpin();
                                tgt_acc.unpin();
                            }
                        } scope_(&scope);

                        scope_.idx_acc = index[i].access(scope_);
                        scope_.tgt_acc = target[i].allocate_lite(scope_);
                        for (size_t j = 0, k = i * ChunkSize;
                             j < ChunkSize && k < index_size; j++, k++) {
                            Index ii = (*scope_.idx_acc)[j];
                            Data &tt = (*scope_.tgt_acc)[j];
                            auto data_acc = data[ii / ChunkSize].access(scope_);
                            tt = (*data_acc)[ii % ChunkSize];
                        }
                    }
                }
            },
            index_size);

        // reset target
        for (size_t i = 0; i < target.size(); i++) {
            target[i] = {};
        }
    }
}

template <typename Data = uint64_t, typename Index = size_t>
void test_stl(size_t data_size, size_t index_size, size_t eval_count) {
    std::vector<Data> data(data_size);
    std::vector<Index> index(index_size);
    std::vector<Data> target;

    for (Data &x : data) x = 0;
    srand(0);
    for (size_t i = 0; i < index_size; i++)
        index[i] = std::min(i * data_size / index_size, data_size - 1);
    // for (size_t i = 0; i < index_size; i++) index[i] = rand() % data_size;

    for (size_t n = 0; n < eval_count; n++) {
        time_for(
            [&] {
                for (size_t i = 0; i < index_size; i++) {
                    target.push_back(data[index[i]]);
                }
            },
            index_size);

        target.clear();
    }
}

}  // namespace

int main(int argc, const char *const argv[]) {
    if (argc != 2) {
        std::cout << "usage: " << argv[0] << " <configure file>" << std::endl;
        return -1;
    }
    rdma::Configure config;
    config.from_file(argv[1]);
    runtime_init(config);

    std::cout << "STL: " << std::endl;
    test_stl(10'000'000, 2'000'000, 5);

    std::cout << "Far Memory: " << std::endl;
    test(10'000'000, 2'000'000, 5);

    std::cout << "Far Memory (pararoutine): " << std::endl;
    test(10'000'000, 2'000'000, 5, true);

    runtime_destroy();

    return 0;
}