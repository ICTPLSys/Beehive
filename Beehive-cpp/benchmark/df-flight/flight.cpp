#include <cassert>
#include <cstddef>
#include <functional>
#include <iomanip>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "async/scoped_inline_task.hpp"
#include "cache/accessor.hpp"
#include "cache/cache.hpp"
#include "cache/handler.hpp"
#include "read_csv.hpp"
#include "utils/cpu_cycles.hpp"
#include "utils/debug.hpp"
#include "utils/fork_join.hpp"
#include "utils/perf.hpp"
#include "utils/stats.hpp"
#include "utils/uthreads.hpp"

using namespace FarLib;

struct Evaluation {
    using Element = ShortString<8>;
    constexpr static size_t ArrayChunkSize =
        (4096 - allocator::BlockHeadSize) / sizeof(Element);
    using ArrayChunk = std::array<Element, ArrayChunkSize>;
    using AppArray = std::vector<UniqueFarPtr<ArrayChunk>>;

    using Map = std::unordered_map<Element, size_t>;

    // fields
    size_t NumMutatorThreads = 16;
    AppArray data;   // column for the flight data
    AppArray dummy;  // dummy data for other columns, used to flush cache
    std::vector<Map> hist;

    void init(const char *file_name, size_t column_index) {
        RootDereferenceScope scope;
        UniqueFarPtr<ArrayChunk> current_chunk;
        LiteAccessor<ArrayChunk, true> accessor =
            current_chunk.allocate_lite(scope);
        size_t offset = 0;
        read_csv_file(file_name, [&](std::string_view line) {
            const char *p = line.data();
            const char *end = p + line.size();
            for (size_t i = 0; i < column_index; i++) {
                p = skip(p, end, ',');
            }
            const char *next = skip(p, end, ',');
            Element value = std::string_view(p, next - p);
            (*accessor)[offset] = value;
            offset++;
            if (offset == ArrayChunkSize) {
                data.push_back(std::move(current_chunk));
                accessor = current_chunk.allocate_lite(scope);
                for (size_t i = 0; i < 3; i++) {
                    dummy.emplace_back().allocate_lite(scope);
                }
                offset = 0;
            }
        });
    }

    enum OptMode {
        NoOpt,
        Prefetch,
        Yield,
        Async,
    };

    // no optimization
    void thread_work(size_t tid) {
        RootDereferenceScope scope;
        Map &h = hist[tid];
        h.clear();
        h.reserve(32768);
        size_t start = tid * data.size() / NumMutatorThreads;
        size_t end = (tid + 1) * data.size() / NumMutatorThreads;
        for (size_t i = start; i < end; i++) {
            auto &chunk = *data[i].template access<true>(scope);
            for (size_t j = 0; j < ArrayChunkSize; j++) {
                h[chunk[j]]++;
            }
        }
    }

    // optimize: prefetch
    void thread_work_prefetch(size_t tid) {
        RootDereferenceScope scope;
        Map &h = hist[tid];
        h.clear();
        h.reserve(32768);
        size_t start = tid * data.size() / NumMutatorThreads;
        size_t end = (tid + 1) * data.size() / NumMutatorThreads;
        size_t prefetch_idx = start;
        for (size_t i = start; i < end; i++) {
            ON_MISS_BEGIN
                __define_oms__(scope);
                auto c = Cache::get_default();
                auto &pi = prefetch_idx;
                for (pi = std::max(i + 1, pi); pi < std::min(end, i + 256);
                     pi++) {
                    c->prefetch(data[pi].obj(), oms);
                    if (pi % 8 == 0 && cache::check_fetch(__entry__, __ddl__))
                        return;
                }
            ON_MISS_END
            auto &chunk = *data[i].access(__on_miss__, scope);
            for (size_t j = 0; j < ArrayChunkSize; j++) {
                h[chunk[j]]++;
            }
        }
    }

    // optimize: thread yield
    void thread_work_yield(size_t tid) {
        RootDereferenceScope scope;
        Map &h = hist[tid];
        h.clear();
        h.reserve(32768);
        size_t start = tid * data.size() / NumMutatorThreads;
        size_t end = (tid + 1) * data.size() / NumMutatorThreads;
        size_t prefetch_idx = start;
        for (size_t i = start; i < end; i++) {
            ON_MISS_BEGIN
                uthread::yield();
            ON_MISS_END
            auto &chunk = *data[i].access(__on_miss__, scope);
            for (size_t j = 0; j < ArrayChunkSize; j++) {
                h[chunk[j]]++;
            }
        }
    }

    // optimize: async
    void thread_work_async(size_t tid) {
        struct Frame {
            Map *hist_ptr;
            UniqueFarPtr<ArrayChunk> *chunk_ptr;
            LiteAccessor<ArrayChunk, false> accessor;
            int label = 0;

            bool fetched() const { return cache::at_local(accessor); }
            void pin() { accessor.pin(); }
            void unpin() { accessor.unpin(); }
            bool run(DereferenceScope &scope) {
                if (label == 0) {
                    bool at_local = accessor.async_fetch(*chunk_ptr, scope);
                    if (!at_local) {
                        label = 1;
                        return false;
                    }
                }
                for (size_t j = 0; j < ArrayChunkSize; j++) {
                    (*hist_ptr)[(*accessor)[j]]++;
                }
                return true;
            }
        };
        RootDereferenceScope scope;
        Map &h = hist[tid];
        h.clear();
        h.reserve(32768);

        size_t start = tid * data.size() / NumMutatorThreads;
        size_t end = (tid + 1) * data.size() / NumMutatorThreads;
        size_t mid = (end + start) / 2;
        async::for_range(scope, start, end, [&](size_t i) {
            return Frame{.hist_ptr = &h, .chunk_ptr = &(data[i])};
        });
    }

    void prepare_work() {
        hist.clear();
        hist.resize(NumMutatorThreads);
    }

    void work(OptMode mode) {
        std::function<void(size_t)> fn = [this, mode](size_t tid) {
            switch (mode) {
            case NoOpt:
                thread_work(tid);
                break;
            case Prefetch:
                thread_work_prefetch(tid);
                break;
            case Yield:
                thread_work_yield(tid);
            case Async:
                thread_work_async(tid);
                break;
            default:
                assert(false);
            }
        };
        uthread::fork_join(NumMutatorThreads, fn);
    }

    void cool_down() {
        std::function<void(size_t)> fn = [this](size_t tid) {
            RootDereferenceScope scope;
            for (size_t i = tid; i < dummy.size(); i += NumMutatorThreads) {
                Cache::get_default()->prefetch(dummy[i].obj(), scope);
            }
        };
        uthread::fork_join(NumMutatorThreads, fn);
        size_t wait_until = get_cycles() + 30'000;
        while (get_cycles() < wait_until) cache::check_cq();
    }

    void run(const char *file_name, size_t column_index) {
        constexpr size_t PrintWidth = 16;
        const char *PrintTitles[] = {"nthread", "eplased-time", "l3-miss",
                                     "yield-count"};
        constexpr size_t RepeatEvalCnt = 5;
        const std::pair<OptMode, const char *> OptModes[] = {
            {NoOpt, "no-opt"},
            {Prefetch, "prefetch"},
            {Async, "async"},
            {Yield, "thread-yield"}};

        ASSERT(uthread::get_worker_count() == NumMutatorThreads);
        perf_init();
        printf("prepare...\n");
        init(file_name, column_index);
        printf("warm up...\n");
        cool_down();
        prepare_work();
        perf_profile([&] { work(Async); });  // warm up
        printf("bench...\n");
        const size_t NThreadArray[] = {16,  32,  48,  64,  80,  96,  112, 128,
                                       144, 160, 176, 192, 208, 224, 240, 256};

        for (auto title : PrintTitles) {
            std::cout << std::setw(PrintWidth) << title;
        }
        std::cout << std::endl;
        for (auto title : PrintTitles) {
            for (size_t i = 0; i < PrintWidth; i++) std::cout.put('-');
        }
        std::cout << std::endl;

        for (auto [mode, mode_name] : OptModes) {
            std::cout << "\n" << mode_name << " =>" << std::endl;
            for (size_t nthread : NThreadArray) {
                NumMutatorThreads = nthread;
                for (size_t i = 0; i < RepeatEvalCnt; i++) {
                    cool_down();
                    prepare_work();
                    profile::reset_all();
                    profile::start_work();
                    auto perf_result = perf_profile([&] { work(mode); });
                    profile::end_work();
                    std::cout << std::setw(PrintWidth) << nthread
                              << std::setw(PrintWidth) << perf_result.runtime_ms
                              << std::setw(PrintWidth)
                              << perf_result.l3_cache_miss
                              << std::setw(PrintWidth)
                              << profile::collect_yield_count() << std::endl;
                }
            }
        }
        printf("finished\n");
    }
};

int main(int argc, const char *const argv[]) {
    // ./build/benchmark/data_hist/data_hist ./data_hist.config
    if (argc != 2) {
        std::cout << "usage: " << argv[0] << " <configure file>" << std::endl;
        return -1;
    }
    FarLib::rdma::Configure config;
    config.from_file(argv[1]);
    FarLib::runtime_init(config);
    {
        Evaluation eval;
        eval.run("/data/global/airline/airline.csv.shuffle", 23);
    }
    FarLib::runtime_destroy();
    return 0;
}
