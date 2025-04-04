#include <sched.h>

#include <boost/sort/parallel_stable_sort/parallel_stable_sort.hpp>
#include <boost/sort/sort.hpp>
#include <boost/timer/progress_display.hpp>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <vector>

#include "cache/cache.hpp"
#include "utils/parallel.hpp"

namespace FarLib {

template <bool TimeStamp = false,
          size_t ChunkSize /* in byte */ = 4096 - allocator::BlockHeadSize>
class ChunkedGraph {
public:
    using vertex_t = uint32_t;
    using count_t = uint32_t;
    using degree_t = count_t;

    using timestamp_t = uint32_t;

    struct edge_with_ts {
        vertex_t ngh;
        timestamp_t create_ts;
        timestamp_t destroy_ts;
    };

    struct edge_without_ts {
        vertex_t ngh;
    };

    using edge_t = std::conditional_t<TimeStamp, edge_with_ts, edge_without_ts>;

    // list for edges of a vertex
    struct EdgeList {
        vertex_t vertex_id;
        degree_t degree;
        edge_t neighbors[0];

        count_t size_in_bytes() const {
            return sizeof(EdgeList) + degree * sizeof(edge_t);
        }
    };

    struct EdgeListChunkHeader {
        count_t count;          // count of EdgeList
        count_t size_in_bytes;  // size in bytes

        EdgeListChunkHeader()
            : count(0), size_in_bytes(sizeof(EdgeListChunkHeader)) {}

        EdgeList *get_list(size_t offset) {
            return reinterpret_cast<EdgeList *>(
                (reinterpret_cast<char *>(this) + offset));
        }

        const EdgeList *get_list(size_t offset) const {
            return reinterpret_cast<const EdgeList *>(
                (reinterpret_cast<const char *>(this) + offset));
        }
    };

    // referencing an EdgeList in EdgeLists
    struct EdgeListRef {
        uint32_t chunk_idx;
        uint32_t offset;  // in byte
        degree_t degree;
    };

    using ChunkPtr = UniqueFarPtr<void>;

    // edge lists for all vertices
    struct EdgeLists {
        std::vector<ChunkPtr> chunks;

        void swap(EdgeLists &other) { std::swap(chunks, other.chunks); }

        size_t chunk_count() const { return chunks.size(); }

        void add_chunk(ChunkPtr &&chunk) { chunks.push_back(std::move(chunk)); }

        LiteAccessor<EdgeList> get(const EdgeListRef &ref,
                                   DereferenceScope &scope) const {
            const ChunkPtr &ptr = chunks[ref.chunk_idx];
            LiteAccessor<EdgeListChunkHeader> chunk(ptr, scope);
            return LiteAccessor<EdgeList>(chunk, chunk->get_list(ref.offset));
        }

        LiteAccessor<EdgeList> get(const EdgeListRef &ref, __DMH__,
                                   DereferenceScope &scope) const {
            const ChunkPtr &ptr = chunks[ref.chunk_idx];
            LiteAccessor<EdgeListChunkHeader> chunk(ptr, __on_miss__, scope);
            return LiteAccessor<EdgeList>(chunk, chunk->get_list(ref.offset));
        }

        void prefetch(const EdgeListRef &ref, DereferenceScope &scope) const {
            const ChunkPtr &ptr = chunks[ref.chunk_idx];
            Cache::get_default()->prefetch(ptr.obj(), scope);
        }

        // should not in dereference scope
        template <bool Optimize = true, size_t MaxThreadCount = 1,
                  size_t Granularity = 64, std::invocable<const EdgeList *> Fn>
        void for_each(Fn &&fn) const {
            uthread::parallel_for_with_scope<Granularity, Optimize>(
                MaxThreadCount, chunks.size(),
                [&](size_t j, size_t &prefetch_idx, DereferenceScope &scope) {
                    ON_MISS_BEGIN
                        __define_oms__(scope);
                        if constexpr (Optimize) {
                            auto cache = FarLib::Cache::get_default();
                            size_t &i = prefetch_idx;
                            for (i = std::max(i, j + 1);
                                 i < chunks.size() && i < j + 1024; i++) {
                                cache->prefetch(chunks[i].obj(), oms);
                                if (i % 8 == 0 &&
                                    cache::check_fetch(__entry__, __ddl__))
                                    break;
                            }
                        } else {
                            do {
                                uthread::yield();
                            } while (!cache::check_fetch(__entry__, __ddl__));
                        }
                    ON_MISS_END
                    LiteAccessor<EdgeListChunkHeader> chunk(chunks[j],
                                                            __on_miss__, scope);
                    size_t offset = sizeof(EdgeListChunkHeader);
                    for (size_t i = 0; i < chunk->count; i++) {
                        const EdgeList *list = chunk->get_list(offset);
                        fn(list);
                        offset += list->size_in_bytes();
                        assert(offset <= chunk->size_in_bytes);
                    }
                },
                [](size_t) -> size_t { return 0; });
        }

        template <bool Optimize = true, size_t MaxThreadCount = 1,
                  size_t Granularity = 64,
                  std::invocable<const EdgeList *, DereferenceScope &> Fn>
        void for_each_with_scope(Fn &&fn) const {
            uthread::parallel_for_with_scope<Granularity, Optimize>(
                MaxThreadCount, chunks.size(),
                [&](size_t j, size_t &prefetch_idx, DereferenceScope &scope) {
                    ON_MISS_BEGIN
                        __define_oms__(scope);
                        if constexpr (Optimize) {
                            auto cache = FarLib::Cache::get_default();
                            size_t &i = prefetch_idx;
                            for (i = std::max(i, j + 1);
                                 i < chunks.size() && i < j + 1024; i++) {
                                cache->prefetch(chunks[i].obj(), oms);
                                if (i % 8 == 0 &&
                                    cache::check_fetch(__entry__, __ddl__))
                                    break;
                            }
                        } else {
                            do {
                                uthread::yield();
                            } while (!cache::check_fetch(__entry__, __ddl__));
                        }
                    ON_MISS_END
                    LiteAccessor<EdgeListChunkHeader> chunk(chunks[j],
                                                            __on_miss__, scope);
                    size_t offset = sizeof(EdgeListChunkHeader);
                    for (size_t i = 0; i < chunk->count; i++) {
                        const EdgeList *list = chunk->get_list(offset);
                        fn(list, scope);
                        offset += list->size_in_bytes();
                        assert(offset <= chunk->size_in_bytes);
                    }
                },
                [](size_t) -> size_t { return 0; });
        }
    };

    class EdgeListsBuilder {
    private:
        EdgeLists &output;
        ChunkPtr current_chunk;
        LiteAccessor<EdgeListChunkHeader, true> accessor;

    public:
        EdgeListsBuilder(EdgeLists &output) : output(output) {}

        void pin() const { accessor.pin(); }
        void unpin() const { accessor.unpin(); }

        void init(DereferenceScope &scope) {
            accessor = current_chunk.allocate_lite<true>(ChunkSize, scope)
                           .template as<EdgeListChunkHeader>();
            accessor->count = 0;
            accessor->size_in_bytes = sizeof(EdgeListChunkHeader);
        }

        template <std::invocable<EdgeList *> Fn>
        EdgeListRef add(vertex_t vertex_id, degree_t degree, Fn &&initializer,
                        DereferenceScope &scope) {
            EdgeList list_header{.vertex_id = vertex_id, .degree = degree};
            count_t list_size = list_header.size_in_bytes();
            if (accessor->size_in_bytes + list_size > current_chunk.size()) {
                // can not allocate in a chunk
                accessor = {};
                output.add_chunk(std::move(current_chunk));
                size_t chunk_size = std::max(
                    ChunkSize, list_size + sizeof(EdgeListChunkHeader));
                accessor = current_chunk.allocate_lite<true>(chunk_size, scope)
                               .template as<EdgeListChunkHeader>();
                accessor->count = 0;
                accessor->size_in_bytes = sizeof(EdgeListChunkHeader);
            }
            uint32_t offset = accessor->size_in_bytes;
            accessor->count++;
            accessor->size_in_bytes += list_size;
            EdgeList *edge_list = accessor->get_list(offset);
            *edge_list = list_header;
            initializer(edge_list);
            return EdgeListRef{
                .chunk_idx = static_cast<uint32_t>(output.chunk_count()),
                .offset = offset,
                .degree = degree,
            };
        }

        EdgeListRef add(vertex_t vertex_id, degree_t degree,
                        DereferenceScope &scope) {
            return add(vertex_id, degree, [](auto) {}, scope);
        }

        void close() {
            if (accessor->count != 0) {
                accessor = {};
                output.add_chunk(std::move(current_chunk));
            }
        }
    };

private:
    std::vector<EdgeListRef> out_edges;
    std::vector<EdgeListRef> in_edges;
    EdgeLists out_edge_lists;
    EdgeLists in_edge_lists;
    size_t edge_count_;

public:
    uint64_t sparse_edge_map_cycles;
    uint64_t dense_edge_map_cycles;
    uint64_t vertex_map_cycles;

public:
    size_t vertex_count() const { return out_edges.size(); }
    size_t edge_count() const { return edge_count_; }

    void load(std::string file_name) {
        out_edges.clear();
        std::ifstream ifs(file_name);
        std::string header;
        size_t v_count;
        ASSERT(ifs >> header);
        ASSERT(header == "AdjacencyGraph");
        ASSERT(ifs >> v_count >> edge_count_);
        ASSERT(v_count <= std::numeric_limits<vertex_t>::max());

        std::vector<degree_t> degrees;
        degrees.reserve(v_count);

        size_t last_offset;
        ASSERT(ifs >> last_offset);
        ASSERT(last_offset == 0);
        for (size_t i = 1; i < v_count; i++) {
            size_t offset;
            ASSERT(ifs >> offset);
            degrees.push_back(offset - last_offset);
            last_offset = offset;
        }
        degrees.push_back(edge_count() - last_offset);
        ASSERT(degrees.size() == v_count);

        out_edges.reserve(v_count);
        {
            boost::timer::progress_display show_progress(v_count);
            struct Scope : public RootDereferenceScope {
                EdgeListsBuilder builder;
                Scope(EdgeLists &output) : builder(output) {}
                void pin() const override { builder.pin(); }
                void unpin() const override { builder.unpin(); }
            } scope(out_edge_lists);
            scope.builder.init(scope);
            for (size_t i = 0; i < v_count; i++) {
                degree_t degree = degrees[i];
                EdgeListRef ref = scope.builder.add(
                    i, degree,
                    [&](EdgeList *edge_list) {
                        for (size_t i = 0; i < degree; i++) {
                            vertex_t ngh;
                            ASSERT(ifs >> ngh);
                            edge_list->neighbors[i].ngh = ngh;
                            if constexpr (TimeStamp) {
                                edge_list->neighbors[i].create_ts = 0;
                                edge_list->neighbors[i].destroy_ts =
                                    std::numeric_limits<timestamp_t>::max();
                            }
                        }
                    },
                    scope);
                out_edges.push_back(ref);
                ++show_progress;
            }
            scope.builder.close();
        }
        generate_in_edges();
    }

    size_t out_degree(vertex_t vertex_id) {
        return out_edges[vertex_id].degree;
    }
    size_t in_degree(vertex_t vertex_id) { return in_edges[vertex_id].degree; }

    // apply fn on each out edge list of vertex in vertices
    // should not be in a dereference scope
    template <bool Optimize, size_t MaxThreadCount = 1,
              size_t Granularity = 1024, std::invocable<size_t, EdgeList *> Fn>
    void for_each_out_list(const std::vector<vertex_t> &vertices, Fn &&fn) {
        uint64_t start_tsc = get_cycles();
        uthread::parallel_for_with_scope<Granularity, Optimize>(
            MaxThreadCount, vertices.size(),
            [&](size_t i, size_t &prefetch_idx, DereferenceScope &scope) {
                vertex_t vertex = vertices[i];
                LiteAccessor<EdgeList> edge_list;
                ON_MISS_BEGIN
                    __define_oms__(scope);
                    if constexpr (Optimize) {
                        size_t &j = prefetch_idx;
                        for (j = std::max(j, i + 1);
                             j < vertices.size() && j < i + 1024; j++) {
                            out_edge_lists.prefetch(out_edges[vertices[j]],
                                                    oms);
                            if (j % 8 == 0 &&
                                FarLib::cache::check_fetch(__entry__, __ddl__))
                                break;
                        }
                    } else {
                        do {
                            uthread::yield();
                        } while (!cache::check_fetch(__entry__, __ddl__));
                    }
                ON_MISS_END
                edge_list =
                    out_edge_lists.get(out_edges[vertex], __on_miss__, scope);
                fn(i, edge_list.as_ptr());
            },
            [](size_t) -> size_t { return 0; });
        uint64_t end_tsc = get_cycles();
        sparse_edge_map_cycles += end_tsc - start_tsc;
    }

    template <bool Optimize, size_t MaxThreadCount = 1, size_t Granularity = 64,
              std::invocable<const EdgeList *> Fn>
    void for_each_in_list(Fn &&fn) {
        uint64_t start_tsc = get_cycles();
        in_edge_lists
            .template for_each<Optimize, MaxThreadCount, Granularity, Fn>(
                std::forward<Fn>(fn));
        uint64_t end_tsc = get_cycles();
        dense_edge_map_cycles += end_tsc - start_tsc;
    }

    void transpose() {
        in_edges.swap(out_edges);
        in_edge_lists.swap(out_edge_lists);
    }

private:
    void generate_in_edges() {
        in_edges.clear();

        // TODO: maybe we should use remote vector
        std::vector<std::pair<vertex_t, vertex_t>> in_pairs;
        out_edge_lists.for_each_with_scope(
            [&](const EdgeList *list, DereferenceScope &scope) {
                vertex_t src = list->vertex_id;
                for (size_t j = 0; j < list->degree; j++) {
                    vertex_t dst = list->neighbors[j].ngh;
                    in_pairs.push_back({dst, src});
                }
            });

        std::thread([&] {
            cpu_set_t mask;
            CPU_ZERO(&mask);
            int num_cpus = sysconf(_SC_NPROCESSORS_CONF);
            for (int i = 0; i < num_cpus; ++i) {
                CPU_SET(i, &mask);
            }
            pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &mask);
            boost::sort::parallel_stable_sort(in_pairs.begin(), in_pairs.end());
        }).join();

        in_edges.reserve(vertex_count());
        auto in_pairs_it = in_pairs.begin();

        struct Scope : public RootDereferenceScope {
            EdgeListsBuilder builder;
            Scope(EdgeLists &output) : builder(output) {}
            void pin() const override { builder.pin(); }
            void unpin() const override { builder.unpin(); }
        } scope(in_edge_lists);
        scope.builder.init(scope);
        for (size_t i = 0; i < vertex_count(); i++) {
            vertex_t degree = 0;
            for (auto it = in_pairs_it; it != in_pairs.end() && it->first == i;
                 it++) {
                degree++;
            }
            auto ref = scope.builder.add(
                i, degree,
                [&](EdgeList *edge_list) {
                    for (degree_t j = 0; j < degree; j++) {
                        auto &edge = edge_list->neighbors[j];
                        edge.ngh = in_pairs_it->second;
                        if constexpr (TimeStamp) {
                            edge.create_ts = 0;
                            edge.destroy_ts =
                                std::numeric_limits<timestamp_t>::max();
                        }
                        ++in_pairs_it;
                    }
                },
                scope);
            in_edges.push_back(ref);
        }
        scope.builder.close();
    }
};

}  // namespace FarLib
