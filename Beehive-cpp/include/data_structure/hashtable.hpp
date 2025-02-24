#ifndef _REMOTE_HASHTABLE_H
#define _REMOTE_HASHTABLE_H
#include <algorithm>
#include <atomic>
#include <boost/coroutine2/all.hpp>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <functional>
#include <iostream>
#include <list>
#include <map>
#include <numeric>
#include <optional>
#include <thread>
#include <vector>

#include "async/loop.hpp"
#include "cache/cache.hpp"

namespace Beehive {
template <typename K, typename V>
struct HashNode {
    K k;
    V v;
    far_obj_t next_node_obj;

    HashNode(K &k, V &v) : k(k), v(v), next_node_obj(far_obj_t::null()) {}
};

template <typename K, typename V, size_t BucketCount,
          typename Hash = std::hash<K>>
class HashTable {
    using node_accessor = Accessor<HashNode<K, V>>;
    using node_const_accessor = ConstAccessor<HashNode<K, V>>;
    using value_accessor = Accessor<V>;
    using value_const_accessor = ConstAccessor<V>;

private:
    far_obj_t *header;

public:
    HashTable() {
        header = new far_obj_t[BucketCount];
        memset(header, 0, sizeof(far_obj_t) * BucketCount);
    }

    ~HashTable() {
        auto cache = Beehive::Cache::get_default();
        for (int i = 0; i < BucketCount; i++) {
            far_obj_t far_obj = header[i];
            while (!far_obj.is_null()) {
                far_obj_t next_far_obj;
                {
                    node_accessor node_acc(far_obj);
                    next_far_obj = node_acc->next_node_obj;
                }
                cache->deallocate(far_obj);
                far_obj = next_far_obj;
            }
        }
        delete[] header;
    }

    void insert(std::pair<K, V> &p) { insert(p.first, p.second); }

    void insert(K k, V v) {
        uint64_t idx = Hash{}(k) % BucketCount;
        far_obj_t node_obj = header[idx];
        while (!node_obj.is_null()) {
            auto node_acc = node_accessor(node_obj);
            const K &node_k = node_acc->k;
            if (k == node_k) {
                node_acc->v = v;
                return;
            }
            node_obj = node_acc->next_node_obj;
        }
        auto acc = alloc_obj<HashNode<K, V>>(k, v);
        acc->next_node_obj = header[idx];
        header[idx] = acc.get_obj();
    }

    value_const_accessor get(const K &k) {
        uint64_t idx = Hash{}(k) % BucketCount;
        far_obj_t node_obj = header[idx];
        while (!node_obj.is_null()) {
            auto node_acc = node_const_accessor(node_obj);
            const K &node_k = node_acc->k;
            if (k == node_k) {
                return value_const_accessor(std::move(node_acc),
                                            &(node_acc->v));
            }
            node_obj = node_acc->next_node_obj;
        }
        return value_const_accessor();  // null
    }

    value_const_accessor get(const K &k, __DMH__) {
        uint64_t idx = Hash{}(k) % BucketCount;
        far_obj_t node_obj = header[idx];
        while (!node_obj.is_null()) {
            auto node_acc = node_const_accessor(node_obj, __on_miss__);
            const K &node_k = node_acc->k;
            if (k == node_k) {
                return value_const_accessor(std::move(node_acc),
                                            &(node_acc->v));
            }
            node_obj = node_acc->next_node_obj;
        }
        return value_const_accessor();  // null
    }

    value_accessor get_mutable(const K &k) {
        uint64_t idx = Hash{}(k) % BucketCount;
        far_obj_t node_obj = header[idx];
        while (!node_obj.is_null()) {
            auto node_acc = node_accessor(node_obj);
            K &node_k = node_acc->k;
            if (k == node_k) {
                return value_accessor(std::move(node_acc), &(node_acc->v));
            }
            node_obj = node_acc->next_node_obj;
        }
        return value_accessor();  // null
    }

    value_const_accessor get_immutable(const K &k) { return get(k); }

    void erase(K &k) {
        uint64_t idx = Hash{}(k) % BucketCount;
        far_obj_t last_obj = far_obj_t::null();
        far_obj_t node_obj = header[idx];
        bool find = false;
        while (!node_obj.is_null()) {
            auto node_acc = node_accessor(node_obj);
            K &node_k = node_acc->k;
            if (k == node_k) {
                find = true;
                if (last_obj.is_null()) {
                    header[idx] = far_obj_t::null();
                } else {
                    auto last_node_acc = node_accessor(last_obj);
                    last_node_acc->next_node_obj = node_acc->next_node_obj;
                }
                break;
            }
        }
        if (find) {
            Beehive::Cache::get_default()->deallocate(node_obj);
        }
    }

    value_accessor operator[](const K &k) { return get_mutable(k); }

    template <typename Fn>
        requires requires(Fn &&fn, const K *k, std::optional<const V *> v) {
            fn(k, v);
        }
    void multi_find_sequence(std::vector<K> &find_keys, Fn &&fn) {
        for (auto &k : find_keys) {
            value_const_accessor v = get_immutable(k);
            if (v.get_obj().is_null()) {
                fn(&k, std::nullopt);
            } else {
                fn(&k, v.as_ptr());
            }
        }
    }

    template <bool Sync = true, typename Fn>
        requires requires(Fn &&fn, const K *k, std::optional<const V *> v) {
            fn(k, v);
        }
    void multi_find_async_for(std::vector<K> &find_keys, Fn &&fn) {
        ASYNC_FOR(size_t, i, 0, i < find_keys.size(), i++)
            const K &k = find_keys[i];
            value_const_accessor v = get(k, __on_miss__);
            if constexpr (Sync) __sync__();
            if (v.get_obj().is_null()) {
                fn(&k, std::nullopt);
            } else {
                fn(&k, v.as_ptr());
            }
        ASYNC_FOR_END
    }

    template <typename Fn>
        requires requires(Fn &&fn, const K *k, std::optional<const V *> v) {
            fn(k, v);
        }
    void multi_find_buffered_pipeline(const std::vector<K> &find_keys,
                                      Fn &&fn) {
        std::deque<std::tuple<const K *, far_obj_t, bool>> buffered_bucket_head;
        auto key_it = find_keys.begin();
        auto key_it_end = find_keys.end();
        ON_MISS_BEGIN
            static constexpr size_t QueueMaxLength = 1 << 10;
            auto cache = Cache::get_default();
            // continue calculating element in queue
            // ---------------- begin -------------------------
            for (auto &[k, h, f] : buffered_bucket_head) {
                if (f) continue;
                while (cache->prefetch(h) ==
                       Beehive::cache::FetchState::FETCH_PRESENT) {
                    auto node_acc = node_const_accessor(h);
                    if (node_acc->k == *k) {
                        f = true;
                        break;
                    }
                    h = node_acc->next_node_obj;
                }
                if (cache::check_fetch(__entry__, __ddl__)) return;
            }
            // ---------------- end -------------------------
            // just annotate/deannotate this part to open/close this function
            while (buffered_bucket_head.size() < QueueMaxLength &&
                   key_it != key_it_end) {
                const K *key = &*key_it;
                far_obj_t head = header[Hash{}(*key) % BucketCount];
                bool find = false;
                while (cache->prefetch(head) ==
                       Beehive::cache::FetchState::FETCH_PRESENT) {
                    auto head_acc = node_const_accessor(head);
                    if (*key == head_acc->k) {
                        find = true;
                        break;
                    } else {
                        head = head_acc->next_node_obj;
                    }
                }
                buffered_bucket_head.push_back({key, head, find});
                key_it++;
                if (cache::check_fetch(__entry__, __ddl__)) return;
            }
        ON_MISS_END
        size_t prefetch_true = 0;
        while (true) {
            // get bucket head from last level
            const K *key;
            far_obj_t node_obj;
            bool find = false;
            if (buffered_bucket_head.empty()) [[unlikely]] {
                if (key_it == key_it_end) break;
                key = &*key_it;
                node_obj = header[Hash{}(*key) % BucketCount];
                key_it++;
            } else {
                uint64_t start = get_cycles();
                auto [k, h, f] = buffered_bucket_head.front();
                key = k;
                node_obj = h;
                find = f;
                buffered_bucket_head.pop_front();
            }

            if (find) {
                auto node_acc = node_const_accessor(node_obj, __on_miss__);
                fn(key, &(node_acc->v));
                continue;
            }

            while (!node_obj.is_null()) {
                auto node_acc = node_const_accessor(node_obj, __on_miss__);
                if (*key == node_acc->k) {
                    fn(key, &(node_acc->v));
                    find = true;
                    break;
                }
                node_obj = node_acc->next_node_obj;
            }

            if (!find) {
                fn(key, std::nullopt);
            }
        }
    }

    using coro_bool = boost::coroutines2::coroutine<bool>;

    template <bool SYNC_FLAG, typename Fn>
        requires requires(Fn &&fn, const K *k, std::optional<const V *> v) {
            fn(k, v);
        }
    struct CoroPair {
        std::function<void(coro_bool::push_type &)> push_f;
        coro_bool::pull_type pull_f;
        size_t id;

        CoroPair(size_t id, size_t &cursor, std::vector<K> &keys, size_t start,
                 size_t end, far_obj_t *header, Fn &fn)
            : id(id),
              push_f([id, &cursor, start, end, &keys, header,
                      &fn](coro_bool::push_type &push) {
                  push(false);
                  for (size_t i = start; i < end; i++) {
                      const K *key = &keys[i];
                      uint64_t idx = Hash{}(*key) % BucketCount;
                      far_obj_t node = header[idx];
                      auto cache = Cache::get_default();
                      while (!node.is_null()) {
                          while (cache->prefetch(node) !=
                                 cache::FetchState::FETCH_PRESENT) {
                              push(false);
                              // cache->check_cq();
                          }
                          node_const_accessor node_acc(node);
                          if (*key == node_acc->k) {
                              if (SYNC_FLAG) {
                                  while (cursor != id) {
                                      push(false);
                                  }
                              }
                              fn(key, &(node_acc->v));
                              break;
                          }
                          node = node_acc->next_node_obj;
                      }
                  }
                  cursor++;
                  while (true) {
                      push(true);
                  }
              }),
              pull_f(push_f) {}

        CoroPair(size_t id, std::atomic_size_t &cursor, std::vector<K> &keys,
                 size_t start, size_t end, far_obj_t *header, Fn &fn)
            : id(id),
              push_f([id, &cursor, start, end, &keys, header,
                      &fn](coro_bool::push_type &push) {
                  push(false);
                  for (size_t i = start; i < end; i++) {
                      const K *key = &keys[i];
                      uint64_t idx = Hash{}(*key) % BucketCount;
                      far_obj_t node = header[idx];
                      auto cache = Cache::get_default();
                      while (!node.is_null()) {
                          while (cache->prefetch(node) !=
                                 cache::FetchState::FETCH_PRESENT) {
                              push(false);
                              // cache->check_cq();
                          }
                          node_const_accessor node_acc(node);
                          if (*key == node_acc->k) {
                              if (SYNC_FLAG) {
                                  while (cursor != id) {
                                      push(false);
                                  }
                              }
                              fn(key, &(node_acc->v));
                              break;
                          }
                          node = node_acc->next_node_obj;
                      }
                  }
                  cursor++;
                  while (true) {
                      push(true);
                  }
              }),
              pull_f(push_f) {}
    };

    template <bool SYNC_FLAG = true, size_t CORO_N = 16, typename Fn>
        requires requires(Fn &&fn, const K *k, std::optional<const V *> v) {
            fn(k, v);
        }
    void multi_find_coroutine(std::vector<K> &find_keys, Fn &&fn) {
        std::list<CoroPair<SYNC_FLAG, Fn>> coros;
        size_t cursor = 0;
        for (size_t i = 0, cnt = 0, step = find_keys.size() / CORO_N;
             i < find_keys.size(); i += step, cnt++) {
            coros.push_back({cnt, cursor, find_keys, i, i + step, header, fn});
        }
        while (!coros.empty()) {
            for (auto it = coros.begin(); it != coros.end();) {
                it->pull_f();
                if (it->pull_f.get()) {
                    it = coros.erase(it);
                } else {
                    it++;
                }
            }
        }
    }

    template <bool SYNC_FLAG = true, size_t THREAD_N = 16, typename Fn>
        requires requires(Fn &&fn, const K *k, std::optional<const V *> v) {
            fn(k, v);
        }
    void multi_find_thread(std::vector<K> &find_keys, Fn &&fn) {
        std::atomic_size_t cursor = 0;
        std::function<void(size_t, size_t, size_t, far_obj_t *)> process =
            [&](size_t id, size_t start, size_t end, far_obj_t *header) {
                for (int i = start; i < end; i++) {
                    auto &k = find_keys[i];
                    auto val_acc = get(k);
                    if (SYNC_FLAG) {
                        while (cursor != id);
                    }
                    if (val_acc.get_obj().is_null()) {
                        fn(&k, std::nullopt);
                    } else {
                        fn(&k, val_acc.as_ptr());
                    }
                }
                cursor++;
            };
        size_t cnt = 0;
        std::vector<std::thread> threads;
        for (size_t i = 0, step = find_keys.size() / THREAD_N; cnt < THREAD_N;
             i += step, cnt++) {
            threads.emplace_back(process, cnt, i, i + step, header);
        }
        for (auto &&t : threads) {
            t.join();
        }
    }

    template <bool SYNC_FLAG = true, size_t THREAD_N = 16, size_t CORO_N = 4,
              typename Fn>
        requires requires(Fn &&fn, const K *k, std::optional<const V *> v) {
            fn(k, v);
        }
    void multi_find_thread_coro(std::vector<K> &find_keys, Fn &&fn) {
        std::atomic_size_t cursor = 0;
        std::vector<CoroPair<SYNC_FLAG, Fn>> coros;
        for (size_t i = 0, cnt = 0,
                    step = find_keys.size() / (THREAD_N * CORO_N);
             cnt < THREAD_N * CORO_N; i += step, cnt++) {
            coros.push_back({cnt, cursor, find_keys, i, i + step, header, fn});
        }

        std::function<void(size_t, size_t)> thread_func = [&](size_t start,
                                                              size_t end) {
            std::list<size_t> coro_idxs(end - start);
            std::iota(coro_idxs.begin(), coro_idxs.end(), start);
            while (!coro_idxs.empty()) {
                for (auto it = coro_idxs.begin(); it != coro_idxs.end();) {
                    coros[*it].pull_f();
                    if (coros[*it].pull_f.get()) {
                        it = coro_idxs.erase(it);
                    } else {
                        it++;
                    }
                }
            }
        };

        std::vector<std::thread> threads;
        for (size_t i = 0; i < THREAD_N * CORO_N; i += CORO_N) {
            threads.emplace_back(thread_func, i, i + CORO_N);
        }

        for (auto &&t : threads) {
            t.join();
        }
    }

    template <bool SYNC_FLAG = true, size_t THREAD_N = 16, typename Fn>
        requires requires(Fn &&fn, const K *k, std::optional<const V *> v) {
            fn(k, v);
        }
    void multi_find_thread_async_for(std::vector<K> &find_keys, Fn &&fn) {
        std::function<void(size_t, size_t)> thread_func = [&](size_t start,
                                                              size_t end) {
            ASYNC_FOR(size_t, i, start, i < end, i++)
                const K &k = find_keys[i];
                auto val_acc = get(k, __on_miss__);
                if constexpr (SYNC_FLAG) __sync__();
                if (val_acc.get_obj().is_null()) {
                    fn(&k, std::nullopt);
                } else {
                    fn(&k, val_acc.as_ptr());
                }
            ASYNC_FOR_END
        };

        std::vector<std::thread> threads;
        for (size_t i = 0, step = find_keys.size() / THREAD_N, cnt = 0;
             cnt < THREAD_N; i += step, cnt++) {
            threads.emplace_back(thread_func, i, i + step);
        }

        for (auto &&t : threads) {
            t.join();
        }
    }
};
}  // namespace Beehive
#endif