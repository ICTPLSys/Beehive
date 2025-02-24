#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "async/context.hpp"
#include "cache/cache.hpp"
#include "utils/spinlock.hpp"

namespace Beehive {

template <typename Key, typename Val, typename Hash = std::hash<Key>>
class ConcurrentHashMap {
private:
    struct KVData {
        Key key;
        Val val;
    };

    struct BucketEntry {
        constexpr static uintptr_t kBusyPtr = 0x1;
        uint32_t bitmap;
        Spinlock spin;
        std::atomic_uint64_t timestamp;
        UniqueFarPtr<KVData> ptr;

        BucketEntry() : bitmap(0), timestamp(0) {}

        uint64_t get_timestamp() {
            return timestamp.load(std::memory_order::seq_cst);
        }

        void set_timestamp(uint64_t v) {
            timestamp.store(v, std::memory_order::relaxed);
        }

        bool cas_busy() { return ptr.cas_null_to_busy(); }
    };

    constexpr static uint32_t kNeighborhood = 32;
    constexpr static uint32_t kMaxRetries = 2;

    const uint32_t kHashMask_;
    const uint32_t kNumEntries_;
    std::unique_ptr<BucketEntry[]> buckets_;

    void do_remove(BucketEntry *bucket, BucketEntry *entry);

    size_t get_hash(const Key &key) { return Hash{}(key); }

public:
    ConcurrentHashMap(uint32_t num_entries_shift);
    ~ConcurrentHashMap() = default;

    bool get(const Key &key, Val *val, __DMH__, DereferenceScope &scope);
    bool put(const Key &key, Val val, __DMH__, DereferenceScope &scope);
    bool remove(const Key &key, __DMH__, DereferenceScope &scope);

    bool get(const Key &key, Val *val, DereferenceScope &scope) {
        ON_MISS_BEGIN
        ON_MISS_END
        return get(key, val, __on_miss__, scope);
    }
    bool put(const Key &key, Val val, DereferenceScope &scope) {
        ON_MISS_BEGIN
        ON_MISS_END
        return put(key, val, __on_miss__, scope);
    }
    bool remove(const Key &key, DereferenceScope &scope) {
        ON_MISS_BEGIN
        ON_MISS_END
        return remove(key, __on_miss__, scope);
    }

    template <class Context>
        requires requires(Context context, const Val *value) {
            { context.get_key() } -> std::convertible_to<const Key *>;
            {
                context.get_hash_map()
            } -> std::convertible_to<ConcurrentHashMap<Key, Val, Hash> *>;
            context.make_result(value);
        }
    struct GetFrame : Context {
        // local vars
        BucketEntry *bucket;
        uint32_t bitmap;
        uint64_t timestamp;
        uint32_t retry_counter;
        uint32_t bucket_idx;
        LiteAccessor<KVData> data;
        enum { INIT, FETCH, FETCH_LOCK } status;

        size_t conflict_id() const { return bucket_idx; }
        bool fetched() { return cache::at_local(data); }
        void pin() { data.pin(); }
        void unpin() { data.unpin(); }
        void init() {
            std::construct_at(&data);
            status = INIT;
            retry_counter = 0;
            auto hash_map = Context::get_hash_map();
            uint32_t hash = hash_map->get_hash(*Context::get_key());
            bucket_idx = hash & hash_map->kHashMask_;
            bucket = &(hash_map->buckets_[bucket_idx]);
        }
        bool run(DereferenceScope &scope) {
            switch (status) {
            case FETCH:
                goto FETCH;
            case FETCH_LOCK:
                goto FETCH_LOCK;
            }
            // fast path
            do {
                timestamp = bucket->get_timestamp();
                bitmap = bucket->bitmap;
                while (bitmap) {
                    {
                        auto offset = __builtin_ctz(bitmap);
                        BucketEntry *entry = &bucket[offset];
                        bitmap ^= (1 << offset);
                        if (entry->ptr.is_null()) [[unlikely]]
                            continue;
                        bool at_local = data.async_fetch(entry->ptr, scope);
                        if (!at_local) {
                            status = FETCH;
                            return false;
                        }
                    }
                FETCH:
                    if (!data.is_null() && data->key == *Context::get_key()) {
                        Context::make_result(&(data->val));
                        return true;
                    }
                }
            } while (timestamp != bucket->get_timestamp() &&
                     retry_counter++ < kMaxRetries);
            if (timestamp == bucket->get_timestamp()) {
                // not found
                data = {};
                Context::make_result(nullptr);
                return true;
            }
            // slow path
            bucket->spin.lock(scope);
            bitmap = bucket->bitmap;
            while (bitmap) {
                {
                    auto offset = __builtin_ctz(bitmap);
                    BucketEntry *entry = &bucket[offset];
                    bitmap ^= (1 << offset);
                    if (entry->ptr.is_null()) [[unlikely]]
                        continue;
                    bool at_local = data.async_fetch(entry->ptr, scope);
                    if (!at_local) {
                        status = FETCH_LOCK;
                        return false;
                    }
                }
            FETCH_LOCK:
                if (data->key == *Context::get_key()) {
                    Context::make_result(&(data->val));
                    bucket->spin.unlock();
                    return true;
                }
            }
            bucket->spin.unlock();
            // not found
            data = {};
            Context::make_result(nullptr);
            return true;
        }
    };

    template <class Context>
        requires requires(Context context, bool result) {
            { context.get_key() } -> std::convertible_to<const Key *>;
            {
                context.get_hash_map()
            } -> std::convertible_to<ConcurrentHashMap<Key, Val, Hash> *>;
            context.make_result(result);
        }
    struct RemoveFrame : Context {
        // local vars
        BucketEntry *bucket;
        uint32_t bucket_idx;
        uint32_t bitmap;
        uint64_t timestamp;
        BucketEntry *entry;
        LiteAccessor<KVData> data;
        enum { INIT, FETCH } status;

        bool fetched() { return cache::at_local(data); }
        void pin() { data.pin(); }
        void unpin() { data.unpin(); }
        void init() {
            status = INIT;
            std::construct_at(&data);
            auto hash_map = Context::get_hash_map();
            uint32_t hash = hash_map->get_hash(*Context::get_key());
            bucket_idx = hash & hash_map->kHashMask_;
            bucket = &(hash_map->buckets_[bucket_idx]);
        }
        size_t conflict_id() const { return bucket_idx; }
        bool run(DereferenceScope &scope) {
            switch (status) {
            case FETCH:
                goto FETCH;
            }
            bucket->spin.lock(scope);
            bitmap = bucket->bitmap;
            while (bitmap) {
                {
                    auto offset = __builtin_ctz(bitmap);
                    entry = &bucket[offset];
                    bitmap ^= (1 << offset);
                    bool at_local = data.async_fetch(entry->ptr, scope);
                    if (!at_local) {
                        status = FETCH;
                        return false;
                    }
                }
            FETCH:
                if (data->key == *Context::get_key()) {
                    auto hash_map = Context::get_hash_map();
                    hash_map->size--;
                    hash_map->do_remove(bucket, entry);
                    bucket->spin.unlock();
                    Context::make_result(true);
                    return true;
                }
            }
            bucket->spin.unlock();
            // not found
            data = {};
            Context::make_result(false);
            return true;
        }
    };

    template <class Context>
        requires requires(Context context, bool result) {
            { context.get_key() } -> std::convertible_to<const Key *>;
            { context.get_value() } -> std::convertible_to<Val>;
            {
                context.get_hash_map()
            } -> std::convertible_to<ConcurrentHashMap<Key, Val, Hash> *>;
            context.make_result(result);
        }
    struct PutFrame : Context {
        // local vars
        BucketEntry *bucket;
        uint32_t bucket_idx;
        uint32_t orig_bucket_idx;
        uint32_t bitmap;
        uint64_t timestamp;
        LiteAccessor<KVData> data;

        enum { INIT, FETCH_LOCK } status;

        bool fetched() { return cache::at_local(data); }
        void pin() { data.pin(); }
        void unpin() { data.unpin(); }
        size_t conflict_id() const { return orig_bucket_idx; }
        void init() {
            status = INIT;
            std::construct_at(&data);
            auto hash_map = Context::get_hash_map();
            uint32_t hash = hash_map->get_hash(*Context::get_key());
            bucket_idx = hash & hash_map->kHashMask_;
            orig_bucket_idx = bucket_idx;
            bucket = &(hash_map->buckets_[bucket_idx]);
        }
        bool run(DereferenceScope &scope) {
            switch (status) {
            case FETCH_LOCK:
                goto FETCH_LOCK;
            }

            //! 1. key exists
            bucket->spin.lock(scope);
            bitmap = bucket->bitmap;
            while (bitmap) {
                {
                    auto offset = __builtin_ctz(bitmap);
                    BucketEntry *entry = &bucket[offset];
                    bitmap ^= (1 << offset);
                    bool at_local = data.async_fetch(entry->ptr, scope);
                    if (!at_local) {
                        status = FETCH_LOCK;
                        return false;
                    }
                }
            FETCH_LOCK:
                if (data->key == *Context::get_key()) {
                    data.as_mut()->val = Context::get_value();
                    Context::make_result(true);
                    bucket->spin.unlock();
                    return true;
                }
            }

            //! key not exists
            auto hash_map = Context::get_hash_map();
            while (bucket_idx < hash_map->kNumEntries_) {
                if (hash_map->buckets_[bucket_idx].cas_busy()) break;
                bucket_idx++;
            }
            if (bucket_idx == hash_map->kNumEntries_) {
                bucket->spin.unlock();
                Context::make_result(false);
                return true;
            }

            uint32_t distance_to_orig_bucket;
            while ((distance_to_orig_bucket = bucket_idx - orig_bucket_idx) >=
                   hash_map->kNeighborhood) {
                // Try to see if we can move things backward.
                uint32_t distance;
                for (distance = kNeighborhood - 1; distance > 0; distance--) {
                    uint32_t idx = bucket_idx - distance;
                    BucketEntry *anchor_entry = &(hash_map->buckets_[idx]);
                    if (!anchor_entry->bitmap) {
                        continue;
                    }

                    // Lock and recheck bitmap.
                    anchor_entry->spin.lock(scope);
                    auto bitmap = anchor_entry->bitmap;
                    if (!bitmap) {
                        anchor_entry->spin.unlock();
                        continue;
                    }

                    // Get the offset of the first entry within the bucket.
                    auto offset = __builtin_ctz(bitmap);
                    if (idx + offset >= bucket_idx) {
                        anchor_entry->spin.unlock();
                        continue;
                    }

                    // Swap entry [closest_bucket + offset] and [bucket_idx]
                    auto *from_entry = &(hash_map->buckets_[idx + offset]);
                    auto *to_entry = &(hash_map->buckets_[bucket_idx]);

                    from_entry->ptr.atomic_move_to_and_set_busy(to_entry->ptr);
                    assert((anchor_entry->bitmap & (1 << distance)) == 0);
                    anchor_entry->bitmap |= (1 << distance);
                    anchor_entry->set_timestamp(anchor_entry->get_timestamp() +
                                                1);

                    // from_entry->set_busy();
                    assert(anchor_entry->bitmap & (1 << offset));
                    anchor_entry->bitmap ^= (1 << offset);

                    // Jump backward.
                    bucket_idx = idx + offset;
                    anchor_entry->spin.unlock();
                    break;
                }

                if (!distance) {
                    bucket->spin.unlock();
                    Context::make_result(false);
                    return true;
                }
            }

            // Allocate memory.
            BucketEntry *final_entry = &(hash_map->buckets_[bucket_idx]);
            LiteAccessor<KVData, true> data =
                final_entry->ptr.allocate_lite_from_busy(scope);
            hash_map->size++;

            // Write object.
            data->key = *Context::get_key();
            data->val = Context::get_value();

            // Update the bitmap of the final bucket.
            assert((bucket->bitmap & (1 << distance_to_orig_bucket)) == 0);
            bucket->bitmap |= (1 << distance_to_orig_bucket);

            bucket->spin.unlock();
            Context::make_result(true);
            return true;
        }
    };

    std::atomic<size_t> size;

    template <bool Mut>
    struct DefaultGetCont {
        void operator()(LiteAccessor<Val, Mut> result) {}
    };

    template <bool Mut = false, typename Cont = DefaultGetCont<Mut>>
    struct GetContext : public async::GenericOrderedContext {
        using Map = ConcurrentHashMap<Key, Val, Hash>;
        using Result = LiteAccessor<Val, Mut>;
        const Key *key;
        BucketEntry *bucket;
        uint32_t bitmap;
        uint64_t timestamp;
        uint32_t retry_counter;
        LiteAccessor<KVData, Mut> data;
        Cont continuation;
        enum { INIT, FETCH, FETCH_LOCK } status;

        GetContext(Map *map, const Key *key, Cont &&cont = {})
            : key(key), data(), continuation(cont) {
            status = INIT;
            retry_counter = 0;
            uint32_t hash = map->get_hash(*key);
            uint32_t bucket_idx = hash & map->kHashMask_;
            set_conflict_id(bucket_idx);
            bucket = &(map->buckets_[bucket_idx]);
        }

        virtual bool fetched() const override { return cache::at_local(data); }
        virtual void pin() const override { data.pin(); }
        virtual void unpin() const override { data.unpin(); }
        virtual void destruct() override { std::destroy_at(&data); }
        void set_result() {
            if (!data.is_null()) {
                continuation(Result(data, &(data->val)));
            } else {
                continuation(Result{});
            }
        }

        virtual bool run(DereferenceScope &scope) override {
            switch (status) {
            case FETCH:
                goto FETCH;
            case FETCH_LOCK:
                goto FETCH_LOCK;
            }
            // fast path
            do {
                timestamp = bucket->get_timestamp();
                bitmap = bucket->bitmap;
                while (bitmap) {
                    {
                        auto offset = __builtin_ctz(bitmap);
                        BucketEntry *entry = &bucket[offset];
                        bitmap ^= (1 << offset);
                        if (entry->ptr.is_null()) [[unlikely]]
                            continue;
                        bool at_local = data.async_fetch(entry->ptr, scope);
                        if (!at_local) {
                            status = FETCH;
                            return false;
                        }
                    }
                FETCH:
                    if (!data.is_null() && data->key == *key) {
                        set_result();
                        return true;
                    }
                }
            } while (timestamp != bucket->get_timestamp() &&
                     retry_counter++ < kMaxRetries);
            if (timestamp == bucket->get_timestamp()) {
                // not found
                data = {};
                set_result();
                return true;
            }
            // slow path
            bucket->spin.lock(scope);
            bitmap = bucket->bitmap;
            while (bitmap) {
                {
                    auto offset = __builtin_ctz(bitmap);
                    BucketEntry *entry = &bucket[offset];
                    bitmap ^= (1 << offset);
                    if (entry->ptr.is_null()) [[unlikely]]
                        continue;
                    bool at_local = data.async_fetch(entry->ptr, scope);
                    if (!at_local) {
                        status = FETCH_LOCK;
                        return false;
                    }
                }
            FETCH_LOCK:
                if (data->key == *key) {
                    set_result();
                    bucket->spin.unlock();
                    return true;
                }
            }
            bucket->spin.unlock();
            // not found
            data = {};
            set_result();
            return true;
        }
    };

    struct DefaultDelCont {
        void operator()(bool result) {}
    };

    template <typename Cont = DefaultDelCont>
    struct RemoveContext : public async::GenericOrderedContext {
        using Map = ConcurrentHashMap<Key, Val, Hash>;
        using Result = bool;
        Map *map;
        const Key *key;
        BucketEntry *bucket;
        uint32_t bucket_idx;
        uint32_t bitmap;
        uint64_t timestamp;
        BucketEntry *entry;
        LiteAccessor<KVData> data;
        Cont cont;
        enum { INIT, FETCH } status;

        RemoveContext(Map *map, const Key *key, Cont &&cont = {})
            : map(map), key(key), data(), cont(cont) {
            status = INIT;
            uint32_t hash = map->get_hash(*key);
            uint32_t bucket_idx = hash & map->kHashMask_;
            set_conflict_id(bucket_idx);
            bucket = &(map->buckets_[bucket_idx]);
        }

        virtual bool fetched() const override { return cache::at_local(data); }
        virtual void pin() const override { data.pin(); }
        virtual void unpin() const override { data.unpin(); }
        virtual void destruct() override { std::destroy_at(&data); }
        virtual bool run(DereferenceScope &scope) override {
            switch (status) {
            case FETCH:
                goto FETCH;
            }
            bucket->spin.lock(scope);
            bitmap = bucket->bitmap;
            while (bitmap) {
                {
                    auto offset = __builtin_ctz(bitmap);
                    entry = &bucket[offset];
                    bitmap ^= (1 << offset);
                    bool at_local = data.async_fetch(entry->ptr, scope);
                    if (!at_local) {
                        status = FETCH;
                        return false;
                    }
                }
            FETCH:
                if (data->key == *key) {
                    map->size--;
                    map->do_remove(bucket, entry);
                    bucket->spin.unlock();
                    cont(true);
                    return true;
                }
            }
            bucket->spin.unlock();
            // not found
            data = {};
            cont(false);
            return true;
        }
    };

    struct DefaultPutCont {
        void operator()(bool result) {}
    };

    template <typename Cont = DefaultPutCont>
    struct PutContext : public async::GenericOrderedContext {
        using Map = ConcurrentHashMap<Key, Val, Hash>;
        using Result = bool;
        Map *map;
        Key key;
        Val val;
        BucketEntry *bucket;
        uint32_t bucket_idx;
        uint32_t orig_bucket_idx;
        uint32_t bitmap;
        uint64_t timestamp;
        LiteAccessor<KVData> data;
        Cont cont;
        enum { INIT, FETCH_LOCK } status;

        virtual bool fetched() const override { return cache::at_local(data); }
        virtual void pin() const override { data.pin(); }
        virtual void unpin() const override { data.unpin(); }
        virtual void destruct() override {
            std::destroy_at(&key);
            std::destroy_at(&val);
            std::destroy_at(&data);
        }
        PutContext(Map *map, const Key &key, const Val &val, Cont &&cont = {})
            : map(map), key(key), val(val), data(), cont(cont) {
            status = INIT;
            uint32_t hash = map->get_hash(key);
            bucket_idx = hash & map->kHashMask_;
            orig_bucket_idx = bucket_idx;
            bucket = &(map->buckets_[bucket_idx]);
            set_conflict_id(orig_bucket_idx);
        }
        virtual bool run(DereferenceScope &scope) override {
            switch (status) {
            case FETCH_LOCK:
                goto FETCH_LOCK;
            }

            //! 1. key exists
            bucket->spin.lock(scope);
            bitmap = bucket->bitmap;
            while (bitmap) {
                {
                    auto offset = __builtin_ctz(bitmap);
                    BucketEntry *entry = &bucket[offset];
                    bitmap ^= (1 << offset);
                    bool at_local = data.async_fetch(entry->ptr, scope);
                    if (!at_local) {
                        status = FETCH_LOCK;
                        return false;
                    }
                }
            FETCH_LOCK:
                if (data->key == key) {
                    data.as_mut()->val = std::move(val);
                    bucket->spin.unlock();
                    cont(true);
                    return true;
                }
            }

            //! key not exists
            // find a new bucket that not used
            while (bucket_idx < map->kNumEntries_) {
                if (map->buckets_[bucket_idx].cas_busy()) break;
                bucket_idx++;
            }
            if (bucket_idx == map->kNumEntries_) {
                bucket->spin.unlock();
                ERROR("map is full!");
                cont(false);
                return true;
            }

            uint32_t distance_to_orig_bucket;
            while ((distance_to_orig_bucket = bucket_idx - orig_bucket_idx) >=
                   map->kNeighborhood) {
                // Try to see if we can move things backward.
                uint32_t distance;
                for (distance = kNeighborhood - 1; distance > 0; distance--) {
                    uint32_t idx = bucket_idx - distance;
                    BucketEntry *anchor_entry = &(map->buckets_[idx]);
                    if (!anchor_entry->bitmap) {
                        continue;
                    }

                    // Lock and recheck bitmap.
                    anchor_entry->spin.lock(scope);
                    auto bitmap = anchor_entry->bitmap;
                    if (!bitmap) {
                        anchor_entry->spin.unlock();
                        continue;
                    }

                    // Get the offset of the first entry within the bucket.
                    auto offset = __builtin_ctz(bitmap);
                    if (idx + offset >= bucket_idx) {
                        anchor_entry->spin.unlock();
                        continue;
                    }

                    // Swap entry [closest_bucket + offset] and [bucket_idx]
                    auto *from_entry = &(map->buckets_[idx + offset]);
                    auto *to_entry = &(map->buckets_[bucket_idx]);

                    from_entry->ptr.atomic_move_to_and_set_busy(to_entry->ptr);
                    assert((anchor_entry->bitmap & (1 << distance)) == 0);
                    anchor_entry->bitmap |= (1 << distance);
                    anchor_entry->set_timestamp(anchor_entry->get_timestamp() +
                                                1);

                    // from_entry->set_busy();
                    assert(anchor_entry->bitmap & (1 << offset));
                    anchor_entry->bitmap ^= (1 << offset);

                    // Jump backward.
                    bucket_idx = idx + offset;
                    anchor_entry->spin.unlock();
                    break;
                }

                if (!distance) {
                    bucket->spin.unlock();
                    ERROR("bucket cant move!");
                    cont(false);
                    return true;
                }
            }

            // Allocate memory.
            BucketEntry *final_entry = &(map->buckets_[bucket_idx]);
            LiteAccessor<KVData, true> data =
                final_entry->ptr.allocate_lite_from_busy(scope);
            map->size++;

            // Write object.
            data->key = std::move(key);
            data->val = std::move(val);

            // Update the bitmap of the final bucket.
            assert((bucket->bitmap & (1 << distance_to_orig_bucket)) == 0);
            bucket->bitmap |= (1 << distance_to_orig_bucket);

            bucket->spin.unlock();
            cont(true);
            return true;
        }
    };
};

template <typename Key, typename Val, typename Hash>
inline ConcurrentHashMap<Key, Val, Hash>::ConcurrentHashMap(
    uint32_t num_entries_shift)
    : kHashMask_((1 << num_entries_shift) - 1),
      kNumEntries_((1 << num_entries_shift) + kNeighborhood) {
    assert(((kHashMask_ + 1) >> num_entries_shift) == 1);
    buckets_.reset(new BucketEntry[kNumEntries_]);
}

template <typename Key, typename Val, typename Hash>
inline void ConcurrentHashMap<Key, Val, Hash>::do_remove(BucketEntry *bucket,
                                                         BucketEntry *entry) {
    entry->ptr.reset();
    auto offset = entry - bucket;
    assert(bucket->bitmap & (1 << offset));
    bucket->bitmap ^= (1 << offset);
}

template <typename Key, typename Val, typename Hash>
inline bool ConcurrentHashMap<Key, Val, Hash>::get(const Key &key, Val *val,
                                                   __DMH__,
                                                   DereferenceScope &scope) {
    uint32_t hash = get_hash(key);
    uint32_t bucket_idx = hash & kHashMask_;
    BucketEntry *bucket = &(buckets_[bucket_idx]);

    BucketEntry *entry;
    uint64_t timestamp;
    uint32_t retry_counter = 0;

    auto get_once = [&]<bool Lock>() -> bool {
        if constexpr (Lock) {
            bucket->spin.lock(scope);
        }

        timestamp = bucket->get_timestamp();
        uint32_t bitmap = bucket->bitmap;
        while (bitmap) {
            auto offset = __builtin_ctz(bitmap);
            entry = &buckets_[bucket_idx + offset];
            if (!entry->ptr.is_null()) [[likely]] {
                LiteAccessor<KVData> data =
                    entry->ptr.access(__on_miss__, scope);
                if (!data.is_null() && data->key == key) {
                    *val = data->val;
                    if constexpr (Lock) {
                        bucket->spin.unlock();
                    }
                    return true;
                }
            }

            bitmap ^= (1 << offset);
        }

        if constexpr (Lock) {
            bucket->spin.unlock();
        }
        return false;
    };

    // fast path
    do {
        if (get_once.template operator()<false>()) {
            return true;
        }
    } while (timestamp != bucket->get_timestamp() &&
             retry_counter++ < kMaxRetries);

    // slow path
    if (timestamp != bucket->get_timestamp()) {
        if (get_once.template operator()<true>()) {
            return true;
        }
    }

    return false;
}

template <typename Key, typename Val, typename Hash>
inline bool ConcurrentHashMap<Key, Val, Hash>::put(const Key &key, Val val,
                                                   __DMH__,
                                                   DereferenceScope &scope) {
    // 1. get bucket index
    uint32_t hash = get_hash(key);
    uint32_t bucket_idx = hash & kHashMask_;
    BucketEntry *bucket = &(buckets_[bucket_idx]);
    uint32_t orig_bucket_idx = bucket_idx;

    bucket->spin.lock(scope);

    uint32_t bitmap = bucket->bitmap;
    while (bitmap) {
        auto offset = __builtin_ctz(bitmap);
        BucketEntry *entry = bucket + offset;
        assert(!entry->ptr.is_null());
        LiteAccessor<KVData> data = entry->ptr.access(__on_miss__, scope);

        // 2.1 key exists, update
        if (data->key == key) {
            auto mutable_data = data.as_mut();
            mutable_data->val = std::move(val);
            bucket->spin.unlock();
            size++;
            return true;
        }
        bitmap ^= (1 << offset);
    }

    // 2.2 not exists, find empty slot to insert
    while (bucket_idx < kNumEntries_) {
        if (buckets_[bucket_idx].cas_busy()) break;
        bucket_idx++;
    }

    // 2.3 buckets full, can not insert
    if (bucket_idx == kNumEntries_) {
        bucket->spin.unlock();
        ERROR("bucket full!");
        return false;
    }

    // 2.4 buckets not full, move this bucket to the neighborhood
    uint32_t distance_to_orig_bucket;
    while ((distance_to_orig_bucket = bucket_idx - orig_bucket_idx) >=
           kNeighborhood) {
        // Try to see if we can move things backward.
        uint32_t distance;
        for (distance = kNeighborhood - 1; distance > 0; distance--) {
            uint32_t idx = bucket_idx - distance;
            BucketEntry *anchor_entry = &(buckets_[idx]);
            if (!anchor_entry->bitmap) {
                continue;
            }

            // Lock and recheck bitmap.
            anchor_entry->spin.lock(scope);
            auto bitmap = anchor_entry->bitmap;
            if (!bitmap) {
                anchor_entry->spin.unlock();
                continue;
            }

            // Get the offset of the first entry within the bucket.
            auto offset = __builtin_ctz(bitmap);
            if (idx + offset >= bucket_idx) {
                anchor_entry->spin.unlock();
                continue;
            }

            // Swap entry [closest_bucket + offset] and [bucket_idx]
            auto *from_entry = &buckets_[idx + offset];
            auto *to_entry = &buckets_[bucket_idx];

            // now, to is busy, from is valid
            from_entry->ptr.atomic_move_to_and_set_busy(to_entry->ptr);
            // now, from is busy, to is valid
            assert((anchor_entry->bitmap & (1 << distance)) == 0);
            anchor_entry->bitmap |= (1 << distance);
            anchor_entry->set_timestamp(anchor_entry->get_timestamp() + 1);

            assert(anchor_entry->bitmap & (1 << offset));
            anchor_entry->bitmap ^= (1 << offset);

            // Jump backward.
            bucket_idx = idx + offset;
            anchor_entry->spin.unlock();
            break;
        }

        if (!distance) {
            bucket->spin.unlock();
            ERROR("bucket cant move!");
            return false;
        }
    }
    // now, buckets_[bucket_idx] is busy

    // Allocate memory.
    BucketEntry *final_entry = &buckets_[bucket_idx];
    LiteAccessor<KVData, true> data =
        final_entry->ptr.allocate_lite_from_busy(scope);
    size++;

    // Write object.
    data->key = key;
    data->val = std::move(val);

    // Update the bitmap of the final bucket.
    assert((bucket->bitmap & (1 << distance_to_orig_bucket)) == 0);
    bucket->bitmap |= (1 << distance_to_orig_bucket);

    bucket->spin.unlock();

    return true;
}

template <typename Key, typename Val, typename Hash>
inline bool ConcurrentHashMap<Key, Val, Hash>::remove(const Key &key, __DMH__,
                                                      DereferenceScope &scope) {
    uint32_t hash = get_hash(key);
    uint32_t bucket_idx = hash & kHashMask_;
    auto *bucket = &(buckets_[bucket_idx]);

    bucket->spin.lock(scope);

    uint32_t bitmap = bucket->bitmap;
    while (bitmap) {
        auto offset = __builtin_ctz(bitmap);
        BucketEntry *entry = &buckets_[bucket_idx + offset];
        assert(!entry->ptr.is_null());
        LiteAccessor<KVData> data = entry->ptr.access(__on_miss__, scope);

        if (data->key == key) {
            size--;
            do_remove(bucket, entry);
            bucket->spin.unlock();
            return true;
        }
        bitmap ^= (1 << offset);
    }

    bucket->spin.unlock();
    return false;
}

}  // namespace Beehive
