#pragma once

#include <atomic>
#include <utility>

#include "cache/cache.hpp"

namespace Beehive {

template <typename Key, typename Object, typename Hash = std::hash<Key>,
          size_t NeighborhoodSize = 32>
    requires requires(Object object, Key key) {
        static_cast<bool>(object.equals(key));
    }
class Hopscotch {
    using Self = Hopscotch<Key, Object, Hash, NeighborhoodSize>;

public:
    struct Bucket {
        uint32_t bitmap;
        far_obj_t object;

        Bucket() : bitmap(0) {}

        ConstAccessor<Object> get(__DMH__) {
            return ConstAccessor<Object>(object, __on_miss__);
        }

        template <bool Mut = false>
        LiteAccessor<Object, Mut> get(__DMH__, DereferenceScope &scope) {
            return LiteAccessor<Object, Mut>(object, __on_miss__, scope);
        }

        bool empty() const { return object.is_null(); }

        void set(far_obj_t obj) { object = obj; }
    };

private:
    void resize() { TODO("ConcurrentHopscotch::resize not implemented"); }

    size_t get_bucket_idx(const Key &key) const {
        return Hash{}(key)&hash_mask;
    }

    ConstAccessor<Object> get_internal(const Key &key, Bucket *neighborhood,
                                       __DMH__) {
        uint32_t bitmap = neighborhood->bitmap;
        while (bitmap) {
            int offset = __builtin_ctz(bitmap);
            Bucket &bucket = neighborhood[offset];
            ConstAccessor<Object> accessor = bucket.get(__on_miss__);
            if (accessor->equals(key)) {
                return accessor;
            }
            bitmap ^= (1 << offset);
        }
        return {};
    }

    LiteAccessor<Object> get_internal(const Key &key, Bucket *neighborhood,
                                      __DMH__, DereferenceScope &scope) {
        uint32_t bitmap = neighborhood->bitmap;
        while (bitmap) {
            int offset = __builtin_ctz(bitmap);
            Bucket &bucket = neighborhood[offset];
            LiteAccessor<Object> accessor = bucket.get(__on_miss__, scope);
            if (accessor->equals(key)) {
                return accessor;
            }
            bitmap ^= (1 << offset);
        }
        return {};
    }

    ConstAccessor<Object> get_const(const Key &key, __DMH__) {
        size_t bucket_idx = get_bucket_idx(key);
        Bucket *neighborhood = buckets.data() + bucket_idx;
        return get_internal(key, neighborhood, __on_miss__);
    }

    LiteAccessor<Object> get_const(const Key &key, __DMH__,
                                   DereferenceScope &scope) {
        size_t bucket_idx = get_bucket_idx(key);
        Bucket *neighborhood = buckets.data() + bucket_idx;
        return get_internal(key, neighborhood, __on_miss__, scope);
    }

    ConstAccessor<Object> remove_impl(const Key &key, __DMH__) {
        size_t bucket_idx = get_bucket_idx(key);
        Bucket *neighborhood = buckets.data() + bucket_idx;
        uint32_t bitmap = neighborhood->bitmap;
        while (bitmap) {
            int offset = __builtin_ctz(bitmap);
            Bucket &bucket = neighborhood[offset];
            ConstAccessor<Object> accessor = bucket.get(__on_miss__);
            if (accessor->equals(key)) {
                neighborhood->bitmap ^= (1 << offset);
                bucket.object = far_obj_t::null();
                return accessor;
            }
            bitmap ^= (1 << offset);
        }
        return {};
    }

    // insert an object into bucket
    // this is a pure local method
    void insert_impl(size_t bucket_idx, far_obj_t obj) {
        for (size_t idx = bucket_idx; idx < buckets.size(); idx++) {
            if (buckets[idx].empty()) {
                while (idx >= bucket_idx + NeighborhoodSize) {
                    // unfortunately, the empty bucket is too far away
                    for (size_t distance = NeighborhoodSize - 1; distance > 0;
                         distance--) {
                        size_t swap_bucket_idx = idx - distance;
                        Bucket &swap_bucket = buckets[swap_bucket_idx];
                        if (swap_bucket.bitmap != 0) {
                            int offset = __builtin_ctz(swap_bucket.bitmap);
                            assert(swap_bucket.bitmap & (1 << offset));
                            swap_bucket.bitmap = swap_bucket.bitmap ^
                                                 (1 << offset) ^
                                                 (1 << distance);
                            size_t swap_idx = swap_bucket_idx + offset;
                            buckets[idx].object = buckets[swap_idx].object;
                            idx = swap_idx;
                        }
                    }
                    // we can not swap with any bucket
                    TODO("can not find bucket to swap");
                }
                buckets[idx].set(obj);
                Bucket *neighborhood = buckets.data() + bucket_idx;
                assert(((neighborhood->bitmap) & (1 << (idx - bucket_idx))) ==
                       0);
                neighborhood->bitmap |= 1 << (idx - bucket_idx);
                return;
            }
        }
        // can not find empty bucket
        TODO("can not find empty bucket");
    }

    std::pair<Accessor<Object>, bool> put_impl(const Key &key,
                                               size_t object_size, __DMH__) {
        size_t bucket_idx = get_bucket_idx(key);
        Bucket *neighborhood = buckets.data() + bucket_idx;
        ConstAccessor<Object> accessor =
            get_internal(key, neighborhood, __on_miss__);
        if (!accessor.is_null()) {
            return {accessor.as_mut(), false};
        }
        // can not find the object, insert into a new bucket
        auto raw_object = Accessor<void>::allocate(object_size);
        Accessor<Object> object(std::move(raw_object),
                                static_cast<Object *>(raw_object.as_ptr()));
        insert_impl(bucket_idx, object.get_obj());
        return {std::move(object), true};
    }

    std::pair<LiteAccessor<Object, true>, bool> put_impl(
        const Key &key, size_t object_size, __DMH__, DereferenceScope &scope) {
        size_t bucket_idx = get_bucket_idx(key);
        Bucket *neighborhood = buckets.data() + bucket_idx;
        LiteAccessor<Object> accessor =
            get_internal(key, neighborhood, __on_miss__, scope);
        if (!accessor.is_null()) {
            return {accessor.as_mut(), false};
        }
        // can not find the object, insert into a new bucket
        auto raw_object =
            LiteAccessor<void, true>::allocate(object_size, scope);
        LiteAccessor<Object, true> object(
            raw_object, static_cast<Object *>(raw_object.as_ptr()));
        insert_impl(bucket_idx, object.get_obj());
        return {std::move(object), true};
    }

public:
    Hopscotch(size_t capacity_shift) {
        size_t capacity = 1 << capacity_shift;
        hash_mask = capacity - 1;
        buckets.resize(capacity + NeighborhoodSize);
    }

    ~Hopscotch() {
        for (Bucket bucket : buckets) {
            if (!bucket.object.is_null()) {
                Cache::get_default()->deallocate(bucket.object);
            }
        }
    }

    Accessor<Object> get_mut(const Key &key, __DMH__) {
        return get_const(key, __on_miss__).as_mut();
    }

    ConstAccessor<Object> get(const Key &key, __DMH__) {
        return get_const(key, __on_miss__);
    }

    LiteAccessor<Object, true> get_mut(const Key &key, __DMH__,
                                       DereferenceScope &scope) {
        return get_const(key, __on_miss__, scope).as_mut();
    }

    LiteAccessor<Object> get(const Key &key, __DMH__, DereferenceScope &scope) {
        return get_const(key, __on_miss__, scope);
    }

    template <class Context>
        requires requires(Context context, LiteAccessor<Object> result) {
            { context.get_this() } -> std::convertible_to<const Self *>;
            { context.get_key() } -> std::convertible_to<const Key &>;
            context.make_result(std::move(result));
        }
    struct GetContext : Context {
        Bucket *neighborhood;
        uint32_t bitmap;
        LiteAccessor<Object> accessor;

        enum { INIT, FETCH } status = INIT;

        bool fetched() { return cache::at_local(accessor); }
        void pin() { accessor.pin(); }
        void unpin() { accessor.unpin(); }
        bool run(DereferenceScope &scope) {
            if (status == FETCH) [[unlikely]] {
                goto FETCH;
            }
            {
                Self *this_ptr = Context::get_this();
                const Key &key = Context::get_key();
                size_t bucket_idx = this_ptr->get_bucket_idx(key);
                neighborhood = this_ptr->buckets.data() + bucket_idx;
                bitmap = neighborhood->bitmap;
            }
            while (bitmap) {
                {
                    int offset = __builtin_ctz(bitmap);
                    bitmap ^= (1 << offset);
                    Bucket &bucket = neighborhood[offset];
                    bool at_local = accessor.async_fetch(bucket.object, scope);
                    if (!at_local) {
                        status = FETCH;
                        return false;
                    }
                }
            FETCH:
                if (accessor->equals(Context::get_key())) {
                    Context::make_result(std::move(accessor));
                    return true;
                }
                accessor = {};
            }
            Context::make_result(std::move(accessor));
            return true;
        }
    };

    // if key not exists: insert a new object and return (accessor, true)
    // if key exists: return (accessor, false)
    std::pair<Accessor<Object>, bool> put(const Key &key, size_t object_size,
                                          __DMH__) {
        return put_impl(key, object_size, __on_miss__);
    }

    // if key not exists: insert a new object and return (accessor, true)
    // if key exists: return (accessor, false)
    std::pair<LiteAccessor<Object, true>, bool> put(const Key &key,
                                                    size_t object_size, __DMH__,
                                                    DereferenceScope &scope) {
        return put_impl(key, object_size, __on_miss__, scope);
    }

    // if found, remove it and return the accessor of the object
    // user can deallocate this object
    ConstAccessor<Object> remove(const Key &key, __DMH__) {
        return remove_impl(key, __on_miss__);
    }

private:
    std::vector<Bucket> buckets;
    size_t hash_mask;
};

}  // namespace Beehive