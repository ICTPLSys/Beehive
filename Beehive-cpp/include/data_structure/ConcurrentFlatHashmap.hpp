#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>

#include "utils/spinlock.hpp"

namespace Beehive {

/*
    Key and Val should have a small size
*/
template <typename Key, typename Val, typename Hash = std::hash<Key>>
class ConcurrentFlatMap {
private:
    struct State {
        enum { FREE, IMMUT, WRITE, BUSY } state : 3;
        int read_cnt : 29;
    };

    template <typename T>
    struct Data {
        std::byte data_mem[sizeof(T)];

        T *data() { return reinterpret_cast<T *>(data_mem); }

        operator T *() { return data(); }
        void construct() { std::construct_at(data()); }

        void destroy() { std::destroy_at(data()); }
    };

    using KeyData = Data<Key>;
    using ValData = Data<Val>;
    struct BucketEntry {
        uint32_t bitmap;
        Spinlock spin;
        std::atomic_uint64_t timestamp;
        std::atomic<State> state;
        KeyData key_data;
        ValData val_data;

        BucketEntry()
            : bitmap(0),
              timestamp(0),
              state({
                  .state = State::FREE,
                  .read_cnt = 0,
              }) {}

        BucketEntry(BucketEntry &&) = default;
        BucketEntry &operator=(BucketEntry &&) = default;
        void move_from(BucketEntry &&entry) {
            timestamp = entry.timestamp.load();
            auto st = entry.state.load();
            assert(st.state != State::FREE);
            state.store(st);
            key() = std::move(entry.key());
            val() = std::move(entry.val());
        }

        uint64_t get_timestamp() {
            return timestamp.load(std::memory_order::relaxed);
        }

        void set_timestamp(uint64_t v) {
            timestamp.store(v, std::memory_order::relaxed);
        }

        void cas_free() {
            // assert: entry has locked
            // assert: entry is busy
            auto old_state = state.load();
            assert(old_state.state == State::BUSY);
            State free_state{
                .state = State::FREE,
                .read_cnt = 0,
            };
            bool res = state.compare_exchange_strong(old_state, free_state);
            assert(res);
        }

        bool cas_free_to_busy() {
            // assert: entry has locked
            State free_state{
                .state = State::FREE,
                .read_cnt = 0,
            };
            State busy_state{
                .state = State::BUSY,
                .read_cnt = 0,
            };
            return state.compare_exchange_strong(free_state, busy_state);
        }

        void cas_busy() {
            // assert: entry has locked
            // assert: entry is imut
        retry_load:
            auto old_state = state.load();
        retry:
            auto new_state = old_state;
            assert(new_state.state == State::IMMUT);
            if (new_state.read_cnt > 0) {
                uthread::yield();
                goto retry_load;
            }
            new_state.state = State::BUSY;
            if (!state.compare_exchange_weak(old_state, new_state)) {
                goto retry;
            }
        }

        void cas_imut() {
            // assert: entry has locked
            // assert: emtry is busy
            auto old_state = state.load();
            auto new_state = old_state;
            assert(new_state.state == State::BUSY && new_state.read_cnt == 0);
            new_state.state = State::IMMUT;
            bool res = state.compare_exchange_strong(old_state, new_state);
            assert(res);
        }

        void write_lock() {
            // assert: entry has locked
        retry_load:
            auto old_state = state.load();
        retry:
            auto new_state = old_state;
            if (new_state.read_cnt > 0) {
                uthread::yield();
                goto retry_load;
            }
            assert(new_state.state == State::IMMUT);
            new_state.state = State::WRITE;
            if (!state.compare_exchange_weak(old_state, new_state)) {
                goto retry;
            }
        }

        void write_unlock() {
            auto old_state = state.load();
            auto new_state = old_state;
            assert(new_state.state == State::WRITE && new_state.read_cnt == 0);
            new_state.state = State::IMMUT;
            bool res = state.compare_exchange_strong(old_state, new_state);
            assert(res);
        }

        template <bool BucketLock>
        bool read_lock() {
        retry_load:
            auto old_state = state.load();
        retry:
            auto new_state = old_state;
            if constexpr (BucketLock) {
                assert(new_state.state == State::FREE ||
                       new_state.state == State::IMMUT);
            }
            if (new_state.state == State::FREE ||
                new_state.state == State::BUSY) {
                return false;
            }
            if (!BucketLock && new_state.state == State::WRITE) {
                uthread::yield();
                goto retry_load;
            }
            new_state.read_cnt++;
            if (!state.compare_exchange_weak(old_state, new_state)) {
                goto retry;
            }
            return true;
        }

        void read_unlock() {
            auto old_state = state.load();
        retry:
            auto new_state = old_state;
            assert(new_state.state == State::IMMUT && new_state.read_cnt > 0);
            new_state.read_cnt--;
            if (!state.compare_exchange_weak(old_state, new_state)) {
                goto retry;
            }
        }

        Key &key() { return *key_data.data(); }
        Val &val() { return *val_data.data(); }

        bool is_free() const { return state.load().state == State::FREE; }
        bool is_busy() const { return state.load().state == State::BUSY; }
    };

    constexpr static uint32_t kNeighborhood = 32;
    constexpr static uint32_t kMaxRetries = 2;

    const uint32_t kHashMask_;
    const uint32_t kNumEntries_;
    std::unique_ptr<BucketEntry[]> buckets_;
    std::atomic<size_t> size_;

    size_t get_hash(const Key &key) { return Hash{}(key); }

    void do_remove(BucketEntry *bucket, BucketEntry *entry) {
        entry->cas_busy();
        // assert entry has set busy
        entry->key_data.destroy();
        entry->val_data.destroy();
        entry->cas_free();
        // assert: bucket is locked
        auto offset = entry - bucket;
        assert(bucket->bitmap & (1 << offset));
        bucket->bitmap ^= (1 << offset);
        bucket->timestamp++;
    }

public:
    ConcurrentFlatMap(uint32_t num_entries_shift)
        : kHashMask_((1 << num_entries_shift) - 1),
          kNumEntries_((1 << num_entries_shift) + kNeighborhood) {
        assert(((kHashMask_ + 1) >> num_entries_shift) == 1);
        buckets_.reset(new BucketEntry[kNumEntries_]);
    }
    ~ConcurrentFlatMap() {
        for (size_t i = 0; i < kNumEntries_; i++) {
            if (!buckets_[i].is_free()) {
                buckets_[i].key_data.destroy();
                buckets_[i].val_data.destroy();
            }
        }
    }

    size_t size() const { return size_.load(); }
    template <typename Read>
        requires requires(Read read, const Key key, Val val) { read(key, val); }
    bool get(const Key &key, Read &&read, DereferenceScope *scope) {
        uint32_t hash = get_hash(key);
        uint32_t bucket_idx = hash & kHashMask_;
        BucketEntry *bucket = &(buckets_[bucket_idx]);

        BucketEntry *entry;
        uint64_t timestamp;
        uint32_t retry_counter = 0;

        auto get_once = [&]<bool Lock>() -> bool {
            if constexpr (Lock) {
                if (scope) {
                    bucket->spin.lock(*scope);
                } else {
                    bucket->spin.lock();
                }
            }

            timestamp = bucket->timestamp;
        retry:
            uint32_t bitmap = bucket->bitmap;
            while (bitmap) {
                auto offset = __builtin_ctz(bitmap);
                auto *entry = &buckets_[bucket_idx + offset];
                if (entry->key() == key) {
                    if (entry->template read_lock<Lock>()) {
                        // if not locked, entry key may changed,
                        // if locked, entry key must be equal to key
                        assert(!Lock || entry->key() == key);
                        if (Lock || entry->key() == key) {
                            read(key, entry->val());
                            entry->read_unlock();
                            if constexpr (Lock) {
                                bucket->spin.unlock();
                            }
                            return true;
                        }
                    }
                }
                bitmap ^= (1 << offset);
            }

            if constexpr (Lock) {
                bucket->spin.unlock();
            }
            return false;
        };

        // fast path, without lock index
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

    bool remove(const Key &key, DereferenceScope *scope) {
        uint32_t hash = get_hash(key);
        uint32_t bucket_idx = hash & kHashMask_;
        auto *bucket = &(buckets_[bucket_idx]);
        // lock index
        if (scope) {
            bucket->spin.lock(*scope);
        } else {
            bucket->spin.lock();
        }
        uint32_t bitmap = bucket->bitmap;
        while (bitmap) {
            auto offset = __builtin_ctz(bitmap);
            BucketEntry *entry = &buckets_[bucket_idx + offset];

            if (entry->key() == key) {
                size_--;
                do_remove(bucket, entry);
                bucket->spin.unlock();
                return true;
            }
            bitmap ^= (1 << offset);
        }

        bucket->spin.unlock();
        return false;
    }

    bool put(const Key &key, const Val &val, DereferenceScope *scope) {
        // 1. get bucket index
        uint32_t hash = get_hash(key);
        uint32_t bucket_idx = hash & kHashMask_;
        BucketEntry *bucket = &(buckets_[bucket_idx]);
        uint32_t orig_bucket_idx = bucket_idx;
        // lock the index
        if (scope) {
            bucket->spin.lock(*scope);
        } else {
            bucket->spin.lock();
        }

        uint32_t bitmap = bucket->bitmap;
        while (bitmap) {
            auto offset = __builtin_ctz(bitmap);
            BucketEntry *entry = bucket + offset;

            // 2.1 key exists, update
            if (entry->key() == key) {
                entry->write_lock();
                entry->val() = val;  // TODO ??
                entry->write_unlock();
                bucket->spin.unlock();
                return true;
            }
            bitmap ^= (1 << offset);
        }

        // 2.2 not exists, find empty slot to insert
        while (bucket_idx < kNumEntries_) {
            if (buckets_[bucket_idx].cas_free_to_busy()) break;
            bucket_idx++;
        }

        // 2.3 buckets full, can not insert
        if (bucket_idx == kNumEntries_) {
            bucket->spin.unlock();
            ERROR("hashmap is full");
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
                if (scope) {
                    anchor_entry->spin.lock(*scope);
                } else {
                    anchor_entry->spin.lock();
                }
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
                to_entry->move_from(std::move(*from_entry));
                assert((anchor_entry->bitmap & (1 << distance)) == 0);
                anchor_entry->bitmap |= (1 << distance);
                anchor_entry->timestamp++;

                from_entry->cas_busy();
                assert(anchor_entry->bitmap & (1 << offset));
                anchor_entry->bitmap ^= (1 << offset);

                // Jump backward.
                bucket_idx = idx + offset;
                anchor_entry->spin.unlock();
                break;
            }

            if (!distance) {
                bucket->spin.unlock();
                return false;
            }
        }

        // Allocate memory.
        // final entry has been locked
        BucketEntry *final_entry = &buckets_[bucket_idx];
        assert(final_entry->is_busy());
        size_++;
        final_entry->key() = key;
        final_entry->val() = val;

        // Update the bitmap of the final bucket.
        assert((bucket->bitmap & (1 << distance_to_orig_bucket)) == 0);
        bucket->bitmap |= (1 << distance_to_orig_bucket);
        final_entry->cas_imut();
        bucket->spin.unlock();

        return true;
    }

    template <bool Allocate = true, typename Update>
        requires requires(Update update, const Key key, Val val) {
            update(key, val);
        }
    bool update(const Key &key, Update &&update, DereferenceScope *scope) {
        // 1. get bucket index
        uint32_t hash = get_hash(key);
        uint32_t bucket_idx = hash & kHashMask_;
        BucketEntry *bucket = &(buckets_[bucket_idx]);
        uint32_t orig_bucket_idx = bucket_idx;

        // lock the index
        if (scope) {
            bucket->spin.lock(*scope);
        } else {
            bucket->spin.lock();
        }

        uint32_t bitmap = bucket->bitmap;
        while (bitmap) {
            auto offset = __builtin_ctz(bitmap);
            BucketEntry *entry = bucket + offset;

            // 2.1 key exists, update
            if (entry->key() == key) {
                entry->write_lock();
                update(key, entry->val());
                entry->write_unlock();
                bucket->spin.unlock();
                return true;
            }
            bitmap ^= (1 << offset);
        }

        // 2.2 not exists, find empty slot to insert
        while (bucket_idx < kNumEntries_) {
            if (buckets_[bucket_idx].cas_free_to_busy()) break;
            bucket_idx++;
        }

        // 2.3 buckets full, can not insert
        if (bucket_idx == kNumEntries_) {
            bucket->spin.unlock();
            ERROR("hashmap is full");
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
                if (scope) {
                    anchor_entry->spin.lock(*scope);
                } else {
                    anchor_entry->spin.lock();
                }
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
                to_entry->move_from(std::move(*from_entry));
                assert((anchor_entry->bitmap & (1 << distance)) == 0);
                anchor_entry->bitmap |= (1 << distance);
                anchor_entry->timestamp++;

                from_entry->cas_busy();
                assert(anchor_entry->bitmap & (1 << offset));
                anchor_entry->bitmap ^= (1 << offset);

                // Jump backward.
                bucket_idx = idx + offset;
                anchor_entry->spin.unlock();
                break;
            }

            if (!distance) {
                bucket->spin.unlock();
                return false;
            }
        }

        // Allocate memory.
        // final entry has been locked
        BucketEntry *final_entry = &buckets_[bucket_idx];
        if constexpr (!Allocate) {
            goto update_end;
        }
        assert(final_entry->is_busy());
        size_++;
        final_entry->key() = key;
        if constexpr (Allocate) {
            std::construct_at(&(final_entry->val()));
        }
        update(key, final_entry->val());

        // Update the bitmap of the final bucket.
        assert((bucket->bitmap & (1 << distance_to_orig_bucket)) == 0);
        bucket->bitmap |= (1 << distance_to_orig_bucket);
    update_end:
        final_entry->cas_imut();
        bucket->spin.unlock();

        return true;
    }

    template <typename Construct, typename Update>
        requires requires(Construct construct, Update update, Val *ptr,
                          const Key key, Val val) {
            construct(ptr);
            update(key, val);
        }
    bool insert_or_update(const Key &key, Construct &&construct,
                          Update &&update, DereferenceScope *scope) {
        // 1. get bucket index
        uint32_t hash = get_hash(key);
        uint32_t bucket_idx = hash & kHashMask_;
        BucketEntry *bucket = &(buckets_[bucket_idx]);
        uint32_t orig_bucket_idx = bucket_idx;

        // lock the index
        if (scope) {
            bucket->spin.lock(*scope);
        } else {
            bucket->spin.lock();
        }

        uint32_t bitmap = bucket->bitmap;
        while (bitmap) {
            auto offset = __builtin_ctz(bitmap);
            BucketEntry *entry = bucket + offset;

            // 2.1 key exists, update
            if (entry->key() == key) {
                entry->write_lock();
                update(key, entry->val());
                entry->write_unlock();
                bucket->spin.unlock();
                return true;
            }
            bitmap ^= (1 << offset);
        }

        // 2.2 not exists, find empty slot to insert
        while (bucket_idx < kNumEntries_) {
            if (buckets_[bucket_idx].cas_free_to_busy()) break;
            bucket_idx++;
        }

        // 2.3 buckets full, can not insert
        if (bucket_idx == kNumEntries_) {
            bucket->spin.unlock();
            ERROR("hashmap is full");
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
                if (scope) {
                    anchor_entry->spin.lock(*scope);
                } else {
                    anchor_entry->spin.lock();
                }
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
                to_entry->move_from(std::move(*from_entry));
                assert((anchor_entry->bitmap & (1 << distance)) == 0);
                anchor_entry->bitmap |= (1 << distance);
                anchor_entry->timestamp++;

                from_entry->cas_busy();
                assert(anchor_entry->bitmap & (1 << offset));
                anchor_entry->bitmap ^= (1 << offset);

                // Jump backward.
                bucket_idx = idx + offset;
                anchor_entry->spin.unlock();
                break;
            }

            if (!distance) {
                bucket->spin.unlock();
                return false;
            }
        }

        // Allocate memory.
        // final entry has been locked
        BucketEntry *final_entry = &buckets_[bucket_idx];
        assert(final_entry->is_busy());
        size_++;
        final_entry->key() = key;
        construct(&(final_entry->val()));
        update(key, final_entry->val());

        // Update the bitmap of the final bucket.
        assert((bucket->bitmap & (1 << distance_to_orig_bucket)) == 0);
        bucket->bitmap |= (1 << distance_to_orig_bucket);
        final_entry->cas_imut();
        bucket->spin.unlock();

        return true;
    }
};

}  // namespace Beehive