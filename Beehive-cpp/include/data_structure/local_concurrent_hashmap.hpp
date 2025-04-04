#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "utils/spinlock.hpp"

namespace FarLib {

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
        std::atomic<KVData *> ptr;

        BucketEntry() : bitmap(0), timestamp(0), ptr(nullptr) {}

        uint64_t get_timestamp() {
            return timestamp.load(std::memory_order::relaxed);
        }

        void set_timestamp(uint64_t v) {
            timestamp.store(v, std::memory_order::relaxed);
        }

        bool cas_busy() {
            if (ptr.load(std::memory_order::relaxed) != nullptr) return false;
            KVData *expected_nullptr = nullptr;
            return ptr.compare_exchange_strong(
                expected_nullptr, reinterpret_cast<KVData *>(kBusyPtr));
        }

        void set_busy() {
            ptr.store(reinterpret_cast<KVData *>(kBusyPtr),
                      std::memory_order::relaxed);
        }
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
    ~ConcurrentHashMap();

    bool get(const Key &key, Val *val);
    bool put(const Key &key, Val val);
    bool remove(const Key &key);

    std::atomic<size_t> size;
};

template <typename Key, typename Val, typename Hash>
inline ConcurrentHashMap<Key, Val, Hash>::~ConcurrentHashMap() {
    for (size_t i = 0; i < kNumEntries_; i++) {
        auto &data_ptr = buckets_[i].ptr;
        KVData *data = data_ptr.load(std::memory_order::relaxed);
        if (data != nullptr) {
            delete data;
            data_ptr.store(nullptr, std::memory_order::relaxed);
        }
    }
}

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
    delete entry->ptr.load(std::memory_order::relaxed);
    entry->ptr.store(nullptr, std::memory_order::relaxed);
    auto offset = entry - bucket;
    assert(bucket->bitmap & (1 << offset));
    bucket->bitmap ^= (1 << offset);
}

template <typename Key, typename Val, typename Hash>
inline bool ConcurrentHashMap<Key, Val, Hash>::get(const Key &key, Val *val) {
    uint32_t hash = get_hash(key);
    uint32_t bucket_idx = hash & kHashMask_;
    BucketEntry *bucket = &(buckets_[bucket_idx]);

    BucketEntry *entry;
    uint64_t timestamp;
    uint32_t retry_counter = 0;

    auto get_once = [&]<bool Lock>() -> bool {
        if constexpr (Lock) {
            bucket->spin.lock();
        }

        timestamp = bucket->timestamp;
        uint32_t bitmap = bucket->bitmap;
        while (bitmap) {
            auto offset = __builtin_ctz(bitmap);
            entry = &buckets_[bucket_idx + offset];
            KVData *data = entry->ptr.load(std::memory_order::relaxed);

            if (data->key == key) {
                *val = data->val;
                if constexpr (Lock) {
                    bucket->spin.unlock();
                }
                return true;
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
inline bool ConcurrentHashMap<Key, Val, Hash>::put(const Key &key, Val val) {
    // 1. get bucket index
    uint32_t hash = get_hash(key);
    uint32_t bucket_idx = hash & kHashMask_;
    BucketEntry *bucket = &(buckets_[bucket_idx]);
    uint32_t orig_bucket_idx = bucket_idx;

    bucket->spin.lock();

    uint32_t bitmap = bucket->bitmap;
    while (bitmap) {
        auto offset = __builtin_ctz(bitmap);
        BucketEntry *entry = bucket + offset;
        KVData *data = entry->ptr.load(std::memory_order::relaxed);

        // 2.1 key exists, update
        if (data->key == key) {
            data->val = std::move(val);
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
            anchor_entry->spin.lock();
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

            to_entry->ptr.store(from_entry->ptr.load(),
                                std::memory_order::relaxed);
            assert((anchor_entry->bitmap & (1 << distance)) == 0);
            anchor_entry->bitmap |= (1 << distance);
            anchor_entry->timestamp++;

            from_entry->set_busy();
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
    BucketEntry *final_entry = &buckets_[bucket_idx];
    KVData *data = new KVData;
    size++;
    final_entry->ptr.store(data, std::memory_order::relaxed);

    // Write object.
    *data = {.key = key, .val = std::move(val)};

    // Update the bitmap of the final bucket.
    assert((bucket->bitmap & (1 << distance_to_orig_bucket)) == 0);
    bucket->bitmap |= (1 << distance_to_orig_bucket);

    bucket->spin.unlock();

    return true;
}

template <typename Key, typename Val, typename Hash>
inline bool ConcurrentHashMap<Key, Val, Hash>::remove(const Key &key) {
    uint32_t hash = get_hash(key);
    uint32_t bucket_idx = hash & kHashMask_;
    auto *bucket = &(buckets_[bucket_idx]);

    bucket->spin.lock();

    uint32_t bitmap = bucket->bitmap;
    while (bitmap) {
        auto offset = __builtin_ctz(bitmap);
        BucketEntry *entry = &buckets_[bucket_idx + offset];
        auto *data = entry->ptr.load(std::memory_order::relaxed);

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

}  // namespace FarLib
