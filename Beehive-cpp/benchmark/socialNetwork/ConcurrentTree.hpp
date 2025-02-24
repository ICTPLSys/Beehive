#pragma once

#include <pthread.h>

#include <ext/pb_ds/assoc_container.hpp>
#include <ext/pb_ds/tree_policy.hpp>
#include <functional>
#include <shared_mutex>
#include <utility>

// template<typename Key, typename Compare = std::greater<Key>>
// class ConcurrentTree {
// private:
//     using TreeType = __gnu_pbds::tree<
//         Key,
//         __gnu_pbds::null_type,
//         Compare,
//         __gnu_pbds::rb_tree_tag,
//         __gnu_pbds::tree_order_statistics_node_update>;
//     TreeType tree;
//     mutable std::shared_mutex mutex;

// public:
//     // Inserts a new element
//     void insert(const Key& key) {
//         std::unique_lock<std::shared_mutex> lock(mutex);
//         tree.insert(key);
//     }

//     // Erases the element if it exists
//     bool erase(const Key& key) {
//         std::unique_lock<std::shared_mutex> lock(mutex);
//         auto it = tree.find(key);
//         if (it != tree.end()) {
//             tree.erase(it);
//             return true;
//         }
//         return false;
//     }

//     // Checks if the tree contains the given key
//     bool contains(const Key& key) const {
//         std::shared_lock<std::shared_mutex> lock(mutex);
//         return tree.find(key) != tree.end();
//     }

//     // Size of the ConcurrentTree
//     size_t size() const {
//         std::shared_lock<std::shared_mutex> lock(mutex);
//         return tree.size();
//     }

//     // Clear the ConcurrentTree
//     void clear() {
//         std::unique_lock<std::shared_mutex> lock(mutex);
//         tree.clear();
//     }

//     // Order-statistic operations
//     // Find_by_order: Returns an iterator to the k-th largest element
//     (counting from zero) auto find_by_order(size_t order) const {
//         std::shared_lock<std::shared_mutex> lock(mutex);
//         return tree.find_by_order(order);
//     }

//     // Order_of_key: Returns the number of items that are strictly smaller
//     than our item size_t order_of_key(const Key& key) const {
//         std::shared_lock<std::shared_mutex> lock(mutex);
//         return tree.order_of_key(key);
//     }
// };