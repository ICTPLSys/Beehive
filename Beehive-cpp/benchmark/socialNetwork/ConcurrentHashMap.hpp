#pragma once
#include <functional>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>

// template <typename KeyType, typename ValueType> class ConcurrentHashMap {
// private:
//   std::unordered_map<KeyType, ValueType> map;
//   // mutable std::shared_mutex mutex;

// public:
//   // Inserts a new element or assigns to the existing element if the key
//   already
//   // exists
//   void put(const KeyType &key, ValueType &&value) {
//     // std::unique_lock<std::shared_mutex> lock(mutex);
//     map[key] = value;
//   }

//   void put(const KeyType &key, const ValueType &value) {
//     // std::unique_lock<std::shared_mutex> lock(mutex);
//     map[key] = value;
//   }

//   // Retrieves the value associated with the given key
//   std::optional<ValueType> get(const KeyType &key) {
//     // std::shared_lock<std::shared_mutex> lock(mutex);
//     auto it = map.find(key);
//     if (it != map.end()) {
//       return it->second;
//     }
//     return std::nullopt;
//   }

//   // Retrieves the value associated with the given key
//   std::optional<ValueType*> getPoint(const KeyType &key) {
//     // std::shared_lock<std::shared_mutex> lock(mutex);
//     auto it = map.find(key);
//     if (it != map.end()) {
//       return &(it->second);
//     }
//     return std::nullopt;
//   }

//   // Erases the element if the key exists
//   bool erase(const KeyType &key) {
//     // std::unique_lock<std::shared_mutex> lock(mutex);
//     return map.erase(key) > 0;
//   }

//   // Checks if the map contains the given key
//   bool contains(const KeyType &key) const {
//     // std::shared_lock<std::shared_mutex> lock(mutex);
//     return map.find(key) != map.end();
//   }

//   // Size of the ConcurrentHashMap
//   size_t size() const {
//     // std::shared_lock<std::shared_mutex> lock(mutex);
//     return map.size();
//   }

//   // Clear the ConcurrentHashMap
//   void clear() {
//     // std::unique_lock<std::shared_mutex> lock(mutex);
//     map.clear();
//   }

//   void forEach(std::function<void(const KeyType&, ValueType&)> func) {
//     // std::shared_lock<std::shared_mutex> lock(mutex);
//     for (auto& pair : map) {
//       func(pair.first, pair.second);
//     }
//   }
// };
