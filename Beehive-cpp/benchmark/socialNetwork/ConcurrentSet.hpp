#pragma once

#include <mutex>
#include <set>
#include <shared_mutex>

// template <typename T, typename Compare = std::less<T>> class ConcurrentSet {
// private:
//   std::set<T, Compare> set;
//   mutable std::shared_mutex mutex;

// public:
//   // Inserts an element into the set
//   bool insert(const T &value) {
//     std::unique_lock<std::shared_mutex> lock(mutex);
//     auto [it, inserted] = set.insert(value);
//     return inserted;
//   }

//   // Removes an element from the set
//   bool erase(const T &value) {
//     std::unique_lock<std::shared_mutex> lock(mutex);
//     auto erased = set.erase(value);
//     return erased > 0;
//   }

//   // Checks if the set contains a given value
//   bool contains(const T &value) const {
//     std::shared_lock<std::shared_mutex> lock(mutex);
//     return set.find(value) != set.end();
//   }

//   // Returns the size of the set
//   size_t size() const {
//     std::shared_lock<std::shared_mutex> lock(mutex);
//     return set.size();
//   }

//   // Clears the set
//   void clear() {
//     std::unique_lock<std::shared_mutex> lock(mutex);
//     set.clear();
//   }

//   // Returns an iterator to the beginning
//   typename std::set<T>::iterator begin(){
//       std::shared_lock<std::shared_mutex> lock(mutex);
//       return set.begin();
//   }

//   // Returns an iterator to the end
//   typename std::set<T>::iterator end(){
//       std::shared_lock<std::shared_mutex> lock(mutex);
//       return set.end();
//   }
// };
