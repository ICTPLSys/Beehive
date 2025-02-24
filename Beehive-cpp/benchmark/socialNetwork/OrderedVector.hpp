#include <iostream>
#include <utility>  // For std::pair
#include <vector>

inline bool compareInt64(const int64_t &a, const int64_t &b) { return a < b; }

inline bool pair_compare(const std::pair<int64_t, int64_t> &a,
                         const std::pair<int64_t, int64_t> &b) {
    return a.first < b.first;
};

// template <typename T, bool (*compare)(const T &, const T &)>
// class OrderedVector {
// public:
//   void insert(const T &value) {
//     elements.emplace_back(value, true);
//     sizeElements++;
//     int i = elements.size() - 1;
//     while (i > 0 &&
//            (!elements[i - 1].second || compare(elements[i - 1].first,
//            value))) {
//       std::swap(elements[i], elements[i - 1]);
//       --i;
//     }
//   }

//   void erase(const T &value) {
//     int i = elements.size() - 1;
//     while (i >= 0) {
//       if (elements[i].second && elements[i].first == value) {
//         elements[i].second = false;
//         sizeElements--;
//       }
//       --i;
//     }
//   }

//   void print() const {
//     for (const auto &element : elements) {
//       if (element.second) {
//         std::cout << element.first << " ";
//       }
//     }
//     std::cout << std::endl;
//   }

//   std::vector<T> getValidElements() {
//     std::vector<T> validElements;
//     int i = elements.size() - 1;
//     while (i >= 0) {
//       if (elements[i].second) {
//         validElements.push_back(elements[i].first);
//       }
//       --i;
//     }
//     return validElements;
//   }

//   std::vector<T> getRangeElements(T start, T stop) {
//     std::vector<T> rangeElements;
//     int i = elements.size() - 1;
//     while (i >= 0) {
//       if (elements[i].second) {
//         if (compare(stop, elements[i].first)) {
//           if (compare(start, elements[i].first)) {
//             break;
//           }
//           rangeElements.push_back(elements[i].first);
//         }
//       }
//       --i;
//     }
//     return rangeElements;
//   }

//   size_t size() const { return sizeElements; }

// private:
//   std::vector<std::pair<T, bool>> elements;
//   size_t sizeElements = 0;
// };
