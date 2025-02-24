#pragma once

#include <cstring>
#include <optional>
#include <stack>
#include <tuple>
#include <utility>

#include "cache/cache.hpp"
#include "utils/debug.hpp"

namespace Beehive {

template <typename K, typename V, size_t NodeSize>
class BPlusTreeMap {
public:
    using value_type = std::pair<K, V>;
    using value_accessor = Accessor<V>;
    using const_value_accessor = ConstAccessor<V>;

    struct Node {
        bool is_index;
        uint32_t size;
        K keys[NodeSize];
    };

    struct Index : public Node {
        far_obj_t children[NodeSize + 1];

        uint32_t find(const K &key) {
            for (uint32_t i = 0; i < this->size; i++) {
                if (key < this->keys[i]) return i;
            }
            return this->size;
        }
    };

    struct Leaf : public Node {
        V values[NodeSize];

        std::pair<bool, uint32_t> find(const K &key) {
            for (uint32_t i = 0; i < this->size; i++) {
                if (key < this->keys[i]) return {false, i};
                if (key == this->keys[i]) return {true, i};
            }
            return {false, this->size};
        }
    };

private:
    using node_accessor = Accessor<Node>;
    using const_node_accessor = ConstAccessor<Node>;

    // ( exists before, value accessor, new node if splitted )
    static std::tuple<bool, value_accessor,
                      std::optional<std::pair<K, far_obj_t>>>
    insert(node_accessor &&node, const K &key) {
        if (node->is_index) {
            return insert_index(std::move(node), key);
        } else {
            return insert_leaf(std::move(node), key);
        }
    }

    static std::tuple<bool, value_accessor,
                      std::optional<std::pair<K, far_obj_t>>>
    insert_leaf(node_accessor &&node, const K &key) {
        auto [exists, idx] = node.template cast<Leaf>()->find(key);
        if (exists) {
            V *ptr = &(node.template cast<Leaf>()->values[idx]);
            return {true, value_accessor(std::move(node), ptr), std::nullopt};
        }
        if (node->size == NodeSize) {
            return split_leaf(std::move(node), key, idx);
        } else {
            size_t move_count = node->size - idx;
            V *values_base = node.template cast<Leaf>()->values;
            std::memmove(node->keys + idx + 1, node->keys + idx,
                         move_count * sizeof(K));
            std::memmove(values_base + idx + 1, values_base + idx,
                         move_count * sizeof(V));
            node->keys[idx] = key;
            V *ptr = values_base + idx;
            node->size++;
            return {false, value_accessor(std::move(node), ptr), std::nullopt};
        }
    }

    static std::tuple<bool, value_accessor,
                      std::optional<std::pair<K, far_obj_t>>>
    insert_index(node_accessor &&node, const K &key) {
        auto idx = node.template cast<Index>()->find(key);
        far_obj_t child = node.template cast<Index>()->children[idx];
        auto [exists, value, split_opt] = insert(node_accessor(child), key);
        if (split_opt.has_value()) {
            auto [split_key, split_child] = split_opt.value();
            // we should insert node_split into keys[idx] & children[idx + 1]
            if (node->size == NodeSize) {
                return split_index(std::move(node), split_key, split_child, idx,
                                   std::move(value));
            } else {
                size_t move_count = node->size - idx;
                far_obj_t *children_base =
                    node.template cast<Index>()->children;
                std::memmove(node->keys + idx + 1, node->keys + idx,
                             move_count * sizeof(K));
                std::memmove(children_base + idx + 2, children_base + idx + 1,
                             move_count * sizeof(far_obj_t));
                node->keys[idx] = split_key;
                children_base[idx + 1] = split_child;
                node->size++;
                return {exists, std::move(value), std::nullopt};
            }
        } else {
            return {exists, std::move(value), std::nullopt};
        }
    }

    static std::tuple<bool, value_accessor,
                      std::optional<std::pair<K, far_obj_t>>>
    split_leaf(node_accessor &&node, const K &key, uint32_t idx) {
        auto left_leaf = node.template cast<Leaf>();
        auto right_leaf = alloc_obj<Leaf>();
        right_leaf->is_index = false;
        uint32_t left_size = NodeSize / 2;
        uint32_t right_size = NodeSize - left_size;
        right_leaf->size = right_size;
        left_leaf->size = left_size;
        if (idx <= left_size) {
            // should insert into left node
            left_leaf->size++;
            std::memcpy(right_leaf->keys, left_leaf->keys + left_size,
                        right_size * sizeof(K));
            std::memcpy(right_leaf->values, left_leaf->values + left_size,
                        right_size * sizeof(V));
            std::memmove(left_leaf->keys + idx + 1, left_leaf->keys + idx,
                         (left_size - idx) * sizeof(K));
            std::memmove(left_leaf->values + idx + 1, left_leaf->values + idx,
                         (left_size - idx) * sizeof(V));
            left_leaf->keys[idx] = key;
            return {false,
                    value_accessor(std::move(node), left_leaf->values + idx),
                    std::make_pair(right_leaf->keys[0], right_leaf.get_obj())};
        } else {
            // should insert into right node
            right_leaf->size++;
            uint32_t right_idx = idx - left_size;
            std::memcpy(right_leaf->keys, left_leaf->keys + left_size,
                        right_idx * sizeof(K));
            std::memcpy(right_leaf->values, left_leaf->values + left_size,
                        right_idx * sizeof(V));
            right_leaf->keys[right_idx] = key;
            std::memcpy(right_leaf->keys + right_idx + 1, left_leaf->keys + idx,
                        (NodeSize - idx) * sizeof(K));
            std::memcpy(right_leaf->values + right_idx + 1,
                        left_leaf->values + idx, (NodeSize - idx) * sizeof(V));
            auto split_node =
                std::make_pair(right_leaf->keys[0], right_leaf.get_obj());
            return {false,
                    value_accessor(std::move(right_leaf),
                                   right_leaf->values + right_idx),
                    split_node};
        }
    }

    static std::tuple<bool, value_accessor,
                      std::optional<std::pair<K, far_obj_t>>>
    split_index(node_accessor &&node, const K &key, far_obj_t child,
                uint32_t idx, value_accessor &&value) {
        auto left_index = node.template cast<Index>();
        auto right_index = alloc_obj<Index>();
        right_index->is_index = true;
        static_assert(NodeSize / 2 > 1);
        uint32_t left_size = NodeSize / 2 - 1;
        uint32_t right_size = NodeSize - left_size - 1;
        right_index->size = right_size;
        left_index->size = left_size;
        K split_key = left_index->keys[left_size];
        auto split_node = std::make_pair(split_key, right_index.get_obj());
        if (idx <= left_size) {
            // should insert into left node
            left_index->size++;
            std::memcpy(right_index->keys, left_index->keys + left_size + 1,
                        right_size * sizeof(K));
            std::memcpy(right_index->children,
                        left_index->children + left_size + 1,
                        (right_size + 1) * sizeof(far_obj_t));
            std::memmove(left_index->keys + idx + 1, left_index->keys + idx,
                         (left_size - idx) * sizeof(K));
            std::memmove(left_index->children + idx + 2,
                         left_index->children + idx + 1,
                         (left_size - idx) * sizeof(far_obj_t));
            left_index->keys[idx] = key;
            left_index->children[idx + 1] = child;
        } else {
            // should insert into right node
            right_index->size++;
            uint32_t right_idx = idx - left_size - 1;
            std::memcpy(right_index->keys, left_index->keys + left_size + 1,
                        right_idx * sizeof(K));
            std::memcpy(right_index->children,
                        left_index->children + left_size + 1,
                        (right_idx + 1) * sizeof(far_obj_t));
            right_index->keys[right_idx] = key;
            right_index->children[right_idx + 1] = child;
            std::memcpy(right_index->keys + right_idx + 1,
                        left_index->keys + idx, (NodeSize - idx) * sizeof(K));
            std::memcpy(right_index->children + right_idx + 2,
                        left_index->children + idx + 1,
                        (NodeSize - idx) * sizeof(far_obj_t));
        }
        return {false, std::move(value), split_node};
    }

    void destroy_node(node_accessor &&node) {
        if (node->is_index) {
            for (uint32_t i = 0; i <= node->size; i++) {
                destroy_node(
                    node_accessor(node.template cast<Index>()->children[i]));
            }
        } else {
            node.deallocate();
        }
    }

    template <typename Fn>
        requires requires(Fn &fn, const K &k, const V &v) { fn(k, v); }
    static void const_iterate_recursion_impl(far_obj_t node, Fn &fn, __DMH__) {
        auto accessor = const_node_accessor(node, __on_miss__);
        if (accessor->is_index) {
            const Index *index = accessor.template cast<Index>();
            int i;
            ON_MISS_BEGIN_X
                auto cache = Cache::get_default();
                for (int j = i + 1; j <= index->size; j++) {
                    if (prefetch_const_iterate_recursion_impl(
                            index->children[j], __entry__, __ddl__))
                        return;
                }
            ON_MISS_END_X
            for (i = 0; i <= index->size; i++) {
                const_iterate_recursion_impl(index->children[i], fn,
                                             __on_miss__);
            }
        } else {
            const Leaf *leaf = accessor.template cast<Leaf>();
            for (int i = 0; i < leaf->size; i++) {
                const K &k = leaf->keys[i];
                const V &v = leaf->values[i];
                fn(k, v);
            }
        }
    }

    static bool prefetch_const_iterate_recursion_impl(
        far_obj_t node, cache::FarObjectEntry *entry, cache::fetch_ddl_t ddl) {
        auto prefetch_state = Cache::get_default()->prefetch(node);
        if (prefetch_state == cache::FETCH_PRESENT) [[likely]] {
            auto accessor = const_node_accessor(node);
            if (accessor->is_index) {
                const Index *index = accessor.template cast<Index>();
                for (int i = 0; i <= index->size; i++) {
                    if (prefetch_const_iterate_recursion_impl(
                            index->children[i], entry, ddl))
                        return true;
                }
            }
            return cache::check_fetch(entry, ddl);
        }
        return false;
    }

public:
    BPlusTreeMap() : size_(0) {
        auto root = alloc_obj<Leaf>();
        root->is_index = false;
        root->size = 0;
        root_ = root.get_obj();
    }

    ~BPlusTreeMap() { destroy_node(node_accessor(root_)); }

    size_t size() const { return size_; }

    // if exists, the first bool value is true
    std::pair<bool, value_accessor> get_or_insert(const K &key) {
        auto [exists, value, split_opt] = insert(node_accessor(root_), key);
        if (split_opt.has_value()) {
            auto [split_key, split_child] = split_opt.value();
            auto new_root = alloc_obj<Index>();
            static_assert(NodeSize >= 2);
            new_root->is_index = true;
            new_root->size = 1;
            new_root->keys[0] = split_key;
            new_root->children[0] = root_;
            new_root->children[1] = split_child;
            root_ = new_root.get_obj();
        }
        if (!exists) {
            size_++;
        }
        return {exists, std::move(value)};
    }

    value_accessor get_mut(const K &key) { TODO("not implemented"); }

    const_value_accessor get(const K &key) const { TODO("not implemented"); }

    template <typename Fn>
        requires requires(Fn &&fn, const K &k, const V &v) { fn(k, v); }
    void const_iterate_recursion(Fn &&fn) const {
        ON_MISS_BEGIN
        /* empty handler */
        ON_MISS_END
        const_iterate_recursion_impl(root_, fn, __on_miss__);
    }

    template <typename Fn>
        requires requires(Fn &&fn, const K &k, const V &v) { fn(k, v); }
    void const_iterate_prefetch(Fn &&fn) const {
        std::stack<far_obj_t> work_stack;
        work_stack.push(root_);
        while (!work_stack.empty()) {
            far_obj_t node = work_stack.top();
            work_stack.pop();
            auto accessor = const_node_accessor(node);
            if (accessor->is_index) {
                for (int i = accessor->size; i >= 0; i--) {
                    far_obj_t obj =
                        accessor.template cast<Index>()->children[i];
                    Cache::get_default()->prefetch(obj);
                    work_stack.push(obj);
                }
            } else {
                for (int i = 0; i < accessor->size; i++) {
                    const K &k = accessor->keys[i];
                    const V &v = accessor.template cast<Leaf>()->values[i];
                    fn(k, v);
                }
            }
        }
    }

    template <typename Fn>
        requires requires(Fn &&fn, const K &k, const V &v) { fn(k, v); }
    void const_iterate(Fn &&fn) const {
        std::stack<far_obj_t> work_stack;
        work_stack.push(root_);
        while (!work_stack.empty()) {
            far_obj_t node = work_stack.top();
            work_stack.pop();
            auto accessor = const_node_accessor(node);
            if (accessor->is_index) {
                for (int i = accessor->size; i >= 0; i--) {
                    work_stack.push(
                        accessor.template cast<Index>()->children[i]);
                }
            } else {
                for (int i = 0; i < accessor->size; i++) {
                    const K &k = accessor->keys[i];
                    const V &v = accessor.template cast<Leaf>()->values[i];
                    fn(k, v);
                }
            }
        }
    }

    template <typename Fn>
        requires requires(Fn &&fn, const K &k, V &v) { fn(k, v); }
    void iterate(Fn &&fn) {
        std::stack<far_obj_t> work_stack;
        work_stack.push(root_);
        while (!work_stack.empty()) {
            far_obj_t node = work_stack.top();
            work_stack.pop();
            auto accessor = node_accessor(node);
            if (accessor->is_index) {
                for (int i = accessor->size; i >= 0; i--) {
                    work_stack.push(
                        accessor.template cast<Index>()->children[i]);
                }
            } else {
                for (int i = 0; i < accessor->size; i++) {
                    const K &k = accessor->keys[i];
                    V &v = accessor.template cast<Leaf>()->values[i];
                    fn(k, v);
                }
            }
        }
    }

private:
    size_t size_;
    far_obj_t root_;
};

}  // namespace Beehive
