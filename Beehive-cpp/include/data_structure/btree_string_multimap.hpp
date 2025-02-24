#pragma once

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "cache/accessor.hpp"
#include "cache/cache.hpp"
#include "cache/region_based_allocator.hpp"
#include "data_structure/dyn_sized_object.hpp"
#include "utils/debug.hpp"

namespace Beehive {

template <typename V>
class KVPairs {
private:
    static_assert(std::is_trivial_v<V>);
    static constexpr size_t ChunkSizeInByte = 4096 - allocator::BlockHeadSize;

    DynObjUniqueRef<ChunkSizeInByte> ref;

public:
    struct Head {
        uint32_t key_size;
        uint32_t value_size;
        uint32_t hash;
        uint32_t capacity;  // in bytes
        char key[0];

        std::string_view get_key() const {
            return std::string_view(key, key_size);
        }
    };

private:
    static constexpr size_t MinValueCapacity = 4;
    static constexpr size_t ValueAlignment = 4;
    static_assert(allocator::BlockHeadSize % ValueAlignment == 0);
    static constexpr size_t ValuePerChunk = ChunkSizeInByte / sizeof(V);

    static constexpr size_t value_offset(size_t key_size) {
        size_t unaligned_offset = sizeof(Head) + key_size;
        return (unaligned_offset + ValueAlignment - 1) / ValueAlignment *
               ValueAlignment;
    }

    static constexpr size_t min_capacity(size_t key_size, size_t value_size) {
        size_t size = value_offset(key_size) + value_size * sizeof(V);
        return std::bit_ceil(allocator::BlockHeadSize + size) -
               allocator::BlockHeadSize;
    }

    static constexpr size_t get_chunk(size_t key_size, size_t idx) {
        size_t inline_cap =
            (ChunkSizeInByte - value_offset(key_size)) / sizeof(V);
        return (idx + ValuePerChunk - inline_cap) / ValuePerChunk;
    }

    static constexpr size_t offset_in_chunk(size_t key_size, size_t idx) {
        size_t inline_cap =
            (ChunkSizeInByte - value_offset(key_size)) / sizeof(V);
        return idx < inline_cap
                   ? value_offset(key_size) + idx * sizeof(V)
                   : (idx - inline_cap) % ValuePerChunk * sizeof(V);
    }

public:
    KVPairs() = default;
    KVPairs(std::string_view key, uint32_t hash, DereferenceScope &scope) {
        const size_t cap = min_capacity(key.size(), MinValueCapacity);
        ASSERT(cap <= ChunkSizeInByte);
        ref.allocate(
            cap,
            [=](void *p) {
                Head *head = static_cast<Head *>(p);
                head->key_size = key.size();
                head->value_size = 0;
                head->hash = hash;
                head->capacity = cap;
                std::memcpy(head->key, key.data(), key.size());
            },
            scope);
    }

    void construct(std::string_view key, V value, uint32_t hash,
                   DereferenceScope &scope) {
        const size_t cap = min_capacity(key.size(), MinValueCapacity);
        ASSERT(cap <= ChunkSizeInByte);
        ref.allocate(
            cap,
            [=](void *p) {
                Head *head = static_cast<Head *>(p);
                head->key_size = key.size();
                head->value_size = 1;
                head->hash = hash;
                head->capacity = cap;
                std::memcpy(head->key, key.data(), key.size());
                *reinterpret_cast<V *>((std::byte *)(p) +
                                       value_offset(key.size())) = value;
            },
            scope);
    }

    UniqueFarPtr<void> &get_head_chunk() {
        if (!ref.is_chunked()) {
            return *ref.as_inlined();
        } else {
            return ref.chunk(0);
        }
    }

    void append(V value, DereferenceScope &scope) {
        if (!ref.is_chunked()) [[likely]] {
            // inline
            LiteAccessor<void, true> acc =
                ref.as_inlined()->template access<true>(scope);
            Head *head = static_cast<Head *>(acc.as_ptr());
            size_t v_off = value_offset(head->key_size);
            if (v_off + sizeof(V) * (head->value_size + 1) <= head->capacity) {
                // no reallocation
                auto v_base =
                    reinterpret_cast<V *>((std::byte *)(acc.as_ptr()) + v_off);
                v_base[head->value_size] = value;
                head->value_size++;
            } else {
                // need reallocation
                size_t inline_cap =
                    min_capacity(head->key_size, head->value_size + 1);
                if (inline_cap <= ChunkSizeInByte) {
                    // inline after reallocation
                    ref.reallocate(head->capacity, inline_cap, scope);
                    assert(!ref.is_chunked());
                    acc = ref.as_inlined()->template access<true>(scope);
                    head = static_cast<Head *>(acc.as_ptr());
                    auto v_base = reinterpret_cast<V *>(
                        (std::byte *)(acc.as_ptr()) + v_off);
                    v_base[head->value_size] = value;
                    head->value_size++;
                    head->capacity = inline_cap;
                } else {
                    // chunked after reallocation
                    size_t old_cap = head->capacity;
                    size_t new_cap = ChunkSizeInByte * 2;
                    head->value_size++;
                    head->capacity = new_cap;
                    ref.reallocate(old_cap, new_cap, scope);
                    assert(ref.is_chunked());
                    auto p = ref.chunk(1).template access<true>(scope);
                    V *chunk = reinterpret_cast<V *>(p.as_ptr());
                    chunk[0] = value;
                }
            }
        } else {
            // chunked
            auto head_acc = ref.chunk(0).template access<true>(scope);
            Head *head = reinterpret_cast<Head *>(head_acc.as_ptr());
            size_t idx = head->value_size;
            size_t chunk_idx = get_chunk(head->key_size, idx);
            size_t offset = offset_in_chunk(head->key_size, idx);
            head->value_size++;
            if (chunk_idx >= ref.n_chunks()) {
                // need reallocation
                size_t new_chunk_count = chunk_idx * 2;
                size_t new_cap = new_chunk_count * ChunkSizeInByte;
                head->capacity = new_cap;
                ref.reallocate_chunked(new_cap, scope);
            }
            auto chunk_acc = ref.chunk(chunk_idx).template access<true>(scope);
            *reinterpret_cast<V *>((std::byte *)(chunk_acc.as_ptr()) + offset) =
                value;
        }
    }

    V *__async_append(V value, LiteAccessor<void, true> &accessor,
                      DereferenceScope &scope) {
        if (!ref.is_chunked()) [[likely]] {
            // inline
            Head *head = static_cast<Head *>(accessor.as_ptr());
            size_t v_off = value_offset(head->key_size);
            if (v_off + sizeof(V) * (head->value_size + 1) <= head->capacity) {
                // no reallocation
                auto v_base = reinterpret_cast<V *>(
                    (std::byte *)(accessor.as_ptr()) + v_off);
                v_base[head->value_size] = value;
                head->value_size++;
            } else {
                // need reallocation
                size_t inline_cap =
                    min_capacity(head->key_size, head->value_size + 1);
                if (inline_cap <= ChunkSizeInByte) {
                    // inline after reallocation
                    ref.reallocate(head->capacity, inline_cap, scope);
                    assert(!ref.is_chunked());
                    accessor = ref.as_inlined()->template access<true>(scope);
                    head = static_cast<Head *>(accessor.as_ptr());
                    auto v_base = reinterpret_cast<V *>(
                        (std::byte *)(accessor.as_ptr()) + v_off);
                    v_base[head->value_size] = value;
                    head->value_size++;
                    head->capacity = inline_cap;
                } else {
                    // chunked after reallocation
                    size_t old_cap = head->capacity;
                    size_t new_cap = ChunkSizeInByte * 2;
                    head->value_size++;
                    head->capacity = new_cap;
                    accessor = {};
                    ref.reallocate(old_cap, new_cap, scope);
                    assert(ref.is_chunked());
                    bool at_local = accessor.async_fetch(ref.chunk(1), scope);
                    V *chunk = reinterpret_cast<V *>(accessor.as_ptr());
                    if (at_local) [[likely]] {
                        chunk[0] = value;
                    } else {
                        return chunk;
                    }
                }
            }
        } else {
            // chunked
            Head *head = reinterpret_cast<Head *>(accessor.as_ptr());
            size_t idx = head->value_size;
            size_t chunk_idx = get_chunk(head->key_size, idx);
            size_t offset = offset_in_chunk(head->key_size, idx);
            head->value_size++;
            if (chunk_idx >= ref.n_chunks()) {
                // need reallocation
                size_t new_chunk_count = chunk_idx * 2;
                size_t new_cap = new_chunk_count * ChunkSizeInByte;
                head->capacity = new_cap;
                accessor = {};
                ref.reallocate_chunked(new_cap, scope);
            }
            bool at_local = accessor.async_fetch(ref.chunk(chunk_idx), scope);
            V *vptr = reinterpret_cast<V *>((std::byte *)(accessor.as_ptr()) +
                                            offset);
            if (at_local) [[likely]] {
                *vptr = value;
            } else {
                return vptr;
            }
        }
        return nullptr;
    }

    template <std::invocable<size_t, const V &> Fn>
    void for_each_value(Fn &&fn, DereferenceScope &scope) {
        if (!ref.is_chunked()) [[likely]] {
            LiteAccessor<void> acc = ref.as_inlined()->template access(scope);
            const Head *head = static_cast<const Head *>(acc.as_ptr());
            size_t v_offset = value_offset(head->key_size);
            size_t size = head->value_size;
            auto v_base = reinterpret_cast<const V *>(
                (const std::byte *)(acc.as_ptr()) + v_offset);
            for (size_t i = 0; i < size; i++) {
                fn(i, v_base[i]);
            }
        } else {
            auto head_acc = ref.chunk(0).access(scope);
            const Head *head =
                reinterpret_cast<const Head *>(head_acc.as_ptr());
            size_t size = head->value_size;
            size_t idx;
            // first chunk
            size_t v_offset = value_offset(head->key_size);
            size_t head_cap = (ChunkSizeInByte - v_offset) / sizeof(V);
            auto v_base = reinterpret_cast<const V *>(
                (const std::byte *)(head_acc.as_ptr()) + v_offset);
            for (idx = 0; idx < std::min(head_cap, size); idx++) {
                fn(idx, v_base[idx]);
            }
            // following chunks
            for (size_t chunk_idx = 1; idx < size; chunk_idx++) {
                auto chunk_acc = ref.chunk(chunk_idx).access(scope);
                auto v_ptr = reinterpret_cast<const V *>(chunk_acc.as_ptr());
                for (size_t i = 0; i < ValuePerChunk && idx < size;
                     i++, idx++) {
                    fn(idx, v_ptr[i]);
                }
            }
        }
    }
};

// A MultiMap, with string as Key and Values can be multiple
template <typename V>
class BTreeMultiMap {
    static_assert(sizeof(KVPairs<V>) == 16);

public:
    static_assert(std::is_trivially_destructible_v<V>);
    using Tree = BTreeMultiMap<V>;

    static constexpr size_t Order = 3;
    static constexpr size_t FanOut = Order * 2 + 2;

    struct InternalNode;

    struct Node {
        InternalNode *parent;
        int size;
        Node() : parent(nullptr), size(0) {}
    };

    struct Leaf : public Node {
        KVPairs<V> pairs[FanOut];
        Leaf *next;

        // return true if this is the first value of the key
        bool insert(std::string_view key, V value, uint32_t hash,
                    DereferenceScope &scope) {
            LiteAccessor<void> accessor;
            // find where to insert
            int l = 0;
            int r = this->size - 1;
            while (l <= r) {
                const int m = (l + r) / 2;
                accessor = pairs[m].get_head_chunk().access(scope);
                auto head =
                    static_cast<const KVPairs<V>::Head *>(accessor.as_ptr());
                std::string_view k = head->get_key();
                if (k == key) {
                    // found, just append and return
                    pairs[m].append(value, scope);
                    return false;
                }
                if (k < key)
                    l = m + 1;
                else
                    r = m - 1;
                if (l > r) {
                    break;
                }
            }
            // not found, insert at pos
            int pos = l;
            if (pos < this->size) {
                for (int i = this->size; i > pos; i--) {
                    pairs[i] = std::move(pairs[i - 1]);
                }
            }
            this->size++;
            pairs[pos].construct(key, value, hash, scope);
            return true;
        }

        bool need_split() const { return this->size == FanOut; }

        Leaf *split() {
            assert(this->size == FanOut);
            this->size = Order + 1;
            Leaf *right = new Leaf;
            right->size = Order + 1;
            right->next = next;
            right->parent = this->parent;
            for (int i = 0; i < Order + 1; i++) {
                right->pairs[i] = std::move(pairs[i + Order + 1]);
            }
            next = right;
            return right;
        }
    };

    struct InternalNode : public Node {
        std::pair<std::string, Node *> children[FanOut];

        Node *upper_bound(const std::string_view &key) const {
            size_t pos = upper_bound_pos(key);
            return children[pos].second;
        }

        size_t upper_bound_pos(const std::string_view &key) const {
            if (this->size == 0) return 0;
            bool found = false;
            int l = 0, r = this->size - 1;
            while (l < r) {
                const int m = (l + r) / 2;
                auto &k = children[m].first;
                if (k == key) return m + 1;
                if (k < key)
                    l = m + 1;
                else
                    r = m - 1;
            }
            if (l > r) return l;
            return l + (children[l].first <= key);
        }

        bool need_split() const { return this->size == FanOut - 1; }

        InternalNode *split() {
            assert(this->size == FanOut - 1);
            this->size = Order;
            InternalNode *right = new InternalNode;
            right->size = Order;
            right->parent = this->parent;
            for (int i = 0; i < Order + 1; i++) {
                right->children[i] = std::move(children[i + Order + 1]);
            }
            return right;
        }

        void assign(size_t pos, Node *left, std::string_view key, Node *right) {
            children[pos].second = left;
            children[pos].first = key;
            children[pos + 1].second = right;
        }

        void assign_right(size_t pos, std::string_view key, Node *right) {
            children[pos].first = key;
            children[pos + 1].second = right;
        }
    };

public:
    size_t size_;
    size_t nlevel_;
    Node *root_;

public:
    Leaf *get_leaf(std::string_view key) {
        if (nlevel_ == 0) {
            Leaf *root = new Leaf;
            root->size = 0;
            root->next = nullptr;
            root_ = root;
            nlevel_ = 1;
            size_ = 0;
            return root;
        }
        Node *node = root_;
        for (size_t i = 1; i < nlevel_; i++) {
            InternalNode *inode = static_cast<InternalNode *>(node);
            node = inode->upper_bound(key);
        }
        return static_cast<Leaf *>(node);
    }

    void insert_internal(std::string_view key, Node *left, Node *right) {
        while (left != root_) {
            InternalNode *parent = left->parent;
            // where to insert?
            int ikey = parent->upper_bound_pos(key);
            // move elements
            for (int i = parent->size - 1; i >= ikey; i--) {
                parent->children[i + 1].first =
                    std::move(parent->children[i].first);
            }
            for (int i = parent->size; i >= ikey + 1; i--) {
                parent->children[i + 1].second = parent->children[i].second;
            }
            // asign
            parent->assign_right(ikey, key, right);
            parent->size++;
            // check if parent need split
            if (parent->need_split()) {
                InternalNode *right_parent = parent->split();
                // update variables
                key = std::string_view(parent->children[Order].first);
                left = parent;
                right = right_parent;
                // fix parent pointers
                for (int i = 0; i < right_parent->size + 1; i++) {
                    Node *child = right_parent->children[i].second;
                    child->parent = right_parent;
                }
                continue;
            } else {
                return;
            }
        }
        assert(left == root_);
        InternalNode *new_root = new InternalNode;
        new_root->size = 1;
        new_root->assign(0, left, key, right);
        left->parent = new_root;
        right->parent = new_root;
        root_ = new_root;
        ++nlevel_;
    }

    Leaf *first_leaf() {
        if (nlevel_ == 0) return nullptr;
        Node *node = root_;
        for (int i = 0; i < nlevel_ - 1; i++) {
            node = static_cast<InternalNode *>(node)->children[0].second;
        }
        return static_cast<Leaf *>(node);
    }

    void clear(Node *node, size_t level) {
        if (level == 0) {
            delete static_cast<Leaf *>(node);
        } else {
            InternalNode *inode = static_cast<InternalNode *>(node);
            for (size_t i = 0; i <= inode->size; i++) {
                clear(inode->children[i].second, level - 1);
            }
            delete (inode);
        }
    }

public:
    BTreeMultiMap() : size_(0), nlevel_(0), root_(nullptr) {}
    ~BTreeMultiMap() { clear(); }
    BTreeMultiMap(BTreeMultiMap &&) = default;

    size_t size() const { return size_; }

    void clear() {
        if (nlevel_ > 0) clear(root_, nlevel_ - 1);
        size_ = 0;
        nlevel_ = 0;
    }

    bool insert(std::string_view key, V value, uint32_t hash,
                DereferenceScope &scope) {
        Leaf *leaf = get_leaf(key);
        bool new_key = leaf->insert(key, value, hash, scope);
        if (new_key) {
            if (leaf->need_split()) {
                Leaf *right = leaf->split();
                LiteAccessor<void> pair =
                    right->pairs[0].get_head_chunk().access(scope);
                auto head =
                    static_cast<const KVPairs<V>::Head *>(pair.as_ptr());
                std::string_view split_key = head->get_key();
                insert_internal(split_key, leaf, right);
            }
            size_++;
        }
        return new_key;
    }

    struct InsertFrame {
        // local vars
        size_t conflict_id_;
        Tree *tree;
        LiteAccessor<void> accessor;
        LiteAccessor<void, true> mut_accessor;
        std::string_view key;
        V value;
        uint32_t hash;
        bool new_key;
        Leaf *leaf;
        int l, r, m;
        Leaf *right;
        V *value_ptr;
        enum { INIT, CMP_KEY, APPEND_VALUE, SPLIT } status = INIT;
        size_t conflict_id() const { return conflict_id_; }
        bool fetched() {
            return cache::at_local(accessor) && cache::at_local(mut_accessor);
        }
        void pin() {
            accessor.pin();
            mut_accessor.pin();
        }
        void unpin() {
            accessor.unpin();
            mut_accessor.unpin();
        }
        bool run(DereferenceScope &scope) {
            switch (status) {
            case CMP_KEY:
                goto CMP_KEY;
            case APPEND_VALUE: {
                *value_ptr = value;
                goto LEAF_INSERT_END;
            }
            case SPLIT:
                goto SPLIT;
            }
            leaf = tree->get_leaf(key);
            // find where to insert
            l = 0;
            r = leaf->size - 1;
            while (l <= r) {
                {
                    m = (l + r) / 2;
                    auto &chunk = leaf->pairs[m].get_head_chunk();
                    if (!accessor.async_fetch(chunk, scope)) {
                        status = CMP_KEY;
                        return false;
                    }
                }
            CMP_KEY:
                auto head =
                    static_cast<const KVPairs<V>::Head *>(accessor.as_ptr());
                std::string_view k = head->get_key();
                if (k == key) {
                    // found, just append and return
                    mut_accessor = accessor.as_mut();
                    value_ptr = leaf->pairs[m].__async_append(
                        value, mut_accessor, scope);
                    new_key = false;
                    if (value_ptr) {
                        status = APPEND_VALUE;
                        return false;
                    }
                    goto LEAF_INSERT_END;
                }
                if (k < key)
                    l = m + 1;
                else
                    r = m - 1;
                if (l > r) {
                    break;
                }
            }
            // not found, insert at pos
            {
                int pos = l;
                if (pos < leaf->size) {
                    for (int i = leaf->size; i > pos; i--) {
                        leaf->pairs[i] = std::move(leaf->pairs[i - 1]);
                    }
                }
                leaf->size++;
                leaf->pairs[pos].construct(key, value, hash, scope);
                new_key = true;
            }
        LEAF_INSERT_END:
            if (new_key) {
                if (leaf->need_split()) {
                    {
                        right = leaf->split();
                        auto &chunk = right->pairs[0].get_head_chunk();
                        if (!accessor.async_fetch(chunk, scope)) {
                            status = SPLIT;
                            return false;
                        }
                    }
                SPLIT:
                    auto head = static_cast<const KVPairs<V>::Head *>(
                        accessor.as_ptr());
                    std::string_view split_key = head->get_key();
                    tree->insert_internal(split_key, leaf, right);
                }
                tree->size_++;
            }
            return true;
        }
    };

    bool insert_f(std::string_view key, V value, uint32_t hash,
                  DereferenceScope &scope) {
        InsertFrame frame{
            .tree = this,
            .key = key,
            .value = value,
            .hash = hash,
        };
        while (!frame.run(scope)) {
            do {
                cache::check_cq();
            } while (!frame.fetched());
        }
        return frame.new_key;
    }

    struct iterator {
    private:
        Leaf *leaf;
        size_t i;

    public:
        iterator(Leaf *leaf = nullptr) : leaf(leaf), i(0) {}
        iterator &operator=(const iterator &) = default;
        void next() {
            if (leaf && i + 1 == leaf->size) {
                leaf = leaf->next;
                i = 0;
            } else {
                assert(leaf);
                i++;
            }
        }
        void operator++(int) { next(); }
        void operator++() { next(); }
        bool operator==(const iterator &a) {
            return (leaf == nullptr && a.leaf == nullptr) ||
                   (leaf == a.leaf && i == a.i);
        }
        bool operator!=(const iterator &a) { return !(*this == a); }
        KVPairs<V> *operator->() { return &leaf->pairs[i]; }
        KVPairs<V> &operator*() { return leaf->pairs[i]; }
    };

    iterator begin() {
        if (nlevel_ == 0) {
            return end();
        }
        return iterator(first_leaf());
    }
    iterator end() { return iterator(nullptr); }
};

}  // namespace Beehive