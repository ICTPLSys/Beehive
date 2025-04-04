#pragma once
#include <cassert>

namespace FarLib {
namespace utils {

template <typename Impl>
struct SingleLinkedNodeBase {
    SingleLinkedNodeBase<Impl> *next;
};

template <typename T>
struct SingleLinkedNode : public SingleLinkedNodeBase<SingleLinkedNode<T>>,
                          public T {};

template <typename Node>
struct SingleLinkedList {
    using NodeBase = SingleLinkedNodeBase<Node>;
    NodeBase *head = nullptr;
    NodeBase *tail = nullptr;
    bool empty() const { return head == nullptr; }
    void push_back(NodeBase *node) {
        node->next = nullptr;
        if (empty()) [[unlikely]] {
            head = node;
            tail = node;
        } else {
            tail->next = node;
            tail = node;
        }
    }
    void pop_front() { head = head->next; }
    NodeBase *front() { return head; }
    const NodeBase *front() const { return head; }
};

template <typename Impl>
struct DoubleLinkedNodeBase {
    DoubleLinkedNodeBase<Impl> *prev;
    DoubleLinkedNodeBase<Impl> *next;
};

template <typename T>
struct DoubleLinkedNode : public DoubleLinkedNodeBase<DoubleLinkedNode<T>>,
                          public T {};

template <typename Node>
struct DoubleLinkedList {
    using NodeBase = DoubleLinkedNodeBase<Node>;
    NodeBase dummy;
#ifndef NDEBUG
    size_t size;
#endif
    DoubleLinkedList() {
        dummy.next = &dummy;
        dummy.prev = &dummy;
#ifndef NDEBUG
        size = 0;
#endif
    }
    bool empty() const { return dummy.next == &dummy; }
    void push_back(NodeBase *node) {
        if (empty()) [[unlikely]] {
            node->next = &dummy;
            node->prev = &dummy;
            dummy.next = node;
            dummy.prev = node;
        } else {
            node->next = &dummy;
            node->prev = dummy.prev;
            dummy.prev->next = node;
            dummy.prev = node;
        }
#ifndef NDEBUG
        size++;
        assert(dummy.next->prev == &dummy);
        assert(size <= 1 || node->next != node->prev);
#endif
    }
    void pop_front() {
        assert(!empty());
        NodeBase *node = dummy.next;
        dummy.next = node->next;
        dummy.next->prev = &dummy;
        if (node->next == &dummy) [[unlikely]] {
            // empty after pop
            dummy.prev = &dummy;
        }
#ifndef NDEBUG
        size--;
        assert(dummy.next->prev == &dummy);
        assert(size <= 1 || dummy.next != dummy.prev);
#endif
    }
    void remove(NodeBase *node) {
        assert(!empty());
        NodeBase *next = node->next;
        NodeBase *prev = node->prev;
        prev->next = next;
        next->prev = prev;
#ifndef NDEBUG
        size--;
        assert(dummy.next->prev == &dummy);
        assert(size <= 1 || dummy.next != dummy.prev);
#endif
    }
    NodeBase *front() { return dummy.next; }
    const NodeBase *front() const { return dummy.next; }
    NodeBase *after_tail() { return &dummy; }
    const NodeBase *after_tail() const { return &dummy; }
};

}  // namespace utils
}  // namespace FarLib
