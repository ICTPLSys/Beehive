/******************************************************************************
    Copyright (C) Martin Karsten 2015-2023

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
#ifndef _Container_h_
#define _Container_h_ 1

#include "runtime/Assertions.h"
#include "runtime/ContainerLink.h"

#define Sentinel (Node*)0xdeadbeef

template<typename Node, Node*&(*Next)(Node&)>
class Stack {
  Node* head;

public:
  Stack() : head(Sentinel) {}
  template<bool = false>
  bool empty() const { return head == Sentinel; }

  Node*              front()       { return head; }
  const Node*        front() const { return head; }

  static Node*       next(      Node& elem) { return Next(elem); }
  static const Node* next(const Node& elem) { return Next(elem); }

  void push(Node& first, Node& last) {
    RASSERT(!Next(last), FmtHex(Next(last))); // assume link invalidated at pop
    Next(last) = head;
    head = &first;
  }

  void push(Node& elem) {
    push(elem, elem);
  }

  Node* pop() {
    if (empty()) return nullptr;
    Node* last = head;
    head = Next(*last);
    Next(*last) = nullptr;               // invalidate link
    return last;
  }

  Node* pop(size_t& count) {             // returns pointer to last element popped
    if (empty()) return nullptr;
    Node* last = head;
    for (size_t i = 1; i < count; i += 1) {
      if (Next(*last) == nullptr) count = i; // breaks loop and sets count
      else last = Next(*last);
    }
    head = Next(*last);
    Next(*last) = nullptr;               // invalidate link
    return last;
  }
};

template<typename Node, Node*&(*Next)(Node&)>
class Queue {
  Node* head;
  Node* tail;

  void popHelper(Node*&) {}

  void popHelper(Node*& elem, size_t& count) {
    for (size_t i = 1; i < count; i += 1) {
      if (Next(*elem) == Sentinel) count = i; // breaks loop and sets count
      else elem = Next(*elem);
    }
  }

public:
  Queue() : head(Sentinel), tail(Sentinel) {}
  template<bool Clean = false>
  bool empty() const {
    RASSERT(Clean || ((head == Sentinel) == (tail == Sentinel)), FmtHex(this));
    return head == Sentinel;
  }

  Node*              front()       { return head; }
  const Node*        front() const { return head; }
  Node*              back()        { return tail; }
  const Node*        back()  const { return tail; }
  Node*              edge()        { return Sentinel; }
  const Node*        edge()  const { return Sentinel; }

  static Node*       next(      Node& elem) { return Next(elem); }
  static const Node* next(const Node& elem) { return Next(elem); }

  void push(Node& first, Node& last) {
    RASSERT(!Next(last), FmtHex(Next(last))); // assume link invalidated at pop
    Next(last) = Sentinel; // validate link
    if (empty()) head = &first;
    else Next(*tail) = &first;
    tail = &last;
  }

  void push(Node& elem) {
    push(elem, elem);
  }

  template<typename...Args>
  Node* pop(Args&... args) {
    if (empty()) return nullptr;
    Node* elem = head;
    popHelper(elem, args...);
    head = Next(*elem);
    if (tail == elem) tail = Sentinel;
    Next(*elem) = nullptr; // invalidate link
    return elem;
  }
};

template<typename Node, Node*&(*Next)(Node&), Node*&(*Prev)(Node&)>
class Ring {

  static void take_out(Node& first, Node& last) {
    RASSERT(Prev(first) && Next(first), FmtHex(&first));
    RASSERT(Prev(last) && Next(last), FmtHex(&last));
    Next(*Prev(first)) = Next(last);
    Prev(*Next(last))  = Prev(first);
  }

  static void combine_before(Node& next, Node& first, Node&last) {
    RASSERT(Prev(next) && Next(next), FmtHex(&next));
    Next(last) = &next;
    Next(*Prev(next)) = &first;
    Prev(first) = Prev(next);
    Prev(next) = &last;
  }

  static void combine_after(Node& prev, Node& first, Node& last) {
    RASSERT(Prev(prev) && Next(prev), FmtHex(&prev));
    Prev(first) = &prev;
    Prev(*Next(prev)) = &last;
    Next(last) = Next(prev);
    Next(prev) = &first;
  }

public:
  static Node*       next(      Node& elem) { return Next(elem); }
  static const Node* next(const Node& elem) { return Next(elem); }
  static Node*       prev(      Node& elem) { return Prev(elem); }
  static const Node* prev(const Node& elem) { return Prev(elem); }

  static void close(Node& first, Node& last) {
    Prev(first) = &last;
    Next(last) = &first;
  }

  static void close(Node& elem) {
    close(elem, elem);
  }

  static void insert_before(Node& next, Node& first, Node&last) {
    RASSERT(!Prev(first), FmtHex(&first)); // assume link invalidated at pop
    RASSERT(!Next(last), FmtHex(&last));   // assume link invalidated at pop
    combine_before(next, first, last);
  }

  static void insert_before(Node& next, Node& elem) {
    insert_before(next, elem, elem);
  }

  template<bool force = false>
  static void insert_after(Node& prev, Node& first, Node& last) {
    RASSERT(force || !Prev(first), FmtHex(Prev(first))); // assume link invalidated at pop
    RASSERT(force || !Next(last), FmtHex(Next(last)));   // assume link invalidated at pop
    combine_after(prev, first, last);
  }

  template<bool force = false>
  static void insert_after(Node& prev, Node& elem) {
    insert_after<force>(prev, elem, elem);
  }

  static Node* remove(Node& first, Node& last) {
    take_out(first, last);
    Prev(first) = Next(last) = nullptr;    // invalidate link
    return &last;
  }

  static Node* remove(Node& elem) {
    return remove(elem, elem);
  }

  static void join_before(Node& next, Node& first, Node&last) {
    RASSERT(Prev(first) == &last, FmtHex(&first));
    RASSERT(Next(last) == &first, FmtHex(&last));
    combine_before(next, first, last);
  }

  static void join_before(Node& next, Node& elem) {
    combine_before(next, elem, elem);
  }

  static void join_after(Node& prev, Node& first, Node&last) {
    RASSERT(Prev(first) == &last, FmtHex(&first));
    RASSERT(Next(last) == &first, FmtHex(&last));
    combine_after(prev, first, last);
  }

  static void join_after(Node& prev, Node& elem) {
    combine_after(prev, elem, elem);
  }

  static Node* split(Node& first, Node& last) {
    take_out(first, last);
    close(first, last);
    return &last;
  }

  static Node* split(Node& elem) {
    return split(elem, elem);
  }
};

// NOTE WELL: Static cast from Link to Node must work! If downcasting,
//            Link must the first class that Node inherits from.
template<typename Node, typename Link, Node*&(*Next)(Node&), Node*&(*Prev)(Node&)>
class List : public Ring<Node,Next,Prev> {
  Link anchorLink;
  Node* anchor;

public:
  List() : anchor(static_cast<Node*>(&anchorLink)) {
    Next(*anchor) = Prev(*anchor) = anchor;
  }

  using Ring<Node,Next,Prev>::insert_before;
  using Ring<Node,Next,Prev>::insert_after;
  using Ring<Node,Next,Prev>::remove;

  Node*       front()       { return Next(*anchor); }
  const Node* front() const { return Next(*anchor); }
  Node*       back()        { return Prev(*anchor); }
  const Node* back()  const { return Prev(*anchor); }
  Node*       edge()        { return anchor; }
  const Node* edge()  const { return anchor; }

  bool        empty() const {
    RASSERT((front() == edge()) == (back() == edge()), FmtHex(this));
    return front() == edge();
  }

  Node* remove(Node& first, size_t& count) {
    RASSERT(Prev(first) && Next(first), FmtHex(&first));
    Node* last = &first;
    for (size_t i = 1; i < count; i += 1) {
      if (Next(*last) == edge()) count = i; // breaks loop and sets count
      else last = Next(*last);
    }
    return remove(first, *last);
  }

  Node* removeAll() { return remove(*front(), *back()); }

  void push_front(Node& elem)               { insert_before(*front(), elem); }
  void push_back(Node& elem)                { insert_after (*back(),  elem); }
  void splice_back(Node& first, Node& last) { insert_after (*back(),  first, last); }

  Node* pop_front() { RASSERT(!empty(), FmtHex(this)); return remove(*front()); }
  Node* pop_back()  { RASSERT(!empty(), FmtHex(this)); return remove(*back()); }
};

template<typename T, size_t NUM=0, size_t CNT=1, typename LT=SingleLink<T,CNT>>
struct IntrusiveStack : public Stack<T,LT::template Next<NUM>> {};

template<typename T, size_t NUM=0, size_t CNT=1, typename LT=SingleLink<T,CNT>>
struct IntrusiveQueue : public Queue<T,LT::template Next<NUM>> {};

template<typename T, size_t NUM=0, size_t CNT=1, typename LT=DoubleLink<T,CNT>>
struct IntrusiveRing : public Ring<T,LT::template Next<NUM>,LT::template Prev<NUM>> {};

// NOTE WELL: The intrusive design leads to downcasting from link to element.
//            This only works, if LT is the first class that T inherits from.
template<typename T, size_t NUM=0, size_t CNT=1, typename LT=DoubleLink<T,CNT>>
struct IntrusiveList : public List<T,LT,LT::template Next<NUM>,LT::template Prev<NUM>> {};

#endif /* _Container_h_ */
