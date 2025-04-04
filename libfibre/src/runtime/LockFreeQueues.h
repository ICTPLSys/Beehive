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
#ifndef _LockFreeQueues_h_
#define _LockFreeQueues_h_ 1

#include "runtime/SpinLocks.h"
#include "runtime/ContainerLink.h"

// https://doi.org/10.1145/103727.103729
// the MCS queue can be used to construct an MCS lock or the Nemesis queue
// next() might stall, if the queue contains one element and the producer of a second elements waits before setting 'prev->vnext'
template<typename Node, Node* volatile&(*Next)(Node&)>
class QueueMCS {
  Node* volatile tail;

public:
  QueueMCS() : tail(nullptr) {}
  template<bool = false>
  bool empty() const { return !tail; }

  bool tryPushEmpty(Node& last) {
    RASSERT(!Next(last), FmtHex(Next(last)));  // assume link invalidated at pop
    if (!empty()) return false;
    Node* expected = nullptr;
    return __atomic_compare_exchange_n(&tail, &expected, &last, false, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED);
  }

  bool push(Node& first, Node& last) {
    RASSERT(!Next(last), FmtHex(&last));       // assume link invalidated at pop
    Node* prev = __atomic_exchange_n(&tail, &last, __ATOMIC_SEQ_CST); // swing tail to last of new element(s)
    if (!prev) return true;
    Next(*prev) = &first;
    return false;
  }

  bool push(Node& elem) { return push(elem, elem); }

  Node* pop(Node& elem) {
    Node* expected = &elem;
    if (__atomic_compare_exchange_n(&tail, &expected, nullptr, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) return nullptr;
    while (!Next(elem)) Pause(); // producer in push()
    Node* next = Next(elem);
    Next(elem) = nullptr;                      // invalidate link
    return next;
  }
};

// https://doi.org/10.1109/CCGRID.2006.31, similar to MCS lock
// note that modifications to 'head' need to be integrated with basic MCS operations in this particular way
// pop() inherits the potential stall from MCS queue's pop() (see base class above)
template<typename Node, Node* volatile&(*Next)(Node&)>
class QueueNemesis : public QueueMCS<Node,Next> {
  Node* volatile head;

public:
  QueueNemesis() : head(nullptr) {}

  bool push(Node& first, Node& last) {
    if (!QueueMCS<Node,Next>::push(first, last)) return false;
    __atomic_store_n(&head, &first, __ATOMIC_RELEASE);
    return true;
  }

  bool push(Node& elem) { return push(elem, elem); }

  template<typename... Args>
  Node* pop(Args&... args) {
    AcquireSpinLock(args...);
    Node* elem = __atomic_load_n(&head, __ATOMIC_ACQUIRE); // return head
    if (!elem) {
      ReleaseSpinLock(args...);
      return nullptr;
    }
    Node* next = Next(*elem);
    head = next;                               // potentially invalidate head
    ReleaseSpinLock(args...);
    if (next) {
      Next(*elem) = nullptr;                   // invalidate link
    } else {
      next = QueueMCS<Node,Next>::pop(*elem);  // memory sync, potential atomic swing of tail
      if (next) head = next;                   // head update is safe, if queue not empty
    }
    return elem;
  }

  Node* pop(MCSLock& lk) {
    MCSLock::Node node;
    return pop(lk, node);
  }
};

// http://doc.cat-v.org/inferno/concurrent_gc/concurrent_gc.pdf
// https://www.cs.rice.edu/~johnmc/papers/cqueues-mellor-crummey-TR229-1987.pdf
// http://www.1024cores.net/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue
// https://github.com/samanbarghi/MPSCQ/blob/master/src/MPSCQueue.h

// NOTE WELL: Static cast from Link to Node must work! If downcasting,
//            Link must the first class that Node inherits from.
template<typename Node, typename Link, Node* volatile&(*Next)(Node&), bool Blocking>
class QueueStub {
  Link anchorLink;
  Node* stub;
  Node* head;
  Node* volatile tail;

  // peek/pop operate in chunks of elements and re-append stub after each chunk
  // after re-appending stub, tail points to stub, if no further insertions -> empty!
  bool checkStub() {
    if (head == stub) {                             // if current front chunk is empty
      if (Blocking) {                               // BLOCKING:
        Node* expected = stub;                      //   check if tail also points at stub -> empty?
        Node* xchg = (Node*)(uintptr_t(expected) | 1);    //   if yes, mark queue empty
        bool empty = __atomic_compare_exchange_n((Node**)&tail, &expected, xchg, false, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED);
        if (empty) return false;                    //   queue is empty and is marked now
        if (uintptr_t(expected) & 1) return false;  //   queue is empty and was marked before
      } else {                                      // NONBLOCKING:
        if (tail == stub) return false;             //   check if tail also points at stub -> empty?
      }
      while (!Next(*stub)) Pause();                 // producer in push()
      head = Next(*stub);                           // remove stub
      Next(*stub) = nullptr;                        // invalidate link
      push(*stub);                                  // re-append stub at end
    }
    return true;
  }

public:
  QueueStub() : stub(static_cast<Node*>(&anchorLink)) {
    head = tail = stub;
    Next(*stub) = nullptr;                             // invalidate link
    if (Blocking) tail = (Node*)(uintptr_t(tail) | 1); // mark queue empty
  }
  template<bool = false>
  bool empty() const { return (head == stub && tail == stub); }

  bool push(Node& first, Node& last) {
    RASSERT(!Next(last), FmtHex(&last));               // assume link invalidated at pop
    Node* prev = __atomic_exchange_n((Node**)&tail, &last, __ATOMIC_SEQ_CST); // swing tail to last of new element(s)
    bool empty = false;
    if (Blocking) {                                    // BLOCKING:
      empty = uintptr_t(prev) & 1;                     //   check empty marking
      prev = (Node*)(uintptr_t(prev) & ~uintptr_t(1)); //   clear marking
    }
    Next(*prev) = &first;                              // append segments to previous tail
    return empty;
  }

  bool push(Node& elem) { return push(elem, elem); }

  template<typename... Args>
  Node* pop(Args&... args) {
    AcquireSpinLock(args...);
    if (!checkStub()) {
      ReleaseSpinLock(args...);
      return nullptr;
    }
    Node* retval = head;                               // head will be returned
    while (!Next(*head)) Pause();                      // producer in push()
    head = Next(*head);                                // remove head
    ReleaseSpinLock(args...);
    Next(*retval) = nullptr;                           // invalidate link
    return retval;
  }

  Node* pop(MCSLock& lk) {
    MCSLock::Node node;
    return pop(lk, node);
  }
};

template<typename T, size_t NUM=0, size_t CNT=1, typename LT=SingleLink<T,CNT>>
using IntrusiveQueueNemesis = QueueNemesis<T,LT::template VNext<NUM>>;

// NOTE WELL: The intrusive design leads to downcasting in the QueueStub class. This only works, if LT is the first class that T inherits from.
template<typename T, size_t NUM=0, size_t CNT=1, typename LT=SingleLink<T,CNT>, bool Blocking=false>
using IntrusiveQueueStub = QueueStub<T,LT,LT::template VNext<NUM>,Blocking>;

#endif /* _LockFreeQueues_h_ */
