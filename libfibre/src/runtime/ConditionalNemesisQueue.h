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
#ifndef _ConditionalNemesisQueue_h_
#define _ConditionalNemesisQueue_h_ 1

template<typename Node, Node* volatile&(*Next)(Node&), Node* &(*Prev)(Node&)>
class NemesisDoubleQueue {
  Node* volatile head;
  Node* volatile tail;

  // concurremt remove -> lock
  template<typename... Args>
  void removeHead(Node& elem, Args&... args) {
    Node* next = Next(elem);
    head = next;                              // nullptr -> pause later remover
    if (!next) {
      Node* expected = &elem;
      if (!__atomic_compare_exchange_n(&tail, &expected, nullptr, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        while (!Next(elem)) Pause();          // earlier thread in push() or remove()
        head = Next(elem);                    // continue later remover
      }
    }
    ReleaseSpinLock(args...);
    Next(elem) = nullptr;                     // invalidate link
  }

  template<typename... Args>
  void removeInternal(Node& elem, Args&... args) {
    Node* next = Next(elem);
    Node* prev = Prev(elem);
    Next(*prev) = next;
    if (next) {
      Prev(*next) = prev;
    } else {
      Node* expected = &elem;
      if (!__atomic_compare_exchange_n(&tail, &expected, prev, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        while (!Next(elem)) Pause();          // earlier thread in push() or remove()
        next = Next(elem);
        Prev(*next) = prev;
        Next(*prev) = next;
      }
    }
    ReleaseSpinLock(args...);
    Next(elem) = nullptr;                     // invalidate link
  }

  static constexpr bool True(ptr_t) { return true; }

public:
  NemesisDoubleQueue() : head(nullptr), tail(nullptr) {}
  bool empty() const { return !tail; }

  // append at end -> lock-free
  template<typename... Args>
  bool push(Node& first, Node& last, Args&...) {
    RASSERT(!Next(last), FmtHex(&last));      // assume link invalidated at pop
    Node* prev = __atomic_exchange_n(&tail, &last, __ATOMIC_SEQ_CST); // swing tail to last of new element(s)
    if (!prev) {
      head = &first;
      return true;
    }
    Prev(first) = prev;
    Next(*prev) = &first;                     // continue later remover
    return false;
  }

  template<typename... Args>
  bool push(Node& elem, Args&... args) { return push(elem, elem, args...); }

  bool push(Node& first, Node& last, MCSLock& lk) {
    MCSLock::Node node;
    return push(first, last, lk, node);
  }

  bool push(Node& elem, MCSLock& lk) { return push(elem, elem, lk); }

  template<typename Func, typename... Args>
  Node* pop(Func&& Check, ptr_t p, Args&... args) {
    AcquireSpinLock(args...);
    Node* elem = head;                         // return head
    if (elem && Check(elem, p)) {
      removeHead(*elem, args...);
      return elem;
    } else {
      ReleaseSpinLock(args...);
      return nullptr;
    }
  }

  template<typename Func>
  Node* pop(Func&& Check, ptr_t p, MCSLock& lk) {
    MCSLock::Node node;
    return pop(Check, p, lk, node);
  }

  template<typename... Args>
  Node* pop(Args&... args) {
    return pop(True, nullptr, args...);
  }

  template<typename Func>
  Node* pop(MCSLock& lk) {
    MCSLock::Node node;
    return pop(lk, node);
  }

  template<typename... Args>
  void remove(Node& elem, Args&... args) {
    AcquireSpinLock(args...);
    if (&elem == head) {
      removeHead(elem, args...);
    } else {
      removeInternal(elem, args...);
    }
  }

  void remove(Node& elem, MCSLock& lk) {
    MCSLock::Node node;
    remove(elem, lk, node);
  }
};

template<typename T, size_t NUM=0, size_t CNT=1, typename LT=SingleLink<T,CNT>>
using IntrusiveNemesisDoubleQueue = NemesisDoubleQueue<T,LT::template VNext<NUM>,LT::template Prev<NUM>>;

template <size_t NUM> using FredNemesisDoubleQueue =
IntrusiveNemesisDoubleQueue<Fred,NUM,FredLinkCount,DoubleLink<Fred,FredLinkCount>>;

template<typename Lock>
class ConditionalNemesisQueue {
  Lock lock;
  FredNemesisDoubleQueue<FredReadyLink> queue;

  static inline bool raceResume(Fred* f, ptr_t p) { return f->raceResume(p); }

public:
  ~ConditionalNemesisQueue() { reset(); }
  void reset() { RASSERT0(queue.empty()); }

  template<typename Func>
  bool block(Fred* cf, Func&& func, bool wait = true) {
    RASSERT0(wait);
    Suspender::prepareRace(*cf);
    RuntimeDisablePreemption();
    queue.push(*cf);
    if (func()) {
      Suspender::suspend<false>(*cf);
    } else {
      RuntimeEnablePreemption();
      if (cf->raceResume(&queue)) queue.remove(*cf, lock);
      else Suspender::suspend<false>(*cf);
    }
    return true;
  }

  template<typename Func>
  bool block(Fred* cf, Func&& func, const Time& timeout) {
    if (timeout <= Runtime::Timer::now()) return false;
    Suspender::prepareRace(*cf);
    RuntimeDisablePreemption();
    queue.push(*cf);
    if (func()) {
      ptr_t winner = Runtime::Timer::CurrTimerQueue().blockTimeout(*cf, timeout);
      if (winner == &queue) return true;   // blocking completed;
      queue.remove(*cf, lock);
      return false;                        // blocking cancelled
    } else {
      RuntimeEnablePreemption();
      if (cf->raceResume(&queue)) queue.remove(*cf, lock);
      else Suspender::suspend<false>(*cf);
      return true;
    }
  }

  template<bool Enqueue = true>
  Fred* unblock() {
    Fred* next = queue.pop(raceResume, &queue, lock);
    if (Enqueue && next) next->resume();
    return next;
  }
};

#endif /* _ConditionalNemesisQueue_h_ */
