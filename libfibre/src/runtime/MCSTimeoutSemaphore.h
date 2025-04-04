/******************************************************************************
    Copyright (C) Matthew May 2021

    Based on pseudocode by Michael L. Scott and Bill Scherer (2001):
    https://www.cs.rochester.edu/research/synchronization/pseudocode/timeout.html#mcs-try
    https://www.cs.rochester.edu/u/scott/papers/2001_PPoPP_Timeout.pdf

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
#ifndef _MCSTimeoutSemaphore_h_
#define _MCSTimeoutSemaphore_h_ 1

#include "runtime/BlockingSync.h"
#include "runtime/ContainerLink.h"
#include "runtime/Fred.h"
#include "runtime/SpinLocks.h"
#include "runtime-glue/RuntimeTimer.h"

template <typename T>
static inline bool _MyCAS(T volatile* ptr, T expected, T desired,
                          int success_memorder = __ATOMIC_SEQ_CST,
                          // TODO: is this the right memorder?
                          int failure_memorder = __ATOMIC_SEQ_CST) {
  return _CAS(ptr, expected, desired, success_memorder, failure_memorder);
}

// for first SpinCnt iterations, spin with exponential backoff
// then for later iterations, yield
template <int SpinStart, int SpinEnd, int SpinCnt, typename CallableCond, typename CallableUpdate>
static inline void yieldingSpinOnCondition(Fred& cf, CallableCond cond, CallableUpdate update) {
  int spin = SpinStart;
  int spins = 0;
  for (;;) {
    update();
    if (!cond()) break;
    if (spins < SpinCnt) {
      for (int i = 0; i < spin; i += 1) Pause();
      if (spin < SpinEnd) spin += spin;
      spins += 1;
    } else {
      cf.yield();
    }
  }
}

template <int SpinStart, int SpinEnd, typename CallableCond, typename CallableUpdate>
static inline void spinOnCondition(CallableCond cond, CallableUpdate update) {
  int spin = SpinStart;
  for (;;) {
    update();
    if (!cond()) break;
    for (int i = 0; i < spin; i += 1) Pause();
    if (spin < SpinEnd) spin += spin;
  }
}

template <int SpinStart, int SpinEnd, int SpinCnt, typename Lock>
class MCSTimeoutQueue {
  /*  We assume that all valid node pointers are word-aligned addresses
    (so the two low-order bits are zero) and are larger than 0x10.  We
    use low-order bits 01 to indicate restoration of a prev pointer that
    had temporarily been changed.  We also use it to indicate that a next
    pointer has been restored to nullptr, but the process that restored it
    has not yet fixed the global tail pointer.  We use low-order bits of
    10 to indicate that a node is at the head (If it is granted to a
    process that had been planning to leave, the process needs to know
    who its [final] predecessor was, so it can synch up and allow that
    predecessor to return from release).  Those two bits are mutually
    exclusive, though we don't take advantage of the fact.  We use bogus
    pointer values with low-order 0 bits to indicate that a process or
    its neighbor intends to leave, or has recently done so. */
  #define leaving_self ((Node*)0x4)
  #define leaving_other ((Node*)0x8)
  #define gone ((Node*)0xc)

  #define restored_tag ((unsigned long)0x1)
  #define transient_tag restored_tag
  #define at_head_tag ((unsigned long)0x2)

  using PredecessorLock = BinaryLock<>;

  struct Node : public DoubleLink<Node> {
    Fred& fred;
    const bool hasTimeout;
    PredecessorLock predLock;  // used to ensure predecessor does not leave from
                               // head while successor has a reference to it
    Node(Fred& cf, bool timeout = false) : fred(cf), hasTimeout(timeout) {}
  };
  static inline Node* volatile& Next(Node& n) { return Node::VNext(n); }
  static inline Node* volatile& Prev(Node& n) { return Node::VPrev(n); }

  // ensure Node* (address of a Node) ends with bits 00 by making Node at
  // least 4 byte aligned
  static_assert(alignof(Node) >= 4, "Node type must be at least 4 byte aligned");

  Node* volatile head = nullptr;
  Lock headLock;
  Node* volatile tail = nullptr;

 public:
  bool empty() const { return tail == nullptr; }

  // no timeout
  void pushAndWaitUntilPopped(Fred& cf) {
    Node n(cf);
    Node* pred;
    if (swapWithTail(n, pred)) {
      head = &n;
      Suspender::suspend(cf);
      RASSERT0(head != &n);
      return;
    }
    // else, not empty
    linkTailToPred(n, pred);

    // wait until popped
    for (;;) {
      ptr_t winner = Suspender::suspend(cf);
      // At this point, could be awake due to:
      //  1. Our predecessor left so we need to relink
      //  2. Popped from queue

      if (winner == nullptr) {  // were we popped?
        return;
      }

      // else, we have a new predecessor
      // reset resume race before relinking
      Suspender::prepareRace(cf);

      Node* temp = Prev(n);
      RASSERT0(temp != pred && is_real(temp));

      // relink single new predecessor only so correct runState is maintained
      pred = temp;
      temp = __atomic_exchange_n(&Next(*pred), &n, __ATOMIC_SEQ_CST);
      if (temp == leaving_self) {
        /* Wait for precedessor to notice I've set its
            next pointer and move on to step 2
            (or, if I'm slow, step 5). */
        myYieldingSpinOnCondition(
            cf, [&temp, pred] { return temp == pred; },
            [&temp, &n] { temp = Prev(n); });
      }
    }
  }

  // returns true if popped, false if timeout
  bool pushAndWaitUntilPopped(Fred& cf, const Time& absTimeout, TimerQueue& tq = Runtime::Timer::CurrTimerQueue()) {
    Node n(cf, true);
    TimerQueue::Node timeoutNode = {cf, false};
    Node* pred;
    if (swapWithTail(n, pred)) {
      Suspender::prepareRace(cf);
      head = &n;
      TimerQueue::Handle handle = tq.enqueue(timeoutNode, absTimeout);
      ptr_t winner = Suspender::suspend(cf);
      if (winner == &head) {
        // we were popped and resumed
        RASSERT0(head != &n);
        tq.erase(handle, timeoutNode);
        return true;
      }
      // timeout has occured
      handleTimeoutAtHead(n);
      return false;
    }

    // else, not empty
    linkTailToPred(n, pred);

    // start timer
    TimerQueue::Handle handle = tq.enqueue(timeoutNode, absTimeout);
    Node* temp;
    for (;;) {
      ptr_t winner = Suspender::suspend(cf);
      // At this point, could be awake due to:
      //  1. Timeout
      //  2. Popped
      //  3. Our predecessor left so we need to relink
      // A combination of (1, 3) is also possible

      if (winner == &head) {  // were we popped?
        break;
      }

      // must setup resume race BEFORE checking for timeout. This
      // ensures we don't miss a timeout signal:
      // - If the timer fires before the check, we will see the timer fired
      // when we do the check
      // - If the timer fires after the check, then it will win this resume
      // race and early resume us
      Suspender::prepareRace(cf);

      // there are two checks here since:
      // - if timer won the resume race, it must have fired
      // - timer may have lost the resume race to a temporary relink wakeup,
      //   but it may still have expired, in which case must not suspend
      if (winner != &tail || tq.didExpireAfterLosingRace(timeoutNode)) {
        // first, cancel the resume race we reset above
        winner = Suspender::cancelRunningResumeRace(cf);
        if (winner != nullptr) {
          // pop or relink won second resume race and has or is about to resume
          // us. Block until we are actually resumed, but since this will likely
          // happen very soon, spin to hopefully avoid a full suspension
          Suspender::suspend<true, 1, 1024>(cf);
          if (winner == &head) {
            return true;
          }
        }
        goto timeout;  // 2-level break
      }

      // We are getting a new predecessor
      // Wait until we see this change, since resume happens before signal
      mySpinOnCondition([&temp, pred] { return temp == pred; },
                        [&temp, &n] { temp = Prev(n); });
      if (temp == leaving_other) {
        // Predecessor is leaving.
        // wait for identity of new predecessor.
        mySpinOnCondition([&temp] { return temp == leaving_other; },
                          [&temp, &n] { temp = Prev(n); });
      }
      // relink single new predecessor only so correct runState is maintained
      pred = temp;
      temp = __atomic_exchange_n(&Next(*pred), &n, __ATOMIC_SEQ_CST);
      if (temp == leaving_self) {
        /* Wait for precedessor to notice I've set its
            next pointer and move on to step 2
            (or, if I'm slow, step 5). */
        myYieldingSpinOnCondition(
            cf, [&temp, pred] { return temp == pred; },
            [&temp, &n] { temp = Prev(n); });
        // if we have another new predecessor at this point,
        // we will have been resumed and see the change next iteration.
        // we purposely DO NOT update pred here before suspending,
        // since otherwise after early resumption we will spin forever on
        // while(temp == pred)
      }
    }
    // we were popped and resumed
    RASSERT0(head != &n);
    tq.erase(handle, timeoutNode);
    return true;

  timeout:  // Timeout has expired; try to leave.

    /* Step 1: Atomically identify successor (if any)
        and indicate intent to leave. */
    Node* succ;
    while (1) {
      mySpinOnCondition(
          [&n, &succ] { return !_MyCAS(&Next(n), succ, (Node*)leaving_self); },
          [&succ, &n] {
            mySpinOnCondition([&succ] { return is_transient(succ); },
                              [&succ, &n] { succ = Next(n); });
          });
      // don't replace a transient value
      if (succ == gone) {
        succ = nullptr;
      }
      if (succ == nullptr) {
        /* No visible successor; we'll end up skipping steps 2 and
            doing an alternative step 6. */
        break;
      }
      if (succ == leaving_other) {
        // Wait for new successor to complete step 6.
        myYieldingSpinOnCondition(
            cf, [&temp] { return temp == leaving_self; },
            [&temp, &n] { temp = Next(n); });
        continue;
      }
      break;
    }

    /* Step 2: Tell successor I'm leaving.
        This takes precedence over successor's desire to leave. */
    if (succ != nullptr) {
      temp = __atomic_exchange_n(&Prev(*succ), leaving_other, __ATOMIC_SEQ_CST);
      if (temp == leaving_self) {
        /* Successor is also trying to leave, and beat me to the
            punch.  Must wait until it tries to modify my next
            pointer, at which point it will know I'm leaving, and
            will wait for me to finish. */
        myYieldingSpinOnCondition(
            cf, [&temp, succ] { return temp != succ; },
            [&temp, &n] { temp = Next(n); });
        /* I'm waiting here for my successor to fail step 4.
            Once it realizes I got here first, it will change my
            next pointer to point back to its own qnode. */
      }
    }

    /* Step 3: Atomically identify predecessor
        and indicate intent to leave. */
    while (1) {
      n.predLock.acquire();
      temp = __atomic_exchange_n(&Prev(n), leaving_self, __ATOMIC_SEQ_CST);
      if (is_at_head(temp)) {
        n.predLock.release();
        goto serendipity;
      }
      if (temp == leaving_other) {
        n.predLock.release();
        // Predecessor is also trying to leave; it takes precedence.
        myYieldingSpinOnCondition(
            cf,
            [&temp] { return temp == leaving_self || temp == leaving_other; },
            [&temp, &n] { temp = Prev(n); });
        if (is_at_head(temp)) {
          /* I must have been asleep for a while.  Predecessor won
              the race, then experienced serendipity itself, restored
              my prev pointer, finished its critical section, and
              released. */
          goto serendipity;
        }
        relink(temp, pred, n);
        continue;
      }
      if (temp != pred) {
        relink(temp, pred, n);
        /* I'm about the change Next(*pred) to leaving_other or
            transient(nil), so why set it to I first?  Because other-
            wise there is a race.  I need to avoid the possibility
            that my (new) predecessor will swap leaving_self into its
            next field, see leaving_other (left over from its old
            successor), and assume I won the race, after which I come
            along, swap leaving_other in, get leaving_self back, and
            assume my predecessor won the race. */
      }

      // Step 4: Tell predecessor I'm leaving.
      Node* predNext;
      if (succ == nullptr) {
        predNext = __atomic_exchange_n(&Next(*pred), transient(nullptr),
                                       __ATOMIC_SEQ_CST);
      } else {
        predNext =
            __atomic_exchange_n(&Next(*pred), leaving_other, __ATOMIC_SEQ_CST);
      }
      n.predLock.release();
      if (is_at_head(predNext)) {
        // Predecessor is passing me the lock; wait for it to do so.
        mySpinOnCondition([&temp] { return temp == leaving_self; },
                          [&temp, &n] { temp = Prev(n); });
        goto serendipity;
      }
      if (predNext == leaving_self) {
        /* Predecessor is also trying to leave.  I got to step 3
            before it got to step 2, but it got to step 1 before
            I got to step 4.  It will wait in Step 2 for me to
            acknowledge that my step 4 has failed. */
        Next(*pred) = &n;
        // Wait for new predecessor
        // (completion of old predecessor's step 5)
        myYieldingSpinOnCondition(
            cf,
            [&temp] {
              return temp == leaving_self /* what I set */
                     ||
                     temp == leaving_other; /* what pred will set in step 2 */
            },
            [&temp, &n] { temp = Prev(n); });
        if (is_at_head(temp)) {
          goto serendipity;
        }
        relink(temp, pred, n);
        continue;
      }
      break;
    }

    if (succ == nullptr) {
      // Step 5: Try to fix global pointer.
      if (_MyCAS(&tail, &n, pred)) {
        // Step 6: Try to clear transient tag.
        Node* exp = (Node*)transient(nullptr);
        if (!__atomic_compare_exchange_n(
                &Next(*pred), &exp, (Node*)gone, false, __ATOMIC_SEQ_CST,
                __ATOMIC_SEQ_CST)) {  // TODO: right mem order?
          Node* actualPredNext = cleaned(exp);
          Next(*pred) = actualPredNext;
          /* New successor got into the timing window, and is now
              waiting for me to signal it. */
          (void)_MyCAS(&Prev(*actualPredNext), (Node*)transient(nullptr),
                       (Node*)nullptr);
        }
      } else {
        /* Somebody has gotten into line.  It will discover that
            I'm leaving and wait for me to tell it who the real
            predecessor is (see pre-timeout loop above). */
        mySpinOnCondition([&succ] { return succ == leaving_self; },
                          [&succ, &n] { succ = Next(n); });
        Next(*pred) = leaving_other;
        resumeSuccForRelink(*succ, *pred);
      }
    } else {
      // Step 5: Tell successor its new predecessor.
      resumeSuccForRelink(*succ, *pred);
    }
    // Step 6: Count on successor to introduce itself to predecessor.

    return false;

  serendipity:
    // while timing out, I became the queue head
    // temp contains an at_head value read from my prev pointer.

    if (succ == nullptr) {
      // I don't think I have a successor.  Try to undo step 1.
      Node* exp = (Node*)leaving_self;
      if (!__atomic_compare_exchange_n(
              &Next(n), &exp, succ, false, __ATOMIC_SEQ_CST,
              __ATOMIC_SEQ_CST)) {  // TODO: right memorder?
        /* I have a successor after all.  It will be waiting for me
            to tell it who its real predecessor is.  Since it's me
            after all, I have to tag the value so successor will
            notice change. */
        // Undo step 2:
        // exp is my real successor now
        Prev(*exp) = restored(&n);
      }
    } else {
      /* I have a successor, which may or may not have been trying
          to leave.  In either case, Next(n) is now correct. */
      // Undo step 1:
      Next(n) = succ;
      // Undo step 2:
      Prev(*succ) = restored(&n);
    }

    // wait until predecessor no longer has a reference to this node,
    // by waiting until it sets us as the new head
    mySpinOnCondition([&n, this] { return head != &n; });
    handleTimeoutAtHead(n);
    return false;
  }

  Fred* tryPop() {
    MCSLock::Node n;
    headLock.acquire(n);
    Node* poppedElem = head;
    const bool canPop =
        poppedElem &&
        (!poppedElem->hasTimeout || poppedElem->fred.raceResume((ptr_t)&head));
    if (!canPop) {
      headLock.release(n);
      return nullptr;
    }
    // change head to nullptr before releasing lock, so next pop doesn't
    // see outdated head
    head = nullptr;
    headLock.release(n);
    popFromHead(*poppedElem);
    return &poppedElem->fred;
  }

 private:
  template <typename CallableCond, typename CallableUpdate>
  static inline void myYieldingSpinOnCondition(Fred& cf, CallableCond cond,
                                               CallableUpdate update) {
    yieldingSpinOnCondition<SpinStart, SpinEnd, SpinCnt>(cf, cond, update);
  }

  template <typename CallableCond, typename CallableUpdate>
  static inline void mySpinOnCondition(CallableCond cond,
                                       CallableUpdate update) {
    spinOnCondition<SpinStart, SpinEnd>(cond, update);
  }

  template <typename CallableCond>
  static inline void mySpinOnCondition(CallableCond cond) {
    mySpinOnCondition(cond, [] {});
  }

  static bool is_restored(Node* p) { return (uintptr_t)p & restored_tag; }
  static bool is_transient(Node* p) { return is_restored(p); }
  static bool is_at_head(Node* p) { return (uintptr_t)p & at_head_tag; }
  static Node* restored(Node* p) { return (Node*)((uintptr_t)p | restored_tag); }
  static Node* transient(Node* p) { return restored(p); }
  static Node* at_head(Node* p) { return (Node*)((uintptr_t)p | at_head_tag); }
  static Node* cleaned(Node* p) { return (Node*)((uintptr_t)p & ~0x3); }
  static bool is_real(Node* p) { return !is_at_head(p) && (uintptr_t)p > 0x10; }

  void popFromHead(Node& n) {
    int spin = SpinStart;
    int spins = 0;
    while (true) {
      Node* succ = Next(n);
      if (succ == leaving_other || is_transient(succ)) {
        // successor is leaving, but will update me.
        if (spins < SpinCnt) {
          for (int i = 0; i < spin; i += 1) Pause();
          if (spin < SpinEnd) spin += spin;
          spins += 1;
        } else {
          Context::CurrFred()->yield();
        }
        continue;
      }
      if (succ == gone) {
        if (_MyCAS(&Next(n), gone, (Node*)nullptr)) {
          succ = nullptr;
        } else {
          continue;
        }
      }
      if (succ == nullptr) {
        // Try to fix global pointer.
        if (_MyCAS(&tail, &n, (Node*)nullptr)) {
          /* Make sure anybody who happened to sneak in and leave
              again has finished looking at my qnode. */
          mySpinOnCondition([&succ] { return is_transient(succ); },
                            [&succ, &n] { succ = Next(n); });
          return;  // I was last in line.
        }
        // else, I'm not tail anymore, someone was added
        mySpinOnCondition([&succ] { return succ == nullptr; },
                          [&succ, &n] { succ = Next(n); });
        continue;
      }
      if (_MyCAS(&Next(n), succ, at_head(nullptr))) {
        /* I have atomically identified my successor and
            indicated that I'm releasing; now tell successor it
            has the lock. */
        if (succ->hasTimeout) {
          // make sure we don't leave while successor has a reference to us
          succ->predLock.acquire();
          Prev(*succ) = at_head(&n);
          succ->predLock.release();
        }
        head = succ;
        return;
      }
      /* Else successor changed.  Continue loop.  Note that every
          iteration sees a different (real) successor, so there isn't
          really a global spin */
    }
  }

  // returns if queue was empty or not
  bool swapWithTail(Node& n, Node*& pred) {
    Next(n) = nullptr;
    pred = __atomic_exchange_n(&tail, &n, __ATOMIC_SEQ_CST);
    return pred == nullptr;
  }

  void linkTailToPred(Node& n, Node*& pred) const {
    Prev(n) = transient(nullptr);  // word on which to spin
    // Make predecessor point to me, but preserve transient tag if set.

    // once we set Next(*pred) pointer to point to us, we could be resumed at
    // any time so get ready now to make sure we don't miss our signal
    Suspender::prepareRace(n.fred);

    Node* temp;
    mySpinOnCondition(
        [pred, &temp, &n] {
          return !_MyCAS(&Next(*pred), temp,
                         (is_transient(temp) ? transient(&n) : &n));
        },
        [pred, &temp] { temp = Next(*pred); });
    /* Old value (temp) can't be at_head, because predecessor sets that
        only via CAS from a legitimate pointer value.  Might be nullptr
        (possibly transient) or leaving_self, though. */
    if (is_transient(temp)) {
      mySpinOnCondition([&n] { return Prev(n) == transient(nullptr); });
      /* Wait for process that used to occupy my slot in the queue
          to clear the transient tag on our (common) predecessor's
          next pointer.  The predecessor needs this cleared so it
          knows when it's safe to return and deallocate its qnode.
          I need to know in case I ever time out and try to leave:
          the previous setting has to clear before I can safely
          set it again. */
      /* Store pred for future reference, if predecessor has not
          modified my prev pointer already. */
      _MyCAS(&Prev(n), (Node*)nullptr, pred);
    } else if (temp == leaving_self) {
      /* I've probably got the wrong predecessor.
          Must wait to learn id of real one, *without timing out*. */
      myYieldingSpinOnCondition(
          n.fred, [&temp] { return temp == transient(nullptr); },
          [&temp, &n] { temp = Prev(n); });
      if (is_real(temp)) {
        relink<true>(temp, pred, n);
      }
    } else {
      /* Store pred for future reference, if predecessor has not
          modified my prev pointer already. */
      _MyCAS(&Prev(n), (Node*)transient(nullptr), pred);
    }
  }

  void handleTimeoutAtHead(Node& n) {
    head = nullptr;
    popFromHead(n);
    // tryPop may have a reference to this node (saw it as head), so wait
    // until its done losing raceResume before potentially deallocating the node
    // ScopedLock<Lock> sl(headLock);
    MCSLock::Node mcs;
    headLock.acquire(mcs);
    headLock.release(mcs);
  }

  void resumeSuccForRelink(Node& succ, Node& pred) const {
    if (succ.hasTimeout) {
      if (succ.fred.raceResume((ptr_t)&tail)) {
        succ.fred.resume();
      }
      Prev(succ) = &pred;
    } else {
      Prev(succ) = &pred;
      RASSERT0(succ.fred.raceResume((ptr_t)&tail));
      succ.fred.resume();
    }
  }

  /* prev pointer has changed from pred to temp, and we know temp is
      a real (though possibly restored) pointer.  If temp represents a
      new predecessor, update pred to point to it, clear restored flag, if
      any on prev pointer, and set next field of new predecessor (if not
      restored) to point to me.  In effect, relink is step 6.  */
  template <bool PreSuspend = false>
  void relink(Node*& temp, Node*& pred, Node& n) const {
    do {
      if (is_restored(temp)) {
        (void)_MyCAS(&Prev(n), temp, (Node*)cleaned(temp));
        /* remove tag bit from pointer, if it hasn't
            been changed to something else */
        temp = cleaned(temp);
      } else {
        pred = temp;
        if (PreSuspend) {
          /* Since signal happens after resume, we must have been resumed by
            now to perform a relink. If we are pre suspend, must cancel the
            early resume so we don't see this signal when we suspend later */
          Suspender::cancelEarlyResume(n.fred);
          Suspender::prepareRace(n.fred);
        }
        temp = __atomic_exchange_n(&Next(*pred), &n, __ATOMIC_SEQ_CST);
        if (temp == leaving_self) {
          /* Wait for precedessor to notice I've set its
              next pointer and move on to step 2
              (or, if I'm slow, step 5). */
          myYieldingSpinOnCondition(
              n.fred, [&temp, &pred] { return temp == pred; },
              [&temp, &n] { temp = Prev(n); });
          continue;
        }
      }
      break;
    } while (is_real(temp));
  }

  #undef leaving_self
  #undef leaving_other
  #undef gone

  #undef restored_tag
  #undef transient_tag
  #undef at_head_tag
};

template <int SpinStart, int SpinEnd, int SpinCnt, typename Lock>
class LimitedTimeoutSemaphore {
  MCSTimeoutQueue<SpinStart, SpinEnd, SpinCnt, Lock> queue;
  volatile ssize_t counter;  // > 1 means (counter - 1) V()s should be
                             // cancelled, otherwise regular semaphore value

  void handleTimeout() {
    ssize_t prev;
    // if counter == 0 before we update it, this means
    //  - we are the last node to leave the queue (its now empty)
    //  - a V occured and saw a value for counter < 0, and is trying to find a
    //    Fred to resume.
    // so if we see counter == 0, set it to special value 2 so the V
    // will stop looking and reset the counter to 1.
    spinOnCondition<SpinStart, SpinEnd>(
        [&prev, this] {
          return !_CAS(&counter, prev, prev == 0 ? 2 : prev + 1);
        },
        [&prev, this] { prev = counter; });
  }

 public:
  LimitedTimeoutSemaphore(ssize_t cnt = 0) : counter(cnt) {
    RASSERT(cnt == 1 || cnt == 0, cnt);
  }

  ~LimitedTimeoutSemaphore() { reset(); }

  void reset(ssize_t cnt = 0) {
    RASSERT0(queue.empty());
    RASSERT(cnt == 1 || cnt == 0, cnt);
    counter = cnt;
  }

  SemaphoreResult tryP() {
    ssize_t c = counter;
    return (c == 1 &&
            __atomic_compare_exchange_n(&counter, &c, 0, false,
                                        __ATOMIC_SEQ_CST, __ATOMIC_RELAXED))
               ? SemaphoreWasOpen
               : SemaphoreTimeout;
  }

  SemaphoreResult P(bool wait) { return wait ? P() : tryP(); }

  SemaphoreResult P() {
    Fred& cf = *Context::CurrFred();
    ssize_t prevCounter;
    yieldingSpinOnCondition<SpinStart, SpinEnd, SpinCnt>(
        cf,
        [&prevCounter, this] {
          // don't change counter while we are in the "Cancel V" state
          return prevCounter >= 2 ||
                 !_MyCAS(&counter, prevCounter, prevCounter - 1);
        },
        [&prevCounter, this] { prevCounter = counter; });

    if (prevCounter > 0) {
      RASSERT(prevCounter == 1, prevCounter);
      return SemaphoreWasOpen;
    }
    queue.pushAndWaitUntilPopped(cf);
    return SemaphoreSuccess;
  }

  SemaphoreResult P(const Time& timeout,
                    TimerQueue& tq = Runtime::Timer::CurrTimerQueue()) {
    Fred& cf = *Context::CurrFred();
    int spin = SpinStart;
    int spins = 0;
    ssize_t prevCounter;
    Time now;
    for (;;) {
      now = Runtime::Timer::now();
      if (timeout <= now) {
        return SemaphoreTimeout;
      }
      prevCounter = counter;
      if (prevCounter < 2 && _MyCAS(&counter, prevCounter, prevCounter - 1))
        break;
      if (spins < SpinCnt) {
        for (int i = 0; i < spin; i += 1) Pause();
        if (spin < SpinEnd) spin += spin;
        spins += 1;
      } else {
        cf.yield();
      }
    }
    if (prevCounter > 0) {
      RASSERT(prevCounter == 1, prevCounter);
      return SemaphoreWasOpen;
    }

    const bool wasPopped =
        queue.pushAndWaitUntilPopped(cf, timeout, tq);
    if (wasPopped) {
      return SemaphoreSuccess;
    }
    handleTimeout();
    return SemaphoreTimeout;
  }

  template <bool Enqueue = true, bool DirectSwitch = false>
  Fred* V() {
    for (ssize_t c = 0;;) {
      if (__atomic_compare_exchange_n(&counter, &c, c + 1, false,
                                      __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        if (c == 0) {
          return nullptr;  // queue is empty
        }
        RASSERT(c < 0, c);
        break;
      } else {
        if (c >= 1) {
          return nullptr;  // queue is empty
        }
        Pause();
      }
    }

    // Queue is not empty, find node to resume
    // there are 2 possibilities:
    //  1. We find a Fred to resume
    //  2. All the Freds timeout and counter is set to 2 by the last Fred to
    //  timeout.
    int spin = SpinStart;
    int spins = 0;
    Fred* poppedElem;
    for (;;) {
      poppedElem = queue.tryPop();
      if (poppedElem) break;

      // if we were cancelled, exit
      ssize_t c = counter;
      if (c >= 2 && _CAS(&counter, c, c - 1)) {
        return nullptr;
      }

      if (spins < SpinCnt) {
        for (int i = 0; i < spin; i += 1) Pause();
        if (spin < SpinEnd) spin += spin;
        spins += 1;
      } else {
        Context::CurrFred()->yield();
      }
    }
    if (Enqueue) poppedElem->resume<DirectSwitch>();
    return poppedElem;
  }
};

template <typename Lock>
#if TESTING_MCS_SPIN_BEFORE_YIELD
using MCSTimeoutSemaphore = LimitedTimeoutSemaphore<1, 1, 64, Lock>;
#else
using MCSTimeoutSemaphore = LimitedTimeoutSemaphore<0, 0, 0, Lock>;
#endif

#endif /* _MCSTimeoutSemaphore_h_ */
