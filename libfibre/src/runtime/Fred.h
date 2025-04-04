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
#ifndef _Fred_h_
#define _Fred_h_ 1

#include "runtime/Container.h"
#include "runtime/LockFreeQueues.h"
#include "runtime/Stack.h"

class EventScope;
class BaseProcessor;
class IdleManager;
class KernelProcessor;
class Scheduler;
struct Suspender;

static const size_t FredReadyLink = 0;
#if TESTING_ENABLE_DEBUGGING
static const size_t FredDebugLink = 1;
static const size_t FredLinkCount = 2;
#else
static const size_t FredLinkCount = 1;
#endif

template <size_t NUM> using FredList =
IntrusiveList<Fred,NUM,FredLinkCount,DoubleLink<Fred,FredLinkCount>>;
template <size_t NUM> using FredQueue =
IntrusiveQueue<Fred,NUM,FredLinkCount,DoubleLink<Fred,FredLinkCount>>;
template <size_t NUM> using FredQueueNemesis =
IntrusiveQueueNemesis<Fred,NUM,FredLinkCount,DoubleLink<Fred,FredLinkCount>>;
template <size_t NUM, bool Blocking=false> using FredQueueStub =
IntrusiveQueueStub<Fred,NUM,FredLinkCount,DoubleLink<Fred,FredLinkCount>,Blocking>;

#if TESTING_STUB_QUEUE
template <size_t NUM, bool Blocking=false> using FredMPSC = FredQueueStub<NUM,Blocking>;
#else
template <size_t NUM, bool Blocking=false> using FredMPSC = FredQueueNemesis<NUM>;
#endif

#if TESTING_LOCKED_READYQUEUE
typedef FredQueue<FredReadyLink> FredReadyQueue;
#else
typedef FredMPSC<FredReadyLink,false> FredReadyQueue;
#endif

class Fred : public DoubleLink<Fred,FredLinkCount> {
public:
  enum Priority : size_t { TopPriority = 0, DefaultPriority = 1, LowPriority = 2, NumPriority = 3 };

#if TESTING_DEFAULT_AFFINITY
  static const bool DefaultAffinity = true;
#else
  static const bool DefaultAffinity = false;
#endif

private:
  vaddr          stackPointer; // holds stack pointer while stack inactive
  BaseProcessor* processor;    // next resumption on this processor
  Priority       priority;     // scheduling priority
  size_t         affinity;     // affinity to worker

  enum RunState : size_t { Parked = 0, Running = 1, ResumedEarly = 2 };
  RunState volatile runState;    // 0 = parked, 1 = running, 2 = early resume
  ptr_t    volatile resumeInfo;

  Fred(const Fred&) = delete;
  const Fred& operator=(const Fred&) = delete;

  // central fred switching routine
  enum SwitchCode { Idle = 'I', Yield = 'Y', Resume = 'R', Suspend = 'S', Terminate = 'T' };
  template<SwitchCode> inline void switchFred(Fred& nextFred);

  // these routines are called immediately after the stack switch
  static void postIdle     (Fred* prevFred);
  static void postYield    (Fred* prevFred);
  static void postResume   (Fred* prevFred);
  static void postSuspend  (Fred* prevFred);
  static void postTerminate(Fred* prevFred);

  void resumeDirect();
  void resumeInternal();

  // these routines must be called with 'this' being the current fred
  void suspendInternal();
  inline void yieldTo(Fred& nextFred);
  inline void yieldResume(Fred& nextFred);

protected:
  // constructor/destructors can only be called by derived classes
  Fred(BaseProcessor& proc); // main constructor
  Fred(Scheduler&);          // uses delegation
  ~Fred() { RASSERT(runState == Running, FmtHex(this), runState); }

  void initStackPointer(vaddr sp) {
    stackPointer = align_down(sp, stackAlignment);
  }

public:
  // direct switch to new fred
  void direct(ptr_t func, _friend<KernelProcessor>) __noreturn {
    stackDirect(stackPointer, func, nullptr, nullptr, nullptr);
  }

  // set up fred stack without immediate start
  void setup(ptr_t func, ptr_t p1 = nullptr, ptr_t p2 = nullptr, ptr_t p3 = nullptr) {
    stackPointer = stackInit(stackPointer, func, p1, p2, p3);
  }

  // set up new fred and resume for concurrent execution
  void start(ptr_t func, ptr_t p1 = nullptr, ptr_t p2 = nullptr, ptr_t p3 = nullptr) {
    setup(func, p1, p2, p3);
    resumeInternal();
  }

  // context switching - static -> apply to Context::CurrFred()
  static bool yield();
  static bool yieldGlobal();
  static void idleYieldTo(Fred& nextFred, _friend<BaseProcessor>);
  static void preempt();
  static void terminate() __noreturn;

  template<size_t SpinStart = 1, size_t SpinEnd = 0>
  ptr_t suspend(_friend<Suspender>) {
    size_t spin = SpinStart;
    for (;;) {
      RunState exp = ResumedEarly; // resumed already? then skip suspend
      if (__atomic_compare_exchange_n(&runState, &exp, Running, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) return resumeInfo;
      if (spin > SpinEnd) break;
      for (size_t i = 0; i < spin; i += 1) Pause();
      spin += spin;
    }
    suspendInternal();
    return resumeInfo;
  }

  template<bool DirectSwitch = false>
  void resume() {
    size_t prev = __atomic_fetch_add(&runState, RunState(1), __ATOMIC_SEQ_CST);
    if (prev == Parked) {               // Parked -> Running
      if (DirectSwitch) resumeDirect();
      else resumeInternal();
    } else {                            // Running -> ResumedEarly
      RASSERT(prev == Running, prev);
    }
  }

  void cancelEarlyResume(_friend<Suspender>) { runState = Running; }

  void prepareResumeRace(_friend<Suspender>) {
    RASSERT(runState == Running, runState);
    resumeInfo = nullptr;
  }

  ptr_t cancelRunningResumeRace(_friend<Suspender>) {
    return __atomic_exchange_n(&resumeInfo, (ptr_t)0x1, __ATOMIC_SEQ_CST);
  }

  bool raceResume(ptr_t ri) {
    ptr_t exp = nullptr;
    return __atomic_compare_exchange_n(&resumeInfo, &exp, ri, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
  }

  Priority getPriority() const  { return priority; }
  Fred* setPriority(Priority p) { priority = p; return this; }

  bool  getAffinity() const { return affinity; }
  Fred* setAffinity(bool a) { affinity = a; return this; }

  // check affinity and potentially update processor during work-stealing
  bool checkAffinity(BaseProcessor& newProcessor, _friend<BaseProcessor>) {
    if (affinity) return true;
    processor = &newProcessor;
    return false;
  }

  BaseProcessor& getProcessor(_friend<EventScope>) {
    RASSERT0(processor);
    return *processor;
  }

  // migration
  static BaseProcessor& migrate(BaseProcessor&);
  static BaseProcessor& migrate(Scheduler&);
};

#endif /* _Fred_h_ */
