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
#include "runtime/Scheduler.h"
#include "runtime/Fred.h"
#include "runtime-glue/RuntimeFred.h"

Fred::Fred(BaseProcessor& proc)
: stackPointer(0), processor(&proc), priority(DefaultPriority), affinity(DefaultAffinity), runState(Running) {
  processor->stats->create.count();
}

Fred::Fred(Scheduler& scheduler) : Fred(scheduler.placement(_friend<Fred>())) {}

template<Fred::SwitchCode Code>
inline void Fred::switchFred(Fred& nextFred) {
  // various checks
  static_assert(Code == Idle || Code == Yield || Code == Resume || Code == Suspend || Code == Terminate, "Illegal SwitchCode");
  CHECK_PREEMPTION(0);
  RASSERT(this == Context::CurrFred() && this != &nextFred, FmtHex(this), ' ', FmtHex(Context::CurrFred()), ' ', FmtHex(&nextFred));

  // context switch
  DBG::outl(DBG::Level::Scheduling, "Fred switch <", char(Code), "> on ", FmtHex(&Context::CurrProcessor()),": ", FmtHex(this), " (to ", FmtHex(processor), ") -> ", FmtHex(&nextFred));
  RuntimePreFredSwitch(*this, nextFred, _friend<Fred>());
  switch (Code) {
    case Idle:      stackSwitch(this, postIdle,      &stackPointer, nextFred.stackPointer); break;
    case Yield:     stackSwitch(this, postYield,     &stackPointer, nextFred.stackPointer); break;
    case Resume:    stackSwitch(this, postResume,    &stackPointer, nextFred.stackPointer); break;
    case Suspend:   stackSwitch(this, postSuspend,   &stackPointer, nextFred.stackPointer); break;
    case Terminate: stackSwitch(this, postTerminate, &stackPointer, nextFred.stackPointer); break;
  }
  stackPointer = 0;                              // mark stack in use for gdb
  RuntimePostFredSwitch(*this, _friend<Fred>()); // runtime-specific functionality
}

// idle fred -> do nothing
void Fred::postIdle(Fred*) {
  CHECK_PREEMPTION(0);
}

// yield -> resume right away
void Fred::postYield(Fred* prevFred) {
  CHECK_PREEMPTION(0);
  prevFred->processor->enqueueYield(*prevFred, _friend<Fred>());
}

// yield -> resume right away
void Fred::postResume(Fred* prevFred) {
  CHECK_PREEMPTION(0);
  prevFred->resumeInternal();
}

// if resumption already triggered -> resume right away
void Fred::postSuspend(Fred* prevFred) {
  CHECK_PREEMPTION(0);
  size_t prev = __atomic_fetch_sub(&prevFred->runState, RunState(1), __ATOMIC_SEQ_CST);
  if (prev == ResumedEarly) prevFred->resumeInternal(); // ResumedEarly -> Running
  else RASSERT(prev == Running, prev);                  // Running -> Parked
}

// destroy fred
void Fred::postTerminate(Fred* prevFred) {
  CHECK_PREEMPTION(0);
  RuntimeFredDestroy(*prevFred, _friend<Fred>());
}

// a new fred starts in stubInit() and then jumps to this routine
extern "C" void invokeFred(funcvoid3_t func, ptr_t arg1, ptr_t arg2, ptr_t arg3) {
  CHECK_PREEMPTION(0);
  RuntimeEnablePreemption();
  RuntimeStartFred(func, arg1, arg2, arg3);
  RuntimeDisablePreemption();
  Fred::terminate();
}

void Fred::resumeDirect() {
  Context::CurrFred()->yieldResume(*this);
}

void Fred::resumeInternal() {
  processor->enqueueResume(*this, *processor, _friend<Fred>());
}

void Fred::suspendInternal() {
  switchFred<Suspend>(Context::CurrProcessor().scheduleFull(_friend<Fred>()));
}

inline void Fred::yieldTo(Fred& nextFred) {
  CHECK_PREEMPTION(1);           // expect preemption still enabled
  RuntimeDisablePreemption();
  switchFred<Yield>(nextFred);   // yield without updating fredCounter
  RuntimeEnablePreemption();
}

inline void Fred::yieldResume(Fred& nextFred) {
  CHECK_PREEMPTION(1);           // expect preemption still enabled
  RuntimeDisablePreemption();
  switchFred<Resume>(nextFred);  // yield and increase fredCounter
  RuntimeEnablePreemption();
}

bool Fred::yield() {
#if 0
  Fred* nextFred = Context::CurrProcessor().tryScheduleLocal(_friend<Fred>());
  if (nextFred) Context::CurrFred()->yieldTo(*nextFred);
#else
    CHECK_PREEMPTION(1);  // expect preemption still enabled
    RuntimeDisablePreemption();
    BaseProcessor& current = Context::CurrProcessor();
    Fred* nextFred = current.readyQueue.dequeue();
    if (nextFred) {
        Fred* currFred = Context::CurrFred();
        RuntimePreFredSwitch(*currFred, *nextFred, _friend<Fred>());
        stackSwitch(currFred, postYield, &currFred->stackPointer,
                    nextFred->stackPointer);
        currFred->stackPointer = 0;  // mark stack in use for gdb
        RuntimePostFredSwitch(
            *currFred, _friend<Fred>());  // runtime-specific functionality
    }
    RuntimeEnablePreemption();
#endif
    return nextFred;
}

bool Fred::yieldGlobal() {
    if (yield()) return true;
    Fred* nextFred = Context::CurrProcessor().tryScheduleSteal(_friend<Fred>());
    if (nextFred) Context::CurrFred()->yieldTo(*nextFred);
    return nextFred;
}

void Fred::idleYieldTo(Fred& nextFred, _friend<BaseProcessor>) {
  CHECK_PREEMPTION(1);          // expect preemption still enabled
  RuntimeDisablePreemption();
  Context::CurrFred()->switchFred<Idle>(nextFred);
  RuntimeEnablePreemption();
}

void Fred::preempt() {
  CHECK_PREEMPTION(0);
  Fred* currFred = Context::CurrFred();
  Fred* nextFred = Context::CurrProcessor().tryScheduleGlobal(_friend<Fred>());
  if (currFred != nextFred) currFred->switchFred<Yield>(*nextFred);
}

void Fred::terminate() {
  CHECK_PREEMPTION(0);
  Context::CurrFred()->switchFred<Terminate>(Context::CurrProcessor().scheduleFull(_friend<Fred>()));
  RABORT0();
}

BaseProcessor& Fred::migrate(BaseProcessor& proc) {
  Fred* f = Context::CurrFred();
  BaseProcessor* cproc = f->processor;
  f->processor = &proc;
  if (&cproc->getScheduler() == &proc.getScheduler() && f->yieldGlobal()) return *cproc;
  Fred& nextFred = Context::CurrProcessor().scheduleFull(_friend<Fred>());
  f->yieldResume(nextFred);
  return *cproc;
}

BaseProcessor& Fred::migrate(Scheduler& scheduler) {
  return migrate(scheduler.placement(_friend<Fred>()));
}
