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
#ifndef _Fibre_h_
#define _Fibre_h_ 1

/** @file */

#include "runtime/BaseProcessor.h"
#include "runtime/BlockingSync.h"
#include "runtime-glue/RuntimeContext.h"

#include <vector>
#include <string>
#include <sys/mman.h> // mmap, munmap, mprotect

extern size_t _lfPagesize; // Bootstrap.cc

#ifdef SPLIT_STACK
extern "C" void __splitstack_block_signals(int* next, int* prev);
extern "C" void __splitstack_block_signals_context(void *context[10], int* next, int* prev);
extern "C" void __splitstack_getcontext(void *context[10]);
extern "C" void __splitstack_setcontext(void *context[10]);
extern "C" void* __splitstack_makecontext(size_t, void *context[10], size_t *);
extern "C" void __splitstack_releasecontext(void *context[10]);
#endif

#if TESTING_ENABLE_DEBUGGING
extern WorkerLock*              _lfFredDebugLock;
extern FredList<FredDebugLink>* _lfFredDebugList;
extern size_t                   _lfFredDebugLink;
#endif

class Cluster;

class FibreSpecific {
public:
  static const size_t FIBRE_KEYS_MAX = bitsize<mword>();
  typedef void (*Destructor)(void*);
private:
  static FastMutex mutex;
  static Bitmap<FIBRE_KEYS_MAX> bitmap;
  static std::vector<Destructor> destructors;
  std::vector<void*> values;
protected:
  FibreSpecific(size_t e = 0) : values(e) {}
  void clearSpecific() {
    size_t start = bitmap.find();
    if (start >= FIBRE_KEYS_MAX) return;
    size_t idx = start;
    do {
      if (destructors[idx] && idx < values.size() && values[idx]) destructors[idx](values[idx]);
      idx = bitmap.findnext(idx);
    } while (idx > start);
  }
public:
  void setspecific(size_t idx, const void *value) {
    RASSERT(idx < FIBRE_KEYS_MAX, idx);
    RASSERT(bitmap.test(idx), idx);
    if (idx >= values.size()) {
      if (values.size() == 0) values.resize(1);
      while (idx >= values.size()) values.resize(values.size() * 2);
    }
    values[idx] = (void*)value;
  }
  void* getspecific(size_t idx) {
    RASSERT(idx < FIBRE_KEYS_MAX, idx);
    RASSERT(bitmap.test(idx), idx);
    return idx < values.size() ? values[idx] : nullptr;
  }
  static size_t key_create(Destructor d = nullptr) {
    ScopedLock<FastMutex> sl(mutex);
    size_t idx = bitmap.find(false);
    RASSERT(idx < FIBRE_KEYS_MAX, idx);
    bitmap.set(idx);
    if (idx >= destructors.size()) {
      if (destructors.size() == 0) destructors.resize(1);
      while (idx >= destructors.size()) destructors.resize(destructors.size() * 2);
    }
    destructors[idx] = d;
    return idx;
  }
  static void key_delete(size_t idx) {
    ScopedLock<FastMutex> sl(mutex);
    RASSERT(idx < FIBRE_KEYS_MAX, idx);
    RASSERT(bitmap.test(idx), idx);
    bitmap.clr(idx);
    destructors[idx] = nullptr;
  }
};

struct FibreLocalVariables {
  int scope_state = 0;
};

/** A Fibre object represents an independent execution context backed by a stack. */
class Fibre : public Fred, public FibreSpecific, public FibreLocalVariables {
public:
#ifdef SPLIT_STACK
  static const size_t DefaultStackSize  = 4096;
  static const size_t DefaultStackGuard = 0;
#else
  static const size_t DefaultStackSize  = 65536;
  static const size_t DefaultStackGuard = 4096;
#endif

private:
  FloatingPointFlags fp;       // FP context
  size_t stackSize;            // stack size (including guard)
#ifdef SPLIT_STACK
  void* splitStackContext[10]; // memory for split-stack context
#else
  vaddr stackBottom;           // bottom of allocated memory for stack (including guard)
#endif
  SyncPoint<WorkerLock> done;  // synchronization (join) at destructor
  ptr_t result;                // result transferred to join
#if TESTING_ENABLE_DEBUGGING
  std::string name;
#endif

  size_t stackAlloc(size_t size, size_t guard) {
#ifdef SPLIT_STACK
    (void)guard;
    vaddr stackBottom = (vaddr)__splitstack_makecontext(size, splitStackContext, &size);
    int off = 0; // do not block signals (blocking signals is slow!)
    __splitstack_block_signals_context(splitStackContext, &off, nullptr);
#else
    // check that requested size/guard is a multiple of page size
    RASSERT(aligned(size, _lfPagesize), size);
    RASSERT(aligned(guard, _lfPagesize), size);
    // reserve/map size + protection
    size += guard;
    // add PROT_EXEC here to make stack executable (needed for nested C functions)
    ptr_t ptr = mmap(0, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
    RASSERT0(ptr != MAP_FAILED);
    // set up protection page
    if (guard) SYSCALL(mprotect(ptr, guard, PROT_NONE));
    stackBottom = vaddr(ptr);
#endif
    Fred::initStackPointer(stackBottom + size);
    return size;
  }

  void stackFree() {
#ifdef SPLIT_STACK
    if (stackSize) __splitstack_releasecontext(splitStackContext);
#else
    if (stackSize) SYSCALL(munmap(ptr_t(stackBottom), stackSize));
#endif
  }

  void initDebug() {
#if TESTING_ENABLE_DEBUGGING
    ScopedLock<WorkerLock> sl(*_lfFredDebugLock);
    _lfFredDebugList->push_back(*this);
#endif
  }

  void clearDebug() {
#if TESTING_ENABLE_DEBUGGING
    ScopedLock<WorkerLock> sl(*_lfFredDebugLock);
    _lfFredDebugList->remove(*this);
#endif
  }

  void start(ptr_t func, ptr_t p1, ptr_t p2, ptr_t p3) { // hide base class start()
    Fred::start(func, p1, p2, p3);
  }

  Fibre* runInternal(ptr_t func, ptr_t p1, ptr_t p2, Fibre* This) {
    start(func, p1, p2, This);
    return this;
  }

public:
  struct ExitException {};

  /** Constructor. */
  Fibre(Scheduler& sched = Context::CurrProcessor().getScheduler(), size_t size = DefaultStackSize, size_t guard = DefaultStackGuard)
  : Fred(sched), stackSize(stackAlloc(size, guard)) { initDebug(); }

  // system constructor for idle/main loop (bootstrap) on existing pthread stack (size = 0)
  // system constructor with setting affinity to processor (size != 0)
  Fibre(BaseProcessor &p, _friend<Cluster>, size_t size = DefaultStackSize, size_t guard = DefaultStackGuard)
  : Fred(p), stackSize(size ? stackAlloc(size, guard) : 0) { initDebug(); }

  //  explicit final notification for idle loop or main loop (bootstrap) on pthread stack
  void endDirect(_friend<Cluster>) { done.post(); }

  /** Destructor with synchronization. */
  ~Fibre() { join(); }
  /** Explicit join. Called automatically by destructor. */
  ptr_t join() { done.wait(); return result; }
  /** Detach fibre (no waiting for join synchronization). */
  void detach() { done.detach(); }
  /** Exit fibre (with join, if not detached). */
  static void exit(ptr_t p = nullptr) __noreturn;

  // callback after Fred's main routine has finished
  void finalize(ptr_t e) {
    result = e;
    clearSpecific();
  }

  // callback from Fred via Runtime after final context switch
  void destroy(_friend<Fred>) {
    clearDebug();
    stackFree();
    done.post();
  }

  void setup(ptr_t func, ptr_t p1, ptr_t p2, ptr_t p3, _friend<Cluster>) { // hide base class setup()
    Fred::setup(func, p1, p2, p3);
  }

#if TESTING_ENABLE_DEBUGGING
  Fibre* setName(const std::string& n) {
    name = n.size() >= 2 && n[1] == ':' ? n : "u:" + n;
    return this;
  }
  const std::string& getName() const { return name; }
#else
  Fibre* setName(const std::string&) { return this; }
  const std::string getName() const { return std::string(); }
#endif

  /** Start fibre. */
  Fibre* run(void (*func)()) {
    return runInternal((ptr_t)func, nullptr, nullptr, this);
  }
  /** Start fibre. */
  template<typename T1>
  Fibre* run(void (*func)(T1*), T1* p1) {
    return runInternal((ptr_t)func, (ptr_t)p1, nullptr, this);
  }
  /** Start fibre with pthread-type run function. */
  template<typename T1>
  Fibre* run(void* (*func)(T1*), T1* p1) {
    return runInternal((ptr_t)func, (ptr_t)p1, nullptr, this);
  }

  /** Sleep. */
  static void nanosleep(const Time& t) {
    sleepFred(t);
  }

  /** Sleep. */
  static void usleep(uint64_t usecs) {
    sleepFred(Time::fromUS(usecs));
  }

  /** Sleep. */
  static void sleep(uint64_t secs) {
    sleepFred(Time(secs, 0));
  }

  // context switching interface
  void deactivate(Fibre& next, _friend<Fred>) {
    fp.save();
#ifdef SPLIT_STACK
    __splitstack_getcontext(splitStackContext);
    __splitstack_setcontext(next.splitStackContext);
#else
    (void)next;
#endif
  }
  void activate(_friend<Fred>) {
    fp.restore();
  }
};

/** @brief Obtain pointer to current Fibre object. */
inline Fibre* CurrFibre() {
  return (Fibre*)Context::CurrFred();
}

#endif /* _Fibre_h_ */
