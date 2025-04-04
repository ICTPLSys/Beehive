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
#if defined(__cforall)

#define _GNU_SOURCE
#include <clock.hfa>
#include <math.hfa>
#include <stdio.h>
#include <stdlib.hfa>
extern "C" {
#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <string.h>
#include <unistd.h> // getopt
}
#define nullptr 0p

#else

#include <chrono>
#include <iostream>
#include <string>
#include <cassert>
#include <cmath>
#include <csignal>
#include <cstring>
#include <unistd.h> // getopt
using namespace std;

#endif

#if defined(__FreeBSD__)
#include <sys/cpuset.h>
#include <pthread_np.h>
typedef cpuset_t cpu_set_t;
#endif

#ifdef VARIANT
#include "runtime/Platform.h"
#include "syscall_macro.h"
#include VARIANT
#else
#include "include/libfibre.h"
#endif /* VARIANT */

// configuration default settings
static unsigned int duration = 10;
static unsigned int fibreCount = 4;
static unsigned int lockCount = 1;
#if HASTIMEDLOCK
static unsigned int timeout = 1000;
#endif
static unsigned int threadCount = 2;
static unsigned int work_unlocked = 10000;
static unsigned int work_locked = 10000;

static bool affinityFlag = false;
static bool calibrationFlag = false;
static bool randomizedFlag = false;
static bool serialFlag = false;
static bool yieldFlag = false;

static bool yieldExperiment = false;
static char lockType = 'B';

// worker descriptor
struct Worker {
  shim_thread_t* runner;
  volatile unsigned long long counter;
  volatile unsigned long long failed;
};

// lock descriptor
struct Lock {
  shim_mutex_t mtx;
  volatile unsigned long long counter;
};

// arrays of worker/lock descriptors
static Worker* workers = nullptr;
static Lock* locks = nullptr;

static shim_barrier_t* cbar = nullptr;
static shim_barrier_t* sbar = nullptr;

// manage experiment duration
static unsigned int ticks = 0;
static volatile bool running = false;

// alarm invoked every second
static void alarmHandler(int) {
  ticks += 1;
  char buf[64];
  if (ticks >= duration) {
    sprintf(buf, "\r");
    running = false;
  } else {
    sprintf(buf, "\r%u", ticks);
  }
  SYSCALLIO(write(STDERR_FILENO, buf, strlen(buf)));
  fsync(STDERR_FILENO);
}

// help message
static void usage(const char* prog) {
  fprintf(stdout, "usage:\n");
  fprintf(stdout, " %s -d <duration (secs)> -f <total fibres> -l <locks> -t <system threads> -u <unlocked work> -w <locked work> -L <lock type (B,S,T,Y)> -a -c -s -y -Y", prog);
#if HASTIMEDLOCK
  fprintf(stdout, " -o <timeout in nsec>\n");
#else
  fprintf(stdout, "\n");
#endif
  fprintf(stdout, "defaults:\n");
	fprintf(stdout, " -d 10 -f 4 -l 1 -t 2 -u 10000 -w 10000 -L B");
#if HASTIMEDLOCK
  fprintf(stdout, " -o 1000\n");
#else
  fprintf(stdout, "\n");
#endif
  fprintf(stdout, "flags:\n");
  fprintf(stdout, " -a  set pthread-to-core affinity\n");
  fprintf(stdout, " -c  run calibration\n");
  fprintf(stdout, " -s  acquire locks in serial order (instead of random)\n");
  fprintf(stdout, " -y  yield after each locked iteration\n");
  fprintf(stdout, " -Y  brute force yield experiment (no locks, no work)\n");
}

// command-line option processing
static bool opts(int argc, char** argv) {
  for (;;) {
    int option = getopt(argc, argv, "acd:f:l:o:rst:u:w:yL:Yh?");
    if ( option < 0 ) break;
    switch(option) {
    case 'a': affinityFlag = true; break;
    case 'c': calibrationFlag = true; break;
    case 'd': duration = atoi(optarg); break;
    case 'f': fibreCount = atoi(optarg); break;
    case 'l': lockCount = atoi(optarg); break;
#if HASTIMEDLOCK
    case 'o': timeout = atoi(optarg); break;
#endif
    case 'r': randomizedFlag = true; break;
    case 's': serialFlag = true; break;
    case 't': threadCount = atoi(optarg); break;
    case 'u': work_unlocked = atoi(optarg); break;
    case 'w': work_locked = atoi(optarg); break;
    case 'y': yieldFlag = true; break;
    case 'L': lockType = optarg[0]; break;
    case 'Y': yieldExperiment = true; break;
    case 'h':
    case '?':
      usage(argv[0]);
      return false;
    default:
      fprintf(stderr, "unknown option - %c\n", (char)option);
      usage(argv[0]);
      return false;
    }
  }
  if (argc != optind) {
    fprintf(stderr, "unknown argument - %s\n", argv[optind]);
    usage(argv[0]);
    return false;
  }
  if (duration == 0 || fibreCount == 0 || lockCount == 0 || threadCount == 0) {
    fprintf(stderr, "none of -d, -f, -l, -t can be zero\n");
    usage(argv[0]);
    return false;
  }
  if (lockType >= 'a') lockType -= 32;
  switch (lockType) {
#if HASTRYLOCK
    case 'Y':
    case 'S':
#endif
#if HASTIMEDLOCK
    case 'T':
#endif
    case 'B': break;
    default: fprintf(stderr, "lock type %c not supported\n", lockType);
    return false;
  }
#if defined MORDOR_MAIN
  if (!yieldFlag) {
    cout << "Mordor always runs with -y flag set" << endl;
    yieldFlag = true;
  }
#endif
#if defined MORDOR_MAIN || defined BOOST_VERSION || defined QTHREAD_VERSION
  if (affinityFlag) {
    cerr << "boost, mordor, and qthreads do not support affinity at this time" << endl;
    return false;
  }
#endif
#if defined ARACHNE_H_
  unsigned int maxFibreCount = Arachne::minNumCores * 55;
  if (fibreCount > maxFibreCount) fibreCount = maxFibreCount;
#endif
  return true;
}

static const int workBufferSize = 16;

// Marsaglia shift-XOR PRNG with thread-local state
static inline unsigned int pseudoRandom() {
  static __thread unsigned long long R = 0;
	if (R == 0) {
    static const unsigned long long Mix64K = 0xdaba0b6eb09322e3ull;
    R = (uintptr_t)&R;
	  R = (R ^ (R >> 32)) * Mix64K;
    R = (R ^ (R >> 32)) * Mix64K;
    R =  R ^ (R >> 32);
  }
	unsigned long long v = R;
	unsigned long long n = v;
	n ^= n << 6;
	n ^= n >> 21;
	n ^= n << 7;
	R = n;
	return v;
}

static inline void dowork(volatile int* buffer, unsigned int steps) {
  int value = 0;
  if (randomizedFlag) steps = pseudoRandom() % steps;
  for (unsigned int i = 0; i < steps; i += 1) {
    // a little more work than just a single memory access helps with stability
    value += (buffer[i % workBufferSize] * 17) / 23 + 55;
  }
  buffer[0] += value;
}

#if defined(__cforall)

typedef Time mytime_t;
static inline mytime_t now() { return getTime(); }
int64_t diff_to_ns( mytime_t a, mytime_t b ) {
  return (a - b)`ns;
}

#else

typedef std::chrono::high_resolution_clock::time_point mytime_t;
static inline mytime_t now() { return std::chrono::high_resolution_clock::now(); };
int64_t diff_to_ns( mytime_t a, mytime_t b ) {
  std::chrono::nanoseconds d = a -b;
  return d.count();
}

#endif

static uint64_t timerOverhead = 0;

static void calibrateTimer() {
  mytime_t start = now();
  mytime_t tmp;
  for (unsigned int i = 0; i < (1 << 24) - 1; i += 1) {
    tmp = now();
  }
  (void)tmp;
  mytime_t end = now();
  timerOverhead = diff_to_ns(end, start) / (1 << 24);
}

static int compare(const void * lhs, const void * rhs) {
  return ((unsigned long)lhs) < ((unsigned long)rhs);
}

static unsigned int calibrateInterval(unsigned int period) {
  // set up work buffer
  int buffer[workBufferSize];
  for (int i = 0; i < workBufferSize; i += 1) buffer[i] = random() % 1024;

  unsigned int low = 1;
  unsigned int high = 2;
  unsigned int runs = (1<<28) / period;
  printf("%uns - upper bound:", period);
  for (;;) {
    printf(" %u", high);
    fflush(stdout);
    mytime_t start = now();
    for (unsigned int i = 0; i < runs; i++) dowork(buffer, high);
    mytime_t end = now();
    if ((diff_to_ns(end, start) - timerOverhead) / runs > period) break;
    high = high * 2;
  }
  printf("\nbinary search:");
  for (;;) {
    printf(" [%u:%u]", low, high);
    fflush(stdout);
    unsigned int next = (low + high) / 2;
    if (next == low) break;
    static const int SampleCount = 3;
    unsigned long samples[SampleCount];
    for (int s = 0; s < SampleCount; s += 1) {
      mytime_t start = now();
      for (unsigned int i = 0; i < runs; i++) dowork(buffer, next);
      mytime_t end = now();
      samples[s] = ( diff_to_ns(end, start) - timerOverhead) / runs;
    }
    qsort( samples, SampleCount, sizeof(unsigned long), compare );
    if (samples[SampleCount/2] > period) high = next;
    else low = next;
  }
  printf("\n");
  assert(low + 1 == high);
  return high;
}

static void yielder(void* arg) {
  // signal creation
  shim_barrier_wait(cbar);
  // wait for start signal
  shim_barrier_wait(sbar);
  unsigned int num = (uintptr_t)arg;

  unsigned long long count = 0;
  while (running) {
    shim_yield();
    count += 1;
  }
  workers[num].counter = count;
}

// worker routine
static void worker(void* arg) {
  // set up work buffer
  int buffer[workBufferSize];
  for (int i = 0; i < workBufferSize; i += 1) buffer[i] = random() % 1024;
  // initialize
  unsigned int num = (uintptr_t)arg;
  unsigned int lck = random() % lockCount;
  // signal creation
  shim_barrier_wait(cbar);
  // wait for start signal
  shim_barrier_wait(sbar);
  // run loop
  while (running) {
    // unlocked work
    dowork(buffer, work_unlocked);
    // locked work and counters
    switch (lockType) {
      // regular blocking lock
      case 'B': shim_mutex_lock(&locks[lck].mtx); break;
#if HASTRYLOCK
      // plain spin lock
      case 'S': while (!shim_mutex_trylock(&locks[lck].mtx)) Pause(); break;
      // yield-based busy-locking (as in qthreads, boost)
      case 'Y': while (!shim_mutex_trylock(&locks[lck].mtx)) shim_yield(); break;
#endif
#if HASTIMEDLOCK
      case 'T':
        if (!shim_mutex_timedlock(&locks[lck].mtx, randomizedFlag ? pseudoRandom() % timeout : timeout)) {
          workers[num].failed += 1;
          goto lock_failed;
        } else break;
#endif
      default: fprintf(stderr, "internal error: lock type\n"); abort();
    }
    locks[lck].counter += 1;
    dowork(buffer, work_locked);
    workers[num].counter += 2;
    locks[lck].counter += 1;
    shim_mutex_unlock(&locks[lck].mtx);
#if HASTIMEDLOCK
lock_failed:
#endif
    if (yieldFlag) shim_yield();
    // pick next lock, serial or random
    if (serialFlag) lck += 1;
    else lck = random();
    lck %= lockCount;
  }
}

#if defined __U_CPLUSPLUS__
_PeriodicTask AlarmTask {
  void main() { alarmHandler(0); if (!running) return; }
public:
  AlarmTask(uDuration period) : uPeriodicBaseTask(period) {};
};
#endif

// main routine
#if defined ARACHNE_H_
void AppMain(int argc, char* argv[]) {
#elif defined MORDOR_MAIN
MORDOR_MAIN(int argc, char *argv[]) {
#else
int main(int argc, char* argv[]) {
#endif
  // parse command-line arguments
  if (!opts(argc, argv)) exit(1);

  // print configuration
  printf("threads: %u fibres: %u", threadCount, fibreCount);
  if (!yieldExperiment) {
    printf(" locks: %u", lockCount);
    switch (lockType) {
      case 'B': printf(" blocking"); break;
      case 'S': printf(" spin"); break;
      case 'Y': printf(" yield"); break;
#if HASTIMEDLOCK
      case 'T': printf(" timeout: %u", timeout);
                if (randomizedFlag) printf(" randomized");
                break;
#endif
    }
    if (affinityFlag) printf(" affinity");
    if (serialFlag) printf(" serial");
    if (yieldFlag) printf(" yield");
  }
  printf("\n");
  printf("duration: %u", duration);
  if (!yieldExperiment) {
    printf(" locked work: %u", work_locked);
    printf(" unlocked work: %u", work_unlocked);
  }
  printf("\n");

  // set up random number generator
  srandom(time(nullptr));

  // run timer calibration, if requested
  if (calibrationFlag) {
    calibrateTimer();
    printf("time overhead: %lu\n", timerOverhead);
    unsigned int l = calibrateInterval(work_locked);
    printf("WORK: -w %u\n", l);
    unsigned int u = calibrateInterval(work_unlocked);
    printf("UNLOCKED work: -u %u\n", u);
    printf("\n");
    printf("WARNING: these numbers are not necessarily very accurate."
           " Double-check the actual runtime with 'perf'\n");
    printf("\n");
    exit(0);
  }

  // create system processors (pthreads)
#if defined MORDOR_MAIN
  poolScheduler = new WorkerPool(threadCount);
#elif defined BOOST_VERSION
  boost_init(threadCount);
#elif defined __LIBFIBRE__
  FibreInit();
  CurrCluster().addWorkers(threadCount - 1);
#elif defined __U_CPLUSPLUS__
  uProcessor* proc = new uProcessor[threadCount - 1];
#elif defined QTHREAD_VERSION
  setenv("QTHREAD_STACK_SIZE", "65536", 1);
  qthread_init(threadCount);
#elif defined _FIBER_FIBER_H_
  fiber_manager_init(threadCount);
#endif

#if 0 // test deadlock behaviour
  shim_barrier_t* deadlock = shim_barrier_create(2);
  shim_barrier_wait(deadlock);
#endif

#if defined __LIBFIBRE__ || __U_CPLUSPLUS__
  if (affinityFlag) {
#if defined __LIBFIBRE__
    pthread_t* tids = (pthread_t*)calloc(sizeof(pthread_t), threadCount);
    size_t tcnt = CurrCluster().getWorkerSysIDs(tids, threadCount);
    assert(tcnt == threadCount);
#endif
    cpu_set_t onecpu, allcpus;
    CPU_ZERO(&onecpu);
    CPU_ZERO(&allcpus);
#if defined __LIBFIBRE__
    SYSCALL(pthread_getaffinity_np(pthread_self(), sizeof(allcpus), &allcpus));
#else
    uThisProcessor().getAffinity(allcpus);
#endif
    int cpu = 0;
    for (unsigned int i = 0; i < threadCount; i += 1) {
      while (!CPU_ISSET(cpu, &allcpus)) cpu = (cpu + 1) % CPU_SETSIZE;
      CPU_SET(cpu, &onecpu);
#if defined __LIBFIBRE__
      SYSCALL(pthread_setaffinity_np(tids[i], sizeof(onecpu), &onecpu));
#else
      if (i == threadCount - 1) uThisProcessor().setAffinity(onecpu);
      else proc[i].setAffinity(onecpu);
#endif
      CPU_CLR(cpu, &onecpu);
      cpu = (cpu + 1) % CPU_SETSIZE;
    }
#if defined __LIBFIBRE__
    free(tids);
#endif
  }
#endif

  // create barriers
  cbar = shim_barrier_create(fibreCount + 1);
  sbar = shim_barrier_create(fibreCount + 1);

  // create locks
  locks = (Lock*)calloc(sizeof(Lock), lockCount);
  for (unsigned int i = 0; i < lockCount; i += 1) {
    shim_mutex_init(&locks[i].mtx);
    locks[i].counter = 0;
  }

  // create threads
  workers = (Worker*)calloc(sizeof(Worker), fibreCount);
  for (unsigned int i = 0; i < fibreCount; i += 1) {
    workers[i].runner = shim_thread_create(yieldExperiment ? yielder : worker, (void*)((uintptr_t)i));
    workers[i].counter = 0;
    workers[i].failed = 0;
  }

#if defined PTHREADS
  if (affinityFlag) {
    cpu_set_t allcpus;
    CPU_ZERO(&allcpus);
    cpu_set_t onecpu;
    CPU_ZERO(&onecpu);
    SYSCALL(pthread_getaffinity_np(pthread_self(), sizeof(allcpus), &allcpus));
    int cpu = 0;
    for (unsigned int i = 0; i < fibreCount; i += 1) {
      while (!CPU_ISSET(cpu, &allcpus)) cpu = (cpu + 1) % CPU_SETSIZE;
      CPU_SET(cpu, &onecpu);
      SYSCALL(pthread_setaffinity_np(*workers[i].runner, sizeof(onecpu), &onecpu));
      CPU_CLR(cpu, &onecpu);
      cpu = (cpu + 1) % CPU_SETSIZE;
    }
  }
#endif

  // wait for thread/fibre creation
  shim_barrier_wait(cbar);

  // set up alarm
#if !defined __U_CPLUSPLUS__
  struct sigaction sa;
  sa.sa_handler = alarmHandler;
  sa.sa_flags = SA_RESTART;
  sigemptyset(&sa.sa_mask);
  SYSCALL(sigaction(SIGALRM, &sa, 0));
  timer_t timer;
  SYSCALL(timer_create(CLOCK_REALTIME, nullptr, &timer));
  itimerspec tval = { {1,0}, {1,0} };
#endif

  // start experiment
  running = true;
#if defined __U_CPLUSPLUS__
  ticks -= 1;
  AlarmTask at(uDuration(1,0));
#else
  SYSCALL(timer_settime(timer, 0, &tval, nullptr));
#endif

  // signal start
  mytime_t startTime = now();
  shim_barrier_wait(sbar);

  // join threads
  for (unsigned int i = 0; i < fibreCount; i += 1) shim_thread_destroy(workers[i].runner);
  mytime_t endTime = now();

#if defined MORDOR_MAIN
  poolScheduler->stop();
#endif

  // collect and print work results
  unsigned long long wsum = 0;
  double wsum2 = 0;
  unsigned long long fsum = 0;
  double fsum2 = 0;
  for (unsigned int i = 0; i < fibreCount; i += 1) {
    wsum += workers[i].counter;
    wsum2 += pow(workers[i].counter, 2);
    fsum += workers[i].failed;
    fsum2 += pow(workers[i].failed, 2);
  }
  // collect and print lock results
  unsigned long long lsum = 0;
  double lsum2 = 0;
  for (unsigned int i = 0; i < lockCount; i += 1) {
    lsum += locks[i].counter;
    lsum2 += pow(locks[i].counter, 2);
  }

  // check correctness
  int exitcode = (!yieldExperiment && wsum != lsum);

  // correct for double-counting during loop - make output comparable to earlier versions
  wsum /= 2; wsum2 /= 4;
  lsum /= 2; lsum2 /= 4;

  // print all results
  unsigned long long wavg = wsum/fibreCount;
  unsigned long long wstd = (unsigned long long)sqrt(wsum2 / fibreCount - pow(wavg, 2));
  printf("loops/fibre - total: %llu rate: %llu average: %llu stddev: %llu\n", wsum, wsum/duration, wavg, wstd);
  unsigned long long lavg = lsum/lockCount;
  unsigned long long lstd = (unsigned long long)sqrt(lsum2 / lockCount - pow(lavg, 2));
  if (!yieldExperiment) {
    printf("loops/lock  - total: %llu rate: %llu average: %llu stddev: %llu\n", lsum, lsum/duration, lavg, lstd );
  }
#if HASTIMEDLOCK
  unsigned long long favg = fsum/fibreCount;
  unsigned long long fstd = (unsigned long long)sqrt(fsum2 / fibreCount - pow(favg, 2));
  if (lockType == 'T') {
    printf("fails/fibre - total: %llu rate: %llu average: %llu stddev: %llu\n", fsum, fsum/duration, favg, fstd);
  }
#endif

  // print timing information
  if (yieldExperiment) {
    printf("time spent (nanoseconds): %ld\n" , diff_to_ns(endTime, startTime));
    printf("time per yield: %lld\n", diff_to_ns(endTime, startTime) / (wsum / threadCount));
  }

  // exit hard for performance experiments
  if (exitcode) printf("CHECKSUM ERROR: total work %llu != %llu total lock\n", wsum, lsum);
  exit(exitcode);

  // clean up
  free(workers);
  free(locks);

  // destroy fibre processors
#if defined MORDOR_MAIN
  delete poolScheduler;
#elif defined BOOST_VERSION
  boost_finalize(threadCount);
#elif defined  __U_CPLUSPLUS__
  delete [] proc;
#elif defined QTHREAD_VERSION
  qthread_finalize();
#endif

  shim_barrier_destroy(sbar);
  shim_barrier_destroy(cbar);
}

#if defined ARACHNE_H_
int main(int argc, char** argv){
  Arachne::init(&argc, (const char**)argv);
  AppMain(argc, argv);
  Arachne::shutDown();
  Arachne::waitForTermination();
}
#endif
