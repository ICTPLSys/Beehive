#include "fibre.h"

#include <iostream>
#include <sys/wait.h> // waitpid

using namespace std;

static volatile size_t counter = 0;

static FredMutex testmtx1;
static FredMutex testmtx2;

static fibre_once_t once_test = PTHREAD_ONCE_INIT;
static fibre_key_t key_test;

static void key_finish(void* value) {
  cout << "finish " << (char)(uintptr_t)value << endl;
}

static void once_init() {
  cout << "once init" << endl;
  SYSCALL(fibre_key_create(&key_test, key_finish));
}

static void* f1main(void*) {
  fibre_once(&once_test, once_init);
  SYSCALL(fibre_setspecific(key_test, (void*)'A'));
  cout << "F1 1" << endl;
  Fibre::yield();
  cout << "F1 2" << endl;
  cout << "F1 3" << endl;
  for (size_t i = 0; i < 100000; i += 1) {
    ScopedLock<FredMutex> sl(testmtx2);
    testmtx1.acquire();
    counter += 1;
    testmtx1.release();
  }
  cout << "F1 specific " << (char)(uintptr_t)fibre_getspecific(key_test) << endl;
  return (void*)0xdeadbeef;
}

static void f2main() {
  fibre_once(&once_test, once_init);
  SYSCALL(fibre_setspecific(key_test, (void*)'B'));
  cout << "F2 1" << endl;
  Fibre::yield();
  cout << "F2 2" << endl;
  cout << "F2 3" << endl;
  for (size_t i = 0; i < 100000; i += 1) {
    ScopedLock<FredMutex> sl(testmtx2);
    testmtx1.acquire();
    counter += 1;
    testmtx1.release();
  }
  cout << "F2 specific " << (char)(uintptr_t)fibre_getspecific(key_test) << endl;
}

static FredSemaphore tmx(0);

static void f3main() {
  fibre_once(&once_test, once_init);
  SYSCALL(fibre_setspecific(key_test, (void*)'C'));
  Time ct;
  SYSCALL(clock_gettime(CLOCK_REALTIME, &ct));
  cout << ct.tv_sec << '.' << ct.tv_nsec << endl;
  Time to = ct + Time(1,0);
  if (!tmx.P(to)) {
    cout << "timeout" << endl;
  }
  SYSCALL(clock_gettime(CLOCK_REALTIME, &ct));
  cout << ct.tv_sec << '.' << ct.tv_nsec << endl;
  cout << "F3 specific " << (char)(uintptr_t)fibre_getspecific(key_test) << endl;
}

int main(int argc, char** argv) {
  FibreInit();
  pid_t p = SYSCALLIO(FibreFork());
  cout << "Hello world " << getpid() << endl;
  if (p) {
    SYSCALLIO(waitpid(p, nullptr, 0));
    cout << "Child " << p << " finished" << endl;
  }
  Time ct;
  SYSCALL(clock_gettime(CLOCK_REALTIME, &ct));
  cout << ct.tv_sec << '.' << ct.tv_nsec << endl;
  if (argc > 1) Fibre::usleep(Time::USEC * atoi(argv[1]));
  else Fibre::usleep(1000);
  SYSCALL(clock_gettime(CLOCK_REALTIME, &ct));
  cout << ct.tv_sec << '.' << ct.tv_nsec << endl;
  Context::CurrCluster().addWorkers(1);
  Fibre* f1 = (new Fibre)->setName("Fibre 1")->run(f1main, (void*)nullptr);
  Fibre* f2 = (new Fibre)->setName("Fibre 2")->run(f2main);
  Fibre* f3 = (new Fibre)->setName("Fibre 3")->run(f3main);

  cout << "M 1" << endl;
  Fibre::yield();
  cout << "M 2" << endl;
  void* jofel = f1->join();
  cout << FmtHex(jofel) << endl;
  cout << f1->getName() << endl;
  delete f1;
  cout << "f1 gone" << endl;
  cout << f2->getName() << endl;
  delete f2;
  cout << "f2 gone" << endl;
  cout << f3->getName() << endl;
  delete f3;
  cout << "f3 gone" << endl;
  cout << counter << endl;

  SYSCALL(fibre_key_delete(key_test));
  return 0;
}
