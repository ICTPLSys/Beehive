#include "fibre.h"

#include <cassert>
#include <iostream>

using namespace std;

int f1real(int i) __attribute__((__noinline__));
int f2real(int j) __attribute__((__noinline__));

int f1real(int i) {
  cout << "i: " << i << endl;
  Fibre::yield();
  int x = random() % 1024;
  int test = x;
  int tmp = 0;
  if (i < 500000) tmp = f2real(i + 1);
  assert(test == x);
  (void)test;
  x += tmp;
  return x;
}

void f1main(int* result) {
  *result = f1real(0);
}

int f2real(int j) {
  cout << "j: " << j << endl;
  if (j > 10000) Fibre::exit();
  Fibre::yield();
  int y = random() % 1024;
  int test = y;
  int tmp = 0;
  if (j < 500000) tmp = f1real(j + 1);
  assert(test == y);
  (void)test;
  y += tmp;
  return y;
}

void f2main(int* result) {
  *result =  f2real(0);
}

int main() {
  FibreInit();
  Context::CurrCluster().addWorkers(3);
  static const int fcount = 10;
  Fibre f1s[fcount];
  Fibre f2s[fcount];
  int r1, r2;
  for (int i = 0; i < fcount; i++) {
    f1s[i].run(f1main, &r1);
    f2s[i].run(f2main, &r2);
  }
  for (int i = 0; i < fcount; i++) {
    f1s[i].join();
    f2s[i].join();
  }
  cout << "result: " << r1 + r2 << endl;
  return 0;
}
