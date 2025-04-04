#include "cfibre.h"

void foo(void* ptr, size_t id) { printf("hi, i'm %zu\n", id); }
int main() {
    uthread_init(4);
    uthread_run(16, foo, nullptr);
}