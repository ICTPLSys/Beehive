#include "libfibre/fibre.h"
typedef void (*task_func)(void *, size_t);
extern "C" {
void uthread_init(size_t workers);
void uthread_run(size_t thread_count, task_func tf, void *closure);
}
