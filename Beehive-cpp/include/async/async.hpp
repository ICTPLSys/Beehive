#pragma once
#include "task.hpp"

namespace Beehive {
namespace async {
// not nullptr: in OoO mode
extern thread_local TaskBase *out_of_order_task;

extern thread_local cache::FarObjectEntry *main_block_entry;
extern thread_local cache::fetch_ddl_t main_block_ddl;

#ifndef NEW_SCHEDULE
inline void __sync__() {
    while (out_of_order_task) {
        out_of_order_task->yield(SYNC);
    }
}
using async::__sync__;
#endif
}  // namespace async

}  // namespace Beehive
