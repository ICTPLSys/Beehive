#include "async/async.hpp"

namespace Beehive {
namespace async {
#ifndef NEW_SCHEDULE
thread_local TaskBase *out_of_order_task = nullptr;
#endif
thread_local cache::FarObjectEntry *main_block_entry;
thread_local cache::fetch_ddl_t main_block_ddl;

}  // namespace async
}  // namespace Beehive
