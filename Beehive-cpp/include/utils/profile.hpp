#include "async/stream_runner.hpp"
#include "stats.hpp"
namespace Beehive {

namespace profile {

template <typename Fn>
inline void beehive_profile(Fn &&fn) {
    async::StreamRunnerProfiler::reset();
    profile::reset_all();
    profile::start_work();
    profile::start_record_bandwidth();
    // profile::thread_start_work();
    fn();
    // profile::thread_end_work();
    profile::end_record_bandwidth();
    profile::end_work();
    profile::print_profile_data();
    std::cout << "stream runner: total cycles = "
              << async::StreamRunnerProfiler::get_app_cycles() << std::endl;
    std::cout << "stream runner: schedule cycles = "
              << async::StreamRunnerProfiler::get_sched_cycles() << std::endl;
}
}  // namespace profile
}  // namespace Beehive
