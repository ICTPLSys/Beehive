#include "utils/signal.hpp"

#include "utils/debug.hpp"

namespace FarLib {

namespace signal {

#ifdef SIGNAL_ENABLED

thread_local std::function<void(void)> on_sigusr1 = [] {};
thread_local SignalManager signal_manager;
thread_local std::atomic_bool signal_enabled = true;
thread_local std::atomic_bool signaled = false;

// TODO: support uthread
void call_on_sigusr1(int) {
    if (signal_enabled.load(std::memory_order::relaxed)) {
        if (!signaled.load(std::memory_order::relaxed)) {
            on_sigusr1();
        }
    } else {
        signaled.store(true, std::memory_order::relaxed);
    }
}

SignalManager::SignalManager() {
    struct sigaction action;
    action.sa_handler = call_on_sigusr1;
    sigemptyset(&action.sa_mask);
    action.sa_flags = SA_NODEFER;
    if (sigaction(SIGUSR1, &action, nullptr) != 0) [[unlikely]] {
        ERROR("can not register signal handler for SIGUSR1");
    }
}

SignalManager::~SignalManager() {}

#endif

}  // namespace signal

}  // namespace FarLib
