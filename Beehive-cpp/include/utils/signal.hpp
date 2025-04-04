#pragma once

// #define SIGNAL_ENABLED

#ifdef SIGNAL_ENABLED
#include <signal.h>

#include <atomic>
#include <functional>
#endif

namespace FarLib {

namespace signal {

#ifdef SIGNAL_ENABLED

inline void signal(pthread_t pthread) { pthread_kill(pthread, SIGUSR1); }

// thread local
struct SignalManager {
    SignalManager();
    ~SignalManager();
};

extern thread_local std::function<void(void)> on_sigusr1;
extern thread_local SignalManager signal_manager;
extern thread_local std::atomic_bool signal_enabled;
extern thread_local std::atomic_bool signaled;

inline void disable_signal() {
    signal_enabled.store(false, std::memory_order::relaxed);
    std::atomic_signal_fence(std::memory_order::acq_rel);
}

inline void enable_signal() {
    std::atomic_signal_fence(std::memory_order::acq_rel);
    signal_enabled.store(true, std::memory_order::relaxed);
    if (signaled.load(std::memory_order::relaxed)) {
        on_sigusr1();
        signaled.store(false, std::memory_order::relaxed);
    }
}

#else

inline void disable_signal() {}

inline void enable_signal() {}

#endif

}  // namespace signal

}  // namespace FarLib
