#pragma once
#include <atomic>

#include "cache/cache.hpp"
#include "utils/uthreads.hpp"

namespace Beehive {

class Spinlock {
public:
    Spinlock() = default;

    Spinlock(const Spinlock& obj) = delete;
    Spinlock& operator=(const Spinlock& obj) = delete;
    Spinlock(const Spinlock&& obj) = delete;
    Spinlock& operator=(const Spinlock&& obj) = delete;

    void lock() {
        while (m_flag.test_and_set(std::memory_order_acquire)) {
            uthread::yield();
        }
    }

    void unlock() {
        // printf("%p unlock %p\n", fibre_self(), this);
        m_flag.clear(std::memory_order_release);
    }

    void lock(DereferenceScope& scope) {
        // printf("%p scope lock %p\n", fibre_self(), this);
        while (m_flag.test_and_set(std::memory_order_acquire)) {
            cache::check_memory_low(scope);
            uthread::yield();
        }
        // printf("%p scope lock end %p\n", fibre_self(), this);
    }

private:
    std::atomic_flag m_flag = ATOMIC_FLAG_INIT;
};

}  // namespace Beehive
