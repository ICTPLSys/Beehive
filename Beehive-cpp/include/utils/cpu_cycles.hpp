#pragma once

#include <x86intrin.h>

#include <chrono>
#include <cstdint>

inline uint64_t get_cycles() {
    unsigned int _;
    return __rdtscp(&_);
}

inline uint64_t get_time_ns() {
    std::chrono::nanoseconds ns =
        std::chrono::high_resolution_clock::now().time_since_epoch();
    return ns.count();
}
