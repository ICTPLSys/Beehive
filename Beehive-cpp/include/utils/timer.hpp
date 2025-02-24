#ifndef _MY_TIMER_H
#define _MY_TIMER_H

#include <stdint.h>
#include <string>
#define CPU_PER_NS 2.4
class CpuTimer {
private:
    uint64_t start, end;

    uint64_t rdtsc() {
        unsigned int lo, hi;
        __asm__ volatile ("" : : : "memory");
        __asm__ volatile ("rdtsc" : "=a" (lo), "=d" (hi));
        __asm__ volatile ("" : : : "memory");
        return ((uint64_t)hi << 32) | lo;
    }
public:

    void setTimer() {
        start = rdtsc();
    }

    void stopTimer() {
        end = rdtsc();
    }

    auto getDuration() {
        return end - start;
    }

    std::string getDurationStr() {
        return getDurationCpuTime();
    }

    std::string getDurationCpuTime() {
        auto duration = end - start;
        return std::to_string((double)duration / (CPU_PER_NS * 1000 * 1000)) +
               " ms";
    }

    void record() {
        auto duration = end - start;
        // assert(hdr != nullptr);
        // hdr_record_value_atomic(hdr, (double) duration / CPU_PER_NS);
    }
};

template <typename F>
double time_for_ms(F &&f, const char *name = nullptr) {
    auto start = std::chrono::high_resolution_clock::now();
    f();
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end - start;
    if (name) {
        std::cout << "time for " << name << ": " << duration.count() << " ms"
                  << std::endl;
    }
    return duration.count();
}

#endif