#pragma once

#include <concepts>
#include <utility>

/*
    Usage:

    DEFER({ clean_up_code(); });

    run clean_up_code() when current scope exists
*/

template <std::invocable F>
class Defer {
public:
    Defer(const Defer &) = delete;
    Defer(Defer &&) = delete;
    Defer &operator=(const Defer &) = delete;
    Defer &operator=(Defer &&) = delete;
    Defer(F &&f) : cleanup(std::move(f)) {}
    ~Defer() { cleanup(); }

private:
    F cleanup;
};

#define DEFER_ACTUALLY_JOIN(x, y) x##y
#define DEFER_JOIN(x, y) DEFER_ACTUALLY_JOIN(x, y)
#ifdef __COUNTER__  // GCC Common Predefined Macros
#define DEFER_UNIQUE_VARNAME(x) DEFER_JOIN(x, __COUNTER__)
#else
#define DEFER_UNIQUE_VARNAME(x) DEFER_JOIN(x, __LINE__)
#endif
#define DEFER(lambda__)                                          \
    [[maybe_unused]] const auto &DEFER_UNIQUE_VARNAME(__defer) = \
        Defer([&]() lambda__)
