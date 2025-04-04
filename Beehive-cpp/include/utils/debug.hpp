#pragma once

#include <cassert>
#include <cstdlib>
#include <iostream>

namespace FarLib {

namespace debug {

inline void assert_fail(const char *expr, const char *file, int line,
                        const char *function) {
    std::cerr << "Assertion failed: " << expr << " at " << file << ":" << line
              << " in function " << function << std::endl;
    abort();
}

inline void check_err(int err, const char *msg) {
    if (err != 0) {
        std::cerr << "Error (" << errno << ") on " << msg << std::endl;
        abort();
    }
}

[[noreturn]] inline void todo(const char *msg) {
    std::cerr << "TODO: " << msg << std::endl;
    abort();
}

[[noreturn]] inline void error(const char *msg) {
    std::cerr << "ERROR: " << msg << std::endl;
    abort();
}

inline void warn(const char *msg) { std::cerr << "WARN: " << msg << std::endl; }

inline void info(const char *msg) { std::cout << "INFO: " << msg << std::endl; }

}  // namespace debug

}  // namespace FarLib

#define TODO(MSG) FarLib::debug::todo(MSG)
#define ERROR(MSG) FarLib::debug::error(MSG)
#define WARN(MSG) FarLib::debug::warn(MSG)
#define INFO(MSG) FarLib::debug::info(MSG)
#define CHECK_ERR(X) FarLib::debug::check_err(X, #X)
#define DEBUG_ASSERT assert
#define ASSERT(X)         \
    (static_cast<bool>(X) \
         ? void(0)        \
         : FarLib::debug::assert_fail(#X, __FILE__, __LINE__, __func__))
