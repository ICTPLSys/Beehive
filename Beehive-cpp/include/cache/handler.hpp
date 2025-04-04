#pragma once
#include <x86intrin.h>

#include <cstdint>
// #define NPREFETCH
#define MISS_FOR_COUNT
namespace FarLib {

namespace cache {

class FarObjectEntry;

using fetch_ddl_t = uint64_t;
static_assert(sizeof(fetch_ddl_t) <= sizeof(uint64_t));

inline fetch_ddl_t create_fetch_ddl() {
    fetch_ddl_t start_time = __rdtsc();
    return start_time + 4000;
}

inline fetch_ddl_t delay_fetch_ddl(fetch_ddl_t last_ddl) {
    return last_ddl + 1000;
}

template <typename Fn>
concept DataMissHandlerFn = requires(Fn &fn, FarObjectEntry *on_demand,
                                     fetch_ddl_t ddl) { fn(on_demand, ddl); };

class DataMissHandler {
public:
    virtual void call(FarObjectEntry *on_demand, fetch_ddl_t ddl) const = 0;

    void operator()(FarObjectEntry *on_demand, fetch_ddl_t ddl) const {
        return call(on_demand, ddl);
    }
};

template <DataMissHandlerFn Fn>
class DataMissHandlerImpl : public DataMissHandler {
private:
    Fn fn;

public:
    DataMissHandlerImpl(Fn &&fn) : fn(fn) {}

    virtual void call(FarObjectEntry *on_demand,
                      fetch_ddl_t ddl) const override {
        profile::start_on_miss();
        fn(on_demand, ddl);
        profile::end_on_miss();
    }
};

}  // namespace cache

#define __DMH__ const FarLib::cache::DataMissHandler &__on_miss__

#define ON_MISS_BEGIN_X                   \
    auto &__last_on_miss__ = __on_miss__; \
    auto __on_miss__ = FarLib::cache::DataMissHandlerImpl([&](auto __entry__, auto __ddl__) {
#ifndef MISS_FOR_COUNT
#define ON_MISS_END_X                                           \
    if (FarLib::cache::check_fetch(__entry__, __ddl__)) return; \
    __last_on_miss__(__entry__, __ddl__);                       \
    });
#else
#define ON_MISS_END_X                     \
    __last_on_miss__(__entry__, __ddl__); \
    });
#endif

#ifndef NPREFETCH
#define ON_MISS_BEGIN \
    auto __on_miss__ = FarLib::cache::DataMissHandlerImpl([&](auto __entry__, auto __ddl__) {
#define ON_MISS_END \
    });

#else
/* clang-format off */
#define ON_MISS_BEGIN \
    auto __on_miss__ = FarLib::cache::DataMissHandlerImpl([&](auto __entry__, auto __ddl__) {   \
        auto f = [&] () {
#define ON_MISS_END \
    };              \
    });
/* clang-format on */
#endif
}  // namespace FarLib
