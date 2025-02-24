#pragma once

#include <cstddef>

#include "object.hpp"

namespace Beehive {

namespace cache {

template <typename Fn>
concept TaskFn = requires(Fn &fn, far_obj_t obj, void *ptr) { fn(obj, ptr); };

class Task {
public:
    virtual void call(far_obj_t object, void *local_ptr) = 0;
};

template <TaskFn Fn>
class RefTaskImpl : public Task {
private:
    Fn &fn;

public:
    RefTaskImpl(Fn &fn) : fn(fn) {}

    virtual void call(far_obj_t object, void *local_ptr) override {
        fn(object, local_ptr);
    }
};

}  // namespace cache

}  // namespace Beehive