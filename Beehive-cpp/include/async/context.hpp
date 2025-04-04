#pragma once
#include <concepts>
#include <type_traits>

#include "cache/cache.hpp"
#include "utils/debug.hpp"

namespace FarLib {
namespace async {

/*
    Co-design with dereference scope:
    The RootDereferenceScope should not be constructed in OoO contexts.
    The evictor pins all contexts and call methods in the parent scope.
*/
template <typename Context>
concept ScopedRunnableContext =
    std::is_move_assignable_v<Context> &&
    requires(Context context, cache::DereferenceScope &scope) {
        // return true if finished
        // return false if data miss
        { context.run(scope) } -> std::convertible_to<bool>;

        // return true if fetched
        { context.fetched() } -> std::convertible_to<bool>;

        // pin/unpin all lite accessors in context
        context.pin();
        context.unpin();
    };

class GenericContext {
public:
    int32_t *await_cnt;

    virtual void pin() const {}
    virtual void unpin() const {}
    virtual bool fetched() const { ERROR("not implemented"); }
    virtual bool run(cache::DereferenceScope &scope) {
        ERROR("not implemented");
    }
    virtual void destruct() {}
};

class GenericOrderedContext : public GenericContext {
private:
    size_t conf_id;

protected:
    void set_conflict_id(size_t id) { conf_id = id; }

public:
    size_t conflict_id() const { return conf_id; }
};

}  // namespace async
}  // namespace FarLib
