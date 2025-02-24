#pragma once
#include "entry.hpp"

namespace Beehive {
namespace cache {
class DereferenceScope {
protected:
    const DereferenceScope *parent;

    void unmark() const {
        pin();
        unpin();
    }
    virtual void pin() const {}
    virtual void unpin() const {}
    void enter();
    void exit();

    void recursive_pin() const {
        pin();
        if (parent) {
            parent->recursive_pin();
        }
    }

    void recursive_unpin() const {
        unpin();
        if (parent) {
            parent->recursive_unpin();
        }
    }

public:
    explicit DereferenceScope(const DereferenceScope *parent)
        : parent(parent) {}

    DereferenceScope(const DereferenceScope &) = delete;

    DereferenceScope &operator=(const DereferenceScope &) = delete;

    DereferenceScope(DereferenceScope &&) = delete;
    DereferenceScope &operator=(DereferenceScope &&) = delete;
    void recursive_unmark() const {
        unmark();
        if (parent) {
            parent->recursive_unmark();
        }
    }

    void begin_eviction() {
        recursive_pin();
        exit();
    }
    void end_eviction() {
        enter();
        recursive_unpin();
    }

    DereferenceScope *as_scope() { return this; }
};

class RootDereferenceScope : public DereferenceScope {
public:
    RootDereferenceScope() : DereferenceScope(nullptr) { enter(); }
    ~RootDereferenceScope() { exit(); }
};

struct OnMissScope : public DereferenceScope {
    FarObjectEntry *entry;
    OnMissScope(FarObjectEntry *entry, const DereferenceScope *parent)
        : entry(entry), DereferenceScope(parent) {}
    void pin() const override { entry->pin(); }
    void unpin() const override { entry->unpin(); }
};

}  // namespace cache
};  // namespace Beehive