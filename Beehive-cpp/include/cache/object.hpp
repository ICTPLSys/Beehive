#pragma once

#include <cstdint>

typedef uint64_t obj_id_t;
namespace Beehive {

namespace cache {

class FarObjectEntry;

struct FarObject {
    uint64_t size : 16;
    obj_id_t obj_id : 48;

    bool operator==(const FarObject &other) const {
        return size == other.size && obj_id == other.obj_id;
    }

    static FarObject null() {
        return FarObject{
            .size = 0,
            .obj_id = 0,
        };
    }

    bool is_null() const { return size == 0 && obj_id == 0; }

    uint64_t to_uint64_t() const {
        return *reinterpret_cast<const uint64_t *>(this);
    }

    static FarObject from_uint64_t(uint64_t v) {
        return *reinterpret_cast<const FarObject *>(&v);
    }

    FarObjectEntry *get_entry_ptr() const {
        return reinterpret_cast<FarObjectEntry *>(obj_id);
    }
};

typedef FarObject far_obj_t;

}  // namespace cache

}  // namespace Beehive

namespace std {
template <>
struct hash<Beehive::cache::far_obj_t> {
    std::size_t operator()(const Beehive::cache::far_obj_t &far_obj) const {
        return std::hash<uint64_t>{}(far_obj.obj_id);
    }
};
}  // namespace std
