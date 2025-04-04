#pragma once
#include "data_structure/far_vector.hpp"
#include "utils/stats.hpp"
#ifndef FORCE_INLINE
#define FORCE_INLINE inline __attribute__((always_inline))
// #define FORCE_INLINE
#endif
namespace FarLib {
template <VecElementType T, size_t GroupSize>
template <bool Mut>
class FarVector<T, GroupSize>::LiteIterator : public GenericIterator<Mut> {
private:
    friend class FarVector<T, GroupSize>;
    using InIteratorType = GenericIterator<Mut>::InIteratorType;
    using Self = LiteIterator<Mut>;
    static constexpr size_t PREFETCH_COUNT = 32UL;

public:
    using difference_type = GenericIterator<Mut>::difference_type;
    using value_type = GenericIterator<Mut>::value_type;
    using pointer = GenericIterator<Mut>::pointer;
    using reference = GenericIterator<Mut>::reference;
    using iterator_category = std::random_access_iterator_tag;
    uint64_t prefetch_cnt = 0;
    uint64_t prefetch_success_cnt = 0;
    uint64_t miss_cnt = 0;

private:
    LiteAccessor<Group, Mut> lite_accessor;
    /* update LiteIterator's accessor
        when iterator is calculated.
        update metadata in GenericIterator if necessary */
    template <bool Fast = false, bool Prefetch = false,
              bool neg_check_forward = false, bool pos_check_forward = false>
    FORCE_INLINE void accessor_sync(DereferenceScope &scope,
                                    const int64_t step) {
        static_assert(!(neg_check_forward & pos_check_forward));
        if (!neg_check_forward && this->idx < 0) [[unlikely]] {
            this->template metadata_sync<Fast, true, false>();
            if (!this->valid()) [[unlikely]] {
                lite_accessor = LiteAccessor<Group, Mut>();
                return;
            }
            if constexpr (Prefetch) {
                ON_MISS_BEGIN
                    __define_oms__(scope);
                    for (InIteratorType *obj_p = this->cursor - 1;
                         obj_p >= this->fetch_begin; obj_p--) {
                        Cache::get_default()->prefetch(obj_p->obj(), oms);
                        if (Cache::get_default()->check_fetch(__entry__,
                                                              __ddl__)) {
                            return;
                        }
                    }
                ON_MISS_END
                lite_accessor = LiteAccessor<Group, Mut>(*(this->cursor),
                                                         __on_miss__, scope);
            } else {
                // ON_MISS_BEGIN
                //     profile::tlpd.data_miss_count++;
                // ON_MISS_END
                lite_accessor =
                    LiteAccessor<Group, Mut>(*(this->cursor), scope);
            }
        } else if (!pos_check_forward && this->idx >= GroupSize) [[unlikely]] {
            this->template metadata_sync<Fast, false, true>();
            if (!this->valid()) [[unlikely]] {
                lite_accessor = LiteAccessor<Group, Mut>();
                return;
            }
            if constexpr (Prefetch) {
                ON_MISS_BEGIN
                    __define_oms__(scope);
                    for (InIteratorType *obj_p = this->cursor + 1;
                         obj_p < this->fetch_end; obj_p++) {
                        Cache::get_default()->prefetch(obj_p->obj(), oms);
                        if (Cache::get_default()->check_fetch(__entry__,
                                                              __ddl__)) {
                            return;
                        }
                    }
                ON_MISS_END
                lite_accessor = LiteAccessor<Group, Mut>(*(this->cursor),
                                                         __on_miss__, scope);
            } else {
                // ON_MISS_BEGIN
                //     profile::tlpd.data_miss_count++;
                // ON_MISS_END
                lite_accessor =
                    LiteAccessor<Group, Mut>(*(this->cursor), scope);
            }
        }
    }

    template <bool Fast = false, bool Prefetch = false,
              bool neg_check_forward = false, bool pos_check_forward = false>
    FORCE_INLINE void accessor_sync(DereferenceScope &scope, __DMH__) {
        static_assert(!(neg_check_forward & pos_check_forward));
        if (!neg_check_forward && this->idx < 0) [[unlikely]] {
            this->template metadata_sync<Fast, true, false>();
            if (!this->valid()) {
                lite_accessor = LiteAccessor<Group, Mut>();
                return;
            }
            if constexpr (Prefetch) {
                ON_MISS_BEGIN_X
                    miss_cnt++;

                    // profile::tlpd.data_miss_count++;
                    const InIteratorType *lim =
                        std::max(this->fetch_begin,
                                 (const InIteratorType *)(this->cursor) -
                                     PREFETCH_COUNT);
                    cache::OnMissScope oms(__entry__, &scope);
                    for (InIteratorType *obj_p = this->cursor - 1; obj_p >= lim;
                         obj_p--) {
                        auto res =
                            Cache::get_default()->prefetch(obj_p->obj(), oms);
                        if (Cache::get_default()->check_fetch(__entry__,
                                                              __ddl__)) {
                            return;
                        }
                    }
                ON_MISS_END_X
                lite_accessor = LiteAccessor<Group, Mut>(*(this->cursor),
                                                         __on_miss__, scope);
            } else {
                // ON_MISS_BEGIN
                //     profile::tlpd.data_miss_count++;
                // ON_MISS_END
                lite_accessor =
                    LiteAccessor<Group, Mut>(*(this->cursor), scope);
            }
        } else if (!pos_check_forward && this->idx >= GroupSize) [[unlikely]] {
            this->template metadata_sync<Fast, false, true>();
            if (!this->valid()) {
                lite_accessor = LiteAccessor<Group, Mut>();
                return;
            }
            if constexpr (Prefetch) {
                ON_MISS_BEGIN_X
                    miss_cnt++;

                    // profile::tlpd.data_miss_count++;
                    const InIteratorType *lim =
                        std::min(this->fetch_end,
                                 (const InIteratorType *)(this->cursor) +
                                     PREFETCH_COUNT);
                    cache::OnMissScope oms(__entry__, &scope);
                    for (InIteratorType *obj_p = this->cursor + 1; obj_p < lim;
                         obj_p++) {
                        auto res =
                            Cache::get_default()->prefetch(obj_p->obj(), oms);
                        if (Cache::get_default()->check_fetch(__entry__,
                                                              __ddl__)) {
                            return;
                        }
                    }
                ON_MISS_END_X
                lite_accessor = LiteAccessor<Group, Mut>(*(this->cursor),
                                                         __on_miss__, scope);
            } else {
                // ON_MISS_BEGIN
                //     profile::tlpd.data_miss_count++;
                // ON_MISS_END
                lite_accessor =
                    LiteAccessor<Group, Mut>(*(this->cursor), scope);
            }
        }
    }

    FORCE_INLINE void force_sync(DereferenceScope &scope, __DMH__) {
        {
            ON_MISS_BEGIN_X
            ON_MISS_END_X
            this->metadata_sync();
            lite_accessor = this->valid()
                                ? LiteAccessor<Group, Mut>(*(this->cursor),
                                                           __on_miss__, scope)
                                : LiteAccessor<Group, Mut>();
        }
        assert(at_local());
    }

    FORCE_INLINE void force_sync(DereferenceScope &scope) {
        ON_MISS_BEGIN
        ON_MISS_END
        this->metadata_sync();
        lite_accessor = this->valid() ? LiteAccessor<Group, Mut>(
                                            *(this->cursor), __on_miss__, scope)
                                      : LiteAccessor<Group, Mut>();
    }

public:
    LiteIterator(InIteratorType *cursor, index_type idx,
                 const in_iterator_t *fetch_begin,
                 const in_iterator_t *fetch_end, DereferenceScope &scope)
        : GenericIterator<Mut>(cursor, idx, fetch_begin, fetch_end) {
        force_sync(scope);
    }

    LiteIterator(InIteratorType *cursor, index_type idx,
                 const in_iterator_t *fetch_begin,
                 const in_iterator_t *fetch_end, DereferenceScope &scope,
                 __DMH__)
        : GenericIterator<Mut>(cursor, idx, fetch_begin, fetch_end) {
        force_sync(scope, __on_miss__);
    }

    LiteIterator(InIteratorType *cursor, index_type idx, const Self &begin,
                 const Self &end, DereferenceScope &scope)
        : GenericIterator<Mut>(cursor, idx, begin, end) {
        force_sync(scope);
    }

    LiteIterator(InIteratorType *cursor, index_type idx,
                 const in_iterator_t *fetch_begin,
                 const in_iterator_t *fetch_end)
        : GenericIterator<Mut>(cursor, idx, fetch_begin, fetch_end) {
        this->metadata_sync();
    }

    LiteIterator(InIteratorType *cursor, index_type idx, const Self &begin,
                 const Self &end)
        : GenericIterator<Mut>(cursor, idx, begin, end) {
        this->metadata_sync();
    }

    LiteIterator(const Self &that, index_type offs, DereferenceScope &scope)
        : GenericIterator<Mut>(that) {
        this->idx += offs;
        force_sync(scope);
    }

    LiteIterator(const Self &that, index_type offs, DereferenceScope &scope,
                 __DMH__)
        : GenericIterator<Mut>(that) {
        this->idx += offs;
        force_sync(scope, __on_miss__);
    }

    LiteIterator(const Self &that, index_type offs, DereferenceScope &scope,
                 const in_iterator_t *begin, const in_iterator_t *end)
        : GenericIterator<Mut>(that) {
        this->fetch_begin = begin;
        this->fetch_end = end;
        this->idx += offs;
        force_sync(scope);
    }

    LiteIterator(const Self &that, index_type offs, DereferenceScope &scope,
                 __DMH__, const in_iterator_t *begin, const in_iterator_t *end)
        : GenericIterator<Mut>(that) {
        this->fetch_begin = begin;
        this->fetch_end = end;
        this->idx += offs;
        force_sync(scope, __on_miss__);
    }

    // iterator for boundary hint and will not access
    LiteIterator(const Self &that, index_type offs)
        : GenericIterator<Mut>(that) {
        this->idx += offs;
        this->metadata_sync();
    }

    LiteIterator(const Self &that) : GenericIterator<Mut>(that) {
        this->lite_accessor = that.lite_accessor;
        this->metadata_sync();
    }

    LiteIterator(const Self &that, DereferenceScope &scope)
        : GenericIterator<Mut>(that) {
        this->force_sync(scope);
    }

    LiteIterator() = default;

    difference_type operator-(const Self &that) const {
        return (this->cursor - that.cursor) * GroupSize + this->idx - that.idx;
    }

    bool operator==(const Self &that) const {
        return this->cursor == that.cursor && this->idx == that.idx;
    }

    bool operator!=(const Self &that) const {
        return this->cursor != that.cursor || this->idx != that.idx;
    }

    bool operator<(const Self &that) const {
        if (this->cursor == that.cursor) {
            return this->idx < that.idx;
        }
        return this->cursor < that.cursor;
    }

    bool operator<=(const Self &that) const {
        if (this->cursor == that.cursor) {
            return this->idx <= that.idx;
        }
        return this->cursor <= that.cursor;
    }

    bool operator>(const Self &that) const {
        if (this->cursor == that.cursor) {
            return this->idx > that.idx;
        }
        return this->cursor > that.cursor;
    }

    bool operator>=(const Self &that) const {
        if (this->cursor == that.cursor) {
            return this->idx >= that.idx;
        }
        return this->cursor >= that.cursor;
    }

    FORCE_INLINE constexpr reference operator*() {
        return (*lite_accessor)[this->idx];
    }

    FORCE_INLINE constexpr pointer operator->() {
        return &((*lite_accessor)[this->idx]);
    }

    bool in_same_group_with(const Self &that) const {
        return this->cursor == that.cursor;
    }

    FORCE_INLINE int64_t group_diff(const Self &that) const {
        return this->cursor - that.cursor;
    }

    FORCE_INLINE Self &next(DereferenceScope &scope, __DMH__) {
        this->idx++;
        accessor_sync<true, true, true, false>(scope, __on_miss__);
        return *this;
    }

    FORCE_INLINE Self &next(DereferenceScope &scope) {
        this->idx++;
        accessor_sync<true, true, true, false>(scope, 1);
        return *this;
    }

    FORCE_INLINE Self &nextn(index_type offs, DereferenceScope &scope,
                             __DMH__) {
        this->idx += offs;
        accessor_sync<false, true, true>(scope, __on_miss__);
        return *this;
    }

    FORCE_INLINE Self &nextn(index_type offs, DereferenceScope &scope) {
        this->idx += offs;
        accessor_sync<false, true, true>(scope, offs);
        return *this;
    }

    // only modify metadata
    // need fetch manually
    FORCE_INLINE Self &nextn(index_type offs) {
        this->idx += offs;
        this->metadata_sync();
        return *this;
    }

    // only modify metadata
    // need fetch manually
    FORCE_INLINE Self &next() {
        this->idx++;
        this->template metadata_sync<true>();
        return *this;
    }

    Self &prev(DereferenceScope &scope) {
        this->idx--;
        accessor_sync<true, true, false, true>(scope, -1);
        return *this;
    }

    Self &prev(DereferenceScope &scope, __DMH__) {
        this->idx--;
        accessor_sync<true, true, false, true>(scope, __on_miss__);
        return *this;
    }

    // only modify metadata
    // need fetch manually
    Self &prevn(index_type offs, DereferenceScope &scope) {
        this->idx -= offs;
        accessor_sync<false, true>(scope, -offs);
        return *this;
    }

    Self &prevn(index_type offs) {
        this->idx -= offs;
        this->metadata_sync();
        return *this;
    }

    Self nextn_copy(index_type offs, DereferenceScope &scope) {
        return Self(*this, offs, scope);
    }

    Self prevn_copy(index_type offs, DereferenceScope &scope) {
        return nextn_copy(-offs, scope);
    }

    void pin() const { lite_accessor.pin(); }

    void unpin() const { lite_accessor.unpin(); }

    bool is_null() const { return lite_accessor.is_null(); }

    bool async_fetch(DereferenceScope &scope) {
        // bool at_remote = this->cursor->at_remote();
        bool ret = lite_accessor.async_fetch(*(this->cursor), scope);
        return ret;
    }

    bool at_local() const { return cache::at_local(lite_accessor); }

    void sync_accessor() {
        // assert(lite_accessor.as_ptr() == nullptr);
        lite_accessor.sync();
        // assert(lite_accessor.as_ptr() != nullptr);
    }

    Self &next_group() {
        this->idx = 0;
        this->cursor++;
        return *this;
    }

    Self &next_group(DereferenceScope &scope) {
        next_group();
        this->force_sync(scope);
        return *this;
    }

    Self &prev_group() {
        this->idx = 0;
        this->cursor--;
        return *this;
    }

    Self &prev_group(DereferenceScope &scope) {
        prev_group();
        this->force_sync(scope);
        return *this;
    }

    auto get_group_accessor() const {
        assert(at_local());
        return lite_accessor;
    }
};
}  // namespace FarLib