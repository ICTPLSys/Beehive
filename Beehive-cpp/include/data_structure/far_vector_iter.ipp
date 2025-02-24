#pragma once
#include "data_structure/far_vector.hpp"

namespace Beehive {

template <VecElementType T, size_t GroupSize>
template <bool Mut>
class FarVector<T, GroupSize>::GenericIterator {
public:
    using difference_type = int64_t;
    using value_type = T;
    using pointer = std::conditional_t<Mut, T *, const T *>;
    using reference = std::conditional_t<Mut, T &, const T &>;

protected:
    friend class FarVector;
    using InIteratorType =
        std::conditional_t<Mut, in_iterator_t, const in_iterator_t>;
    InIteratorType *cursor;
    index_type idx;
    // [begin, end)
    const in_iterator_t *fetch_begin;
    const in_iterator_t *fetch_end;

    /* update GenericIterator's member
        when iterator is calculated */
    template <bool Fast = false, bool neg_overflow_ensure = false,
              bool pos_overflow_ensure = false>
    void metadata_sync() {
        static_assert(!(pos_overflow_ensure & neg_overflow_ensure));
        constexpr bool ensure = neg_overflow_ensure | pos_overflow_ensure;
        if (neg_overflow_ensure || (!ensure && this->idx < 0)) {
            if (Fast) {
                this->idx = GroupSize - 1;
                this->cursor--;
            } else {
                index_type offset = (this->idx - (index_type)GroupSize + 1) /
                                    (index_type)GroupSize;
                this->idx %= GroupSize;
                this->cursor += offset;
            }
        } else if (pos_overflow_ensure || (!ensure && this->idx >= GroupSize)) {
            if (Fast) {
                this->idx = 0;
                this->cursor++;
            } else {
                index_type offset = this->idx / GroupSize;
                this->idx %= GroupSize;
                this->cursor += offset;
            }
        }
    }

public:
    GenericIterator() = default;
    GenericIterator(InIteratorType *cursor, index_type idx,
                    const in_iterator_t *fetch_begin,
                    const in_iterator_t *fetch_end)
        : cursor(cursor),
          idx(idx),
          fetch_begin(fetch_begin),
          fetch_end(fetch_end) {}
    GenericIterator(in_iterator_t *cursor, index_type idx,
                    GenericIterator &begin, GenericIterator &end)
        : cursor(cursor),
          idx(idx),
          fetch_begin(begin.cursor),
          fetch_end(end.idx > 0 ? end.cursor + 1 : end.cursor) {}
    GenericIterator(const GenericIterator &that)
        : cursor(that.cursor),
          idx(that.idx),
          fetch_begin(that.fetch_begin),
          fetch_end(that.fetch_end) {}
    void set_fetch_begin(in_iterator_t *fetch) { fetch_begin = fetch; }

    void set_fetch_begin(const GenericIterator &iter) {
        set_fetch_begin(iter.cursor);
    }

    void set_fetch_end(in_iterator_t *fetch) { fetch_end = fetch; }

    void set_fetch_end(const GenericIterator &iter) {
        set_fetch_end(iter.fetch_end);
    }

    void reset_to_group_begin() { this->idx = 0; }

    bool valid() const { return cursor >= fetch_begin && cursor < fetch_end; }
};

template <VecElementType T, size_t GroupSize>
template <bool Mut>
class FarVector<T, GroupSize>::Iterator : public GenericIterator<Mut> {
private:
    friend class FarVector<T, GroupSize>;
    using InIteratorType = GenericIterator<Mut>::InIteratorType;
    using ItAccessor =
        std::conditional_t<Mut, Accessor<Group>, ConstAccessor<Group>>;
    using Self = Iterator<Mut>;

public:
    using difference_type = GenericIterator<Mut>::difference_type;
    using value_type = GenericIterator<Mut>::value_type;
    using pointer = GenericIterator<Mut>::pointer;
    using reference = GenericIterator<Mut>::reference;
    using iterator_category = std::random_access_iterator_tag;

private:
    ItAccessor accessor;
    template <bool Fast = false, bool Prefetch = false,
              bool neg_check_forward = false, bool pos_check_forward = false>
    void accessor_sync() {
        static_assert(!(neg_check_forward & pos_check_forward));
        if (!neg_check_forward && this->idx < 0) {
            this->template metadata_sync<Fast, true, false>();
            if (!this->valid()) {
                accessor = ItAccessor();
                return;
            }
            if constexpr (Prefetch) {
                ON_MISS_BEGIN
                    for (InIteratorType *obj_p = this->cursor - 1;
                         obj_p >= this->fetch_begin; obj_p--) {
                        Cache::get_default()->prefetch(obj_p->obj());
                        if (Cache::get_default()->check_fetch(__entry__,
                                                              __ddl__)) {
                            return;
                        }
                    }
                ON_MISS_END
                accessor = ItAccessor(*(this->cursor), __on_miss__);
            } else {
                accessor = ItAccessor(*(this->cursor));
            }
        } else if (!pos_check_forward && this->idx >= GroupSize) {
            this->template metadata_sync<Fast, false, true>();
            if (!this->valid()) {
                accessor = ItAccessor();
                return;
            }
            if constexpr (Prefetch) {
                ON_MISS_BEGIN
                    for (InIteratorType *obj_p = this->cursor + 1;
                         obj_p < this->fetch_end; obj_p++) {
                        Cache::get_default()->prefetch(obj_p->obj());
                        if (Cache::get_default()->check_fetch(__entry__,
                                                              __ddl__)) {
                            return;
                        }
                    }
                ON_MISS_END
                accessor = ItAccessor(*(this->cursor), __on_miss__);
            } else {
                accessor = ItAccessor(*(this->cursor));
            }
        }
    }

    void force_sync() {
        this->metadata_sync();
        accessor = this->valid() ? ItAccessor(*(this->cursor)) : ItAccessor();
    }

public:
    Iterator(InIteratorType *cursor, index_type idx,
             const in_iterator_t *fetch_begin, const in_iterator_t *fetch_end)
        : GenericIterator<Mut>(cursor, idx, fetch_begin, fetch_end) {
        force_sync();
    }

    Iterator(InIteratorType *cursor, index_type idx, const Self &begin,
             const Self &end)
        : GenericIterator<Mut>(cursor, idx, begin, end) {
        force_sync();
    }

    Iterator(const Self &that, const index_type offs = 0)
        : GenericIterator<Mut>(that) {
        this->idx += offs;
        force_sync();
    }

    Self &operator++() {
        this->idx++;
        accessor_sync<true, true, true, false>();
        return *this;
    }

    Self operator++(int) {
        Self it(*this);
        this->idx++;
        accessor_sync<true, true, true, false>();
        return it;
    }

    Self &operator+=(int64_t offs) {
        this->idx += offs;
        accessor_sync<false, true>();
        return *this;
    }

    Self operator+(int64_t offs) const { return Self(*this, offs); }

    Self &operator--() {
        this->idx--;
        accessor_sync<true, true, false, true>();
        return *this;
    }

    Self operator--(int) {
        Self it(*this);
        this->idx--;
        accessor_sync<true, true, false, true>();
        return it;
    }

    difference_type operator-(const Self &that) const {
        return (this->cursor - that.cursor) * GroupSize + this->idx - that.idx;
    }

    Self operator-(int64_t offs) const { return *this + (-offs); }

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

    Self &operator=(const Self &that) {
        this->cursor = that.cursor;
        this->idx = that.idx;
        return *this;
        // accessor_sync();
    }

    constexpr reference operator*() { return (*accessor)[this->idx]; }

    constexpr reference operator*() const { return (*accessor)[this->idx]; }

    constexpr pointer operator->() { return &((*accessor)[this->idx]); }

    constexpr pointer operator->() const { return &((*accessor)[this->idx]); }

    bool in_same_group_with(const Self &that) const {
        return *(this->cursor) == *(that.cursor);
    }

    int64_t group_diff(const Self &that) const {
        return this->cursor - that.cursor;
    }

    Self &next() {
        ++(*this);
        return *this;
    }
};

template <VecElementType T, size_t GroupSize>
template <bool Mut>
class FarVector<T, GroupSize>::ReverseIterator {
private:
    friend class FarVector<T, GroupSize>;
    using Self = ReverseIterator<Mut>;
    using WrappedIt = Iterator<Mut>;

public:
    using difference_type = GenericIterator<Mut>::difference_type;
    using value_type = GenericIterator<Mut>::value_type;
    using pointer = GenericIterator<Mut>::pointer;
    using reference = GenericIterator<Mut>::reference;
    using iterator_category = std::random_access_iterator_tag;

private:
    WrappedIt iterator;
    friend WrappedIt;

public:
    ReverseIterator(const WrappedIt &it) : iterator(WrappedIt(it)) {}
    ReverseIterator(const WrappedIt &&it) : iterator(it) {}
    ReverseIterator(const Self &that) : ReverseIterator(that.iterator) {}
    Self &operator++() {
        --iterator;
        return *this;
    }

    Self operator++(int) {
        Self it(*this);
        iterator--;
        return it;
    }

    Self &operator+=(int64_t offs) {
        iterator -= offs;
        return *this;
    }

    Self operator+(int64_t offs) const { return Self(iterator - offs); }

    Self &operator--() {
        ++iterator;
        return *this;
    }

    Self operator--(int) {
        Self it(*this);
        iterator++;
        return it;
    }

    difference_type operator-(const Self &that) const {
        return -(iterator - that.iterator);
    }

    Self operator-(int64_t offs) const { return *this + (-offs); }

    bool operator==(const Self &that) const {
        return this->iterator == that.iterator;
    }

    bool operator!=(const Self &that) const {
        return this->iterator != that.iterator;
    }

    bool operator<(const Self &that) const {
        return this->iterator > that.iterator;
    }

    bool operator<=(const Self &that) const {
        return this->iterator >= that.iterator;
    }

    bool operator>(const Self &that) const {
        return this->iterator < that.iterator;
    }

    bool operator>=(const Self &that) const {
        return this->iterator <= that.iterator;
    }

    void operator=(const Self &that) { iterator = that.iterator; }

    Self &next() {
        ++(*this);
        return *this;
    }

    constexpr reference operator*() { return *iterator; }
    constexpr pointer operator->() { return iterator.operator->(); }
};
}  // namespace Beehive
