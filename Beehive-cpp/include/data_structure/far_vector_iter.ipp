#pragma once
#include "cache/cache.hpp"
#include "data_structure/far_vector.hpp"

namespace FarLib {

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
}