/******************************************************************************
    Copyright (C) Martin Karsten 2015-2023

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
#ifndef _Bitmap_h_
#define _Bitmap_h_ 1

#include "runtime/Basics.h"

template<size_t X> constexpr mword BitmapEmptyHelper(const mword* bits) {
  return bits[X] | BitmapEmptyHelper<X-1>(bits);
}

template<> constexpr mword BitmapEmptyHelper<0>(const mword* bits) {
  return bits[0];
}

template<size_t X> constexpr mword BitmapFullHelper(const mword* bits) {
  return bits[X] & BitmapFullHelper<X-1>(bits);
}

template<> constexpr mword BitmapFullHelper<0>(const mword* bits) {
  return bits[0];
}

template<size_t X> constexpr mword BitmapCountHelper(const mword* bits) {
  return popcount(bits[X]) + BitmapCountHelper<X-1>(bits);
}

template<> constexpr mword BitmapCountHelper<0>(const mword* bits) {
  return popcount(bits[0]);
}

template<bool atomic=false>
static inline void bit_set(mword& a, size_t idx) {
  mword b = mword(1) << idx;
  if (atomic) __atomic_or_fetch(&a, b, __ATOMIC_RELAXED);
  else a |= b;
}

template<bool atomic=false>
static inline void bit_clr(mword& a, size_t idx) {
  mword b = ~(mword(1) << idx);
  if (atomic) __atomic_and_fetch(&a, b, __ATOMIC_RELAXED);
  else a &= b;
}

template<bool atomic=false>
static inline void bit_flp(mword& a, size_t idx) {
  mword b = mword(1) << idx;
  if (atomic) __atomic_xor_fetch(&a, b, __ATOMIC_RELAXED);
  else a ^= b;
}

static inline bool bit_tst(const mword& a, size_t idx) {
  mword b = mword(1) << idx;
  return a & b;
}

// B: number of bits (good choices: machine word, cache line)
template<size_t B = bitsize<mword>()>
class Bitmap {
  static const int N = divup(B,bitsize<mword>());
  mword bits[N];
public:
  explicit Bitmap( mword b = 0 ) : bits{b} {}
  static constexpr bool valid( mword idx ) { return idx < N * bitsize<mword>(); }
  static constexpr Bitmap filled() { return Bitmap(~mword(0)); }
  void setB() { for (size_t i = 0; i < N; i++) bits[i] = ~mword(0); }
  void clrB() { for (size_t i = 0; i < N; i++) bits[i] =  mword(0); }
  void flpB() { for (size_t i = 0; i < N; i++) bits[i] = ~bits[i]; }
  template<bool atomic=false> void set( mword idx ) {
    bit_set<atomic>(bits[idx / bitsize<mword>()], idx % bitsize<mword>());
  }
  template<bool atomic=false> void clr( mword idx ) {
    bit_clr<atomic>(bits[idx / bitsize<mword>()], idx % bitsize<mword>());
  }
  template<bool atomic=false> void flp( mword idx ) {
    bit_flp<atomic>(bits[idx / bitsize<mword>()], idx % bitsize<mword>());
  }
  constexpr bool test( mword idx ) const {
    return  bit_tst(bits[idx / bitsize<mword>()], idx % bitsize<mword>());
  }
  constexpr bool empty()  const { return BitmapEmptyHelper<N-1>(bits) ==  mword(0); }
  constexpr bool full()   const { return  BitmapFullHelper<N-1>(bits) == ~mword(0); }
  constexpr mword count() const { return BitmapCountHelper<N-1>(bits); }
  constexpr mword find(bool findset = true) const {
    return multiscan<N>(bits, findset);
  }
  constexpr mword findnext(mword idx, bool findset = true) const {
    return multiscan_next<N>(bits, idx, findset);
  }
};

template<>
class Bitmap<bitsize<mword>()> {
  mword bits;
public:
  explicit Bitmap() {}
  explicit constexpr Bitmap( mword b ) : bits(b) {}
  static constexpr bool valid( mword idx ) { return idx < bitsize<mword>(); }
  static constexpr Bitmap filled() { return Bitmap(~mword(0)); }
  void setB() { bits = ~mword(0); }
  void clrB() { bits =  mword(0); }
  void flpB() { bits = ~bits; }
  template<bool atomic=false> void set( mword idx ) {
    bit_set<atomic>(bits, idx);
  }
  template<bool atomic=false> void clr( mword idx ) {
    bit_clr<atomic>(bits, idx);
  }
  template<bool atomic=false> void flp( mword idx ) {
    bit_flp<atomic>(bits, idx);
  }
  constexpr bool test( mword idx ) const {
    return bits & (mword(1) << idx);
  }
  constexpr bool empty()  const { return bits == mword(0); }
  constexpr bool full()   const { return bits == ~mword(0); }
  constexpr mword count() const { return popcount(bits); }
  constexpr mword find(bool findset = true) const {
    return lsb(findset ? bits : ~bits);
  }
  constexpr mword findnext(mword idx, bool findset = true) const {
    return lsb((findset ? bits : ~bits) & ~bitmask<mword>(idx));
  }
};

// W: log2 of maximum width of bitmap
// B: number of bits in elementary bitmap
template<size_t W, size_t B = bitsize<mword>()>
class HierarchicalBitmap {
  static const size_t logB = floorlog2(B);
  static const size_t Levels = divup(W,logB);

  static_assert( B == pow2<size_t>(logB), "template parameter B not a power of 2" );
  static_assert( B >= bitsize<mword>(), "template parameter B less than word size" );
  static_assert( W >= logB, "template parameter W smaller than log B" );

  Bitmap<B>* bitmaps[Levels];
  size_t maxBucketCount;

public:
  static size_t memsize( size_t bitcount ) {
    size_t mapcount = 0;
    for (size_t l = 0; l < Levels; l += 1) {
      bitcount = divup(bitcount,B);
      mapcount += bitcount;
    }
    RASSERT0(bitcount == 1);
    return sizeof(Bitmap<B>) * mapcount;
  }

  void init( size_t bitcount, bufptr_t p ) {
    maxBucketCount = divup(bitcount,B);
    for (size_t l = 0; l < Levels; l += 1) {
      bitcount = divup(bitcount,B);
      bitmaps[l] = new (p) Bitmap<B>[bitcount];
      p += sizeof(Bitmap<B>) * bitcount;
    }
    RASSERT0(bitcount == 1);
  }

  void set( size_t idx, size_t botlevel = 0 ) {
    for (size_t l = botlevel; l < Levels; l += 1) {
      size_t r = idx % B;
      idx = idx / B;
      if (bitmaps[l][idx].test(r)) return;
      bitmaps[l][idx].set(r);
    }
    RASSERT0(idx == 0);
  }

  void clr( size_t idx, size_t botlevel = 0 ) {
    for (size_t l = botlevel; l < Levels; l += 1) {
      size_t r = idx % B;
      idx = idx / B;
      bitmaps[l][idx].clr(r);
      if (!bitmaps[l][idx].empty()) return;
    }
    RASSERT0(idx == 0);
  }

  void blockset( size_t idx ) {
    bitmaps[0][idx / B].setB();
    set(idx / B, 1);
  }

  void blockclr( size_t idx ) {
    bitmaps[0][idx / B].clrB();
    clr(idx / B, 1);
  }

  constexpr bool test( size_t idx ) const {
    return bitmaps[0][idx / B].test(idx % B);
  }

  constexpr bool empty() const {
    return bitmaps[Levels-1][0].empty();
  }

  constexpr bool blockempty( size_t idx ) const {
    return bitmaps[0][idx / B].empty();
  }

  constexpr bool blockfull( size_t idx ) const {
    return bitmaps[0][idx / B].full();
  }

  size_t find() const {
    size_t idx = 0;
    for (size_t i = Levels - 1;; i -= 1) {
      size_t ldx = bitmaps[i][idx].find();
      if (ldx == B) return limit<size_t>();
      idx = idx * B + ldx;
      if (i == 0) return idx;
    }
  }

  size_t findnext(size_t idx) const {
    size_t bidx = idx / B;
    size_t ridx = idx % B;
    size_t mbc = maxBucketCount;
    size_t l = 0;
    for (;;) {
      size_t lidx = bitmaps[l][bidx].findnext(ridx);
      if (lidx == B) {
        if (bidx + 1 >= mbc) return limit<size_t>();
        ridx = (bidx + 1) % B; // order important
        bidx = (bidx + 1) / B; // don't modify bidx before computing ridx
        mbc = mbc / B;
        l = l + 1;
      } else {
        bidx = bidx * B + lidx;
        if (l == 0) return bidx;
        ridx = 0;
        mbc = mbc * B;
        l = l - 1;
      }
    }
  }

  size_t findrange(size_t& start, size_t max) const {
    start = findnext(start);
    if (start == limit<size_t>()) return 0;
    size_t end = start + 1;
    // scan to the end of this bucket
    while ((end % B) && test(end)) end += 1;
    // scan full buckets
    while (((end / B) < maxBucketCount) && blockfull(end)) end += B;
    // scan last, non-full bucket
    while (((end / B) < maxBucketCount) && test(end)) end += 1;
    if (end > max) end = max;
    return end - start;
  }
};

#endif /* _Bitmap_h_ */
