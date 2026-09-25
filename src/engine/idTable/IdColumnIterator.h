// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNITERATOR_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNITERATOR_H

#include "backports/three_way_comparison.h"
#include "engine/idTable/IdRef.h"

namespace columnBasedIdTable {

// A random-access iterator over a `BasicIdColumnView<IsConst>`. Dereferencing
// yields a `BasicIdRef<IsConst>` proxy computed from the current pointer
// pair.
//
// NOTE: deliberately does NOT use the generic
// `ad_utility::IteratorForAccessOperator` (pointer-back-to-container +
// index): `BasicIdColumnView` is a lightweight, frequently-temporary view
// (e.g. a `.subspan(...)` result), so an iterator referencing "the view it
// came from" would dangle once that view is destroyed, even though the
// backing arrays are still alive. Holding the two element pointers directly
// instead (like `ql::span<T>`'s own iterator holds a raw `T*`) keeps it
// valid independently of any particular view's lifetime.
template <bool IsConst>
class BasicIdColumnIterator {
 public:
  using PayloadPointer =
      std::conditional_t<IsConst, const uint64_t*, uint64_t*>;
  using DatatypePointer = std::conditional_t<IsConst, const uint8_t*, uint8_t*>;

  using iterator_category = std::random_access_iterator_tag;
  using difference_type = int64_t;
  using value_type = Id;
  using reference = BasicIdRef<IsConst>;
  using pointer = void;

 private:
  PayloadPointer payload_ = nullptr;
  DatatypePointer datatype_ = nullptr;

 public:
  BasicIdColumnIterator() = default;
  BasicIdColumnIterator(PayloadPointer payload, DatatypePointer datatype)
      : payload_{payload}, datatype_{datatype} {}

  reference operator*() const { return {payload_, datatype_}; }
  reference operator[](difference_type n) const {
    return {payload_ + n, datatype_ + n};
  }

  // Direct access to the underlying pointers, e.g. to build a
  // `BasicIdColumnView` spanning a subrange of iterators without going
  // through the (proxy) elements. See `IdColumnZipperJoin.h`.
  PayloadPointer payloadPtr() const { return payload_; }
  DatatypePointer datatypePtr() const { return datatype_; }

  BasicIdColumnIterator& operator++() {
    ++payload_;
    ++datatype_;
    return *this;
  }
  BasicIdColumnIterator operator++(int) {
    BasicIdColumnIterator result{*this};
    ++(*this);
    return result;
  }
  BasicIdColumnIterator& operator--() {
    --payload_;
    --datatype_;
    return *this;
  }
  BasicIdColumnIterator operator--(int) {
    BasicIdColumnIterator result{*this};
    --(*this);
    return result;
  }

  BasicIdColumnIterator& operator+=(difference_type n) {
    payload_ += n;
    datatype_ += n;
    return *this;
  }
  BasicIdColumnIterator& operator-=(difference_type n) {
    return *this += -n;
  }
  friend BasicIdColumnIterator operator+(BasicIdColumnIterator it,
                                         difference_type n) {
    it += n;
    return it;
  }
  friend BasicIdColumnIterator operator+(difference_type n,
                                         BasicIdColumnIterator it) {
    it += n;
    return it;
  }
  friend BasicIdColumnIterator operator-(BasicIdColumnIterator it,
                                         difference_type n) {
    it -= n;
    return it;
  }
  friend difference_type operator-(const BasicIdColumnIterator& a,
                                   const BasicIdColumnIterator& b) {
    return a.payload_ - b.payload_;
  }

  auto compareThreeWay(const BasicIdColumnIterator& rhs) const {
    return ql::compareThreeWay(payload_, rhs.payload_);
  }
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(BasicIdColumnIterator)

  bool operator==(const BasicIdColumnIterator& rhs) const {
    return payload_ == rhs.payload_;
  }
  bool operator!=(const BasicIdColumnIterator& rhs) const {
    return !(*this == rhs);
  }
};

using IdColumnIterator = BasicIdColumnIterator<false>;
using ConstIdColumnIterator = BasicIdColumnIterator<true>;

}  // namespace columnBasedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNITERATOR_H
