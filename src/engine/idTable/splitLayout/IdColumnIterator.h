// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNITERATOR_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNITERATOR_H

#include "backports/concepts.h"
#include "backports/three_way_comparison.h"
#include "engine/idTable/splitLayout/IdRef.h"

namespace columnBasedIdTable::splitLayout {

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

  // The const and the mutable iterator are friends of each other, needed for
  // the conversion constructor below.
  template <bool>
  friend class BasicIdColumnIterator;

  // Implicit conversion from the mutable to the const iterator, like
  // `iterator` -> `const_iterator` of the standard containers.
  CPP_template(typename = void)(requires IsConst)
      /*implicit*/ BasicIdColumnIterator(
          const BasicIdColumnIterator<false>& other)  // NOSONAR,
      // implicit conversion is needed for future IdRefProxy
      : payload_{other.payload_}, datatype_{other.datatype_} {}

  reference operator*() const { return {payload_, datatype_}; }
  reference operator[](difference_type n) const {
    return {payload_ + n, datatype_ + n};
  }

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
    *this += -n;
    return *this;
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

  // Compares only `payload_`: both pointers always advance together, so equal
  // payload pointers imply equal datatype pointers (see the check below).
  auto compareThreeWay(const BasicIdColumnIterator& rhs) const {
    AD_EXPENSIVE_CHECK(ql::compareThreeWay(payload_, rhs.payload_) ==
                       ql::compareThreeWay(datatype_, rhs.datatype_));
    return ql::compareThreeWay(payload_, rhs.payload_);
  }
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(BasicIdColumnIterator)

  // Compares only `payload_`, see `compareThreeWay`.
  bool operator==(const BasicIdColumnIterator& rhs) const {
    AD_EXPENSIVE_CHECK(payload_ != rhs.payload_ || datatype_ == rhs.datatype_);
    return payload_ == rhs.payload_;
  }
#ifdef QLEVER_CPP_17
  bool operator!=(const BasicIdColumnIterator& rhs) const {
    return !(*this == rhs);
  }
#endif
};

using IdColumnIterator = BasicIdColumnIterator<false>;
using ConstIdColumnIterator = BasicIdColumnIterator<true>;

}  // namespace columnBasedIdTable::splitLayout

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNITERATOR_H
