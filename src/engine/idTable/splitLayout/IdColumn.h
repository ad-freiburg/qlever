// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMN_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMN_H

#include <cstddef>
#include <limits>

#include "backports/concepts.h"
#include "backports/span.h"
#include "engine/idTable/splitLayout/IdColumnIterator.h"
#include "engine/idTable/splitLayout/IdRef.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace columnBasedIdTable::splitLayout {

// A view (non-owning, like `ql::span`) of an `Id` column stored in the
// experimental split layout: a contiguous payload-word array and a contiguous
// datatype-byte array. Mirrors `ql::span<[const] Id>`'s interface except for
// `.data()` (the two arrays aren't one contiguous range of `Id`); use
// `rawPayloads()`/`rawDatatypes()` instead, e.g. for raw byte I/O.
//
// This is the split-layout counterpart of the legacy `ql::span<Id>` /
// `ql::span<const Id>` column views (`IdColumnRef`/`ConstIdColumnRef` in
// `IdTable.h`), which stay unchanged. The aliases `IdColumnRef`/
// `ConstIdColumnRef` at the bottom of this file live in the `splitLayout`
// namespace.
template <bool IsConst>
class BasicIdColumnView {
 public:
  using PayloadPointer =
      std::conditional_t<IsConst, const uint64_t*, uint64_t*>;
  using DatatypePointer = std::conditional_t<IsConst, const uint8_t*, uint8_t*>;
  using Reference = BasicIdRef<IsConst>;
  using value_type = Id;
  using iterator = BasicIdColumnIterator<IsConst>;
  using const_iterator = BasicIdColumnIterator<true>;

  static constexpr size_t npos = std::numeric_limits<size_t>::max();

 private:
  PayloadPointer payloads_ = nullptr;
  DatatypePointer datatypes_ = nullptr;
  size_t size_ = 0;

 public:
  // Construct an empty, default view. Required e.g. to store a
  // `BasicIdColumnView` as a member that is initialized later, or in a
  // fixed-size array together with views of other columns.
  BasicIdColumnView() = default;

  BasicIdColumnView(PayloadPointer payloads, DatatypePointer datatypes,
                    const size_t size)
      : payloads_{payloads}, datatypes_{datatypes}, size_{size} {}

  // Any `BasicIdColumnView` may access the (private) members of any other
  // instantiation, needed for the implicit construction of the const view
  // from the mutable view below.
  template <bool>
  friend class BasicIdColumnView;

  // Implicit conversion from the mutable to the const view, like
  // `ql::span<T>` -> `ql::span<const T>`. A converting constructor here
  // (rather than a conversion operator on the mutable view) avoids the
  // mutable instantiation having to name its own type as a target.
  CPP_template(typename = void)(requires IsConst)
      /*implicit*/ BasicIdColumnView(
          const BasicIdColumnView<false>&
              other)  // NOSONAR, implicit conversion is needed for future
                      // IdRefProxy
      : payloads_{other.payloads_},
        datatypes_{other.datatypes_},
        size_{other.size_} {}

  [[nodiscard]] size_t size() const noexcept { return size_; }
  [[nodiscard]] bool empty() const noexcept { return size_ == 0; }

  [[nodiscard]] Reference operator[](size_t i) const {
    AD_CONTRACT_CHECK(i < size_);
    return {payloads_ + i, datatypes_ + i};
  }
  [[nodiscard]] Reference at(size_t i) const { return (*this)[i]; }
  [[nodiscard]] Reference front() const { return (*this)[0]; }
  [[nodiscard]] Reference back() const { return (*this)[size_ - 1]; }

  [[nodiscard]] iterator begin() const { return {payloads_, datatypes_}; }
  [[nodiscard]] iterator end() const {
    return {payloads_ + size_, datatypes_ + size_};
  }

  // Return the subrange `[offset, offset + count)`. If `count == npos`
  // (the default), the subrange reaches until the end of this view.
  [[nodiscard]] BasicIdColumnView subspan(size_t offset,
                                          const size_t count = npos) const {
    AD_CONTRACT_CHECK(offset <= size_);
    size_t actualCount = count == npos ? size_ - offset : count;
    AD_CONTRACT_CHECK(offset + actualCount <= size_);
    return {payloads_ + offset, datatypes_ + offset, actualCount};
  }
  [[nodiscard]] BasicIdColumnView first(const size_t count) const {
    return subspan(0, count);
  }
  [[nodiscard]] BasicIdColumnView last(const size_t count) const {
    AD_CONTRACT_CHECK(count <= size_);
    return subspan(size_ - count, count);
  }

  // Direct access to the two underlying arrays (`.data()` is intentionally
  // not provided), e.g. for raw byte I/O.
  [[nodiscard]] ql::span<std::conditional_t<IsConst, const uint64_t, uint64_t>>
  rawPayloads() const {
    return {payloads_, size_};
  }
  [[nodiscard]] ql::span<std::conditional_t<IsConst, const uint8_t, uint8_t>>
  rawDatatypes() const {
    return {datatypes_, size_};
  }
};

using IdColumnRef = BasicIdColumnView<false>;
using ConstIdColumnRef = BasicIdColumnView<true>;

}  // namespace columnBasedIdTable::splitLayout

// `BasicIdColumnView` is a `borrowed_range` (like `ql::span`): a non-owning
// view whose iterators outlive the view object itself, so passing a
// temporary (e.g. `column.subspan(...)`) into `equal_range` etc. still
// returns a real iterator instead of `ranges::dangling`.
#ifdef QLEVER_CPP_17
template <bool IsConst>
inline constexpr bool ::ranges::enable_borrowed_range<
    columnBasedIdTable::splitLayout::BasicIdColumnView<IsConst>> = true;
#else
template <bool IsConst>
inline constexpr bool std::ranges::enable_borrowed_range<
    columnBasedIdTable::splitLayout::BasicIdColumnView<IsConst>> = true;
#endif

// Also has to be recognized as a `view` (like `ql::span`), so
// `ranges::views::all`/`::zip` etc. copy it directly instead of wrapping the
// passed-in lvalue in a `ranges::ref_view`, which would dangle once that
// (possibly short-lived) lvalue goes out of scope.
#ifdef QLEVER_CPP_17
template <bool IsConst>
inline constexpr bool ::ranges::enable_view<
    columnBasedIdTable::splitLayout::BasicIdColumnView<IsConst>> = true;
#else
template <bool IsConst>
inline constexpr bool std::ranges::enable_view<
    columnBasedIdTable::splitLayout::BasicIdColumnView<IsConst>> = true;
#endif

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMN_H
