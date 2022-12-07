//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <array>
#include <iostream>
#include <type_traits>
#include <variant>
#include <vector>

#include "global/Id.h"
#include "util/UninitializedAllocator.h"

namespace columnBasedIdTable {

// A row of a table of IDs. It stores the IDs as a `std::array` or `std::vector`
// depending on whether `NumCols` is 0 (which means that the number of columns
// is specified at runtime). This class is used as the `value_type` of the
// columns-based `IdTable` and must be used whenever a row (not a reference to a
// row) has to be stored outside the IdTable. The implementation is a rather
// thin wrapper around `std::vector<Id>` or `std::array<Id, NumCols>`
// respectively (see above).
template <int NumCols = 0>
class Row {
 public:
  static constexpr bool isDynamic() { return NumCols == 0; }
  static constexpr int numStaticCols = NumCols;

  // TODO<joka921> We could use a vector type with a small buffer optimization
  // for up to 20 or 30 columns, maybe this increases the performance for the
  // `isDynamic()` case.
  using Data = std::conditional_t<
      isDynamic(),
      std::vector<Id,
                  ad_utility::default_init_allocator<Id, std::allocator<Id>>>,
      std::array<Id, NumCols>>;

 private:
  Data data_;

  static Data initData(size_t numCols) {
    if constexpr (isDynamic()) {
      // The `std::vector` has to explicitly allocated
      return Data(numCols);
    } else {
      return Data{};
    }
  }

 public:
  // For the dynamic case the number of columns must always be specified.
  explicit Row(size_t numCols) requires(isDynamic()) : data_(numCols) {}

  // When the number of columns is statically known, the row can be
  // default-constructed, but  we keep the constructor that has a `numCols`
  // argument (which is ignored) for the easier implementation of generic code
  // for both cases.
  Row() requires(!isDynamic()) = default;
  explicit Row([[maybe_unused]] size_t numCols) requires(!isDynamic())
      : Row() {}

  // Access the i-th element.
  Id& operator[](size_t idx) { return data_[idx]; }
  const Id& operator[](size_t idx) const { return data_[idx]; }

  size_t numColumns() const { return data_.size(); }

  friend void swap(Row& a, Row& b) { std::swap(a.data_, b.data_); }

  bool operator==(const Row& other) const = default;
};

class RowReferenceImpl {
 private:
  template <typename Table, bool isConst>
  friend class RowReference;

  template <int NumCols, typename Allocator, bool isVies>
  friend class IdTable;

  template <typename Table, bool isConst = false>
  class DeducingRowReferenceViaAutoIsLikelyABug {
   public:
    using TablePtr = std::conditional_t<isConst, const Table*, Table*>;
    static constexpr int numStaticCols = Table::numStaticCols;

   protected:
    // The `Table` (as a pointer) and the row (as and index) that this reference
    // points to.

    // TODO<joka921> Only storing the row index makes the implementation easy,
    // but possibly harms the performance as every access to a reference
    // involves a multiplication. But this cannot be simply fixed inside the row
    // reference, but needs iterators/references to single columns and special
    // algorithms that are aware of the column based structure of the `IdTable`.
    TablePtr table_ = nullptr;
    size_t row_ = 0;

   public:
    explicit DeducingRowReferenceViaAutoIsLikelyABug(TablePtr table, size_t row)
        : table_{table}, row_{row} {}
    // Access to the `i-th` columns of this row.
    Id& operator[](size_t i) && requires(!isConst) {
      return (*table_)(row_, i);
    }
    const Id& operator[](size_t i) const& { return (*table_)(row_, i); }
    const Id& operator[](size_t i) const&& { return (*table_)(row_, i); }

    size_t numColumns() const { return table_->cols(); }

    // The `const` and `mutable` variations are friends.
    friend class DeducingRowReferenceViaAutoIsLikelyABug<Table, !isConst>;

    using This = DeducingRowReferenceViaAutoIsLikelyABug;
    // __________________________________________________________________________
    friend void swap(This&& a, This&& b) requires(!isConst) {
      for (size_t i = 0; i < a.numColumns(); ++i) {
        std::swap(std::move(a)[i], std::move(b)[i]);
      }
    }

    // Equality comparison. Works between two `RowReference`s, but also between
    // a `RowReference` and a `Row` if the number of columns match.
    template <typename T>
    bool operator==(const T& other) const
        requires(numStaticCols == T::numStaticCols) {
      if constexpr (numStaticCols == 0) {
        if (numColumns() != other.numColumns()) {
          return false;
        }
      }
      for (size_t i = 0; i < numColumns(); ++i) {
        if ((*this)[i] != other[i]) {
          return false;
        }
      }
      return true;
    }

    // Convert from a `RowReference` to a `Row`.
    operator Row<numStaticCols>() const&& {
      auto numCols = (std::move(*this)).numColumns();
      Row<numStaticCols> result{numCols};
      for (size_t i = 0; i < numCols; ++i) {
        result[i] = std::move(*this)[i];
      }
      return result;
    }

   private:
    // Internal implementation of the assignment from a `Row` as well as a
    // `RowReference`. This assignment actually writes to the underlying table.
    DeducingRowReferenceViaAutoIsLikelyABug& assignmentImpl(const auto& other) {
      if constexpr (numStaticCols == 0) {
        AD_CHECK(numColumns() == other.numColumns());
      }
      for (size_t i = 0; i < numColumns(); ++i) {
        std::move(*this)[i] = other[i];
      }
      return *this;
    }

   public:
    // Assignment from a `Row` with the same number of columns.
    DeducingRowReferenceViaAutoIsLikelyABug& operator=(
        const Row<numStaticCols>& other) && {
      return assignmentImpl(other);
    }

    // Assignment from a `RowReference` with the same number of columns.
    DeducingRowReferenceViaAutoIsLikelyABug& operator=(
        const DeducingRowReferenceViaAutoIsLikelyABug& other) && {
      return assignmentImpl(other);
    }

    // Assignment from a `const` RowReference to a `mutable` RowReference
    DeducingRowReferenceViaAutoIsLikelyABug& operator=(
        const DeducingRowReferenceViaAutoIsLikelyABug<Table, true>& other) &&
        requires(!isConst) {
      return assignmentImpl(other);
    }

   protected:
    // No need to copy this internal type, but the implementation requires it,
    // so the copy Constructor is private.
    DeducingRowReferenceViaAutoIsLikelyABug(
        const DeducingRowReferenceViaAutoIsLikelyABug&) = default;
  };
};

// This class stores a reference to a row in the underlying column-based
// `Table`. Note that this has to be its own class instead of `SomeRowType&`
// because the rows are not materialized in the table but scattered across
// memory due to the column-major order. This design of having to dedicated
// classes for the `value_type` and the `reference` of a container is similar to
// the approach taken for `std::vector<bool>` in the STL. For the caveats of
// this design carefully study the documentation of of the `IdTable` class.
// Template arguments:
//   Table - The `IdTable` type that this class references (Not that`IdTable`
//           is templated on the number of columns)
//   isConst - Is this semantically a const reference or a mutable reference.
template <typename Table, bool isConst = false>
class RowReference
    : public RowReferenceImpl::DeducingRowReferenceViaAutoIsLikelyABug<
          Table, isConst> {
 private:
  using Base =
      RowReferenceImpl::DeducingRowReferenceViaAutoIsLikelyABug<Table, isConst>;
  using Base::row_;
  using Base::table_;

 public:
  using TablePtr = std::conditional_t<isConst, const Table*, Table*>;
  static constexpr int numStaticCols = Table::numStaticCols;

 public:
  // ___________________________________________________________________________
  explicit RowReference(TablePtr table, size_t row) : Base{table, row} {}
  // This is deliberatly implicit! TODO<joka921> comment why.
  RowReference(Base b) : Base{std::move(b)} {}

  // Access to the `i-th` columns of this row.
  Id& operator[](size_t i) requires(!isConst) { return (*table_)(row_, i); }
  const Id& operator[](size_t i) const { return (*table_)(row_, i); }

  size_t numColumns() const { return table_->cols(); }

  // The `const` and `mutable` variations are friends.
  friend class RowReference<Table, !isConst>;

  // __________________________________________________________________________
  template <ad_utility::SimilarTo<RowReference> R>
  friend void swap(R&& a, R&& b) requires(!isConst) {
    for (size_t i = 0; i < a.numColumns(); ++i) {
      std::swap(a[i], b[i]);
    }
  }

  // Equality comparison. Works between two `RowReference`s, but also between
  // a `RowReference` and a `Row` if the number of columns match.
  template <typename T>
  bool operator==(const T& other) const
      requires(numStaticCols == T::numStaticCols) {
    if constexpr (numStaticCols == 0) {
      if (numColumns() != other.numColumns()) {
        return false;
      }
    }
    for (size_t i = 0; i < numColumns(); ++i) {
      if ((*this)[i] != other[i]) {
        return false;
      }
    }
    return true;
  }

  // Convert from a `RowReference` to a `Row`.
  operator Row<numStaticCols>() {
    Row<numStaticCols> result{numColumns()};
    for (size_t i = 0; i < numColumns(); ++i) {
      result[i] = (*this)[i];
    }
    return result;
  }

 private:
  // Internal implementation of the assignment from a `Row` as well as a
  // `RowReference`. This assignment actually writes to the underlying table.
  RowReference& assignmentImpl(const auto& other) {
    if constexpr (numStaticCols == 0) {
      AD_CHECK(numColumns() == other.numColumns());
    }
    for (size_t i = 0; i < numColumns(); ++i) {
      (*this)[i] = other[i];
    }
    return *this;
  }

 public:
  // Assignment from a `Row` with the same number of columns.
  RowReference& operator=(const Row<numStaticCols>& other) {
    return assignmentImpl(other);
  }

  // Assignment from a `RowReference` with the same number of columns.
  RowReference& operator=(const RowReference& other) {
    return assignmentImpl(other);
  }

  // Assignment from a `const` RowReference to a `mutable` RowReference
  RowReference& operator=(const RowReference<Table, true>& other) requires(
      !isConst) {
    return assignmentImpl(other);
  }

  // No need to copy `RowReference`s, because that is most likely a bug.
  // Unfortunately this still allows the pattern
  RowReference(const RowReference&) = delete;
};

}  // namespace columnBasedIdTable