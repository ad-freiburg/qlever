// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <array>
#include <iostream>
#include <ranges>
#include <type_traits>
#include <variant>
#include <vector>

#include "util/Enums.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/Iterators.h"
#include "util/StringUtils.h"
#include "util/TypeTraits.h"
#include "util/UninitializedAllocator.h"

namespace columnBasedIdTable {

// A simple tag enum to differentiate between "views" (non-owning data
// structures) and "ordinary" data structures that own their storage (see
// below).
enum struct IsView { True, False };

// A row of a table of `T`s. It stores the IDs as a `std::array` or
// `std::vector` depending on whether `NumColumns` is 0 (which means that the
// number of columns is specified at runtime). This class is used as the
// `value_type` of the columns-ordered `IdTable` and must be used whenever a row
// (not a reference to a row) has to be stored outside the IdTable. The
// implementation is a rather thin wrapper around `std::vector<T>` or
// `std::array<T, NumColumns>`, respectively (see above).
template <typename T, int NumColumns = 0>
class Row {
 public:
  // Returns true iff the number of columns is only known at runtime and we
  // therefore have to use `std::vector` instead of `std::array`.
  static constexpr bool isDynamic() { return NumColumns == 0; }
  static constexpr int numStaticColumns = NumColumns;

  // TODO<joka921> We could use a vector type with a small buffer optimization
  // for up to 20 or 30 columns, maybe this increases the performance for the
  // `isDynamic()` case.
  using Data = std::conditional_t<
      isDynamic(),
      std::vector<T, ad_utility::default_init_allocator<T, std::allocator<T>>>,
      std::array<T, NumColumns>>;

 private:
  Data data_;

 public:
  // Construct a row for the dynamic case (then the number of columns has to be
  // specified).
  explicit Row(size_t numCols) requires(isDynamic()) : data_(numCols) {}

  // Construct a row when the number of columns is statically known. Besides the
  // default constructor, we also keep the constructor that has a `numCols`
  // argument (which is ignored), in order to facilitate generic code that works
  // for both cases.
  Row() requires(!isDynamic()) = default;
  explicit Row([[maybe_unused]] size_t numCols) requires(!isDynamic())
      : Row() {}

  // Access the i-th element.
  T& operator[](size_t i) { return data_[i]; }
  const T& operator[](size_t i) const { return data_[i]; }

  // Define iterators
  using iterator = ad_utility::IteratorForAccessOperator<
      Row, ad_utility::AccessViaBracketOperator, ad_utility::IsConst::False>;
  using const_iterator = ad_utility::IteratorForAccessOperator<
      Row, ad_utility::AccessViaBracketOperator, ad_utility::IsConst::True>;
  iterator begin() { return {this, 0}; };
  iterator end() { return {this, numColumns()}; };
  const_iterator cbegin() { return {this, 0}; };
  const_iterator cend() { return {this, numColumns()}; };
  const_iterator begin() const { return {this, 0}; };
  const_iterator end() const { return {this, numColumns()}; };

  size_t numColumns() const { return data_.size(); }

  friend void swap(Row& a, Row& b) { std::swap(a.data_, b.data_); }

  bool operator==(const Row& other) const = default;
};

// The following two classes store a reference to a row in the underlying
// column-based `Table`. This has to be its own class instead of `Row&` because
// the rows are not materialized in the table but scattered across memory due to
// the column-major order. This is explained in detail (and with code examples)
// in the comment before class `IdTable`.
//
// Note that all STL algorithms handle this correctly as they explicitly use
// `std::iterator_traits::value_type` and `std::iterator_traits::reference`
// Which are set to the correct types (`Row` and `RowReference`) for the
// iterators of the `IdTable` class (for details, see the `IdTable class).

// First the variation of the `RowReference` that can be created by `auto`
// but has no mutable access for lvalues.
class RowReferenceImpl {
 private:
  // The actual implementation is in a private subclass and only the
  // `RowReference` and the `IdTable` have direct access to it.
  template <typename RowReferenceWithRestrictedAccess>
  friend class RowReference;

  template <typename T, int NumCols, typename Allocator, IsView isView>
  friend class IdTable;

  // The actual implementation of a reference to a row that allows mutable
  // access only for rvalues. The rather long name is chosen deliberately so
  // that the most likely problem becomes visible from the compiler's error
  // message. The class is templated on the underlying table and on whether this
  // is a const or a mutable reference.
  template <typename Table, int NumCols, ad_utility::IsConst isConstTag>
  class RowReferenceWithRestrictedAccess;
};

// This mixin class implements most of the common functionality for the
// restricted row references. In particular, it consists of common code for the
// restricted row references for the dynamic and static IdTables.
// The first template parameter `Impl` is the type of the actual
// `RestrictedRowReference` for which this mixin is used. For this to work, the
// `Impl` class has to provide all the `...Impl` functions that the mixin
// implicitly uses, and the `Impl` must inherit from the mixin.
template <typename Impl, ad_utility::IsConst isConstTag, typename RowType>
class RestrictedRowReferenceMixin {
 public:
  constexpr static bool isConst = isConstTag == ad_utility::IsConst::True;
  // Access to the `i`-th columns of this row. Only allowed for const values
  // and for rvalues.
  auto& operator[](size_t i) && requires(!isConst) {
    return Impl::operatorBracketImpl(static_cast<Impl&>(*this), i);
  }
  const auto& operator[](size_t i) const& {
    return Impl::operatorBracketImpl(static_cast<const Impl&>(*this), i);
  }
  const auto& operator[](size_t i) const&& {
    return Impl::operatorBracketImpl(static_cast<const Impl&&>(*this), i);
  }

 private:
  // Swap two `RowReference`s, but only if they are temporaries (rvalues).
  // This modifies the underlying table. The `localSwapImpl` is needed because
  // we cannot use private functions from the `Impl` in `friend` functions, even
  // if this class is a friend of the `Impl`.
  static void localSwapImpl(Impl&& a, Impl&& b) requires(!isConst) {
    return Impl::swapImpl(AD_FWD(a), AD_FWD(b));
  }

 public:
  friend void swap(Impl&& a, Impl&& b) requires(!isConst) {
    return localSwapImpl(AD_FWD(a), AD_FWD(b));
  }

  // The equality operator for rows can be implemented in terms of elementwise
  // equality.
  friend bool operator==(const Impl& a, const Impl& b) {
    if (a.numColumns() != b.numColumns()) {
      return false;
    }
    for (size_t i = 0; i < a.numColumns(); ++i) {
      if (a[i] != b[i]) {
        return false;
      }
    }
    return true;
  }
  // Assignment from a `Row` with the same number of columns.
  // Note that the copy assignment operators cannot be implemented inside the
  // mixin, because they cannot be inherited.
  Impl& operator=(const RowType& other) && {
    return Impl::assignmentImpl(static_cast<Impl&>(*this), other);
  }

  Impl& operator=(const RowType& other) const&&;

  // Iterators
  // Non-const iterators allow non-const access and are therefore only allowed
  // on rvalues.
  auto begin() && {
    return std::invoke(&Impl::beginImpl, static_cast<Impl&&>(*this));
  };
  auto end() && {
    return std::invoke(&Impl::endImpl, static_cast<Impl&&>(*this));
  };
  // Const iterators are always fine.
  auto cbegin() const {
    return std::invoke(&Impl::cbeginImpl, static_cast<const Impl&>(*this));
  };
  auto cend() const {
    return std::invoke(&Impl::cendImpl, static_cast<const Impl&>(*this));
  };
  auto begin() const& {
    return std::invoke(&Impl::cbeginImpl, static_cast<const Impl&>(*this));
  };
  auto end() const& {
    return std::invoke(&Impl::cendImpl, static_cast<const Impl&>(*this));
  };
  auto begin() const&& {
    return std::invoke(&Impl::cbeginImpl, static_cast<const Impl&&>(*this));
  };
  auto end() const&& {
    return std::invoke(&Impl::cendImpl, static_cast<const Impl&&>(*this));
  };
};

// The restricted row reference for dynamic `IdTables` (with `numStaticCols ==
// 0`).
template <typename Table, ad_utility::IsConst isConstTagV>
class RowReferenceImpl::RowReferenceWithRestrictedAccess<Table, 0, isConstTagV>
    : public RestrictedRowReferenceMixin<
          RowReferenceImpl::RowReferenceWithRestrictedAccess<Table, 0,
                                                             isConstTagV>,
          isConstTagV, typename Table::row_type> {
 public:
  // Several constants and member types, some of those are required by the
  // interface of the `RowReference` class, whereas some others are only defined
  // for convenience.
  static constexpr ad_utility::IsConst isConstTag = isConstTagV;
  static constexpr bool isConst = isConstTag == ad_utility::IsConst::True;
  using table_type = Table;
  using TablePtr = std::conditional_t<isConst, const Table*, Table*>;
  using value_type = typename Table::value_type;
  static constexpr int numStaticColumns = Table::numStaticColumns;
  using row_type = Row<value_type, numStaticColumns>;

  // Grant the `IdTable` class access to the internal details.
  template <typename T, int NumColsOfFriend, typename Allocator, IsView isView>
  friend class IdTable;

  // Make the mixin a friend and use its assignment operator from rows.
  using Mixin = RestrictedRowReferenceMixin<RowReferenceWithRestrictedAccess,
                                            isConstTag, row_type>;
  friend class RestrictedRowReferenceMixin<RowReferenceWithRestrictedAccess,
                                           isConstTag, row_type>;
  using Mixin::operator=;

 private:
  // The `Table` (as a pointer) and the row (as an index) to which this
  // reference points.
  //
  TablePtr table_ = nullptr;
  size_t row_ = 0;

  // The constructor is public, but the whole class is private. the
  // constructor must be public, because `IdTable` must have access to it.
 public:
  explicit RowReferenceWithRestrictedAccess(TablePtr table, size_t row)
      : table_{table}, row_{row} {}
  // The number of columns that this row contains.
  size_t numColumns() const { return table_->numColumns(); }
  size_t size() const { return numColumns(); }

  size_t rowIndex() const { return row_; }

 protected:
  // The actual implementation of operator[].
  static auto& operatorBracketImpl(auto& self, size_t i) {
    return (*self.table_)(self.row_, i);
  }

  // Define iterators over a single row.
  template <typename RowT>
  struct IteratorHelper {
    decltype(auto) operator()(auto&& row, size_t colIdx) const {
      return std::decay_t<decltype(row)>::operatorBracketImpl(AD_FWD(row),
                                                              colIdx);
    }
  };
  using iterator = ad_utility::IteratorForAccessOperator<
      RowReferenceWithRestrictedAccess,
      IteratorHelper<RowReferenceWithRestrictedAccess>,
      ad_utility::IsConst::False>;
  using const_iterator = ad_utility::IteratorForAccessOperator<
      RowReferenceWithRestrictedAccess,
      IteratorHelper<RowReferenceWithRestrictedAccess>,
      ad_utility::IsConst::True>;

  // The implementation of swapping two `RowReference`s (passed either by
  // value or by reference).
  static void swapImpl(auto&& a, auto&& b) requires(!isConst) {
    for (size_t i = 0; i < a.numColumns(); ++i) {
      std::swap(operatorBracketImpl(a, i), operatorBracketImpl(b, i));
    }
  }

  // Protected implementation for the "ordinary" non-const iterators for
  // lvalues s.t. the not restricted child class can access them.
  iterator beginImpl() { return {this, 0}; }
  iterator endImpl() { return {this, numColumns()}; }
  const_iterator cbeginImpl() const { return {this, 0}; };
  const_iterator cendImpl() const { return {this, numColumns()}; };

 public:
  // Equality comparison. Works between two `RowReference`s, but also between
  // a `RowReference` and a `Row` if the number of columns match.
  template <typename U>
  bool operator==(const U& other) const
      requires(numStaticColumns == U::numStaticColumns) {
    if (numColumns() != other.numColumns()) {
      return false;
    }
    for (size_t i = 0; i < numColumns(); ++i) {
      if ((*this)[i] != other[i]) {
        return false;
      }
    }
    return true;
  }

  // Convert from a `RowReference` to a `Row`.
  operator row_type() const {
    auto numCols = (std::move(*this)).numColumns();
    row_type result{numCols};
    for (size_t i = 0; i < numCols; ++i) {
      result[i] = std::move(*this)[i];
    }
    return result;
  }

 protected:
  // Internal implementation of the assignment from a `Row` as well as a
  // `RowReference`. This assignment actually writes to the underlying table.
  static RowReferenceWithRestrictedAccess& assignmentImpl(auto&& self,
                                                          const auto& other) {
    if constexpr (numStaticColumns == 0) {
      AD_CONTRACT_CHECK(self.numColumns() == other.numColumns());
    }
    for (size_t i = 0; i < self.numColumns(); ++i) {
      operatorBracketImpl(self, i) = other[i];
    }
    return self;
  }

 public:
  RowReferenceWithRestrictedAccess& operator=(
      const RowReferenceWithRestrictedAccess& other) && {
    return assignmentImpl(*this, other);
  }

 protected:
  // No need to copy this internal type, but the implementation of the
  // `RowReference` class below requires it,
  // so the copy Constructor is protected.
  RowReferenceWithRestrictedAccess(const RowReferenceWithRestrictedAccess&) =
      default;
};

// The actual `RowReference` type that should be used externally when a
// reference actually needs to be stored. Most of its implementation is
// inherited from or delegated to the `RowReferenceWithRestrictedAccess`
// class above, but it also supports mutable access to lvalues.
template <typename RowReferenceWithRestrictedAccess>
class RowReference : public RowReferenceWithRestrictedAccess {
 public:
  using Base = RowReferenceWithRestrictedAccess;
  using table_type = typename Base::table_type;
  using row_type = typename Base::row_type;
  using T = typename Base::value_type;
  using value_type = T;
  static constexpr bool isConst = Base::isConst;
  using RowReferenceWithRestrictedAccess::isConstTag;
  static constexpr int numStaticColumns = Base::numStaticColumns;

 private:
  // Efficient access to the base class subobject to invoke its functions.
  Base& base() { return static_cast<Base&>(*this); }
  const Base& base() const { return static_cast<const Base&>(*this); }

 public:
  // The constructor from the base class is deliberately implicit because we
  // want the following code to work:
  // `RowReference r = someFunctionThatReturnsABase();`
  RowReference(Base b) : Base{std::move(b)} {}

  // Inherit the constructors from the base class.
  using Base::Base;

  // Access to the `i`-th column of this row.
  T& operator[](size_t i) requires(!isConst) {
    return Base::operatorBracketImpl(base(), i);
  }
  const T& operator[](size_t i) const {
    return Base::operatorBracketImpl(base(), i);
  }

  // The iterators are implemented in the base class and can simply be
  // forwarded.
  auto begin() { return Base::beginImpl(); };
  auto end() { return Base::endImpl(); };
  auto begin() const { return Base::begin(); };
  auto end() const { return Base::end(); };
  // The `cbegin` and `cend` functions are implicitly inherited from `Base`.

  // __________________________________________________________________________
  template <ad_utility::SimilarTo<RowReference> R>
  friend void swap(R&& a, R&& b) requires(!isConst) {
    return Base::swapImpl(AD_FWD(a), AD_FWD(b));
  }

  // Equality comparison. Works between two `RowReference`s, but also between
  // a `RowReference` and a `Row` if the number of columns match.
  template <typename T>
  bool operator==(const T& other) const
      requires(numStaticColumns == T::numStaticColumns) {
    return base() == other;
  }

 public:
  // Assignment from a `Row` with the same number of columns.
  RowReference& operator=(const Row<T, numStaticColumns>& other) & {
    this->assignmentImpl(base(), other);
    return *this;
  }
  RowReference& operator=(const Row<T, numStaticColumns>& other) && {
    this->assignmentImpl(base(), other);
    return *this;
  }
  RowReference& operator=(const Row<T, numStaticColumns>& other) const&&;

  // Assignment from a `RowReference` with the same number of columns.
  RowReference& operator=(const RowReference& other) {
    this->assignmentImpl(base(), other);
    return *this;
  }

  // No need to copy `RowReference`s, because that is also most likely a bug.
  // Currently none of our functions or of the STL-algorithms require it.
  // If necessary, we can still enable it in the future.
  RowReference(const RowReference&) = delete;
};

// A reference to a row for the case where the number of columns is statically
// known. We store a `std::array<T*>` which allows for loop unrolling and other
// compiler optimizations.
template <typename Table, int NumCols, ad_utility::IsConst isConstTagV>
requires(NumCols > 0)
class RowReferenceImpl::RowReferenceWithRestrictedAccess<Table, NumCols,
                                                         isConstTagV>
    : public RestrictedRowReferenceMixin<
          RowReferenceWithRestrictedAccess<Table, NumCols, isConstTagV>,
          isConstTagV, typename Table::row_type> {
 public:
  // Typedefs and constants, some of them are needed as part of the external
  // interface because the `RowReference` template requires them.
  using table_type = Table;
  static constexpr ad_utility::IsConst isConstTag = isConstTagV;
  static constexpr bool isConst = isConstTag == ad_utility::IsConst::True;
  using value_type = typename Table::value_type;
  using row_type = Row<value_type, NumCols>;
  using Ptr = std::conditional_t<isConst, const value_type*, value_type*>;
  // We store one pointer per column of the IdTable.
  using Ptrs = std::array<Ptr, NumCols>;
  static constexpr int numStaticColumns = Table::numStaticColumns;

  using Mixin = RestrictedRowReferenceMixin<RowReferenceWithRestrictedAccess,
                                            isConstTag, row_type>;
  friend class RestrictedRowReferenceMixin<RowReferenceWithRestrictedAccess,
                                           isConstTag, row_type>;

  using Mixin::operator=;

  static_assert(Table::numStaticColumns == NumCols);
  static_assert(NumCols > 0);

  constexpr static size_t numColumns() { return NumCols; }

 private:
  // The actual array of pointers to values in the IdTable.
  Ptrs ptrs_;
  // The iterator needs access to the internals.
  template <typename Reference, typename RestrictedReference>
  friend struct IdTableIterator;

  // Transform the array of pointers to a view of references for easier
  // implementation of several member functions.
  auto transformToRef() {
    return std::views::transform(
        ptrs_, [](auto& ptr) -> decltype(auto) { return *ptr; });
  }

  auto transformToRef() const {
    return std::views::transform(
        ptrs_, [](auto& ptr) -> decltype(auto) { return *ptr; });
  }

 public:
  // Printing for googletest.
  friend void PrintTo(const RowReferenceWithRestrictedAccess& ref,
                      std::ostream* os) {
    *os << ad_utility::lazyStrJoin(ref.transformToRef(), ", ");
  }

 protected:
  // Iterators for iterating over a single row.
  auto beginImpl() { return transformToRef().begin(); };
  auto endImpl() { return transformToRef().end(); };
  auto cbeginImpl() const { return transformToRef().begin(); };
  auto cendImpl() const { return transformToRef().end(); };

  // The actual implementation of operator[].
  static decltype(auto) operatorBracketImpl(auto& self, size_t i) {
    return *self.ptrs_[i];
  }

  // The implementation of swapping two `RowReference`s (passed either by
  // value or by reference).
  static void swapImpl(auto&& a, auto&& b) requires(!isConst) {
    for (size_t i = 0; i < NumCols; ++i) {
      std::swap(operatorBracketImpl(a, i), operatorBracketImpl(b, i));
    }
  }

  // The implementation for assignment from a `Row` and copy assignment.
  static RowReferenceWithRestrictedAccess& assignmentImpl(auto&& self,
                                                          auto&& other) {
    for (size_t i = 0; i < NumCols; ++i) {
      operatorBracketImpl(self, i) = AD_FWD(other)[i];
    }
    return self;
  }

 public:
  // Copy assignment is only allowed for rvalue-references (see the comments at
  // the beginning of the file, this is the implementation of the restricted
  // references).
  RowReferenceWithRestrictedAccess& operator=(
      const RowReferenceWithRestrictedAccess& other) && {
    return assignmentImpl(*this, other);
  }

  RowReferenceWithRestrictedAccess& operator=(
      const RowReferenceWithRestrictedAccess& other) const&&;

  // Constructors.
  RowReferenceWithRestrictedAccess() = default;
  // The copy constructor copies the pointers.
  RowReferenceWithRestrictedAccess(const RowReferenceWithRestrictedAccess&) =
      default;

  // Manually construct from an array of pointers
  RowReferenceWithRestrictedAccess(const Ptrs& arr) : ptrs_(arr) {}

  // Convert from a `RowReference` to a `Row`.
  operator row_type() const {
    row_type result{NumCols};
    for (size_t i = 0; i < NumCols; ++i) {
      result[i] = *ptrs_[i];
    }
    return result;
  }

  // Convert from a `RowReference` to `std::array`.
  operator std::array<value_type, NumCols>() const {
    std::array<value_type, NumCols> result;
    for (size_t i = 0; i < NumCols; ++i) {
      result[i] = *ptrs_[i];
    }
    return result;
  }

  // Change all the pointers by the given offset. This reference then points to
  // the row at `indexOfPreviousRow + offset`.
  void increase(std::ptrdiff_t offset) {
    for (size_t i = 0; i < NumCols; ++i) {
      ptrs_[i] += offset;
    }
  }
};

}  // namespace columnBasedIdTable
