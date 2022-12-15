// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <array>
#include <iostream>
#include <type_traits>
#include <variant>
#include <vector>

#include "global/Id.h"
#include "util/Enums.h"
#include "util/UninitializedAllocator.h"

namespace columnBasedIdTable {

// A simple tag enum to differentiate between "views" (non-owning data
// structures) and "ordinary" data structures that own their storage (see
// below).
enum struct IsView { True, False };

// A row of a table of IDs. It stores the IDs as a `std::array` or `std::vector`
// depending on whether `NumColumns` is 0 (which means that the number of
// columns is specified at runtime). This class is used as the `value_type` of
// the columns-ordered `IdTable` and must be used whenever a row (not a
// reference to a row) has to be stored outside the IdTable. The implementation
// is a rather thin wrapper around `std::vector<Id>` or `std::array<Id,
// NumColumns>`, respectively (see above).
template <int NumColumns = 0>
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
      std::vector<Id,
                  ad_utility::default_init_allocator<Id, std::allocator<Id>>>,
      std::array<Id, NumColumns>>;

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
  Id& operator[](size_t i) { return data_[i]; }
  const Id& operator[](size_t i) const { return data_[i]; }

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
  template <typename Table, ad_utility::IsConst isConst>
  friend class RowReference;

  template <int NumCols, typename Allocator, IsView isView>
  friend class IdTable;

  // The actual implementation of a reference to a row that allows mutable
  // access only for rvalues. The rather long name is chosen deliberately so
  // that the most likely problem becomes visible from the compiler's error
  // message. The class is templated on the underlying table and on whether this
  // is a const or a mutable reference.
  template <typename Table, ad_utility::IsConst isConstTag>
  class RowReferenceWithRestrictedAccess {
   public:
    static constexpr bool isConst = isConstTag == ad_utility::IsConst::True;
    using TablePtr = std::conditional_t<isConst, const Table*, Table*>;
    static constexpr int numStaticColumns = Table::numStaticColumns;

    // Grant the `IdTable` class access to the internal details.
    template <int NumCols, typename Allocator, IsView isView>
    friend class IdTable;

   private:
    // Make the long class type a little shorter where possible.
    using This = RowReferenceWithRestrictedAccess;

   private:
    // The `Table` (as a pointer) and the row (as an index) to which this
    // reference points.
    //
    // TODO<joka921> Only storing the row index makes the implementation easy,
    // but possibly harms the performance because every access to a reference
    // involves a multiplication. However, this cannot simply be fixed inside
    // the row reference, but needs iterators/references to single columns and
    // special algorithms that are aware of the column-based structure of the
    // `IdTable`.
    TablePtr table_ = nullptr;
    size_t row_ = 0;

    // The constructor is public, but the whole class is private. the
    // constructor must be public, because `IdTable` must have access to it.
   public:
    explicit RowReferenceWithRestrictedAccess(TablePtr table, size_t row)
        : table_{table}, row_{row} {}

   protected:
    // The actual implementation of operator[].
    static decltype(auto) getImpl(auto&& self, size_t i) {
      return (*self.table_)(self.row_, i);
    }

   public:
    // Access to the `i`-th columns of this row. Only allowed for const values
    // and for rvalues.
    Id& operator[](size_t i) && requires(!isConst) { return getImpl(*this, i); }
    const Id& operator[](size_t i) const& { return getImpl(*this, i); }
    const Id& operator[](size_t i) const&& { return getImpl(*this, i); }

    // The number of columns that this row contains.
    size_t numColumns() const { return table_->numColumns(); }

   protected:
    // The implementation of swapping two `RowReference`s (passed either by
    // value or by reference).
    static void swapImpl(auto&& a, auto&& b) requires(!isConst) {
      for (size_t i = 0; i < a.numColumns(); ++i) {
        std::swap(getImpl(a, i), getImpl(b, i));
      }
    }

   public:
    // Swap two `RowReference`s, but only if they are temporaries (rvalues).
    // This modifies the underlying table.
    friend void swap(This&& a, This&& b) requires(!isConst) {
      return swapImpl(a, b);
    }

    // Equality comparison. Works between two `RowReference`s, but also between
    // a `RowReference` and a `Row` if the number of columns match.
    template <typename T>
    bool operator==(const T& other) const
        requires(numStaticColumns == T::numStaticColumns) {
      if constexpr (numStaticColumns == 0) {
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
    operator Row<numStaticColumns>() const {
      auto numCols = (std::move(*this)).numColumns();
      Row<numStaticColumns> result{numCols};
      for (size_t i = 0; i < numCols; ++i) {
        result[i] = std::move(*this)[i];
      }
      return result;
    }

   protected:
    // Internal implementation of the assignment from a `Row` as well as a
    // `RowReference`. This assignment actually writes to the underlying table.
    static This& assignmentImpl(auto&& self, const auto& other) {
      if constexpr (numStaticColumns == 0) {
        AD_CHECK(self.numColumns() == other.numColumns());
      }
      for (size_t i = 0; i < self.numColumns(); ++i) {
        getImpl(self, i) = other[i];
      }
      return self;
    }

   public:
    // Assignment from a `Row` with the same number of columns.
    This& operator=(const Row<numStaticColumns>& other) && {
      return assignmentImpl(*this, other);
    }

    // Assignment from a `RowReference` with the same number of columns.
    This& operator=(const RowReferenceWithRestrictedAccess& other) && {
      return assignmentImpl(*this, other);
    }

    // Assignment from a `const` RowReference to a `mutable` RowReference
    This& operator=(const RowReferenceWithRestrictedAccess<
                    Table, ad_utility::IsConst::True>& other) &&
        requires(!isConst) {
      return assignmentImpl(*this, other);
    }

   protected:
    // No need to copy this internal type, but the implementation of the
    // `RowReference` class below requiress it,
    // so the copy Constructor is protected.
    RowReferenceWithRestrictedAccess(const RowReferenceWithRestrictedAccess&) =
        default;
  };
};

// The actual `RowReference` type that should be used externally when a
// reference actually needs to be stored. Most of its implementation is
// inherited from or delegated to the `RowReferenceWithRestrictedAccess`
// class above, but it also supports mutable access to lvalues.
template <typename Table, ad_utility::IsConst isConstTag>
class RowReference
    : public RowReferenceImpl::RowReferenceWithRestrictedAccess<Table,
                                                                isConstTag> {
 private:
  using Base =
      RowReferenceImpl::RowReferenceWithRestrictedAccess<Table, isConstTag>;
  using Base::numStaticColumns;
  using TablePtr = typename Base::TablePtr;
  static constexpr bool isConst = isConstTag == ad_utility::IsConst::True;

  // Efficient access to the base class subobject to invoke its functions.
  Base& base() { return static_cast<Base&>(*this); }
  const Base& base() const { return static_cast<const Base&>(*this); }

 public:
  // The constructor from the base class is deliberately implicit because we
  // want the following code to work:
  // `RowReference r = someFunctionThatReturnsABase();`
  RowReference(Base b) : Base{std::move(b)} {}

  // Access to the `i`-th column of this row.
  Id& operator[](size_t i) requires(!isConst) {
    return Base::getImpl(base(), i);
  }
  const Id& operator[](size_t i) const { return Base::getImpl(base(), i); }

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
  RowReference& operator=(const Row<numStaticColumns>& other) {
    return this->assignmentImpl(base(), other);
  }

  // Assignment from a `RowReference` with the same number of columns.
  RowReference& operator=(const RowReference& other) {
    return assignmentImpl(base(), other);
  }

  // Assignment from a `const` RowReference to a `mutable` RowReference
  RowReference& operator=(const RowReference<Table, ad_utility::IsConst::True>&
                              other) requires(!isConst) {
    return assignmentImpl(base(), other);
  }

  // No need to copy `RowReference`s, because that is also most likely a bug.
  // Currently none of our functions or of the STL-algorithms require it.
  // If necessary, we can still enable it in the future.
  RowReference(const RowReference&) = delete;
};

}  // namespace columnBasedIdTable
