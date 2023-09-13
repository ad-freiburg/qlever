// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <array>
#include <iostream>
#include <type_traits>
#include <variant>
#include <vector>
#include <ranges>

#include "util/Enums.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/Iterators.h"
#include "util/TypeTraits.h"
#include "util/StringUtils.h"
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
  // This modifies the underlying table.
  static void localSwapImpl(Impl&& a, Impl&& b) requires(!isConst) {
    return Impl::swapImpl(AD_FWD(a), AD_FWD(b));
  }

 public:
  friend void swap(Impl&& a, Impl&& b) requires(!isConst) {
    return localSwapImpl(AD_FWD(a), AD_FWD(b));
  }
  // Equality is implemented in terms of the values that the reference points
  // to.
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
  Impl& operator=(const RowType& other) && {
    return Impl::assignmentImpl(static_cast<Impl&>(*this), other);
  }

  Impl& operator=(const RowType& other) const&&;
};

template <typename Table, ad_utility::IsConst isConstTagV>
    class RowReferenceImpl::RowReferenceWithRestrictedAccess<Table, 0, isConstTagV> : public RestrictedRowReferenceMixin<RowReferenceImpl::RowReferenceWithRestrictedAccess<Table, 0, isConstTagV>, isConstTagV, typename Table::row_type> {
    public:
        static constexpr ad_utility::IsConst isConstTag = isConstTagV;
        static constexpr bool isConst = isConstTag == ad_utility::IsConst::True;
        using table_type = Table;
        using TablePtr = std::conditional_t<isConst, const Table*, Table*>;
        using T = typename Table::value_type;
        using value_type = T;
        static constexpr int numStaticColumns = Table::numStaticColumns;
        using row_type = Row<T, numStaticColumns>;
        // TODO<joka921> activate this as soon as we have the static reference activated.

        // Grant the `IdTable` class access to the internal details.
        template <typename T, int NumColsOfFriend, typename Allocator, IsView isView>
        friend class IdTable;

        using Mixin = RestrictedRowReferenceMixin<RowReferenceWithRestrictedAccess, isConstTag, row_type>;
        friend class RestrictedRowReferenceMixin<RowReferenceWithRestrictedAccess, isConstTag, row_type>;
        using Mixin::operator=;

    private:
        // Make the long class type a little shorter where possible.
        using This = RowReferenceWithRestrictedAccess;

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

    protected:
        // The actual implementation of operator[].
        static T& operatorBracketImpl(auto& self, size_t i)
        requires(!std::is_const_v<std::remove_reference_t<decltype(self)>> &&
                 !isConst) {
            return (*self.table_)(self.row_, i);
        }
        static const T& operatorBracketImpl(const auto& self, size_t i) {
            return (*self.table_)(self.row_, i);
        }

    public:
        // Define iterators.
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
        // Non-const iterators allow non-const access and are therefore only allowed
        // on rvalues.
        iterator begin() && { return {this, 0}; };
        iterator end() && { return {this, numColumns()}; };
        // Const iterators are always fine.
        const_iterator cbegin() const { return {this, 0}; };
        const_iterator cend() const { return {this, numColumns()}; };
        const_iterator begin() const& { return {this, 0}; };
        const_iterator end() const& { return {this, numColumns()}; };
        const_iterator begin() const&& { return {this, 0}; };
        const_iterator end() const&& { return {this, numColumns()}; };

        // The number of columns that this row contains.
        size_t numColumns() const { return table_->numColumns(); }

        size_t rowIndex() const { return row_; }

        size_t size() const { return numColumns(); }

        // TODO<joka921> Make protected and the Mixin a friend.
   public:
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
        operator Row<T, numStaticColumns>() const {
            auto numCols = (std::move(*this)).numColumns();
            Row<T, numStaticColumns> result{numCols};
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
                AD_CONTRACT_CHECK(self.numColumns() == other.numColumns());
            }
            for (size_t i = 0; i < self.numColumns(); ++i) {
                operatorBracketImpl(self, i) = other[i];
            }
            return self;
        }
       public:
        This& operator=(const This& other) && {
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
    class RowReference
    : public RowReferenceWithRestrictedAccess {
    public:
  using Base =
     RowReferenceWithRestrictedAccess;
  using table_type = typename Base::table_type;
  using row_type = typename Base::row_type;
  using T = typename Base::T;
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
  // TODO<joka921> get rid of `auto`
  /*
  typename Base::iterator begin() { return Base::beginImpl(); };
  typename Base::iterator end() { return Base::endImpl(); };
  typename Base::const_iterator begin() const { return Base::begin(); };
  typename Base::const_iterator end() const { return Base::end(); };
   */
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

  /*
  // TODO<joka921> actually this should be forbidden, as it violates the constness.
  // But what could maybe be useful is the copy construction of const references from non-const references.
  // Assignment from a `const` RowReference to a `mutable` RowReference
  RowReference& operator=(
      const RowReference<Table, ad_utility::IsConst::True>& other)
      requires(!isConst) {
    this->assignmentImpl(base(), other);
    return *this;
  }
   */

  // No need to copy `RowReference`s, because that is also most likely a bug.
  // Currently none of our functions or of the STL-algorithms require it.
  // If necessary, we can still enable it in the future.
  RowReference(const RowReference&) = delete;
};
// A reference to a row for the case where the number of columns is statically
// known. We store a `std::array<T*>` which allows for loop unrolling and other
// compiler optimizations.
// TODO<joka921> We should also implement the `restriction` mechanism.
// TODO<joka921> Maybe we can make the `ordinary` row reference a template
// specialization of this class to reduce the number of different classes we
// have in this file.
 template <typename Table, int NumCols, ad_utility::IsConst isConstTagV> requires (NumCols > 0)
class RowReferenceImpl::RowReferenceWithRestrictedAccess<Table, NumCols, isConstTagV> : public RestrictedRowReferenceMixin<RowReferenceWithRestrictedAccess<Table, NumCols, isConstTagV>, isConstTagV, typename Table::row_type>{
 public:
  using table_type = Table;
  static constexpr ad_utility::IsConst isConstTag = isConstTagV;
  static constexpr bool isConst = isConstTag == ad_utility::IsConst::True;
  using T = typename Table::value_type;
  using row_type = Row<T, NumCols>;
  using Ptr = std::conditional_t<isConst, const T*, T*>;
  using Ref = std::conditional_t<isConst, const T&, T&>;
  using ConstRef = const T&;
  // We store one pointer per column of the IdTable.
  using Ptrs = std::array<Ptr, NumCols>;
  Ptrs ptrs_;
  static constexpr int numStaticColumns = Table::numStaticColumns;

 using Mixin = RestrictedRowReferenceMixin<RowReferenceWithRestrictedAccess, isConstTag, row_type>;
 friend class RestrictedRowReferenceMixin<RowReferenceWithRestrictedAccess, isConstTag, row_type>;

 using Mixin::operator=;

  static_assert(Table::numStaticColumns == NumCols);
  static_assert(NumCols > 0);

  constexpr static size_t numColumns() { return NumCols; }

  // TODO<joka921> make protected
    // The actual implementation of operator[].
    static decltype(auto) operatorBracketImpl(auto& self, size_t i) {
        return *self.ptrs_[i];
    }

 private:
  // Transform the array of pointers to a view of references for easier
  // implementation of several member functions.
  auto transformToRef() {
    return std::views::transform(ptrs_, [](auto& ptr) -> Ref { return *ptr; });
  }
  auto transformToRef() const {
    return std::views::transform(ptrs_,
                                 [](auto& ptr) -> ConstRef { return *ptr; });
  }

 public:
  // Printing for googletest.
  friend void PrintTo(const RowReferenceWithRestrictedAccess& ref, std::ostream* os) {
    *os << ad_utility::lazyStrJoin(ref.transformToRef(), ", ");
  }

  // Iterators for iterating over a single row.
  auto begin() { return transformToRef().begin(); };
  auto beginImpl() { return transformToRef().begin(); };
  auto end() { return transformToRef().end(); };
  auto endImpl() { return transformToRef().end(); };
  auto begin() const { return transformToRef().begin(); };
  auto end() const { return transformToRef().end(); };
  auto cbegin() const { return transformToRef().begin(); };
  auto cend() const { return transformToRef().end(); };

 protected:
  // The implementation of swapping two `RowReference`s (passed either by
  // value or by reference).
  static void swapImpl(auto&& a, auto&& b) requires(!isConst) {
      for (size_t i = 0; i < NumCols; ++i) {
          std::swap(operatorBracketImpl(a, i), operatorBracketImpl(b, i));
    }
  }


  /*
  // Equality is implemented in terms of the values that the reference points
  // to.
  bool operator==(const RowReferenceWithRestrictedAccess& other) const {
    for (size_t i = 0; i < NumCols; ++i) {
      if ((*this)[i] != other[i]) {
        return false;
      }
    }
    return true;
  }
   */

  // TODO<joka921> solve using `friend`
 public:
  // Assignment from a `Row` and copy assignment.
  static RowReferenceWithRestrictedAccess& assignmentImpl(auto&& self, auto&& other) {
    for (size_t i = 0; i < NumCols; ++i) {
      operatorBracketImpl(self, i) = AD_FWD(other)[i];
    }
    return self;
  }

 public:

  RowReferenceWithRestrictedAccess& operator=(const RowReferenceWithRestrictedAccess& other) && {
    return assignmentImpl(*this, other);
  }
  RowReferenceWithRestrictedAccess& operator=(const RowReferenceWithRestrictedAccess& other) const&&;

  // The copy constructor copies the pointers.
  RowReferenceWithRestrictedAccess(const RowReferenceWithRestrictedAccess&) = default;
  RowReferenceWithRestrictedAccess(const Ptrs& arr) : ptrs_(arr) {}
  RowReferenceWithRestrictedAccess() = default;

  // Convert from a `RowReference` to a `Row`.
  operator Row<T, NumCols>() const {
    Row<T, NumCols> result{NumCols};
    for (size_t i = 0; i < NumCols; ++i) {
      result[i] = *ptrs_[i];
    }
    return result;
  }

  // Convert from a `RowReference` to `std::array`.
  operator std::array<T, NumCols>() const {
    std::array<T, NumCols> result;
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

template <typename Reference>
concept RowRefC = std::is_integral_v<decltype(Reference::numStaticColumns)> && ad_utility::SimilarTo<ad_utility::IsConst, decltype(Reference::isConstTag)>;

template <RowRefC Reference, typename RestrictedReference>
struct IdTableIterator;

// Helper `struct` that stores a pointer to this table and has an `operator()`
// that can be called with a reference to an `IdTable` and the index of a row
// and then returns a `row_reference_restricted` to that row. This struct is
// used to automatically create random access iterators using the
// `ad_utility::IteratorForAccessOperator` template.
    template <typename ReferenceType>
    struct IteratorHelper {
        auto operator()(auto&& idTable, size_t rowIdx) const {
            return ReferenceType{&idTable, rowIdx};
        }
    };

    template <RowRefC Reference, typename RestrictedReference> requires (Reference::numStaticColumns == 0)
struct IdTableIterator<Reference, RestrictedReference>: public
    ad_utility::IteratorForAccessOperatorMixin<IdTableIterator<Reference, RestrictedReference>,
            typename Reference::table_type, IteratorHelper<RestrictedReference>,
            RestrictedReference::isConstTag, typename RestrictedReference::row_type, Reference> {
        using Base = ad_utility::IteratorForAccessOperatorMixin<
            IdTableIterator<Reference, RestrictedReference>,
            typename Reference::table_type, IteratorHelper<RestrictedReference>,
            RestrictedReference::isConstTag,
            typename RestrictedReference::row_type, Reference>;
        using Base::Base;
        IdTableIterator() = default;
        IdTableIterator(const Base& b) : Base{b} {}
    IdTableIterator(Base&& b) : Base{std::move(b)} {}
    };



// A random access iterator for static `IdTables` that uses the
// `StaticRowReference` from above.
template <RowRefC Reference, typename RestrictedReference> requires (RestrictedReference::numStaticColumns > 0)
struct IdTableIterator<Reference, RestrictedReference> {
  using reference = Reference;
  using T = typename reference::value_type;
  static constexpr size_t N = reference::numStaticColumns;
  static constexpr bool isConst = reference::isConst;
  using value_type = Row<T, N>;
  using pointer = value_type*;
  RestrictedReference ref_;
  using iterator_category = std::random_access_iterator_tag;
  using difference_type = int64_t;

  explicit IdTableIterator(const reference::Ptrs& arr) : ref_{arr} {}

  template <ad_utility::SimilarTo<typename Reference::table_type> Table>
  IdTableIterator(Table* table, size_t index)
      : ref_{[&]() {
          typename RestrictedReference::Ptrs arr;
          for (size_t i : std::views::iota(0u, N)) {
            arr[i] = table->getColumn(i).data() + index;
          }
      return arr;
  }()} {}

 private:
 public:
  IdTableIterator& operator=(const IdTableIterator& other) {
    for (size_t i = 0; i < N; ++i) {
      ref_.ptrs_[i] = other.ref_.ptrs_[i];
    }
    return *this;
  }
  IdTableIterator& operator=(IdTableIterator&& other) {
    for (size_t i = 0; i < N; ++i) {
      ref_.ptrs_[i] = other.ref_.ptrs_[i];
    }
    return *this;
  }

  IdTableIterator(const IdTableIterator& other) {
    for (size_t i = 0; i < N; ++i) {
      ref_.ptrs_[i] = other.ref_.ptrs_[i];
    }
  }
  IdTableIterator(IdTableIterator&& other) {
    for (size_t i = 0; i < N; ++i) {
      ref_.ptrs_[i] = other.ref_.ptrs_[i];
    }
  }

  IdTableIterator() = default;

  auto operator<=>(const IdTableIterator& rhs) const {
    return ref_.ptrs_[0] <=> rhs.ref_.ptrs_[0];
  }

  bool operator==(const IdTableIterator& rhs) const {
    return ref_.ptrs_[0] == rhs.ref_.ptrs_[0];
  }

  IdTableIterator& operator+=(difference_type n) {
    ref_.increase(n);
    return *this;
  }
  IdTableIterator operator+(difference_type n) const {
    IdTableIterator result{*this};
    result += n;
    return result;
  }

  IdTableIterator& operator++() {
    ref_.increase(1);
    return *this;
  }
  IdTableIterator operator++(int) & {
    IdTableIterator result{*this};
    ++result;
    return result;
  }

  IdTableIterator& operator--() {
    ref_.increase(-1);
    return *this;
  }
  IdTableIterator operator--(int) & {
    IdTableIterator result{*this};
    --result;
    return result;
  }

  friend IdTableIterator operator+(difference_type n,
                                         const IdTableIterator& it) {
    return it + n;
  }

  IdTableIterator& operator-=(difference_type n) {
    ref_.increase(-n);
    return *this;
  }

  IdTableIterator operator-(difference_type n) const {
    IdTableIterator result{*this};
    result -= n;
    return result;
  }

  difference_type operator-(const IdTableIterator& rhs) const {
    return ref_.ptrs_[0] - rhs.ref_.ptrs_[0];
  }

  // TODO<joka921> We have shallow constness here...
  RestrictedReference operator*() const { return ref_; }
  RestrictedReference operator*() requires(!isConst) { return ref_; }

  RestrictedReference operator[](difference_type n) const {
    auto ref = ref_;
    ref.increase(n);
    return ref;
  }
};

}  // namespace columnBasedIdTable
