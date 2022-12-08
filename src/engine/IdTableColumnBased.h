// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#pragma once

#include <array>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <variant>
#include <vector>

#include "engine/idTable/ColumnBasedRow.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/Iterators.h"
#include "util/LambdaHelpers.h"

namespace columnBasedIdTable {

// A 2D-Array with a fixed number of columns and a variable number of rows.
// With respect to the number of rows it allows for dynamic resizing at runtime,
// similar to `std::vector`. The number of columns can either be set at compile
// time, then `NumColumns != 0` and `NumColumns` is the number of columns. When
// `NumColumns == 0` then the number of columns has to be specified at runtime
// via the constructor or a call to the member function `setNumColumns()` before
// any elements have been inserted.
// The data layout is column-major, i.e. the elements of the same columns are
// adjacent in memory.
// Template parameters:
//   NumColumns: The number of columns (when != 0) or the information, that the
//            number of columns will be set at runtime (see above).
//   Allocator: The allocator type that will be used for the underlying storage.
//   isView : If true, then this is a const, and non-owning view of another
//            `IdTable`. Such views are cheap to copy as they just store
//            a const pointer to another `IdTable`.
// TODO<joka921> The NumColumns should be `size_t` but that requires several
// additional changes in the rest of the code.
template <int NumColumns = 0, typename Allocator = std::allocator<Id>,
          bool isView = false>
class IdTable {
 public:
  static constexpr bool isDynamic = NumColumns == 0;
  // Make the number of (statically known) columns accessible to the outside.
  static constexpr int numStaticColumns = NumColumns;
  // The actual storage is a plain 1-D vector with the logical columns
  // concatenated.
  using Columns = std::vector<Id, Allocator>;
  using Data = std::conditional_t<isView, const Columns*, Columns>;

  // Because of the column-major layout, the `row_type` (a value type that
  // stores the values of a  single row) and the `row_reference` (a type that
  // refers to a specific row of a specific `IdTable` are different. Those are
  // implemented in a way that makes it hard to use them incorrectly, for
  // details see the definition of the `Row` and `RowReference` class.
  using row_type = Row<NumColumns>;
  using row_reference = RowReference<IdTable, false>;
  using const_row_reference = RowReference<IdTable, true>;
  // TODO<joka921> Comment the proxies.
  using row_reference_proxy =
      RowReferenceImpl::DeducingRowReferenceViaAutoIsLikelyABug<IdTable, false>;
  using const_row_reference_proxy =
      RowReferenceImpl::DeducingRowReferenceViaAutoIsLikelyABug<IdTable, true>;
  using const_row_reference_view_proxy =
      RowReferenceImpl::DeducingRowReferenceViaAutoIsLikelyABug<
          IdTable<NumColumns, Allocator, true>, true>;

 private:
  Data data_;
  size_t numColumns_ = NumColumns;
  size_t size_ = 0;
  size_t capacity_ = 0;
  static constexpr size_t growthFactor = 2;

 public:
  // Construct from the number of columns and an allocator. If `NumColumns != 0`
  // Then the argument `numColumns` and `NumColumns` (the static and the
  // dynamic) number of columns) must be equal, else an assertion fails.
  IdTable(size_t numColumns,
          Allocator allocator = Allocator{}) requires(!isView)
      : data_(Columns(std::move(allocator))), numColumns_{numColumns} {
    if constexpr (!isDynamic) {
      AD_CHECK(NumColumns == numColumns);
    }
  }

  // Quasi the default constructor. If `NumColumns != 0` then the table is
  // already set up with the correct number of columns and can be used directly.
  // If `NumColumns == 0` then the number of columns has to be specified via
  // `setNumColumns()`.
  IdTable(Allocator allocator = {}) requires(!isView)
      : IdTable{NumColumns, std::move(allocator)} {};

 private:
  // Make the other instantiations of `IdTable` friends to allow for conversion
  // between them using a private interface.
  template <int, typename, bool>
  friend class IdTable;

  // Construct directly from the underlying storage. This is rather error-prone,
  // as the data has to have the correct layout and the remaining arguments also
  // have to match. For this reason it is private and only used to implement
  // the conversion functions `toStatic`, `toDynamic` and `asStaticView`
  // (see below).
  IdTable(Data data, size_t numColumns, size_t size, size_t capacity)
      : data_{std::move(data)},
        numColumns_{numColumns},
        size_{size},
        capacity_{capacity} {
    if constexpr (!isDynamic) {
      AD_CHECK(numColumns == NumColumns);
    }
    AD_CHECK(size_ <= capacity_);
    AD_CHECK(this->data().size() == numColumns_ * capacity_);
  }

 public:
  // For an empty and dynamic (`NumColumns == 0`) `IdTable`, specify the
  // number of columns.
  void setNumColumns(size_t cols) requires(NumColumns == 0) {
    AD_CHECK(size() == 0);
    numColumns_ = cols;
  }

  // The number of rows in the table. We deliberately have an explicitly named
  // function `numRows` as well as a generic `size` function as the latter can
  // be used to write generic code that also works for e.g.
  // `std::vector<someRowType>`
  size_t size() const { return size_; }
  size_t numRows() const { return size(); }

  // __________________________________________________________________________
  size_t numColumns() const {
    if constexpr (isDynamic) {
      return numColumns_;
    } else {
      return NumColumns;
    }
  }

  // Get access to the underlying `Allocator`.
  // Note: The allocator is always copied, because `std::vector`, which is
  // used internally only gives access to its allocator by value.
  Allocator getAllocator() const { return data().get_allocator(); }

  // Get access to a single element specified by the row and the column.
  // TODO<joka921, C++23> Use the multidimensional subscript operator.
  // TODO<joka921, C++23> Use explicit object parameters ("deducing this").
  Id& operator()(size_t row, size_t col) requires(!isView) {
    return *(startOfColumn(col) + row);
  }
  const Id& operator()(size_t row, size_t col) const {
    return *(startOfColumn(col) + row);
  }

  // Get a reference to the `i`-th row. The returned proxy objects can be
  // implicitly and trivially converted to `row_reference`. For the design
  // rationale behind those proxy types see above for their definition.
  row_reference_proxy operator[](size_t index) requires(!isView) {
    return *(begin() + index);
  }
  const_row_reference_proxy operator[](size_t index) const {
    return *(begin() + index);
  }

  // Set the size of the `IdTable` to `numRows`. If `numRows < size()`, then the
  // last `size() - numRows` rows of the table will be deleted. If
  // `numRows > size()`, `numRows - size()` new, uninitialized rows are appended
  // at the end. If `numRows > capacity_` then all iterators are invalidated,
  // as the function has to allocated new memory. Note: the capacity can be
  // set by the `reserve` function. Note: The semantics of this functions are
  // similar to `std::vector::resize`.
  void resize(size_t numRows) requires(!isView) {
    if (numRows > capacity_) {
      setCapacity(numRows);
    }
    size_ = numRows;
  }

  // If `numRows <= capacity_` then nothing happens. Else reserve enough memory
  // for `numRows` element without changing the actual `size()` of the IdTable.
  // In this case all iterators are invalidated, but you obtain the guarantee,
  // that the insertion of the next `numRows - size()` elements (via `insert`
  // or `push_back` can be done in O(1) without dynamic allocations.
  void reserve(size_t numRows) requires(!isView) {
    if (numRows > capacity_) {
      setCapacity(numRows);
    }
  }

  // Delete all the elements, but keep the allocated memory (`capacity_` stays
  // the same). Runs in O(1). To also free the allocated memory, call
  // `shrinkToFit()` (see below) after calling `clear()` .
  void clear() requires(!isView) { size_ = 0; }

  // Adjust the capacity to exactly match the size. This optimizes the memory
  // consumption of this table. This operation runs in O(size()), allocates
  // memory, and invalidates all iterators.
  void shrinkToFit() requires(!isView) { setCapacity(size()); }

  // Note: The following functions `emplace_back` and `push_back` all have the
  // following property: If `size() < capacity_` (before the operation) they
  // run in O(1). Else they run in O(size()) and all iterators are invalidated.
  // A sequence of `n` `emplace_back/push_back` operations runs in `O(n)` total
  // (The underlying data model is a dynamic array like `std::vector`).

  // Insert a new uninitialized row at the end.
  void emplace_back() requires(!isView) {
    growIfNecessary();
    ++size_;
  }

  // Add the `newRow` add the end. Requires that `newRow.size() ==
  // numColumns()`, else the behavior is undefined (in Release mode) or an
  // assertion will fail (in Debug mode). Note: This behavior is the same for
  // all the overloads of `push_back`. The correct size of `newRow` will be
  // checked at compile time if possible (when both the size of `newRow` and
  // `numColumns()` are known at compile-time. If this check cannot be
  // performed, a wrong size of `newRow` will lead to undefined behavior which
  // is caught by an `assert` in Debug builds.
  void push_back(const std::initializer_list<Id>& newRow) requires(!isView) {
    assert(newRow.size() == numColumns());
    emplace_back();
    auto sz = size();
    for (size_t i = 0; i < numColumns(); ++i) {
      operator()(sz - 1, i) = *(newRow.begin() + i);
    }
  }

  // Overload of `push_back` for `std:array`. If this IdTable is static
  // (`NumColumns != 0`), then this is a safe interface, as the correct size of
  // `newRow` can be statically checked.
  template <size_t N>
  void push_back(const std::array<Id, N>& newRow) requires(!isView &&
                                                           (NumColumns == 0 ||
                                                            NumColumns == N)) {
    if constexpr (NumColumns == 0) {
      assert(newRow.size() == numColumns());
    }
    emplace_back();
    auto sz = size();
    for (size_t i = 0; i < numColumns(); ++i) {
      operator()(sz - 1, i) = *(newRow.begin() + i);
    }
  }

  // Overload of `push_back` for all kinds of `(const_)row_reference(_proxy)`
  // types that are compatible with this `IdTable`. Note: This currently
  // excludes rows from `IdTables` with the correct number of columns, but with
  // a different allocator. If this should ever be needed, we would have to
  // reformulate the `requires()` clause here a little more complicated.
  template <typename T>
  requires ad_utility::isTypeContainedIn<
      T, std::tuple<row_reference, const_row_reference, row_reference_proxy,
                    const_row_reference_proxy, const_row_reference_view_proxy>>
  void push_back(const T& newRow) requires(!isView) {
    if constexpr (NumColumns == 0) {
      assert(newRow.numColumns() == numColumns());
    }
    emplace_back();
    for (size_t i = 0; i < numColumns(); ++i) {
      //(*this)(size() - 1, i) = newRow[i];
      using R = std::decay_t<decltype(newRow)>;
      (*this)(size() - 1, i) = R::getImpl(newRow, i);
    }
  }

  // Create a deep copy of this `IdTable` that owns its memory. In most cases
  // this behaves exactly like the copy constructor with the following
  // exception: If `this` is a views (because the `isView` template parameter is
  // `true`), then the copy constructor will also create a (const and
  // non-owning) view, but `clone` will create a mutable deep copy of the data
  // that the view points to
  IdTable<NumColumns, Allocator, false> clone() const {
    return IdTable<NumColumns, Allocator, false>{data(), numColumns_, size_,
                                                 capacity_};
  }

  // From a dynamic (`NumColumns == 0`) IdTable, create a static (`NumColumns !=
  // 0`) IdTable with `NumColumns == NewNumColumns`. The number of columns
  // actually stored in the dynamic table must be equal to `NewNumColumns`, or
  // the dynamic table must be empty, else a runtime check fails. This table
  // will be moved from, meaning that it is empty after the call. Therefore, it
  // is only allowed to be called on rvalues. Note: This function can also be
  // used with `NewNumColumns == 0`. Then it
  //       in fact moves a dynamic table to a new dynamic table. This makes
  //       generic code that is templated on the number of columns easier to
  //       write.
  template <int NewNumColumns>
  requires(NumColumns == 0 &&
           !isView) IdTable<NewNumColumns, Allocator> toStatic() && {
    if (size() == 0 && NewNumColumns != 0) {
      setNumColumns(NewNumColumns);
    }
    AD_CHECK(numColumns() == NewNumColumns || NewNumColumns == 0);
    auto result = IdTable<NewNumColumns, Allocator>{
        std::move(data()), numColumns(), size(), capacity_};
    size_ = 0;
    capacity_ = 0;
    return result;
  }

  // Move this `IdTable` into a dynamic `IdTable` with `NumColumns == 0`. This
  // function may only be called on rvalues, because the table will be moved
  // from.
  IdTable<0, Allocator> toDynamic() && requires(!isView) {
    auto result = IdTable<0, Allocator>{std::move(data()), numColumns_, size(),
                                        capacity_};
    size_ = 0;
    capacity_ = 0;
    return result;
  }

  // From a dynamic (`NumColumns == 0`) IdTable, create a static (`NumColumns !=
  // 0`) view of an `IdTable` with `NumColumns == NewNumColumns`. The number of
  // columns actually stored in the dynamic table must be equal to
  // `NewNumColumns`, or the dynamic table must be empty, else a runtime check
  // fails. The created view is const and only contains a pointer to the table
  // from which it was created. Therefore, calling this function is cheap
  // (O(1)), but the created view is only valid as long as the original table is
  // valid and unchanged. Note: This function can also be used with
  // `NewNumColumns == 0`. Then it
  //       creates a dynamic view from a dynamic table. This makes generic code
  //       that is templated on the number of columns easier to write.
  template <size_t NewNumColumns>
  requires(NumColumns == 0 &&
           !isView) IdTable<NewNumColumns, Allocator, true> asStaticView()
  const {
    AD_CHECK(numColumns() == NewNumColumns || NewNumColumns == 0);
    return IdTable<NewNumColumns, Allocator, true>{&data(), numColumns_, size_,
                                                   capacity_};
  }

  template <bool isConst>
  struct IteratorHelper {
    using R =
        RowReferenceImpl::DeducingRowReferenceViaAutoIsLikelyABug<IdTable,
                                                                  isConst>;
    using Ptr = std::conditional_t<isConst, const IdTable*, IdTable*>;
    Ptr table_;

    // The default constructor constructs an invalid iterator that may not
    // be dereferenced. It is however required by several algorithms in the
    // STL.
    IteratorHelper() = default;

    explicit IteratorHelper(Ptr table) : table_{table} {}
    auto operator()(auto&&, size_t idx) const { return R{table_, idx}; }
  };

  using iterator =
      ad_utility::IteratorForAccessOperator<IdTable, IteratorHelper<false>,
                                            false, Row<NumColumns>,
                                            RowReference<IdTable, false>>;
  using const_iterator =
      ad_utility::IteratorForAccessOperator<IdTable, IteratorHelper<true>, true,
                                            Row<NumColumns>,
                                            RowReference<IdTable, true>>;

  iterator begin() requires(!isView) {
    return {this, 0, IteratorHelper<false>{this}};
  }
  iterator end() requires(!isView) {
    return {this, size(), IteratorHelper<false>{this}};
  }
  const_iterator begin() const { return {this, 0, IteratorHelper<true>{this}}; }
  const_iterator end() const {
    return {this, size(), IteratorHelper<true>{this}};
  }

  const_iterator cbegin() const { return begin(); }
  const_iterator cend() const { return end(); }

  // TODO<joka921> This operator is rather inefficient, possibly the
  // tests should use something different.
  bool operator==(const IdTable& other) const requires(!isView) {
    if (numColumns() != other.numColumns()) {
      return false;
    }
    for (size_t i = 0; i < numRows(); ++i) {
      for (size_t j = 0; j < numColumns(); ++j) {
        if ((*this)(i, j) != other(i, j)) {
          return false;
        }
      }
    }
    return true;
  }

  // Erase the rows in the half open interval [beginIt, endIt) from this table.
  // `beginIt` and `endIt` must both be iterators pointing into this table and
  // that `begin() <= beginIt <= endIt < end`, else
  // the behavior is undefined.
  // The order of the elements before and after the erased regions remains
  // the same. This behavior is similar to `std::vector::erase`.
  void erase(const iterator& beginIt, const iterator& endIt) requires(!isView) {
    assert(begin() <= beginIt && beginIt <= endIt && endIt <= end());
    auto startIndex = beginIt - begin();
    auto endIndex = endIt - begin();
    auto numErasedElements = endIndex - startIndex;
    // TODO<joka921> If we implement more such algorithms that work columnwise,
    // it would be great to have an interface that lets us iterate over
    // columns directly, s.t. the following block starts with:
    // `for (auto& column: getColumns()) { ...}`
    for (size_t i = 0; i < numColumns(); ++i) {
      std::shift_left(startOfColumn(i) + startIndex, startOfColumn(i + 1),
                      numErasedElements);
    }
    size_ -= numErasedElements;
  }

  // Erase the single row that `it` points to by shifting all the elements
  // after `it` towards the beginning. Requires that `begin() <= it < end()`,
  // else the behavior is undefined.
  void erase(const iterator& it) requires(!isView) { erase(it, it + 1); }

  // Insert all the elements in the range `(beginIt, endIt]` at the end
  // of this `IdTable`. `beginIt` and `endIt` must *not* point into this
  // IdTable, else the behavior is undefined.
  // TODO<joka921> Insert can be done much more efficiently when `beginIt` and
  // `endIt` are iterators to a different column-major `IdTable`. Implement
  // this case.
  void insertAtEnd(auto beginIt, auto endIt) {
    for (; beginIt != endIt; ++beginIt) {
      push_back(*beginIt);
    }
  }

 private:
  // Get direct access to the underlying data() as a reference.
  Columns& data() requires(!isView) { return data_; }
  const Columns& data() const {
    if constexpr (isView) {
      return *data_;
    } else {
      return data_;
    }
  }

  // Set the capacity to `newCapacity` and reinstate the memory layout.
  // If `newCapacity < size()` then the table will also be truncated at the end
  // (this functionality is used for exmple by the `shrinkToFit` function.
  void setCapacity(size_t newCapacity) {
    Columns newData{getAllocator()};
    newData.resize(newCapacity * numColumns());
    size_t sz = std::min(capacity_, newCapacity);
    for (size_t i = 0; i < numColumns(); ++i) {
      std::copy(startOfColumn(i), startOfColumn(i) + sz,
                newData.begin() + i * newCapacity);
    }
    capacity_ = newCapacity;
    size_ = std::min(size_, capacity_);
    data() = std::move(newData);
  }

  // Increase the capacity by the `growthFactor` if the table is completely
  // filled;
  void growIfNecessary() {
    if (size_ == capacity_) {
      setCapacity(std::max(1ul, capacity_ * growthFactor));
    }
  }

  // Return an iterator into the underlying `data()` that points to the start
  // of the `i`-th column. It is safe to call this function with `i ==
  // numColumns()`, then the `end()` iterator of `data()` will be returned.
  auto startOfColumn(size_t i) { return data().begin() + i * capacity_; }
  // TODO<joka921, C++23> use `explicit object parameters` ("deducing this")
  auto startOfColumn(size_t i) const { return data().begin() + i * capacity_; }
};

}  // namespace columnBasedIdTable
