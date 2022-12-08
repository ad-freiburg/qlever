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
#include <span>

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
          IsView isViewTag = IsView::False>
class IdTable {
 public:
  static constexpr bool isView = isViewTag == IsView::True;
  static constexpr bool isDynamic = NumColumns == 0;
  // Make the number of (statically known) columns accessible to the outside.
  static constexpr int numStaticColumns = NumColumns;
  // The actual storage is a plain 1-D vector with the logical columns
  // concatenated.
  using Columns = std::vector<Id, Allocator>;
  using Data = std::conditional_t<isView, const Columns*, Columns>;

  // Because of the column-major layout, the `row_type` (a value type that
  // stores the values of a  single row) and the `row_reference` (a type that
  // refers to a specific row of a specific `IdTable`) are different. Those are
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
          IdTable<NumColumns, Allocator, IsView::True>, true>;

 private:
  Data data_;
  size_t numColumns_ = NumColumns;
  size_t numRows_ = 0;
  size_t capacityRows_ = 0;
  static constexpr size_t growthFactor = 2;

 public:
  // Construct from the number of columns and an allocator. If `NumColumns != 0`
  // Then the argument `numColumns` and `NumColumns` (the static and the
  // dynamic number of columns) must be equal, else a runtime check fails.
  IdTable(size_t numColumns,
          Allocator allocator = Allocator{}) requires(!isView)
      : data_{std::move(allocator)}, numColumns_{numColumns} {
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
  template <int, typename, IsView>
  friend class IdTable;

  // Construct directly from the underlying storage. This is rather error-prone,
  // as the data has to have the correct layout and the remaining arguments also
  // have to match. For this reason it is private and only used to implement
  // the conversion functions `toStatic`, `toDynamic` and `asStaticView`
  // (see below).
  IdTable(Data data, size_t numColumns, size_t numRows, size_t capacityRows)
      : data_{std::move(data)},
        numColumns_{numColumns},
        numRows_{numRows},
        capacityRows_{capacityRows} {
    if constexpr (!isDynamic) {
      AD_CHECK(numColumns == NumColumns);
    }
    AD_CHECK(numRows_ <= capacityRows_);
    AD_CHECK(this->data().size() == numColumns_ * capacityRows_);
  }

 public:
  // For an empty and dynamic (`NumColumns == 0`) `IdTable`, specify the
  // number of columns.
  void setNumColumns(size_t numColumns) requires(isDynamic) {
    AD_CHECK(size() == 0);
    numColumns_ = numColumns;
  }

  // The number of rows in the table. We deliberately have an explicitly named
  // function `numRows` as well as a generic `size` function as the latter can
  // be used to write generic code that also works for e.g.
  // `std::vector<someRowType>`
  size_t numRows() const { return numRows_; }
  size_t size() const { return numRows();}

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
  // used internally, only gives access to its allocator by value.
  Allocator getAllocator() const { return data().get_allocator(); }

  // Get access to a single element specified by the row and the column.
  // TODO<joka921, C++23> Use the multidimensional subscript operator.
  // TODO<joka921, C++23> Use explicit object parameters ("deducing this").
  Id& operator()(size_t row, size_t column) requires(!isView) {
    return getColumn(column)[row];
  }
  const Id& operator()(size_t row, size_t column) const {
    return getColumn(column)[row];
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

  // Resize the `IdTable` to exactly `numRows`. If `numRows < size()`, then the
  // last `size() - numRows` rows of the table will be deleted. If
  // `numRows > size()`, `numRows - size()` new, uninitialized rows are appended
  // at the end. If `numRows > capacityRows_` then all iterators are invalidated,
  // as the function has to allocated new memory.
  // Note: the capacity can be set by the `reserve` function.
  // Note: The semantics of this functions is similar to `std::vector::resize`.
  void resize(size_t numRows) requires(!isView) {
    if (numRows > capacityRows_) {
      setCapacity(numRows);
    }
    numRows_ = numRows;
  }

  // Reserve space for `numRows` rows. If `numRows <= capacityRows_` then nothing happens. Else reserve enough memory
  // for `numRows` rows without changing the actual `size()` of the IdTable.
  // In this case all iterators are invalidated, but you obtain the guarantee,
  // that the insertion of the next `numRows - size()` elements (via `insert`
  // or `push_back` can be done in O(1) without dynamic allocations.
  void reserve(size_t numRows) requires(!isView) {
    if (numRows > capacityRows_) {
      setCapacity(numRows);
    }
  }

  // Delete all the elements, but keep the allocated memory (`capacityRows_` stays
  // the same). Runs in O(1). To also free the allocated memory, call
  // `shrinkToFit()` (see below) after calling `clear()` .
  void clear() requires(!isView) {
    numRows_ = 0; }

  // Adjust the capacity to exactly match the size. This optimizes the memory
  // consumption of this table. This operation runs in O(size()), allocates
  // memory, and invalidates all iterators.
  void shrinkToFit() requires(!isView) { setCapacity(size()); }

  // Note: The following functions `emplace_back` and `push_back` all have the
  // following property: If `size() < capacityRows_` (before the operation) they
  // run in O(1). Else they run in O(size()) and all iterators are invalidated.
  // A sequence of `n` `emplace_back/push_back` operations runs in `O(n)` total
  // (The underlying data model is a dynamic array like `std::vector`).

  // Insert a new uninitialized row at the end.
  void emplace_back() requires(!isView) {
    growIfFull();
    ++numRows_;
  }

  // Add the `newRow` add the end. Requires that `newRow.size() ==
  // numColumns()`, else the behavior is undefined (in Release mode) or an
  // assertion will fail (in Debug mode).
  // Note: This behavior is the same for all the overloads of `push_back`. The correct size of `newRow` will be
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
                                                           (isDynamic ||
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
  // types that are compatible with this `IdTable`.
  // Note: This currently excludes rows from `IdTables` with the correct number of columns, but with
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
      operator()(size() - 1, i) = newRow[i];
    }
  }

  // Create a deep copy of this `IdTable` that owns its memory. In most cases
  // this behaves exactly like the copy constructor with the following
  // exception: If `this` is a view (because the `isView` template parameter is
  // `true`), then the copy constructor will also create a (const and
  // non-owning) view, but `clone` will create a mutable deep copy of the data
  // that the view points to
  IdTable<NumColumns, Allocator, IsView::False> clone() const {
    return IdTable<NumColumns, Allocator, IsView::False>{data(), numColumns_, numRows_,
                                                 capacityRows_};
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
    if (size() == 0 && !isDynamic) {
      setNumColumns(NewNumColumns);
    }
    AD_CHECK(numColumns() == NewNumColumns || NewNumColumns == 0);
    auto result = IdTable<NewNumColumns, Allocator>{
        std::move(data()), numColumns(), size(), capacityRows_};
    numRows_ = 0;
    capacityRows_ = 0;
    return result;
  }

  // Move this `IdTable` into a dynamic `IdTable` with `NumColumns == 0`. This
  // function may only be called on rvalues, because the table will be moved
  // from.
  IdTable<0, Allocator> toDynamic() && requires(!isView) {
    auto result = IdTable<0, Allocator>{std::move(data()), numColumns_, size(),
                                        capacityRows_};
    numRows_ = 0;
    capacityRows_ = 0;
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
           !isView) IdTable<NewNumColumns, Allocator, IsView::True> asStaticView()
  const {
    AD_CHECK(numColumns() == NewNumColumns || NewNumColumns == 0);
    return IdTable<NewNumColumns, Allocator, IsView::True>{&data(), numColumns_,
                                                   numRows_, capacityRows_};
  }

  // This struct stores a pointer to this table and has an `operator()` that
  // can be called with a reference to an `IdTable` and the index of a row and
  // then returns a `row_reference_proxy` to that row. This struct is used to
  // automatically create random access iterators using the
  // `ad_utility::IteratorForAccessOperator` template.

  template <typename ReferenceType>
  struct IteratorHelper {
    auto operator()(auto&& idTable, size_t rowIdx) const {
      return ReferenceType{&idTable, rowIdx};
    }
  };

  // Automatically setup iterators and const iterators. Note that we explicitly
  // set the `value_type` of the iterators to `row_type` and the
  // `reference_type` to `(const_)row_reference`, but dereferencing an iterator
  // actually yields a
  // `(const_)row_reference_proxy. The latter one can be implicitly converted
  // to `row_type` as well as `row_reference`, but binding it to a variable via
  // `auto` will lead to a `row_reference_proxy` on which only const access is
  // allowed unless it's an rvalue. This makes it harder to use those types
  // incorrectly. For more detailed information see the documentation of the
  // `row_reference` and `row_reference_proxy` types.

  // TODO<joka921> We should probably change the names of all those
  // typedefs (`iterator` as well as `row_type` etc.) to `PascalCase` for
  // consistency.
  using iterator =
      ad_utility::IteratorForAccessOperator<IdTable,
                                            IteratorHelper<row_reference_proxy>,
                                            false, row_type, row_reference>;
  using const_iterator = ad_utility::IteratorForAccessOperator<
      IdTable, IteratorHelper<const_row_reference_proxy>, true, row_type,
      const_row_reference>;

  // The usual overloads of `begin()` and `end()` for const and mutable
  // `IdTable`s.
  iterator begin() requires(!isView) { return {this, 0}; }
  iterator end() requires(!isView) { return {this, size()}; }
  const_iterator begin() const { return {this, 0}; }
  const_iterator end() const { return {this, size()}; }

  // `cbegin()` and `cend()` allow to explicitly obtain a const iterator from
  // a mutable object.
  const_iterator cbegin() const { return begin(); }
  const_iterator cend() const { return end(); }

  // Erase the rows in the half open interval [beginIt, endIt) from this table.
  // `beginIt` and `endIt` must both be iterators pointing into this table and
  // that `begin() <= beginIt <= endIt < end`, else
  // the behavior is undefined.
  // The order of the elements before and after the erased regions remains
  // the same. This behavior is similar to `std::vector::erase`.
  // TODO<joka921> Is this function actually used / useful in functions other
  // than unit tests? if not, throw it out.
  // TODO<joka921> It is currently used by the implmentation of DISTINCT,
  // which is currently done via `std::unique` on a full IdTable.
  // but this should be an out-of-place algorithm that doesn't need the final
  // `erase`.
  void erase(const iterator& beginIt, const iterator& endIt) requires(!isView) {
    assert(begin() <= beginIt && beginIt <= endIt && endIt <= end());
    auto startIndex = beginIt - begin();
    auto endIndex = endIt - begin();
    auto numErasedElements = endIndex - startIndex;
    for (auto& column: getColumns()) {
      std::shift_left(column.begin() + startIndex, column.end(),
                      numErasedElements);
    }
    numRows_ -= numErasedElements;
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

  // Check whether two `IdTables` have the same content. Mostly used for unit
  // testing.
  bool operator==(const IdTable& other) const requires(!isView) {
    if (numColumns() != other.numColumns()) {
      return false;
    }
    if (size() != other.numRows()) {
      return false;
    }

    // TODO<joka921, C++23> This can implemented using `zip_view`
    // and `std::ranges::all_of`.
    // The iteration over the columns is cache-friendly.
    const auto& cols = getColumns();
    const auto& otherCols = other.getColumns();
    for (size_t i = 0; i < numColumns(); ++i) {
      for (size_t j = 0; j < numRows(); ++i) {
        if (cols[i][j] != otherCols[i][j]) {
          return false;
        }
      }
    }
    return true;
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
    size_t numRowsToCopy = std::min(capacityRows_, newCapacity);
    const auto& columns = getColumns();
    // TODO<joka921, C++23> this should be an `enumerate` view.
    for (size_t i = 0; i < numColumns(); ++i) {
      const auto& column = columns[i];
      std::copy(column.begin(), column.begin() + numRowsToCopy,
                newData.begin() + i * newCapacity);
    }
    capacityRows_ = newCapacity;
    numRows_ = std::min(numRows_, capacityRows_);
    data() = std::move(newData);
  }

  // Increase the capacity by the `growthFactor` if the table is completely
  // filled;
  void growIfFull() {
    if (numRows_ == capacityRows_) {
      setCapacity(std::max(1ul, capacityRows_ * growthFactor));
    }
  }

  // Get the `i`-th column. It is stored contiguously in memory
  std::span<Id> getColumn(size_t i) {
    return {data().data() + i * capacityRows_, numRows_};
  }
  std::span<const Id> getColumn(size_t i) const {
    return {data().data() + i * capacityRows_, numRows_};
  }

  // Common implementation for const and mutable overloads of `getColumns`
  // (see below).
  static auto getColumnsImpl(auto&& self) {
    using Column = decltype(self.getColumn(0));
    if constexpr (isDynamic) {
      std::vector<Column> columns;
      columns.reserve(self.numColumns());
      for (size_t i = 0; i < self.numColumns(); ++i) {
        columns.push_back(self.getColumn(i));
      }
      return columns;
    } else {
      std::array<Column, NumColumns> columns;
      for (size_t i = 0; i < self.numColumns(); ++i) {
        columns[i] = self.getColumn(i);
      }
      return columns;
    }
  }

  // Return all the columns as a `std::vector` (if `isDynamic`) or as a `std::array` (else).
  // The elements of the vector/array are `std::span<Id>` or `std::span<const Id>`, depending
  // on whether `this` is const.
  auto getColumns() {
    return getColumnsImpl(*this);
  }
auto getColumns() const {
  return getColumnsImpl(*this);
}




};

}  // namespace columnBasedIdTable
