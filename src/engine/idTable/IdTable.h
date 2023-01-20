//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include <array>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <span>
#include <variant>
#include <vector>

#include "engine/idTable/IdTableRow.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/Iterators.h"
#include "util/LambdaHelpers.h"
#include "util/ResetWhenMoved.h"
#include "util/UninitializedAllocator.h"

namespace columnBasedIdTable {
// The `IdTable` class is QLever's central data structure. It is used to store
// all intermediate and final query results in the ID space.
//
// An `IdTable` is a 2D array of `Id`s with a fixed number of columns and a
// variable number of rows. With respect to the number of rows it allows for
// dynamic resizing at runtime, similar to `std::vector`. The template parameter
// `NumColumns` fixes the number of columns at compile time when not zero. When
// zero, the number of columns must be specified at runtime via the constructor
// or via an explicit call to `setNumColumns()` before inserting IDs.
//
// The data layout is column-major, that is, all elements of a particular column
// are contiguous in memory. This is cache-friendly for many typical operations.
// For example, when an operation operates only on a single column (like an
// expression that aggregates a single variable). Or when a join operation has
// two input tables with many rows but only a relatively small result: then
// almost all entries in the join columns have to be accessed, but only a
// fraction of the entries in the other columns.
//
// The `IdTable` has various member functions for directl access to specific
// elements, as well as a generic `iterator` interface with `begin()` and
// `end()` member functions. These iterators are random-access iterators with
// respect to the rows and can be passed to any STL algorithm, e.g. `std::sort`.
//
// The iterator interface has the catch that it has two different types:
// `IdTable::row` (a fully materialized row as an array of `IDs` that is
// independent of a specific `IdTable`) and `IdTable::row_reference` (a proxy
// type that points to a specific row in a specific `IdTable`). We need such a
// proxy type for the reference because of the column-major layout: A `row` is
// not stored in contiguous memory, so we cannot directly reference it as one
// object. This is similar in spirit to `std::vector<bool>`, which also uses a
// proxy type for references. There the reason is that it is not possible to
// form a reference to a single bit.
//
// We have taken great care to make it hard to misuse the proxy type interface.
// Most notably, the following examples work as expected:
//
// IdTable table{1};             // Create an `IdTable` with one column.
// IdTable.push_back({someId});  // Add a single row to the table.
// IdTable[0][0] = someOtherId;  // Changes the IdTable, expected.
//
// The following code changes the respective entry in the `IdTable`, which is
// expected because a reference was explicitly requested.
//
// IdTable::row_reference ref = IdTable[0][0];
// ref = anotherId;
//
// The following code does not change the ID, but only the materialized row:
//
// IdTable::row = IdTable[0];
// row[0] = someId;
//
// In the following code, one would expect that `strange` is a value type (it's
// `auto` and not `auto&`). However, it is a proxy type object, so logically a
// reference. We therefore made sure that the second line does not compile.
//
// auto strange = IdTable[0];
// strange[0] = someId;
//
// Interestingly, for `std::vector<bool>` this code would compile and change the
// vector. This is one of the many reasons why the design of `std::vector<bool>`
// is considered a bad idea in retrospective by many experts.
//
// For more detailed examples for the usage of this interface see the first unit
// test in `IdTableTest.cpp`. For details on the implementation of the reference
// types and the non-compilation of the `auto` example, see `IdTableRow.h`.
// However, fully understanding the internals is not required to safely use the
// `IdTable` class.
//
// Template parameters:
//
// NumColumns: The number of columns (when != 0) or the information, that the
//             number of columns will be set at runtime (see above).
// Allocator: The allocator type that will be used for the underlying storage.
// isView:    If true, then this is a const, and non-owning view of another
//            `IdTable`. Such views are cheap to copy as they just store
//            a const pointer to another `IdTable`.
//
// TODO<joka921> The NumColumns should be `size_t` but that requires several
// additional changes in the rest of the code.
//
template <typename T = Id, int NumColumns = 0,
          typename Storage = std::vector<
              T, ad_utility::default_init_allocator<T, std::allocator<T>>>,
          IsView isViewTag = IsView::False>
class IdTable {
 public:
  static constexpr bool isView = isViewTag == IsView::True;
  static constexpr bool isDynamic = NumColumns == 0;
  // Make the number of (statically known) columns accessible to the outside.
  static constexpr int numStaticColumns = NumColumns;
  // The actual storage is a plain 1D vector with the logical columns
  // concatenated.
  using Data = std::conditional_t<isView, const Storage*, Storage>;

  using value_type = T;
  // Because of the column-major layout, the `row_type` (a value type that
  // stores the values of a  single row) and the `row_reference` (a type that
  // refers to a specific row of a specific `IdTable`) are different. They are
  // implemented in a way that makes it hard to use them incorrectly. For
  // details, see the comment above and the definition of the `Row` and
  // `RowReference` class.
  using row_type = Row<T, NumColumns>;
  using row_reference = RowReference<IdTable, ad_utility::IsConst::False>;
  using const_row_reference = RowReference<IdTable, ad_utility::IsConst::True>;

 private:
  // Assign shorter aliases for some types that are important for the correct
  // handling of the proxy reference, but that are not visible to the outside.
  // For details see `IdTableRow.h`
  using row_reference_restricted =
      RowReferenceImpl::RowReferenceWithRestrictedAccess<
          IdTable, ad_utility::IsConst::False>;
  using const_row_reference_restricted =
      RowReferenceImpl::RowReferenceWithRestrictedAccess<
          IdTable, ad_utility::IsConst::True>;
  using const_row_reference_view_restricted =
      RowReferenceImpl::RowReferenceWithRestrictedAccess<
          IdTable<T, NumColumns, Storage, IsView::True>,
          ad_utility::IsConst::True>;

 private:
  Data data_;
  size_t numColumns_ = NumColumns;
  ad_utility::ResetWhenMoved<size_t, 0> numRows_ = 0;
  ad_utility::ResetWhenMoved<size_t, 0> capacityRows_ = 0;
  static constexpr double growthFactor = 1.5;

 public:
  // Construct from the number of columns and an allocator. If `NumColumns != 0`
  // Then the argument `numColumns` and `NumColumns` (the static and the
  // dynamic number of columns) must be equal, else a runtime check fails.
  IdTable(size_t numColumns, Storage storage = {}) requires(!isView)
      : data_{std::move(storage)}, numColumns_{numColumns} {
    if constexpr (!isDynamic) {
      AD_CHECK(NumColumns == numColumns);
    }
    // The passed in `Storage` must be empty.
    AD_CHECK(data_.empty());
  }

  // Quasi the default constructor. If `NumColumns != 0` then the table is
  // already set up with the correct number of columns and can be used directly.
  // If `NumColumns == 0` then the number of columns has to be specified via
  // `setNumColumns()`.
  IdTable(Storage storage = {}) requires(!isView)
      : IdTable{NumColumns, std::move(storage)} {};

  // `IdTables` are expensive to copy, so we disable accidental copies as they
  // are most likely bugs. To explicitly copy an `IdTable`, the `clone()` member
  // function (see below) can be used.
  IdTable(const IdTable&) requires(!isView) = delete;
  IdTable& operator=(const IdTable&) requires(!isView) = delete;

  // Views are copyable, as they are cheap to copy.
  IdTable(const IdTable&) requires isView = default;
  IdTable& operator=(const IdTable&) requires isView = default;

  // `IdTable`s are movable
  IdTable(IdTable&& other) requires(!isView) = default;
  IdTable& operator=(IdTable&& other) requires(!isView) = default;

 private:
  // Make the other instantiations of `IdTable` friends to allow for conversion
  // between them using a private interface.
  template <typename, int, typename, IsView>
  friend class IdTable;

  // Construct directly from the underlying storage. This is rather error-prone,
  // as the data has to have the correct layout and the remaining arguments also
  // have to match. For this reason, this constructor is private and only used
  // to implement the conversion functions `toStatic`, `toDynamic` and
  // `asStaticView` below.
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
  // function `numRows` as well as a generic `size` function because the latter
  // can be used to write generic code, for example when using STL algorithms on
  // `std::vector<someRowType>`.
  size_t numRows() const { return numRows_; }
  size_t size() const { return numRows(); }
  bool empty() const { return numRows() == 0; }

  // Return the number of columns.
  size_t numColumns() const {
    if constexpr (isDynamic) {
      return numColumns_;
    } else {
      return NumColumns;
    }
  }

  // Get access to the underlying allocator.
  // Note: The allocator is always copied, because `std::vector`, which is
  // used internally, only gives access to its allocator by value.
  auto getAllocator() const { return data().get_allocator(); }

  // Get access to a single element specified by the row and the column.
  // TODO<joka921, C++23> Use the multidimensional subscript operator.
  // TODO<joka921, C++23> Use explicit object parameters ("deducing this").
  T& operator()(size_t row, size_t column) requires(!isView) {
    return getColumn(column)[row];
  }
  const T& operator()(size_t row, size_t column) const {
    return getColumn(column)[row];
  }

  // Get a reference to the `i`-th row. The returned proxy objects can be
  // implicitly and trivially converted to `row_reference`. For the design
  // rationale behind those proxy types see above for their definition.
  row_reference_restricted operator[](size_t index) requires(!isView) {
    return *(begin() + index);
  }
  const_row_reference_restricted operator[](size_t index) const {
    return *(begin() + index);
  }

  // The usual `front` and `back` functions to make the interface similar to
  // `std::vector` aand other containers.
  // TODO<C++23, joka921> Remove the duplicates via explicit object parameters
  // ("deducing this").
  row_reference_restricted front() { return (*this)[0]; }
  const_row_reference_restricted front() const { return (*this)[0]; }
  row_reference_restricted back() { return (*this)[numRows() - 1]; }
  const_row_reference_restricted back() const { return (*this)[numRows() - 1]; }

  // Resize the `IdTable` to exactly `numRows`. If `numRows < size()`, then the
  // last `size() - numRows` rows of the table will be deleted. If
  // `numRows > size()`, `numRows - size()` new, uninitialized rows are appended
  // at the end. If `numRows > capacityRows_` then all iterators are
  // invalidated, as the function has to allocate new memory.
  //
  // Note: The semantics of this function is similar to `std::vector::resize`.
  // To set the capacity, use the `reserve` function.
  void resize(size_t numRows) requires(!isView) {
    if (numRows > capacityRows_) {
      // Increase by at least the `growthFactor` to enforce the amortized O(1)
      // complexity also for patterns like `resize(numRows() + 1)`.
      reserveWithMinimalGrowth(numRows);
    }
    numRows_ = numRows;
  }

  // Reserve space for `numRows` rows. If `numRows <= capacityRows_` then
  // nothing happens. Otherwise, reserve enough memory for `numRows` rows
  // without changing the actual `size()` of the IdTable. In this case, all
  // iterators are invalidated, but you obtain the guarantee, that the insertion
  // of the next `numRows - size()` elements (via `insert` or `push_back`) can
  // be done in O(1) time without dynamic allocations.
  void reserve(size_t numRows) requires(!isView) {
    if (numRows > capacityRows_) {
      setCapacity(numRows);
    }
  }

  // Delete all the elements, but keep the allocated memory (`capacityRows_`
  // stays the same). Runs in O(1) time. To also free the allocated memory, call
  // `shrinkToFit()` after calling `clear()` .
  void clear() requires(!isView) { numRows_ = 0; }

  // Adjust the capacity to exactly match the size. This optimizes the memory
  // consumption of this table. This operation runs in O(size()), allocates
  // memory, and invalidates all iterators.
  void shrinkToFit() requires(!isView) { setCapacity(size()); }

  // Note: The following functions `emplace_back` and `push_back` all have the
  // following property: If `size() < capacityRows_` (before the operation) they
  // run in O(1). Else they run in O(size()) and all iterators are invalidated.
  // A sequence of `n` `emplace_back/push_back` operations runs in total time
  // `O(n)`. The underlying data model is a dynamic array like `std::vector`.

  // Insert a new uninitialized row at the end.
  void emplace_back() requires(!isView) {
    growIfFull();
    ++numRows_;
  }

  // Add the `newRow` at the end. Requires that `newRow.size() == numColumns()`,
  // otherwise the behavior is undefined (in Release mode) or an assertion will
  // fail (in Debug mode).
  //
  // Note: This behavior is the same for all the overloads of `push_back`. The
  // correct size of `newRow` will be checked at compile time if possible (when
  // both the size of `newRow` and `numColumns()` are known at compile time. If
  // this check cannot be performed, a wrong size of `newRow` will lead to
  // undefined behavior which is caught by an `assert` in Debug builds.
  void push_back(const std::initializer_list<T>& newRow) requires(!isView) {
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
  void push_back(const std::array<T, N>& newRow) requires(!isView &&
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
  // Note: This currently excludes rows from `IdTables` with the correct number
  // of columns, but with a different allocator. If this should ever be needed,
  // we would have to reformulate the `requires()` clause here a little more
  // complicated.
  template <typename RowT>
  requires ad_utility::isTypeContainedIn<
      RowT, std::tuple<row_reference, const_row_reference,
                       row_reference_restricted, const_row_reference_restricted,
                       const_row_reference_view_restricted>>
  void push_back(const RowT& newRow) requires(!isView) {
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
  IdTable<T, NumColumns, Storage, IsView::False> clone()
      const requires std::is_copy_constructible_v<Storage> {
    return IdTable<T, NumColumns, Storage, IsView::False>{
        data(), numColumns_, numRows_, capacityRows_};
  }

  // Overload of `clone` for `Storage` types that are not copy constructible.
  // It requires a preconstructed but empty argument of type `Storage` that
  // is then resized and filled with the appropriate contents.
  IdTable<T, NumColumns, Storage, IsView::False> clone(Storage storage) const
      requires(!std::is_copy_constructible_v<Storage>) {
    AD_CHECK(storage.empty());
    storage.resize(data().size());
    std::copy(data().begin(), data().end(), storage.begin());
    return IdTable<T, NumColumns, Storage, IsView::False>{
        std::move(storage), numColumns_, numRows_, capacityRows_};
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
           !isView) IdTable<T, NewNumColumns, Storage> toStatic() && {
    if (size() == 0 && !isDynamic) {
      setNumColumns(NewNumColumns);
    }
    AD_CHECK(numColumns() == NewNumColumns || NewNumColumns == 0);
    auto result = IdTable<T, NewNumColumns, Storage>{
        std::move(data()), numColumns(), std::move(numRows_),
        std::move(capacityRows_)};
    return result;
  }

  // Move this `IdTable` into a dynamic `IdTable` with `NumColumns == 0`. This
  // function may only be called on rvalues, because the table will be moved
  // from.
  IdTable<T, 0, Storage> toDynamic() && requires(!isView) {
    auto result =
        IdTable<T, 0, Storage>{std::move(data()), numColumns_,
                               std::move(numRows_), std::move(capacityRows_)};
    return result;
  }

  // Given a dynamic (`NumColumns == 0`) IdTable, create a static (`NumColumns
  // != 0`) view of an `IdTable` with `NumColumns == NewNumColumns`. The number
  // of columns in the dynamic table must either be equal to `NewNumColumns`, or
  // the dynamic table must be empty; otherwise a runtime check fails. The
  // created view is `const` and only contains a pointer to the table from which
  // it was created. Therefore, calling this function is cheap (O(1)), but the
  // created view is only valid as long as the original table is valid and
  // unchanged.
  //
  // Note: This function can also be used with `NewNumColumns == 0`. Then it
  // creates a dynamic view from a dynamic table. This makes generic code that
  // is templated on the number of columns easier to write.
  template <size_t NewNumColumns>
  requires(NumColumns == 0 && !isView)
      IdTable<T, NewNumColumns, Storage, IsView::True> asStaticView()
  const {
    AD_CHECK(numColumns() == NewNumColumns || NewNumColumns == 0);
    return IdTable<T, NewNumColumns, Storage, IsView::True>{
        &data(), numColumns_, numRows_, capacityRows_};
  }

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

  // Automatically setup iterators and const iterators. Note that we explicitly
  // set the `value_type` of the iterators to `row_type` and the
  // `reference_type` to `(const_)row_reference`, but dereferencing an iterator
  // actually yields a
  // `(const_)row_reference_restricted. The latter one can be implicitly
  // converted to `row_type` as well as `row_reference`, but binding it to a
  // variable via `auto` will lead to a `row_reference_restricted` on which only
  // const access is allowed unless it's an rvalue. This makes it harder to use
  // those types incorrectly. For more detailed information see the
  // documentation of the `row_reference` and `row_reference_restricted` types.

  // TODO<joka921> We should probably change the names of all those
  // typedefs (`iterator` as well as `row_type` etc.) to `PascalCase` for
  // consistency.
  using iterator = ad_utility::IteratorForAccessOperator<
      IdTable, IteratorHelper<row_reference_restricted>,
      ad_utility::IsConst::False, row_type, row_reference>;
  using const_iterator = ad_utility::IteratorForAccessOperator<
      IdTable, IteratorHelper<const_row_reference_restricted>,
      ad_utility::IsConst::True, row_type, const_row_reference>;

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
  // that `begin() <= beginIt <= endIt < end`, else the behavior is undefined.
  // The order of the elements before and after the erased regions remains the
  // same. This behavior is similar to `std::vector::erase`.
  //
  // TODO<joka921> It is currently used by the implementation of DISTINCT, which
  // first copies the sorted input completely, and then calls `std::unique`,
  // followed by `erase` at the end. `DISTINCT` should be implemented via an
  // out-of-place algorithm that only writes the distinct elements. The the
  // follwing two functions can be deleted.
  void erase(const iterator& beginIt, const iterator& endIt) requires(!isView) {
    assert(begin() <= beginIt && beginIt <= endIt && endIt <= end());
    auto startIndex = beginIt - begin();
    auto endIndex = endIt - begin();
    auto numErasedElements = endIndex - startIndex;
    for (auto& column : getColumns()) {
      std::shift_left(column.begin() + startIndex, column.end(),
                      numErasedElements);
    }
    numRows_ -= numErasedElements;
  }

  // Erase the single row that `it` points to by shifting all the elements
  // after `it` towards the beginning. Requires that `begin() <= it < end()`,
  // otherwise the behavior is undefined.
  void erase(const iterator& it) requires(!isView) { erase(it, it + 1); }

  // Insert all the elements in the range `(beginIt, endIt]` at the end
  // of this `IdTable`. `beginIt` and `endIt` must *not* point into this
  // IdTable, else the behavior is undefined.
  //
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

    // TODO<joka921, C++23> This can be implemented using `zip_view` and
    // `std::ranges::all_of`. The iteration over the columns is cache-friendly.
    const auto& cols = getColumns();
    const auto& otherCols = other.getColumns();
    for (size_t i = 0; i < numColumns(); ++i) {
      for (size_t j = 0; j < numRows(); ++j) {
        if (cols[i][j] != otherCols[i][j]) {
          return false;
        }
      }
    }
    return true;
  }

  // Get the `i`-th column. It is stored contiguously in memory.
  std::span<T> getColumn(size_t i) {
    return {data().data() + i * capacityRows_, numRows_};
  }
  std::span<const T> getColumn(size_t i) const {
    return {data().data() + i * capacityRows_, numRows_};
  }

 private:
  // Get direct access to the underlying data() as a reference.
  Storage& data() requires(!isView) { return data_; }
  const Storage& data() const {
    if constexpr (isView) {
      return *data_;
    } else {
      return data_;
    }
  }

  // Set the capacity to `newCapacity` and reinstate the memory layout.
  // If `newCapacity < size()` then the table will also be truncated at the end
  // (this functionality is used for example by the `shrinkToFit` function.
  void setCapacity(size_t newCapacity) {
    if (newCapacity == capacityRows_) {
      return;
    }
    // If the `Storage` can be easily constructed (for example a `std::vector`),
    // we use an implementation that uses a new `Storage` object. This is
    // typically more efficient than resizing the old `Storage` object, because
    // the `resize` operation is typically expensive because it also has to copy
    // the contents.
    if constexpr (requires { Storage{getAllocator()}; }) {
      Storage newData{getAllocator()};
      newData.resize(newCapacity * numColumns());
      size_t numRowsToCopy = std::min(capacityRows_.value_, newCapacity);
      const auto& columns = getColumns();
      // TODO<joka921, C++23> this should be an `enumerate` view.
      for (size_t i = 0; i < numColumns(); ++i) {
        const auto& column = columns[i];
        std::copy(column.begin(), column.begin() + numRowsToCopy,
                  newData.begin() + i * newCapacity);
      }
      data() = std::move(newData);
    } else {
      // If it is complicated to create a`Storage` object (e.g. for file-based
      // data structures like `BufferedVector`), we resize the current `Storage`
      // object and move the contents in place. For file-based storage types the
      // `resize` operation is cheap as only new blocks are appended to the file
      // without copying any data.
      if (newCapacity > capacityRows_) {
        data().resize(newCapacity * numColumns());
        // TODO<joka921, C++23> Use views.
        for (int i = numColumns() - 1; i >= 0; --i) {
          auto oldBegin = i * capacityRows_;
          auto newBegin = i * newCapacity;
          auto newEnd = newBegin + numRows();
          std::shift_right(data().begin() + oldBegin, data().begin() + newEnd,
                           newBegin - oldBegin);
        }
      } else {
        // newCapacity <= capacityRows_
        // TODO<joka921, C++23> Use views.
        for (size_t i = 0; i < numColumns(); ++i) {
          auto oldBegin = i * capacityRows_;
          auto newBegin = i * newCapacity;
          auto oldEnd = oldBegin + numRows();
          std::shift_left(data().begin() + newBegin, data().begin() + oldEnd,
                          oldBegin - newBegin);
        }
        data().resize(newCapacity * numColumns());
      }
    }
    capacityRows_ = newCapacity;
    numRows_ = std::min(numRows_.value_, capacityRows_.value_);
  }

  // Change the capacity s.t. it is at least `newCapacity` but also increases
  // by at least the `growthFactor`. This helper function is used inside
  // `growIfFull` and `resize` to enforce the amortized O(1) guarantee for
  // chains of operations.
  void reserveWithMinimalGrowth(size_t newCapacity) {
    setCapacity(std::max(newCapacity,
                         static_cast<size_t>(capacityRows_ * growthFactor)));
  }

  // Increase the capacity by the `growthFactor` if the table is completely
  // full. Otherwise, do nothing.
  void growIfFull() {
    if (numRows_ == capacityRows_) {
      reserveWithMinimalGrowth(capacityRows_ + 1);
    }
  }

  // Common implementation for const and mutable overloads of `getColumns`
  // (see below).
  static auto getColumnsImpl(auto&& self) {
    using Column = decltype(self.getColumn(0));
    if constexpr (isDynamic) {
      // TODO<joka921, for the dynamic case we could maybe use a vector with
      // a small buffer optimization or a fixed maximal size from folly etc.
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

  // Return all the columns as a `std::vector` (if `isDynamic`) or as a
  // `std::array` (else). The elements of the vector/array are `std::span<T>`
  // or `std::span<const T>`, depending on whether `this` is const.
  auto getColumns() { return getColumnsImpl(*this); }
  auto getColumns() const { return getColumnsImpl(*this); }
};

}  // namespace columnBasedIdTable

namespace detail {
using DefaultAllocator =
    ad_utility::default_init_allocator<Id, ad_utility::AllocatorWithLimit<Id>>;
using IdVector = std::vector<Id, DefaultAllocator>;
}  // namespace detail

/// The general IdTable class. Can be modified and owns its data. If COLS > 0,
/// COLS specifies the compile-time number of columns COLS == 0 means "runtime
/// number of numColumns"
/// Note: This definition cannot be a simple `using` declaration because we add
//        the constructor that takes an `allocator` because this constructor
//        is widely used inside the QLever codebase.
template <int COLS>
class IdTableStatic
    : public columnBasedIdTable::IdTable<Id, COLS, detail::IdVector> {
 public:
  using Base = columnBasedIdTable::IdTable<Id, COLS, detail::IdVector>;
  // Inherit the constructors.
  using Base::Base;

  IdTableStatic(Base&& b) : Base(std::move(b)) {}

  IdTableStatic& operator=(Base&& b) {
    *(static_cast<Base*>(this)) = std::move(b);
    return *this;
  }

  IdTableStatic(detail::DefaultAllocator allocator)
      : Base{detail::IdVector{allocator}} {}
  IdTableStatic(size_t numColumns, detail::DefaultAllocator allocator)
      : Base{numColumns, detail::IdVector{allocator}} {}
};

// This was previously implemented as an alias (`using IdTable =
// IdTableStatic<0, ...>`). However this did not allow forward declarations, so
// we now implement `IdTable` as a subclass of `IdTableStatic<0, ...>` that can
// be implicitly converted to and from `IdTableStatic<0, ...>`.
class IdTable : public IdTableStatic<0> {
 public:
  using Base = IdTableStatic<0>;
  // Inherit the constructors.
  using Base::Base;

  IdTable(Base&& b) : Base(std::move(b)) {}
};

/// A constant view into an IdTable that does not own its data
template <int COLS>
using IdTableView =
    columnBasedIdTable::IdTable<Id, COLS, detail::IdVector,
                                columnBasedIdTable::IsView::True>;
