// Copyright 2021 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDTABLE_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDTABLE_H

#include <array>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <span>
#include <variant>
#include <vector>

#include "backports/algorithm.h"
#include "engine/idTable/IdTableRow.h"
#include "engine/idTable/VectorWithElementwiseMove.h"
#include "global/Id.h"
#include "util/Algorithm.h"
#include "util/AllocatorWithLimit.h"
#include "util/Iterators.h"
#include "util/LambdaHelpers.h"
#include "util/ResetWhenMoved.h"
#include "util/SourceLocation.h"
#include "util/UninitializedAllocator.h"
#include "util/Views.h"

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
          typename ColumnStorage = std::vector<
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
  using Storage = detail::VectorWithElementwiseMove<ColumnStorage>;
  using ViewSpans = std::vector<std::span<const T>>;
  using Data = std::conditional_t<isView, ViewSpans, Storage>;
  using Allocator = decltype(std::declval<ColumnStorage&>().get_allocator());

  static constexpr bool columnsAreAllocatable =
      std::is_constructible_v<ColumnStorage, size_t, Allocator>;

  // The type of a single entry in a row.
  using single_value_type = T;
  // Because of the column-major layout, the `row_type` (a value type that
  // stores the values of a  single row) and the `row_reference` (a type that
  // refers to a specific row of a specific `IdTable`) are different. They are
  // implemented in a way that makes it hard to use them incorrectly. For
  // details, see the comment above and the definition of the `Row` and
  // `RowReference` class.
  using row_type = Row<T, NumColumns>;
  using row_reference = RowReference<IdTable, ad_utility::IsConst::False>;
  using const_row_reference = RowReference<IdTable, ad_utility::IsConst::True>;

  // This alias is required to make the `IdTable` class work with advanced GTest
  // features, because GTest uses `Container::value_type` directly instead of
  // using `std::iterator_traits`.
  using value_type = row_type;

 private:
  // Assign shorter aliases for some types that are important for the correct
  // handling of the proxy reference, but that are not visible to the outside.
  // For details see `IdTableRow.h`.
  // Note: Using the (safer) `restricted` row references is not supported by
  // `libc++` as it enforces that the `reference_type` of an iterator and the
  // type returned by `operator*` are exactly the same.
#ifdef __GLIBCXX__
  using row_reference_restricted =
      RowReferenceImpl::RowReferenceWithRestrictedAccess<
          IdTable, ad_utility::IsConst::False>;
  using const_row_reference_restricted =
      RowReferenceImpl::RowReferenceWithRestrictedAccess<
          IdTable, ad_utility::IsConst::True>;
  using const_row_reference_view_restricted =
      RowReferenceImpl::RowReferenceWithRestrictedAccess<
          IdTable<T, NumColumns, ColumnStorage, IsView::True>,
          ad_utility::IsConst::True>;
#else
  using row_reference_restricted = row_reference;
  using const_row_reference_restricted = const_row_reference;
  using const_row_reference_view_restricted =
      RowReference<IdTable<T, NumColumns, ColumnStorage, IsView::True>,
                   ad_utility::IsConst::True>;
#endif

 private:
  Data data_;
  size_t numColumns_ = NumColumns;
  ad_utility::ResetWhenMoved<size_t, 0> numRows_ = 0;
  static constexpr double growthFactor = 1.5;
  Allocator allocator_{};

 public:
  // Construct from the number of columns and an allocator. If `NumColumns != 0`
  // Then the argument `numColumns` and `NumColumns` (the static and the
  // dynamic number of columns) must be equal, else a runtime check fails.
  // Note: this also allows to create an empty view.
  CPP_template(typename = void)(
      requires columnsAreAllocatable CPP_and std::is_default_constructible_v<
          Allocator>) explicit IdTable(size_t numColumns)
      : IdTable(numColumns, Allocator{}) {}

  CPP_template(typename = void)(
      requires(columnsAreAllocatable)) explicit IdTable(size_t numColumns,
                                                        Allocator allocator)
      : numColumns_{numColumns}, allocator_{std::move(allocator)} {
    if constexpr (!isDynamic) {
      AD_CONTRACT_CHECK(NumColumns == numColumns);
    }

    data_.resize(numColumns, ColumnStorage{allocator_});
  }

  // Construct from the number of columns and a container (e.g. `vector` or
  // `array`) of empty columns. If `NumColumns != 0` then the argument
  // `numColumns` and `NumColumns` (the static and the dynamic number of
  // columns) must be equal, else a runtime check fails. The number of empty
  // `columns` passed in must at least be `numColumns`, else a runtime check
  // fails. Additional columns (if `columns.size() > numColumns`) are deleted.
  // This behavior is useful for unit tests Where we can just generically pass
  // in more columns than are needed in any test.
  CPP_template(typename ColT)(
      requires ql::ranges::forward_range<ColT> CPP_and CPP_NOT(isView))
      IdTable(size_t numColumns, ColT columns)
      : data_{std::make_move_iterator(columns.begin()),
              std::make_move_iterator(columns.end())},
        numColumns_{numColumns} {
    if constexpr (!isDynamic) {
      AD_CONTRACT_CHECK(NumColumns == numColumns);
    }
    AD_CONTRACT_CHECK(data().size() >= numColumns_);
    if (data().size() > numColumns_) {
      data().erase(data().begin() + numColumns_, data().end());
    }
    AD_CONTRACT_CHECK(ql::ranges::all_of(
        data(), [](const auto& column) { return column.empty(); }));
  }

  // Quasi the default constructor. If `NumColumns != 0` then the table is
  // already set up with the correct number of columns and can be used directly.
  // If `NumColumns == 0` then the number of columns has to be specified via
  // `setNumColumns()`.
  CPP_template(typename = void)(
      requires CPP_NOT(isView) CPP_and columnsAreAllocatable CPP_and
          std::is_default_constructible_v<Allocator>) IdTable()
      : IdTable{NumColumns, Allocator{}} {};

  CPP_template(typename = void)(
      requires CPP_NOT(isView)
          CPP_and columnsAreAllocatable) explicit IdTable(Allocator allocator)
      : IdTable{NumColumns, std::move(allocator)} {};

  // `IdTables` are expensive to copy, so we disable accidental copies as they
  // are most likely bugs. To explicitly copy an `IdTable`, the `clone()` member
  // function (see below) can be used.
  // Note: We currently only disable the copy operations in C++20 mode.
  // TODO<joka921> implement a facility (probably via inheritance) where we can
  // also implement the deleted copy operations for C++17
#ifndef QLEVER_CPP_17
  IdTable(const IdTable&) requires(!isView) = delete;
  IdTable& operator=(const IdTable&) requires(!isView) = delete;

  //  Views are copyable, as they are cheap to copy.
  IdTable(const IdTable&) requires isView = default;
  IdTable& operator=(const IdTable&) requires isView = default;

#else
  IdTable(const IdTable&) = default;
  IdTable& operator=(const IdTable&) = default;
#endif

  // `IdTable`s are movable
  IdTable(IdTable&& other) noexcept = default;
  IdTable& operator=(IdTable&& other) noexcept = default;

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
  IdTable(Data data, size_t numColumns, size_t numRows, Allocator allocator)
      : data_{std::move(data)},
        numColumns_{numColumns},
        numRows_{numRows},
        allocator_{std::move(allocator)} {
    if constexpr (!isDynamic) {
      AD_CORRECTNESS_CHECK(numColumns == NumColumns);
    }
    AD_CORRECTNESS_CHECK(this->data().size() == numColumns_);
    AD_CORRECTNESS_CHECK(ql::ranges::all_of(
        this->data(),
        [this](const auto& column) { return column.size() == numRows_; }));
  }

 public:
  // For an empty and dynamic (`NumColumns == 0`) `IdTable`, specify the
  // number of columns.
  CPP_template(typename = void)(
      requires(columnsAreAllocatable)) void setNumColumns(size_t numColumns) {
    AD_CONTRACT_CHECK(empty());
    AD_CONTRACT_CHECK(isDynamic || numColumns == NumColumns);
    numColumns_ = numColumns;
    data().resize(numColumns, ColumnStorage{allocator_});
  }

  // Add a new empty column to the table.
  CPP_template(typename = void)(
      requires columnsAreAllocatable CPP_and isDynamic) void addEmptyColumn() {
    data().emplace_back(size(), allocator_);
    ++numColumns_;
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
  auto getAllocator() const { return allocator_; }

  // Get access to a single element specified by the row and the column.
  // Note: Since this class has a column-based layout, the usage of the
  // column-based interface (`getColumn` and `getColumns`) should be preferred
  // for performance reason whenever possible.
  // TODO<joka921, C++23> Use the multidimensional subscript operator.
  // TODO<joka921, C++23> Use explicit object parameters ("deducing this").
  CPP_template(typename = void)(requires(!isView)) T& operator()(
      size_t row, size_t column) {
    AD_EXPENSIVE_CHECK(column < data().size(), [&]() {
      return absl::StrCat(row, " , ", column, ", ", data().size(), " ",
                          numColumns(), ", ", numStaticColumns);
    });
    AD_EXPENSIVE_CHECK(row < data().at(column).size());
    return data()[column][row];
  }
  const T& operator()(size_t row, size_t column) const {
    return data()[column][row];
  }

  // Get safe access to a single element specified by the row and the column.
  // Throw if the row or the column is out of bounds. See the note for
  // `operator()` above.
  CPP_template(typename = void)(requires(!isView)) T& at(size_t row,
                                                         size_t column)
      requires(!isView) {
    return data().at(column).at(row);
  }
  // TODO<C++26> Remove overload for `isView` and drop requires clause.
  CPP_template(typename = void)(requires(!isView)) const T& at(
      size_t row, size_t column) const {
    return data().at(column).at(row);
  }
  // `std::span::at` is a C++26 feature, so we have to implement it ourselves.
  CPP_template(typename = void)(requires(isView)) const T& at(
      size_t row, size_t column) const requires(isView) {
    const auto& col = data().at(column);
    AD_CONTRACT_CHECK(row < col.size());
    return col[row];
  }

  // Get a reference to the `i`-th row. The returned proxy objects can be
  // implicitly and trivially converted to `row_reference`. For the design
  // rationale behind those proxy types see above for their definition.
  CPP_template(typename = void)(requires(!isView)) row_reference_restricted
  operator[](size_t index) {
    return *(begin() + index);
  }
  const_row_reference_restricted operator[](size_t index) const {
    return *(begin() + index);
  }

  // Same as operator[], but throw an exception if the `row` is out of bounds.
  // This is similar to the behavior of `std::vector::at`.
  CPP_template(typename = void)(requires(!isView)) row_reference_restricted
      at(size_t row) {
    AD_CONTRACT_CHECK(row < numRows());
    return operator[](row);
  }
  const_row_reference_restricted at(size_t row) const {
    AD_CONTRACT_CHECK(row < numRows());
    return operator[](row);
  }

  // The usual `front` and `back` functions to make the interface similar to
  // `std::vector` and other containers.
  // TODO<C++23, joka921> Remove the duplicates via explicit object parameters
  // ("deducing this").
  CPP_template(typename = void)(requires(!isView)) row_reference_restricted
      front() {
    return (*this)[0];
  }

  const_row_reference_restricted front() const { return (*this)[0]; }

  CPP_template(typename = void)(requires(!isView)) row_reference_restricted
      back() {
    return (*this)[numRows() - 1];
  }
  const_row_reference_restricted back() const { return (*this)[numRows() - 1]; }

  // Resize the `IdTable` to exactly `numRows`. If `numRows < size()`, then the
  // last `size() - numRows` rows of the table will be deleted. If
  // `numRows > size()`, `numRows - size()` new, uninitialized rows are appended
  // at the end. If `numRows > capacityRows_` then all iterators are
  // invalidated, as the function has to allocate new memory.
  //
  // Note: The semantics of this function is similar to `std::vector::resize`.
  // To set the capacity, use the `reserve` function.
  CPP_template(typename = void)(requires(!isView)) void resize(size_t numRows) {
    ql::ranges::for_each(data(),
                         [numRows](auto& column) { column.resize(numRows); });
    numRows_ = numRows;
  }

  // Reserve space for `numRows` rows. If `numRows <= capacityRows_` then
  // nothing happens. Otherwise, reserve enough memory for `numRows` rows
  // without changing the actual `size()` of the IdTable. In this case, all
  // iterators are invalidated, but you obtain the guarantee, that the insertion
  // of the next `numRows - size()` elements (via `insert` or `push_back`) can
  // be done in O(1) time without dynamic allocations.
  CPP_template(typename = void)(requires(!isView)) void reserve(
      size_t numRows) {
    ql::ranges::for_each(data(),
                         [numRows](auto& column) { column.reserve(numRows); });
  }

  // Delete all the elements, but keep the allocated memory (`capacityRows_`
  // stays the same). Runs in O(1) time. To also free the allocated memory, call
  // `shrinkToFit()` after calling `clear()` .
  CPP_template(typename = void)(requires(!isView)) void clear() {
    numRows_ = 0;
    ql::ranges::for_each(data(), [](auto& column) { column.clear(); });
  }

  // Adjust the capacity to exactly match the size. This optimizes the memory
  // consumption of this table. This operation runs in O(size()), allocates
  // memory, and invalidates all iterators.
  CPP_template(typename = void)(requires(!isView)) void shrinkToFit() {
    ql::ranges::for_each(data(), [](auto& column) { column.shrink_to_fit(); });
  }

  // Note: The following functions `emplace_back` and `push_back` all have the
  // following property: If `size() < capacityRows_` (before the operation) they
  // run in O(1). Else they run in O(size()) and all iterators are invalidated.
  // A sequence of `n` `emplace_back/push_back` operations runs in total time
  // `O(n)`. The underlying data model is a dynamic array like `std::vector`.

  // Insert a new uninitialized row at the end.
  CPP_template(typename = void)(requires(!isView)) void emplace_back() {
    ql::ranges::for_each(data(), [](auto& column) { column.emplace_back(); });
    ++numRows_;
  }

  // Add the `newRow` at the end. Requires that `newRow.size() == numColumns()`,
  // otherwise the behavior is undefined (in Release mode) or an assertion will
  // fail (in Debug mode). The `newRow` can be any random access range that
  // stores the right type and has the right size.
  CPP_template(typename RowLike)(
      requires CPP_NOT(isView) CPP_and ql::ranges::random_access_range<RowLike>
          CPP_and std::same_as<ql::ranges::range_value_t<RowLike>,
                               T>) void push_back(const RowLike& newRow) {
    AD_EXPENSIVE_CHECK(newRow.size() == numColumns());
    ++numRows_;
    ql::ranges::for_each(ad_utility::integerRange(numColumns()),
                         [this, &newRow](auto i) {
                           data()[i].push_back(*(std::begin(newRow) + i));
                         });
  }

  CPP_template(typename = void)(requires(!isView)) void push_back(
      const std::initializer_list<T>& newRow) {
    push_back(ql::ranges::ref_view{newRow});
  }

  // True iff we can make a copy (via the `clone` function below), because the
  // underlying storage types are copyable.
  static constexpr bool isCloneable =
      std::is_copy_constructible_v<Storage> &&
      std::is_copy_constructible_v<ColumnStorage>;
  // Create a deep copy of this `IdTable` that owns its memory. In most cases
  // this behaves exactly like the copy constructor with the following
  // exception: If `this` is a view (because the `isView` template parameter is
  // `true`), then the copy constructor will also create a (const and
  // non-owning) view, but `clone` will create a mutable deep copy of the data
  // that the view points to
  CPP_template(typename = void)(requires(isCloneable))
      IdTable<T, NumColumns, ColumnStorage, IsView::False> clone() const {
    Storage storage;
    for (const auto& column : getColumns()) {
      storage.emplace_back(column.begin(), column.end(), getAllocator());
    }
    return IdTable<T, NumColumns, ColumnStorage, IsView::False>{
        std::move(storage), numColumns_, numRows_, allocator_};
  }

  // Move or clone returns a copied or moved IdTable depending on the value
  // category of `*this`. The typical usage is
  // `auto newTable = AD_FWD(oldTable).moveOrClone()` which is equivalent to the
  // pattern `auto newX = AD_FWD(oldX)` where the type is copy-constructible
  // (which `IdTable` is not.).
  CPP_member auto moveOrClone() const& -> CPP_ret(IdTable)(
      requires isCloneable) {
    return clone();
  }

  CPP_member auto moveOrClone() && -> CPP_ret(IdTable&&)(requires isCloneable) {
    return std::move(*this);
  }

  // Overload of `clone` for `Storage` types that are not copy constructible.
  // It requires a preconstructed but empty argument of type `Storage` that
  // is then resized and filled with the appropriate contents.
  CPP_template(typename = void)(
      requires(!std::is_copy_constructible_v<ColumnStorage>))
      IdTable<T, NumColumns, ColumnStorage, IsView::False> clone(
          std::vector<ColumnStorage> newColumns,
          Allocator allocator = {}) const {
    AD_CONTRACT_CHECK(newColumns.size() >= numColumns());
    Data newStorage(std::make_move_iterator(newColumns.begin()),
                    std::make_move_iterator(newColumns.begin() + numColumns()));
    ql::ranges::for_each(
        ad_utility::integerRange(numColumns()), [this, &newStorage](auto i) {
          newStorage[i].insert(newStorage[i].end(), data()[i].begin(),
                               data()[i].end());
        });
    return IdTable<T, NumColumns, ColumnStorage, IsView::False>{
        std::move(newStorage), numColumns_, numRows_, allocator};
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
  CPP_template(int NewNumColumns)(
      requires(isDynamic || NewNumColumns == NumColumns || NewNumColumns == 0)
          CPP_and CPP_NOT(
              isView)) IdTable<T, NewNumColumns, ColumnStorage> toStatic() && {
    AD_CONTRACT_CHECK(numColumns() == NewNumColumns || NewNumColumns == 0);
    auto result = IdTable<T, NewNumColumns, ColumnStorage>{
        std::move(data()), numColumns(), std::move(numRows_),
        std::move(allocator_)};
    return result;
  }

  // Move this `IdTable` into a dynamic `IdTable` with `NumColumns == 0`. This
  // function may only be called on rvalues, because the table will be moved
  // from.
  CPP_template(typename = void)(
      requires(!isView)) IdTable<T, 0, ColumnStorage> toDynamic() && {
    auto result = IdTable<T, 0, ColumnStorage>{std::move(data()), numColumns_,
                                               std::move(numRows_),
                                               std::move(allocator_)};
    return result;
  }

  // Given a dynamic (`NumColumns == 0`) IdTable, create a static
  // (`NumColumns
  // != 0`) view of an `IdTable` with `NumColumns == NewNumColumns`. The
  // number of columns in the dynamic table must either be equal to
  // `NewNumColumns`, or the dynamic table must be empty; otherwise a
  // runtime check fails. The created view is `const` and only contains a
  // pointer to the table from which it was created. Therefore, calling this
  // function is cheap (O(1)), but the created view is only valid as long as
  // the original table is valid and unchanged.
  //
  // Note: This function can also be used with `NewNumColumns == 0`. Then it
  // creates a dynamic view from a dynamic table. This makes generic code
  // that is templated on the number of columns easier to write.
  CPP_template(size_t NewNumColumns)(requires(isDynamic || NewNumColumns == 0))
      IdTable<T, NewNumColumns, ColumnStorage, IsView::True> asStaticView()
          const {
    AD_CONTRACT_CHECK(numColumns() == NewNumColumns || NewNumColumns == 0);
    ViewSpans viewSpans(data().begin(), data().end());

    return IdTable<T, NewNumColumns, ColumnStorage, IsView::True>{
        std::move(viewSpans), numColumns_, numRows_, allocator_};
  }

  // Obtain a dynamic and const view to this IdTable that contains a subset of
  // the columns that may be permuted. The subset of the columns is specified by
  // the argument `columnIndices`.
  CPP_template(typename = void)(requires isDynamic)
      IdTable<T, 0, ColumnStorage, IsView::True> asColumnSubsetView(
          std::span<const ColumnIndex> columnIndices) const {
    AD_CONTRACT_CHECK(ql::ranges::all_of(
        columnIndices, [this](size_t idx) { return idx < numColumns(); }));
    ViewSpans viewSpans;
    viewSpans.reserve(columnIndices.size());
    for (auto idx : columnIndices) {
      viewSpans.push_back(getColumn(idx));
    }
    return IdTable<T, 0, ColumnStorage, IsView::True>{
        std::move(viewSpans), columnIndices.size(), numRows_, allocator_};
  }

  // Modify the table, such that it contains only the specified `subset` of the
  // original columns in the specified order. Each index in the `subset`
  // must be `< numColumns()` and must appear at most once in the subset.
  // The column with the old index `subset[i]` will become the `i`-th column
  // after the subset. For example `setColumnSubset({2, 1})` will result in a
  // table with 2 columns, with the original columns with index 2 and 1, with
  // their order switched. The special case where `subset.size() ==
  // numColumns()` implies that the function applies a permutation to the table.
  // For example `setColumnSubset({1, 2, 0})` rotates the columns of a table
  // with three columns left by one element.
  void setColumnSubset(std::span<const ColumnIndex> subset) {
    // First check that the `subset` is indeed a subset of the column
    // indices.
    std::vector<ColumnIndex> check{subset.begin(), subset.end()};
    ql::ranges::sort(check);
    AD_CONTRACT_CHECK(std::unique(check.begin(), check.end()) == check.end());
    AD_CONTRACT_CHECK(!subset.empty() && subset.back() < numColumns());

    AD_CONTRACT_CHECK(
        isDynamic || subset.size() == NumColumns,
        "For `IdTable`s with a statically fixed number of columns, "
        "`setColumnSubset()` must be only called with a permutation of the "
        "columns and not with an actual subset");

    Data newData;
    newData.reserve(subset.size());
    ql::ranges::for_each(subset, [this, &newData](ColumnIndex colIdx) {
      newData.push_back(std::move(data().at(colIdx)));
    });
    data() = std::move(newData);
    numColumns_ = subset.size();
  }

  // Swap the two columns at index `c1` and `c2`
  void swapColumns(ColumnIndex c1, ColumnIndex c2) {
    AD_EXPENSIVE_CHECK(c1 < numColumns() && c2 < numColumns());
    std::swap(data()[c1], data()[c2]);
  }

  // Delete the column with the given column index.
  CPP_template(typename = void)(requires isDynamic) void deleteColumn(
      ColumnIndex colIdx) {
    AD_CONTRACT_CHECK(colIdx < numColumns());
    data().erase(data().begin() + colIdx);
    numColumns_--;
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
  using const_iterator = ad_utility::IteratorForAccessOperator<
      IdTable, IteratorHelper<const_row_reference_restricted>,
      ad_utility::IsConst::True, row_type, const_row_reference>;
  using iterator = std::conditional_t<
      isView, const_iterator,
      ad_utility::IteratorForAccessOperator<
          IdTable, IteratorHelper<row_reference_restricted>,
          ad_utility::IsConst::False, row_type, row_reference>>;

  // The usual overloads of `begin()` and `end()` for const and mutable
  // `IdTable`s.
  CPP_template(typename = void)(requires(!isView)) iterator begin() {
    return {this, 0};
  }
  CPP_template(typename = void)(requires(!isView)) iterator end() {
    return {this, size()};
  }
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
  CPP_member auto erase(const iterator& beginIt, const iterator& endIt)
      -> CPP_ret(void)(requires(!isView)) {
    AD_EXPENSIVE_CHECK(begin() <= beginIt && beginIt <= endIt &&
                       endIt <= end());
    auto startIndex = beginIt - begin();
    auto endIndex = endIt - begin();
    auto numErasedElements = endIndex - startIndex;
    for (auto& column : data()) {
      column.erase(column.begin() + startIndex, column.begin() + endIndex);
    }
    numRows_ -= numErasedElements;
  }

  // Erase the single row that `it` points to by shifting all the elements
  // after `it` towards the beginning. Requires that `begin() <= it < end()`,
  // otherwise the behavior is undefined.
  CPP_member auto erase(const iterator& it)
      -> CPP_ret(void)(requires(!isView)) {
    erase(it, it + 1);
  }

  // Add all entries from the `table` at the end of this IdTable.
  // If `beginIdx` and/or `endIdx` are specified, then only the subrange
  // `[beginIdx, endIdx)` from the input is taken.
  // The input must be some kind of `IdTable`. The parameter `permutation`
  // allows the caller to apply a permutation to the columns of the input table
  // before inserting them into this table. If the permutation is not specified,
  // the columns are inserted in the order they appear in the input table. If
  // the permutation contains an index that is out of bounds for the input
  // table, the corresponding column is filled with the `defaultValue`.
  // TODO<joka921> Can/should we constraint this functions by a concept?
  template <typename Table>
  void insertAtEnd(
      const Table& table, std::optional<size_t> beginIdx = std::nullopt,
      std::optional<size_t> endIdx = std::nullopt,
      std::optional<std::vector<ColumnIndex>> permutation = std::nullopt,
      typename Table::single_value_type defaultValue = {}) {
    AD_CORRECTNESS_CHECK(
        table.numColumns() == numColumns() ||
        (permutation.has_value() && numColumns() == permutation->size()));
    auto begin = beginIdx.value_or(0);
    auto end = endIdx.value_or(table.size());
    AD_CORRECTNESS_CHECK(begin <= end && end <= table.size());
    auto numInserted = end - begin;
    auto oldSize = size();
    resize(numRows() + numInserted);
    ql::ranges::for_each(
        ad_utility::integerRange(numColumns()),
        [this, &table, oldSize, begin, numInserted, &permutation,
         &defaultValue](size_t i) {
          size_t mappedIndex =
              permutation.has_value() ? permutation.value()[i] : i;
          // Map out of index column indices from the default value.
          if (mappedIndex >= table.numColumns()) {
            ql::ranges::fill(getColumn(i).subspan(oldSize), defaultValue);
            return;
          }
          ql::ranges::copy(
              table.getColumn(mappedIndex).subspan(begin, numInserted),
              getColumn(i).begin() + oldSize);
        });
  }

  // Check whether two `IdTables` have the same content. Mostly used for unit
  // testing.
  CPP_template(typename = void)(requires(!isView)) bool operator==(
      const IdTable& other) const {
    if (numColumns() != other.numColumns()) {
      return (empty() && other.empty());
    }
    if (size() != other.numRows()) {
      return false;
    }

    // TODO<joka921, C++23> This can be implemented using `zip_view` and
    // `ql::ranges::all_of`. The iteration over the columns is cache-friendly.
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
  CPP_template(typename = void)(requires(!isView)) std::span<T> getColumn(
      size_t i) {
    return {data().at(i)};
  }
  std::span<const T> getColumn(size_t i) const { return {data().at(i)}; }

  // Return all the columns as a `std::vector` (if `isDynamic`) or as a
  // `std::array` (else). The elements of the vector/array are `std::span<T>`
  // or `std::span<const T>`, depending on whether `this` is const.
  auto getColumns() { return getColumnsImpl(*this); }
  auto getColumns() const { return getColumnsImpl(*this); }

 private:
  // Get direct access to the underlying data() as a reference.
  Data& data() { return data_; }
  const Data& data() const { return data_; }

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

  // This operator is only for debugging and testing. It returns a
  // human-readable representation.
  friend std::ostream& operator<<(std::ostream& os,
                                  const IdTableStatic& idTable) {
    os << "{ ";
    ql::ranges::copy(
        idTable, std::ostream_iterator<columnBasedIdTable::Row<Id>>(os, " "));
    os << "}";
    return os;
  }
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

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDTABLE_H
