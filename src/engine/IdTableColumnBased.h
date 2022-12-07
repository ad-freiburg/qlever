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
//            `IdTable`. Such views are cheap to copy as they just store.
//            A const pointer to another `IdTable`.
// TODO<joka921> The NumColumns should be `size_t` but that requires several
// additional changes in the rest of the code.
template <int NumColumns = 0, typename Allocator = std::allocator<Id>,
          bool isView = false>
class IdTable {
 public:
  static constexpr bool isDynamic = NumColumns == 0;
  // Make the number of (statically known) columns accessible to the outside.
  static constexpr int numStaticColumns = NumColumns;
  // The actual storage is a plain 1-D vector with the logical columns concatenated.
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

 private:
  Data data_;
  size_t numColumns_ = NumColumns;
  size_t size_ = 0;
  size_t capacity_ = 0;
  static constexpr size_t growthFactor = 2;

 public:
  // Construct from the number of columns and an allocator. If `NumColumns != 0`
  // Then the argument `numColumns` and `NumColumns` (the static and the dynamic)
  // number of columns) must be equal, else an assertion fails.
  IdTable(size_t numColumns, Allocator allocator = Allocator{}) requires(!isView)
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
  // so it is currently private.
  // TODO<joka921> Should this (possibly with an explicit unsafe tag) be the
  // function that the `IndexScan` can use?
  IdTable(Data data, size_t numCols, size_t size, size_t capacity)
      : data_{std::move(data)},
        numColumns_{numCols},
        size_{size},
        capacity_{capacity} {
    if constexpr (!isDynamic) {
      AD_CHECK(numCols == NumColumns);
    }
    AD_CHECK(size_ <= capacity_);
    AD_CHECK(this->data().size() == numColumns_ * capacity_);
  }

 public:
  // Get direct access to the underlying data().
  // TODO<joka921> This is public because it is needed by the IndexScan.
  // But probably the IndexScan should fully create the data and then
  // pass it to the `IdTable`.
  Columns& data() requires(!isView) { return data_; }
  const Columns& data() const {
    if constexpr (isView) {
      return *data_;
    } else {
      return data_;
    }
  }

  // TODO<joka921> continue commenting and self-reviewing at this position.
  size_t size() const { return size_; }
  size_t numRows() const { return size(); }
  size_t numColumns() const {
    if constexpr (isDynamic) {
      return numColumns_;
    } else {
      return NumColumns;
    }
  }

  // TODO<joka921, C++23> Use the multidimensional subscript operator.
  Id& operator()(size_t row, size_t col) requires(!isView) {
    return data()[col * capacity_ + row];
  }

  const Id& operator()(size_t row, size_t col) const {
    return data()[col * capacity_ + row];
  }

  void setCols(size_t cols) requires(NumColumns == 0) {
    AD_CHECK(size() == 0);
    numColumns_ = cols;
  }

  void resize(size_t numRows) requires(!isView) {
    setCapacity(numRows);
    size_ = numRows;
  }

  void reserve(size_t numRows) requires(!isView) {
    if (numRows > capacity_) {
      setCapacity(numRows);
    }
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

  // TODO<joka921> Should those following function be implemented direcly
  // via a single templated constructor?
  template <int NewCols>
  requires(NumColumns == 0 && !isView) IdTable<NewCols, Allocator> moveToStatic() {
    // TODO<joka921> Some parts of the code currently assume that
    // calling `moveToStatic<K>` for `K != 0` and an empty input is fine
    // without explicitly specifying the number of columns in a previous call
    // to `setCols`.
    if (size() == 0 && NewCols != 0) {
      setCols(NewCols);
    }
    AD_CHECK(numColumns() == NewCols || NewCols == 0);
    return IdTable<NewCols, Allocator>{std::move(data()), numColumns(), size(),
                                       capacity_};
  }

  IdTable<0, Allocator> moveToDynamic() requires(!isView) {
    return IdTable<0, Allocator>{std::move(data()), numColumns_, size(),
                                 capacity_};
  }

  template <size_t NewCols>
  requires(NumColumns == 0 &&
           !isView) IdTable<NewCols, Allocator, true> asStaticView()
  const {
    // TODO<joka921> It shouldn't be necessary to store an allocator in a view.
    return IdTable<NewCols, Allocator, true>{&data(), numColumns_, size_,
                                             capacity_};
  }

  void emplace_back() requires(!isView) { push_back(); }

  void push_back() requires(!isView) {
    growIfNecessary();
    ++size_;
  }

  void push_back(const std::initializer_list<Id>& init) requires(!isView) {
    assert(init.size() == numColumns());
    emplace_back();
    auto sz = size();
    for (size_t i = 0; i < numColumns(); ++i) {
      operator()(sz - 1, i) = *(init.begin() + i);
    }
  }
  template <size_t N>
  void push_back(const std::array<Id, N>& init) requires(!isView &&
                                                         (NumColumns == 0 || NumColumns == N)) {
    if constexpr (NumColumns == 0) {
      assert(init.size() == numColumns());
    }
    emplace_back();
    auto sz = size();
    for (size_t i = 0; i < numColumns(); ++i) {
      operator()(sz - 1, i) = *(init.begin() + i);
    }
  }

  template <typename T, bool isConst>
  void push_back(const RowReference<T, isConst>& el) requires(!isView) {
    // TODO<joka921> Assert that there is the right number of columns.
    emplace_back();
    for (size_t i = 0; i < numColumns(); ++i) {
      (*this)(size() - 1, i) = el[i];
    }
  }
  template <typename T, bool isConst>
  void push_back(
      const RowReferenceImpl::DeducingRowReferenceViaAutoIsLikelyABug<
          T, isConst>&& el) requires(!isView) {
    // TODO<joka921> Assert that there is the right number of columns.
    emplace_back();
    for (size_t i = 0; i < numColumns(); ++i) {
      (*this)(size() - 1, i) = std::move(el)[i];
    }
  }
  /*
  // TODO<joka921> Is this efficient, should other functions be used, or should
  //  this be implemented differently? Let the `Row` class converge first.
  void push_back(const const_row_type& el) requires(!isView) {
    emplace_back();
    *(end() - 1) = el;
  }
   */

  // TODO<joka921> Should we keep those interfaces? Are they efficient?
  // Or just used in unit tests for convenience?
  auto operator[](size_t index) requires(!isView) { return *(begin() + index); }

  auto operator[](size_t index) const { return *(begin() + index); }

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

  void clear() requires(!isView) {
    // Actually keep the memory.
    // TODO<joka921> Should we implement and use `shrink_to_fit`?
    size_ = 0;
  }

  Allocator getAllocator() const { return data().get_allocator(); }

  IdTable<NumColumns, Allocator, false> clone() const {
    return IdTable<NumColumns, Allocator, false>{data(), numColumns_, size_,
                                              capacity_};
  }

  void erase(const iterator& beginIt, const iterator& endIt) requires(!isView) {
    auto startIndex = beginIt - begin();
    auto endIndex = endIt - begin();
    // TODO<joka921> Factor out variables, comment and test.
    for (size_t i = 0; i < numColumns(); ++i) {
      std::shift_left(data().begin() + i * capacity_ + startIndex,
                      data().begin() + (i + 1) * capacity_,
                      endIndex - startIndex);
    }
    size_ -= endIndex - startIndex;
  }

  void erase(const iterator& it) requires(!isView) { erase(it, it + 1); }

  // TODO<joka921> Insert can be done much more efficient when handled
  // columnwise (also with arbitrary iterators). But that probably needs more
  // cooperation from the iterators.
  // TODO<joka921> The parameters are currently `auto` because the iterators
  // of views and materialized tables have different types. This should
  // be constrained to an efficient implementation.
  void insertAtEnd(auto begin, auto end) {
    for (; begin != end; ++begin) {
      push_back(*begin);
    }
  }

 private:
// Set the capacity to `newCapacity` and reinstate the memory layout.
// If `newCapacity < size()` then the table will also be truncated at the end
// TODO<joka921> what was the use case for this again?
  void setCapacity(size_t newCapacity) {
    Columns newData{getAllocator()};
    newData.resize(newCapacity * numColumns());
    size_t sz = std::min(capacity_, newCapacity);
    for (size_t i = 0; i < numColumns(); ++i) {
      std::copy(data().begin() + i * sz, data().begin() + (i + 1) * sz,
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
};


}  // namespace columnBasedIdTable
