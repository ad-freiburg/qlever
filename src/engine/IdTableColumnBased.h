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
#include "util/UninitializedAllocator.h"

namespace columnBasedIdTable {

// TODO<joka921> The NumCols should be `size_t` but that requires several
// additional changes in the rest of the code.
template <int NumCols = 0, typename Allocator = std::allocator<Id>,
          bool isView = false>
class IdTable {
 public:
  static constexpr bool isDynamic = NumCols == 0;

  // Make the number of (statically known) columns accesible to the outside.
  static constexpr int numStaticCols = NumCols;
  // using Column = std::vector<Id, Allocator>;
  using Columns = std::vector<Id, Allocator>;

  using Data = std::conditional_t<isView, const Columns*, Columns>;

  // TODO<joka921> Make clear that there are value rows and reference rows.
  // using row_type = RowReference<IdTable>;
  using row_type = Row<NumCols>;
  using row_reference = RowReference<IdTable, false>;
  using const_row_reference = RowReference<IdTable, true>;

 private:
  Data data_;
  size_t numCols_ = NumCols;
  size_t size_ = 0;
  size_t capacity_ = 0;
  static constexpr size_t growthFactor = 2;

  void grow(size_t newCapacity) {
    Columns newData{getAllocator()};
    newData.resize(newCapacity * cols());
    size_t sz = std::min(capacity_, newCapacity);
    for (size_t i = 0; i < cols(); ++i) {
      std::copy(data().begin() + i * sz, data().begin() + (i + 1) * sz,
                newData.begin() + i * newCapacity);
    }
    capacity_ = newCapacity;
    data() = std::move(newData);
  }

  void grow() { grow(std::max(1ul, capacity_ * growthFactor)); }

  void growIfNecessary() {
    if (size_ == capacity_) {
      grow();
    }
  }

 public:
  Columns& data() requires(!isView) { return data_; }

  const Columns& data() const {
    if constexpr (isView) {
      return *data_;
    } else {
      return data_;
    }
  }

  IdTable(size_t numCols, Allocator allocator) requires(!isView)
      : data_(Columns(std::move(allocator))), numCols_{numCols} {
    if constexpr (!isDynamic) {
      AD_CHECK(NumCols == numCols);
    }
  }
  IdTable(Allocator allocator = {}) requires(!isView)
      : IdTable{NumCols, std::move(allocator)} {};

  IdTable(Data data, size_t numCols, size_t size, size_t capacity)
      : data_{std::move(data)},
        numCols_{numCols},
        size_{size},
        capacity_{capacity} {
    if constexpr (!isDynamic) {
      AD_CHECK(numCols == NumCols);
    }
  }

  // TODO<joka921> Should we keep track of the size manually for better
  // performance?
  size_t size() const { return size_; }

  size_t rows() const { return size(); }
  size_t cols() const {
    if constexpr (isDynamic) {
      return numCols_;
    } else {
      return NumCols;
    }
  }

  // TODO<joka921, C++23> Use the multidimensional subscript operator.
  Id& operator()(size_t row, size_t col) requires(!isView) {
    return data()[col * capacity_ + row];
  }

  const Id& operator()(size_t row, size_t col) const {
    return data()[col * capacity_ + row];
  }

  void setCols(size_t cols) requires(NumCols == 0) {
    AD_CHECK(size() == 0);
    numCols_ = cols;
  }

  void resize(size_t numRows) requires(!isView) {
    grow(numRows);
    size_ = numRows;
  }

  void reserve(size_t numRows) requires(!isView) {
    if (numRows > capacity_) {
      grow(numRows);
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
                                            false, Row<NumCols>,
                                            RowReference<IdTable, false>>;
  using const_iterator =
      ad_utility::IteratorForAccessOperator<IdTable, IteratorHelper<true>, true,
                                            Row<NumCols>,
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
  requires(NumCols == 0 && !isView) IdTable<NewCols, Allocator> moveToStatic() {
    // TODO<joka921> Some parts of the code currently assume that
    // calling `moveToStatic<K>` for `K != 0` and an empty input is fine
    // without explicitly specifying the number of columns in a previous call
    // to `setCols`.
    if (size() == 0 && NewCols != 0) {
      setCols(NewCols);
    }
    AD_CHECK(cols() == NewCols || NewCols == 0);
    return IdTable<NewCols, Allocator>{std::move(data()), cols(), size(),
                                       capacity_};
  }

  IdTable<0, Allocator> moveToDynamic() requires(!isView) {
    return IdTable<0, Allocator>{std::move(data()), numCols_, size(),
                                 capacity_};
  }

  template <size_t NewCols>
  requires(NumCols == 0 &&
           !isView) IdTable<NewCols, Allocator, true> asStaticView()
  const {
    // TODO<joka921> It shouldn't be necessary to store an allocator in a view.
    return IdTable<NewCols, Allocator, true>{&data(), numCols_, size_,
                                             capacity_};
  }

  void emplace_back() requires(!isView) { push_back(); }

  void push_back() requires(!isView) {
    growIfNecessary();
    ++size_;
  }

  void push_back(const std::initializer_list<Id>& init) requires(!isView) {
    assert(init.size() == cols());
    emplace_back();
    auto sz = size();
    for (size_t i = 0; i < cols(); ++i) {
      operator()(sz - 1, i) = *(init.begin() + i);
    }
  }
  template <size_t N>
  void push_back(const std::array<Id, N>& init) requires(!isView &&
                                                         (NumCols == 0 ||
                                                          NumCols == N)) {
    if constexpr (NumCols == 0) {
      assert(init.size() == cols());
    }
    emplace_back();
    auto sz = size();
    for (size_t i = 0; i < cols(); ++i) {
      operator()(sz - 1, i) = *(init.begin() + i);
    }
  }

  template <typename T, bool isConst>
  void push_back(const RowReference<T, isConst>& el) requires(!isView) {
    // TODO<joka921> Assert that there is the right number of columns.
    emplace_back();
    for (size_t i = 0; i < cols(); ++i) {
      (*this)(size() - 1, i) = el[i];
    }
  }
  template <typename T, bool isConst>
  void push_back(
      const RowReferenceImpl::DeducingRowReferenceViaAutoIsLikelyABug<
          T, isConst>&& el) requires(!isView) {
    // TODO<joka921> Assert that there is the right number of columns.
    emplace_back();
    for (size_t i = 0; i < cols(); ++i) {
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
    if (cols() != other.cols()) {
      return false;
    }
    for (size_t i = 0; i < rows(); ++i) {
      for (size_t j = 0; j < cols(); ++j) {
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

  IdTable<NumCols, Allocator, false> clone() const {
    return IdTable<NumCols, Allocator, false>{data(), numCols_, size_,
                                              capacity_};
  }

  void erase(const iterator& beginIt, const iterator& endIt) requires(!isView) {
    auto startIndex = beginIt - begin();
    auto endIndex = endIt - begin();
    // TODO<joka921> Factor out variables, comment and test.
    for (size_t i = 0; i < cols(); ++i) {
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
};

}  // namespace columnBasedIdTable