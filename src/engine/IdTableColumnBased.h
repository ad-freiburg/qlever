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

template <int NumCols = 0, typename Allocator = std::allocator<Id>,
          bool isView = false>
class IdTable {
 public:
  using Column = std::vector<Id, Allocator>;
  using Columns = std::vector<Column>;

  using Data = std::conditional_t<isView, const Columns*, Columns>;

  using row_type = Row<NumCols, false>;
  using const_row_type = Row<NumCols, true>;

 private:
  Data data_;
  Allocator allocator_{};

 public:
  Columns& data() requires(!isView) { return data_; }

  const Columns& data() const {
    if constexpr (isView) {
      return *data_;
    } else {
      return data_;
    }
  }

  IdTable(int numCols, Allocator allocator) requires(!isView)
      : allocator_{std::move(allocator)} {
    if (NumCols != 0) {
      AD_CHECK(NumCols == numCols);
    }
    data().reserve(numCols);
    for (int i = 0; i < numCols; ++i) {
      data().emplace_back(allocator_);
    }
  }
  IdTable(Allocator allocator = {}) requires(!isView)
      : IdTable{NumCols, std::move(allocator)} {};

  IdTable(Data data, Allocator allocator)
      : data_{std::move(data)}, allocator_{std::move(allocator)} {}
  size_t size() const { return data().empty() ? 0 : data().at(0).size(); }
  size_t rows() const { return size(); }
  size_t cols() const { return data().size(); }
  // TODO<joka921, C++23> Use the multidimensional subscript operator.

  Id& operator()(size_t row, size_t col) requires(!isView) {
    return data()[col][row];
  }
  const Id& operator()(size_t row, size_t col) const {
    return data()[col][row];
  }

  void setCols(size_t cols) requires(NumCols == 0) {
    AD_CHECK(size() == 0);
    data().clear();
    data().reserve(cols);
    for (size_t i = 0; i < cols; ++i) {
      data().emplace_back(allocator_);
    }
  }

  void resize(size_t numRows) {
    std::ranges::for_each(data(), [&](auto& vec) { vec.resize(numRows); });
  }

  void reserve(size_t numRows) {
    std::ranges::for_each(data(), [&](auto& vec) { vec.reserve(numRows); });
  }

  template <bool isConst>
  struct IteratorHelper {
    using R = Row<NumCols, isConst>;
    R baseRow_;

    // The default constructor constructs an invalid iterator that may not
    // be dereferenced. It is however required by several algorithms in the
    // STL.
    IteratorHelper() : baseRow_{NumCols} {}

    explicit IteratorHelper(R baseRow)
        : baseRow_(baseRow, typename R::CloneTag{}, 0) {}
    auto operator()(auto&&, size_t idx) const {
      return R{baseRow_, typename R::CloneTag{}, idx};
    }

    IteratorHelper(const IteratorHelper& other)
        : baseRow_{other.baseRow_, typename R::CloneTag{}, 0} {}
    IteratorHelper& operator=(const IteratorHelper& other) {
      baseRow_.cloneAssign(other.baseRow_);
      return *this;
    }
  };

  using iterator =
      ad_utility::IteratorForAccessOperator<IdTable, IteratorHelper<false>,
                                            false>;

  using const_iterator =
      ad_utility::IteratorForAccessOperator<IdTable, IteratorHelper<true>,
                                            true>;

  auto getBaseRow() requires(!isView) {
    typename Row<NumCols, false>::Ref row;
    if constexpr (NumCols == 0) {
      row.resize(cols());
    }
    for (size_t i = 0; i < cols(); ++i) {
      row[i] = data()[i].data();
    }
    return Row<NumCols, false>{std::move(row)};
  }

  auto getBaseRow() const {
    typename Row<NumCols, true>::Ref row;
    if constexpr (NumCols == 0) {
      row.resize(cols());
    }
    for (size_t i = 0; i < cols(); ++i) {
      row[i] = data()[i].data();
    }
    return Row<NumCols, true>{std::move(row)};
  }

  iterator begin() requires(!isView) {
    return {this, 0, IteratorHelper<false>{getBaseRow()}};
  }
  iterator end() requires(!isView) {
    return {this, size(), IteratorHelper<false>{getBaseRow()}};
  }

  const_iterator begin() const {
    return {this, 0, IteratorHelper<true>{getBaseRow()}};
  }
  const_iterator end() const {
    return {this, size(), IteratorHelper<true>{getBaseRow()}};
  }

  const_iterator cbegin() const { return begin(); }
  const_iterator cend() const { return end(); }

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
    return IdTable<NewCols, Allocator>{std::move(data()),
                                       std::move(allocator_)};
  }

  IdTable<0, Allocator> moveToDynamic() requires(!isView) {
    return IdTable<0, Allocator>{std::move(data()), std::move(allocator_)};
  }

  template <int NewCols>
  requires(NumCols == 0 &&
           !isView) IdTable<NewCols, Allocator, true> asStaticView()
  const {
    // TODO<joka921> It shouldn't be necessary to store an allocator in a view.
    return IdTable<NewCols, Allocator, true>{&data(), allocator_};
  }

  void emplace_back() requires(!isView) { push_back(); }

  void push_back() requires(!isView) {
    for (auto& col : data()) {
      col.emplace_back();
    }
  }

  void push_back(const std::initializer_list<Id>& init) requires(!isView) {
    assert(init.size() == cols());
    for (size_t i = 0; i < cols(); ++i) {
      data()[i].push_back(*(init.begin() + i));
    }
  }
  template <size_t N>
  void push_back(const std::array<Id, N>& init) requires(!isView &&
                                                         (NumCols == 0 ||
                                                          NumCols == N)) {
    if constexpr (NumCols == 0) {
      assert(init.size() == cols());
    }
    for (size_t i = 0; i < cols(); ++i) {
      data()[i].push_back(*(init.begin() + i));
    }
  }

  void push_back(const row_type& el) requires(!isView) {
    emplace_back();
    *(end() - 1) = el;
  }
  // TODO<joka921> Is this efficient, what else should we use?
  void push_back(const const_row_type& el) requires(!isView) {
    emplace_back();
    *(end() - 1) = el;
  }

  // TODO<joka921> Should we keep those interfaces? Are they efficient?
  // Or just used in unit tests for convenience?
  auto operator[](size_t index) requires(!isView) { return *(begin() + index); }

  auto operator[](size_t index) const { return *(begin() + index); }

  bool operator==(const IdTable& other) const requires(!isView) {
    return data() == other.data();
  }

  void clear() requires(!isView) {
    // Note: It is important to only clear the columns and to keep the empty
    // columns, s.t. the number of columns stays the same.
    std::ranges::for_each(data(), &Column::clear);
  }

  Allocator& getAllocator() requires(!isView) { return allocator_; }

  const Allocator& getAllocator() const { return allocator_; }

  IdTable<NumCols, Allocator, false> clone() const {
    return IdTable<NumCols, Allocator, false>{data(), getAllocator()};
  }

  void erase(const iterator& beginIt, const iterator& endIt) requires(!isView) {
    auto startIndex = beginIt - begin();
    auto endIndex = endIt - begin();
    for (auto& column : data()) {
      column.erase(column.begin() + startIndex, column.begin() + endIndex);
    }
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