// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#pragma once

#include <array>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <ostream>
#include <variant>

#include "../global/Id.h"
#include "../util/AllocatorWithLimit.h"
#include "../util/Log.h"
#include "../util/UninitializedAllocator.h"
#include "util/Iterators.h"
#include "util/LambdaHelpers.h"

namespace columnBasedIdTable {

template <int NumCols = 0, bool isConst = false>
class Row {
 public:
  static constexpr bool isDynamic() { return NumCols == 0; }
  using Ptr = std::conditional_t<isConst, const Id*, Id*>;
  using Storage =
      std::conditional_t<isDynamic(), std::vector<Id>, std::array<Id, NumCols>>;
  using Ref = std::conditional_t<isDynamic(), std::vector<Ptr>,
                                 std::array<Ptr, NumCols>>;
  using V = std::variant<Storage, Ref>;

 private:
  V data_;
  static Id& get(Id& i) { return i; }
  static Id& get(Id* i) { return *i; }
  static const Id& get(const Id& i) { return i; }
  static const Id& get(const Id* i) { return *i; }

 public:
  explicit Row(size_t numCols) : data_{Storage(numCols)} {}
  Row() requires(!isDynamic()) = default;
  Row(Ref ids) : data_{std::move(ids)} {}
  bool storesElements() const { return std::holds_alternative<Storage>(data_); }
  Id& operator[](size_t idx) requires(!isConst) {
    return std::visit(
        [idx](auto& vec) -> decltype(auto) { return get(vec[idx]); }, data_);
  }

  const Id& operator[](size_t idx) const {
    return std::visit(
        [idx](const auto& vec) -> const Id& { return get(vec[idx]); }, data_);
  }

  size_t size() const {
    return std::visit([](const auto& vec) { return vec.size(); }, data_);
  }
  size_t cols() const { return size(); }

  template <typename T>
  Row& copyAssignImpl(const T& other) requires(!isConst) {
    auto applyOnVectors = [](auto& target, const auto& src) {
      // TODO<joka921> This loses information if `this` points to references.
      // But this would anyway be a bug.
      target.resize(src.size());
      for (size_t i = 0; i < src.size(); ++i) {
        get(target[i]) = get(src[i]);
      }
    };
    std::visit(applyOnVectors, data_, other.data_);
    return *this;
  }

  template <bool otherIsConst>
  Row& operator=(const Row<NumCols, otherIsConst>& other) requires(!isConst) {
    return copyAssignImpl(other);
  }
  Row& operator=(const Row& other) requires(!isConst) {
    return copyAssignImpl(other);
  }

  template <int otherCols, bool otherIsConst>
  friend class Row;

  struct CopyConstructTag {};
  template <typename T>
  Row(const T& other, CopyConstructTag) : Row(other.cols()) {
    auto applyOnVectors = [](auto& target, const auto& src) {
      for (size_t i = 0; i < src.size(); ++i) {
        if constexpr (!std::is_same_v<
                          std::remove_reference_t<decltype(target[i])>,
                          const Id*>) {
          get(target[i]) = get(src[i]);
        }
        // TODO<joka921> Else AD_FAIL()
      }
    };
    std::visit(applyOnVectors, data_, other.data_);
  }

  template <bool otherIsConst>
  Row(const Row<NumCols, otherIsConst>& other)
      : Row{other, CopyConstructTag{}} {}

  Row(const Row& other) : Row{other, CopyConstructTag{}} {}

  template <ad_utility::SimilarTo<Row> R>
  friend void swap(R&& a, R&& b) requires(!isConst) {
    auto applyOnVectors = [](auto& target, auto& src) {
      for (size_t i = 0; i < src.size(); ++i) {
        std::swap(get(target[i]), get(src[i]));
      }
    };
    std::visit(applyOnVectors, a.data_, b.data_);
  }
};

template <int NumCols = 0,
          typename ActualAllocator =
              ad_utility::default_init_allocator<Id, std::allocator<Id>>>
class IdTable {
  using Column = std::vector<Id, ActualAllocator>;

 private:
  std::vector<Column> data_;

 public:
  size_t size() const { return data_.empty() ? 0 : data_.at(0).size(); }
  size_t cols() const { return data_.size(); }
  // TODO<joka921, C++23> Use the multidimensional subscript operator.
  Id& operator()(size_t row, size_t col) { return data_[col][row]; }
  const Id& operator()(size_t row, size_t col) const { return data_[col][row]; }

  void setCols(size_t cols) requires(NumCols == 0) {
    AD_CHECK(data_.empty());
    data_.resize(cols);
  }

  void resize(size_t numRows) {
    std::ranges::for_each(data_, [&](auto& vec) { vec.resize(numRows); });
  }

  void reserve(size_t numRows) {
    std::ranges::for_each(data_, [&](auto& vec) { vec.reserve(numRows); });
  }

  static constexpr auto getterLambda = [](auto&& vec, size_t idx) {
    static constexpr bool isConst =
        std::is_const_v<std::remove_reference_t<decltype(vec)>>;
    std::vector<decltype(&vec(0, 0))> ids;
    ids.reserve(vec.cols());
    for (size_t i = 0; i < vec.cols(); ++i) {
      ids.push_back(&vec(idx, i));
    }
    return Row<NumCols, isConst>{std::move(ids)};
  };
  static constexpr auto getIthRow =
      ad_utility::makeAssignableLambda(getterLambda);
  static_assert(std::is_copy_constructible_v<decltype(getterLambda)>);
  static_assert(std::is_copy_assignable_v<std::decay_t<decltype(getIthRow)>>);

  // TODO<joka921> The `getIthRow` function is rather inefficient. Implement
  // more efficient iterators manually, especially the iterator arithmetic can
  // is much simpler (add constant offsets to all the pointers).
  using iterator = ad_utility::IteratorForAccessOperator<
      IdTable, std::decay_t<decltype(getIthRow)>, false>;

  iterator begin() { return {this, 0}; }
  iterator end() { return {this, size()}; }
};

}  // namespace columnBasedIdTable