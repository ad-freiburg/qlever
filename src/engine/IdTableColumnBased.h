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
  size_t offset_ = 0;
  Id& get(Id& i) { return i; }
  Id& get(Id* i) { return *(i + offset_); }
  const Id& get(const Id& i) const { return i; }
  const Id& get(const Id* i) const { return *(i + offset_); }

  static Storage initStorage(size_t numCols) {
    if constexpr (isDynamic()) {
      return Storage(numCols);
    } else {
      return Storage{};
    }
  }

 public:
  explicit Row(size_t numCols) : data_{initStorage(numCols)} {}
  Row() requires(!isDynamic()) = default;
  Row(Ref ids) : data_{std::move(ids)} {}
  bool storesElements() const { return std::holds_alternative<Storage>(data_); }

  void setOffset(size_t offset) { offset_ = offset; }
  Id& operator[](size_t idx) requires(!isConst) {
    return std::visit(
        [idx, this](auto& vec) -> decltype(auto) {
          void(this);
          return get(vec[idx]);
        },
        data_);
  }

  const Id& operator[](size_t idx) const {
    return std::visit(
        [idx, this](const auto& vec) -> const Id& {
          void(this);
          return get(vec[idx]);
        },
        data_);
  }

  size_t size() const {
    return std::visit([](const auto& vec) { return vec.size(); }, data_);
  }
  size_t cols() const { return size(); }

  template <typename T>
  Row& copyAssignImpl(const T& other) requires(!isConst) {
    auto applyOnVectors = [this](auto& target, const auto& src) {
      (void)this;
      // TODO<joka921> This loses information if `this` points to references.
      // But this would anyway be a bug.
      // TODO<joka921> is the resize really necessary? we should already have
      // the correct amount of columns.
      if constexpr (isDynamic()) {
        target.resize(src.size());
      }
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
    auto applyOnVectors = [this](auto& target, const auto& src) {
      void(this);
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
    auto applyOnVectors = [&a, &b](auto& target, auto& src) {
      for (size_t i = 0; i < src.size(); ++i) {
        std::swap(a.get(target[i]), b.get(src[i]));
      }
    };
    std::visit(applyOnVectors, a.data_, b.data_);
  }

  struct CloneTag {};
  Row(const Row& other, CloneTag, size_t offset)
      : data_{other.data_}, offset_{offset} {}
  Row& cloneAssign(const Row& other) {
    data_ = other.data_;
    offset_ = other.offset_;
    return *this;
  }

  bool operator==(const Row other) const {
    auto applyOnVectors = [this, &other](auto& a, auto& b) {
      // TODO<joka921> not needed for the dynamic case.
      (void)this;
      if (a.size() != b.size()) {
        return false;
      }
      for (size_t i = 0; i < a.size(); ++i) {
        if (get(a[i]) != other.get(b[i])) {
          return false;
        }
      }
      return true;
    };
    return std::visit(applyOnVectors, data_, other.data_);
  }
};

template <int NumCols = 0,
          typename ActualAllocator =
              ad_utility::default_init_allocator<Id, std::allocator<Id>>,
          bool isView = false>
class IdTable {
 public:
  using Columns = std::vector<std::vector<Id, ActualAllocator>>;

  using Data = std::conditional_t<isView, const Columns*, Columns>;

  using row_type = Row<NumCols, false>;
  using const_row_type = Row<NumCols, true>;

 private:
  Data data_;
  ActualAllocator allocator_{};

 public:
  Columns& data() requires(!isView) { return data_; }

  const Columns& data() const {
    if constexpr (isView) {
      return *data_;
    } else {
      return data_;
    }
  }

  IdTable(int numCols, ActualAllocator allocator) requires(!isView)
      : allocator_{std::move(allocator)} {
    if (NumCols != 0) {
      AD_CHECK(NumCols == numCols);
    }
    data().reserve(numCols);
    for (int i = 0; i < numCols; ++i) {
      data().emplace_back(allocator_);
    }
  }
  IdTable(ActualAllocator allocator = {}) requires(!isView)
      : IdTable{NumCols, std::move(allocator)} {};

  IdTable(Data data, ActualAllocator allocator)
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
    AD_CHECK(data().empty());
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
  const_iterator cend() const { return cend(); }

  template <int NewCols>
  requires(NumCols == 0 &&
           !isView) IdTable<NewCols, ActualAllocator> moveToStatic() {
    return IdTable<NewCols, ActualAllocator>{std::move(data()),
                                             std::move(allocator_)};
  }

  IdTable<0, ActualAllocator> moveToDynamic() requires(!isView) {
    return IdTable<0, ActualAllocator>{std::move(data()),
                                       std::move(allocator_)};
  }

  template <int NewCols>
  requires(NumCols == 0 &&
           !isView) IdTable<NewCols, ActualAllocator, true> asStaticView()
  const {
    // TODO<joka921> It shouldn't be necessary to store an allocator in a view.
    return IdTable<NewCols, ActualAllocator, true>{&data(), allocator_};
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

  void clear() requires(!isView) { data().clear(); }

  ActualAllocator& getAllocator() requires(!isView) { return allocator_; }

  const ActualAllocator& getAllocator() const { return allocator_; }

  IdTable<NumCols, ActualAllocator, false> clone() const {
    return IdTable<NumCols, ActualAllocator, false>{data(), getAllocator()};
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