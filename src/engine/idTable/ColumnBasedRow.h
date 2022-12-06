//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <array>
#include <iostream>
#include <type_traits>
#include <variant>
#include <vector>

#include "global/Id.h"

namespace columnBasedIdTable {

// Represent a reference or a value of a row in a column-major array of `Ids`.
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
    auto applyOnVectors = [this, &other](auto& target, const auto& src) {
      (void)this;
      // TODO<joka921> This loses information if `this` points to references.
      // But this would anyway be a bug.
      // TODO<joka921> is the resize really necessary? we should already have
      // the correct amount of columns.
      if constexpr (isDynamic()) {
        target.resize(src.size());
      }
      for (size_t i = 0; i < src.size(); ++i) {
        get(target[i]) = other.get(src[i]);
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
    auto applyOnVectors = [this, &other](auto& target, const auto& src) {
      void(this);
      for (size_t i = 0; i < src.size(); ++i) {
        if constexpr (!std::is_same_v<
                          std::remove_reference_t<decltype(target[i])>,
                          const Id*>) {
          get(target[i]) = other.get(src[i]);
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

// The row that ALWAYS stores the memory.
template <int NumCols = 0>
class RowO {
 public:
  static constexpr bool isDynamic() { return NumCols == 0; }
  using Storage =
      std::conditional_t<isDynamic(), std::vector<Id>, std::array<Id, NumCols>>;

 private:
  Storage data_;

  static Storage initStorage(size_t numCols) {
    if constexpr (isDynamic()) {
      return Storage(numCols);
    } else {
      return Storage{};
    }
  }

 public:
  explicit RowO(size_t numCols) : data_{initStorage(numCols)} {}
  RowO() requires(!isDynamic()) = default;

  Id& operator[](size_t idx) { return data_[idx]; }

  const Id& operator[](size_t idx) const { return data_[idx]; }

  size_t size() const { return data_.size(); }
  size_t cols() const { return size(); }

  template <ad_utility::SimilarTo<RowO> R>
  friend void swap(R&& a, R&& b) {
    std::swap(a.data_, b.data_);
  }

  bool operator==(const RowO other) const { return data_ == other.data_; }
};

// An identical copy to check, whether algorithms accept those differences.
template <typename Table, bool isConst = false>
class Row2 {
 public:
  using Ptr = std::conditional_t<isConst, const Table*, Table*>;

 private:
  Ptr table_ = nullptr;
  size_t row_ = 0;

 public:
  explicit Row2(Ptr table, size_t row) : table_{table}, row_{row} {}
  Row2() = default;
  Id& operator[](size_t idx) requires(!isConst) { return (*table_)(row_, idx); }

  const Id& operator[](size_t idx) const { return (*table_)(row_, idx); }

  size_t size() const { return table_->cols(); }
  size_t cols() const { return size(); }

  template <typename otherTable, bool otherIsConst>
  friend class Row2;

  template <ad_utility::SimilarTo<Row2> R>
  friend void swap(R&& a, R&& b) requires(!isConst) {
    for (size_t i = 0; i < a.size(); ++i) {
      std::swap(a[i], b[i]);
    }
  }

  // TODO<joka921> Constrain this to reasonable types.
  bool operator==(const auto& other) const {
    for (size_t i = 0; i < size(); ++i) {
      if ((*this)[i] != other[i]) {
        return false;
      }
    }
    return true;
  }

  template <int NumCols>
  operator RowO<NumCols>() {
    RowO<NumCols> result{size()};
    for (size_t i = 0; i < size(); ++i) {
      result[i] = (*this)[i];
    }
    return result;
  }
  template <int NumCols>
  Row2& operator=(const RowO<NumCols>& other) {
    for (size_t i = 0; i < size(); ++i) {
      (*this)[i] = other[i];
    }
    return *this;
  }

  Row2& operator=(const Row2& other) {
    for (size_t i = 0; i < size(); ++i) {
      (*this)[i] = other[i];
    }
    return *this;
  }

  // TODO<joka921> This seems to be needed by Gtest, but why?
  Row2(const Row2&) = default;
};

}  // namespace columnBasedIdTable