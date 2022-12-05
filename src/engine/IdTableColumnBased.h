// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#pragma once

#include <array>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <initializer_list>
#include <ostream>
#include <variant>
#include <functional>

#include "../global/Id.h"
#include "../util/AllocatorWithLimit.h"
#include "../util/Log.h"
#include "../util/UninitializedAllocator.h"

namespace columnBasedIdTable {

class Row {
  using V = std::variant<std::vector<Id>, std::vector<Id*>>;
 private:
  V data_;
  static Id& get(Id& i ) {return i;}
  static Id& get(Id* i ) {return *i;}
  static const Id& get(const Id& i ) {return i;}
  static const Id& get(const Id* i ) {return *i;}
 public:
  explicit Row(size_t cols): data_{std::vector<Id>(cols)}  {}
  Row(std::vector<Id*> ids) : data_{std::move(ids)} {}
  bool storesElements() const {
    return std::holds_alternative<std::vector<Id>>(data_);
  }
  Id& operator[](size_t idx) {
    return std::visit([idx](auto& vec) ->decltype(auto){return get(vec[idx]);}, data_);
  }

  size_t size() const {
    return std::visit([](const auto& vec) {return vec.size();}, data_);
  }
  size_t cols() const {
    return size();
  }

  /*
  const Id& operator[](size_t idx) const {
    return std::visit([idx](const auto& vec) ->decltype(auto){return get(vec[idx]);}, data_);
  }
   */

  Row& operator=(const Row& other) {
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

  Row(const Row& other) : Row(other.cols()) {
    auto applyOnVectors = [](auto& target, const auto& src) {
      for (size_t i = 0; i < src.size(); ++i) {
        get(target[i]) = get(src[i]);
      }
    };
    std::visit(applyOnVectors, data_, other.data_);
  }
};

template <typename ActualAllocator = ad_utility::default_init_allocator<Id, std::allocator<Id>>>
class IdTable {
  using Column = std::vector<Id, ActualAllocator>;
 private:
  std::vector<Column> data_;
 public:
  size_t size() const {return data_.empty() ? 0 : data_.at(0).size();}
  size_t cols() const {return data_.size();}
  // TODO<joka921, C++23> Use the multidimensional subscript operator.
  Id operator()(size_t row, size_t col) {
    return data_[row][col];
  }


};

}  // end of dummy namespace.