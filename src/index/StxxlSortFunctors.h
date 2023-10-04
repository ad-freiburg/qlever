// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <tuple>

#include "../global/Id.h"

using std::array;
using std::tuple;

template <int i0, int i1, int i2>
struct SortTriple {
  using T = std::array<Id, 3>;
  // comparison function
  bool operator()(const auto& a, const auto& b) const {
    if (a[i0] == b[i0]) {
      if (a[i1] == b[i1]) {
        return a[i2] < b[i2];
      }
      return a[i1] < b[i1];
    }
    return a[i0] < b[i0];
  }

  // Value that is strictly smaller than any input element.
  static T min_value() { return {Id::min(), Id::min(), Id::min()}; }

  // Value that is strictly larger than any input element.
  static T max_value() { return {Id::max(), Id::max(), Id::max()}; }
};

using SortByPSO = SortTriple<1, 0, 2>;
using SortByPOS = SortTriple<1, 2, 0>;
using SortBySPO = SortTriple<0, 1, 2>;
using SortBySOP = SortTriple<0, 2, 1>;
using SortByOSP = SortTriple<2, 0, 1>;
using SortByOPS = SortTriple<2, 1, 0>;

// TODO<joka921> Which of those are actually "IDs" and which are something else?
struct SortText {
  using T = std::tuple<TextBlockIndex, TextRecordIndex, WordOrEntityIndex,
                       Score, bool>;
  // comparison function
  bool operator()(const T& a, const T& b) const {
    if (std::get<0>(a) == std::get<0>(b)) {
      if (std::get<4>(a) == std::get<4>(b)) {
        if (std::get<1>(a) == std::get<1>(b)) {
          if (std::get<2>(a) == std::get<2>(b)) {
            return std::get<3>(a) < std::get<3>(b);
          } else {
            return std::get<2>(a) < std::get<2>(b);
          }
        } else {
          return std::get<1>(a) < std::get<1>(b);
        }
      } else {
        return !std::get<4>(a);
      }
    } else {
      return std::get<0>(a) < std::get<0>(b);
    }
  }

  // min sentinel = value which is strictly smaller that any input element
  static T min_value() { return {0, TextRecordIndex::min(), 0, 0, false}; }

  // max sentinel = value which is strictly larger that any input element
  static T max_value() {
    return {std::numeric_limits<TextBlockIndex>::max(), TextRecordIndex::max(),
            std::numeric_limits<WordOrEntityIndex>::max(),
            std::numeric_limits<Score>::max(), true};
  }
};
