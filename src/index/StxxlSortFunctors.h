// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <tuple>

#include "global/Id.h"

template <int i0, int i1, int i2, bool hasGraphColumn = true>
struct SortTriple {
  using T = std::array<Id, 3>;
  // comparison function
  bool operator()(const auto& a, const auto& b) const {
    if constexpr (!hasGraphColumn) {
      AD_EXPENSIVE_CHECK(a.size() >= 3 && b.size() >= 3);
    } else {
      AD_EXPENSIVE_CHECK(a.size() >= ADDITIONAL_COLUMN_GRAPH_ID &&
                         b.size() >= ADDITIONAL_COLUMN_GRAPH_ID);
    }
    constexpr auto compare = &Id::compareWithoutLocalVocab;
    // TODO<joka921> The manual invoking is ugly, probably we could use
    // `ql::ranges::lexicographical_compare`, but we have to carefully measure
    // that this change doesn't slow down the index build.
    auto c1 = std::invoke(compare, a[i0], b[i0]);
    if (c1 != 0) {
      return c1 < 0;
    }
    auto c2 = std::invoke(compare, a[i1], b[i1]);
    if (c2 != 0) {
      return c2 < 0;
    }
    auto c3 = std::invoke(compare, a[i2], b[i2]);
    if constexpr (!hasGraphColumn) {
      return c3 < 0;
    } else {
      if (c3 != 0) {
        return c3 < 0;
      }
      // If the triples are equal, we compare by the Graph column. This is
      // necessary to handle UPDATEs correctly.
      static constexpr auto g = ADDITIONAL_COLUMN_GRAPH_ID;
      auto cGraph = std::invoke(compare, a[g], b[g]);
      return cGraph < 0;
    }
  }

  // Value that is strictly smaller than any input element.
  static T min_value() { return {Id::min(), Id::min(), Id::min()}; }

  // Value that is strictly larger than any input element.
  static T max_value() { return {Id::max(), Id::max(), Id::max()}; }
};

using SortByPSO = SortTriple<1, 0, 2>;
using SortByPSONoGraphColumn = SortTriple<1, 0, 2, false>;
using SortBySPO = SortTriple<0, 1, 2>;
using SortByOSP = SortTriple<2, 0, 1>;

// TODO<joka921> Which of those are actually "IDs" and which are something else?
struct SortText {
  using T = std::tuple<TextBlockIndex, TextRecordIndex, WordOrEntityIndex,
                       Score, bool>;
  // comparison function
  bool operator()(const T& a, const T& b) const {
    auto permute = [](const T& x) {
      using namespace std;
      return tie(get<0>(x), get<4>(x), get<1>(x), get<2>(x), get<3>(x));
    };
    return permute(a) < permute(b);
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
