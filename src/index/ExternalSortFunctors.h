// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_INDEX_EXTERNALSORTFUNCTORS_H
#define QLEVER_SRC_INDEX_EXTERNALSORTFUNCTORS_H

#include <array>
#include <tuple>

#include "global/Id.h"

template <int i0, int i1, int i2, bool hasGraphColumn = true>
struct SortTriple {
  using T = std::array<Id, 3>;
  // comparison function
  template <typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
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
};

using SortByPSO = SortTriple<1, 0, 2>;
using SortByPSONoGraphColumn = SortTriple<1, 0, 2, false>;
using SortBySPO = SortTriple<0, 1, 2>;
using SortByOSP = SortTriple<2, 0, 1>;

struct SortText {
  // < comparator
  bool operator()(const auto& a, const auto& b) const {
    return ql::ranges::lexicographical_compare(
        a, b, [](const Id& x, const Id& y) {
          return x.compareWithoutLocalVocab(y) < 0;
        });
  }
};

#endif  // QLEVER_SRC_INDEX_EXTERNALSORTFUNCTORS_H
