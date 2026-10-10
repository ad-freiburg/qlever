// Copyright 2015 - 2026 The QLever Authors, in particular:
//
// 2015 - 2017 Björn Buchhold <buchhold@cs.uni-freiburg.de>, UFR
// 2023 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025        Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_EXTERNALSORTFUNCTORS_H
#define QLEVER_SRC_INDEX_EXTERNALSORTFUNCTORS_H

#include <array>
#include <tuple>
#include <vector>

#include "global/Id.h"

#ifdef QLEVER_CHEAPER_COMPILATION
#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ConstantsIndexBuilding.h"
#endif

template <int i0, int i1, int i2, bool hasGraphColumn = true>
struct SortTriple {
  using T = std::array<Id, 3>;
  // The comparison below is a lexicographic comparison of the bits of these
  // columns, see `ad_utility::sortByBitwiseKeys` in `BitwiseKeySort.h`.
  static constexpr auto bitwiseKeyColumns = []() {
    if constexpr (hasGraphColumn) {
      return std::array<size_t, 4>{i0, i1, i2, ADDITIONAL_COLUMN_GRAPH_ID};
    } else {
      return std::array<size_t, 3>{i0, i1, i2};
    }
  }();
  // comparison function
  template <typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
    if constexpr (!hasGraphColumn) {
      AD_EXPENSIVE_CHECK(a.size() >= 3 && b.size() >= 3);
    } else {
      AD_EXPENSIVE_CHECK(a.size() >= ADDITIONAL_COLUMN_GRAPH_ID &&
                         b.size() >= ADDITIONAL_COLUMN_GRAPH_ID);
    }
    // Compare the bits of the key columns lexicographically (see
    // `bitwiseKeyColumns`) via small arrays. This is faster than a chain of
    // three-way comparisons, because it compiles to straight-line code (see
    // the NOTE at `pickBitsOfColumns` in `CompressedRelationHelpersImpl.h`).
    auto bits = [](const Id& id) {
      AD_EXPENSIVE_CHECK(id.canBeComparedBitwise());
      return id.getBits();
    };
    auto keys = [&bits](const auto& row) {
      if constexpr (hasGraphColumn) {
        return std::array<Id::T, 4>{bits(row[i0]), bits(row[i1]), bits(row[i2]),
                                    bits(row[ADDITIONAL_COLUMN_GRAPH_ID])};
      } else {
        return std::array<Id::T, 3>{bits(row[i0]), bits(row[i1]),
                                    bits(row[i2])};
      }
    };
    return keys(a) < keys(b);
  }
};

using SortByPSO = SortTriple<1, 0, 2>;
using SortByPSONoGraphColumn = SortTriple<1, 0, 2, false>;
using SortBySPO = SortTriple<0, 1, 2>;
using SortByOSP = SortTriple<2, 0, 1>;

struct SortText {
  // < comparator
  template <typename A, typename B>
  bool operator()(const A& a, const B& b) const {
    return ql::ranges::lexicographical_compare(
        a, b, [](const Id& x, const Id& y) {
          return x.compareWithoutLocalVocab(y) < 0;
        });
  }
};

// A comparator that sorts rows by a runtime-specified list of column indices.
// Uses simple `<` comparison on Ids (internal order).
//
// TODO: This is not as efficient as it could be, because of the runtime state
// (the vector of column indices); see `Sort::computeResultExternal`.
struct SortByColumns {
  std::vector<ColumnIndex> sortColumns_;

  explicit SortByColumns(std::vector<ColumnIndex> sortColumns)
      : sortColumns_{std::move(sortColumns)} {}

  // Default constructor for template requirements.
  SortByColumns() = default;

  template <typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
    for (auto col : sortColumns_) {
      if (a[col] != b[col]) {
        return a[col] < b[col];
      }
    }
    return false;
  }
};

#ifdef QLEVER_CHEAPER_COMPILATION
// Extern-template declarations for the `CompressedExternalIdTableSorter`
// specialisations used during index building. Without these declarations every
// `TU` that includes `IndexImpl.h` or `PatternCreator.h` would instantiate all
// member functions of each specialisation (~100 s cumulative per ftime-trace).
// The corresponding explicit instantiation definitions live in
// `CompressedExternalIdTableSorterInstantiations.cpp`.
//
// NOTE: If you add a new `CompressedExternalIdTableSorter` specialisation used
// by index building, you must add a matching explicit instantiation in
// `CompressedExternalIdTableSorterInstantiations.cpp`, otherwise the build will
// fail at link time when `QLEVER_CHEAPER_COMPILATION` is set.
namespace ad_utility {

extern template class CompressedExternalIdTableSorter<SortByPSONoGraphColumn,
                                                      3>;
extern template class CompressedExternalIdTableSorter<
    SortByOSP, NumColumnsIndexBuilding + 1>;
extern template class CompressedExternalIdTableSorter<SortBySPO,
                                                      NumColumnsIndexBuilding>;
extern template class CompressedExternalIdTableSorter<SortByOSP,
                                                      NumColumnsIndexBuilding>;
extern template class CompressedExternalIdTableSorter<SortByPSO,
                                                      NumColumnsIndexBuilding>;
extern template class CompressedExternalIdTableSorter<
    SortByPSO, NumColumnsIndexBuilding + 2>;
extern template class CompressedExternalIdTableSorter<SortText, 5>;

}  // namespace ad_utility
#endif  // QLEVER_CHEAPER_COMPILATION

#endif  // QLEVER_SRC_INDEX_EXTERNALSORTFUNCTORS_H
