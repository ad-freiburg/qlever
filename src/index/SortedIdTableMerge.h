// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_SORTEDIDTABLEMERGE_H
#define QLEVER_SRC_INDEX_SORTEDIDTABLEMERGE_H

#include <vector>

#include "engine/idTable/IdTable.h"
#include "util/Concepts.h"
#include "util/ParallelMultiwayMerge.h"
#include "util/TransparentFunctors.h"
#include "util/TypeTraits.h"

namespace SortedIdTableMerge {

constexpr static auto sizeOfRow = [](const std::vector<ValueId>& row) {
  return ad_utility::MemorySize::bytes(row.size() * sizeof(ValueId));
};

struct DefaultRowComparator {
  bool operator()(const std::vector<ValueId>& a,
                  const std::vector<ValueId>& b) const {
    return std::lexicographical_compare(
        a.begin(), a.end(), b.begin(), b.end(),
        [](const ValueId& lhs, const ValueId& rhs) {
          return lhs.getBits() < rhs.getBits();
        });
  }
};

// Takes in multiple IdTables sorted w.r.t. comparator, an allocator for the
// output IdTable, a comparator that evaluates to true iff the first row is
// smaller or equal to the second row and a MemorySize for the merge. The
// IdTables should all have the same nof columns. If no comparator is given,
// the row is sorted left to right.
template <typename RowComparator = DefaultRowComparator>
requires ad_utility::InvocableWithExactReturnType<RowComparator, bool,
                                                  const std::vector<ValueId>&,
                                                  const std::vector<ValueId>&>
IdTable mergeIdTables(std::vector<IdTable>&& tablesToMerge,
                      const ad_utility::AllocatorWithLimit<Id>& allocator,
                      const ad_utility::MemorySize& memory,
                      bool distinct = false,
                      const RowComparator& comparator = RowComparator{}) {
  std::vector<cppcoro::generator<std::vector<ValueId>>> generators;
  auto makeGenerator =
      [](const IdTable& table) -> cppcoro::generator<std::vector<ValueId>> {
    for (const auto& row : table) {
      co_yield std::vector<ValueId>(row.begin(), row.end());
    }
  };

  AD_CONTRACT_CHECK(
      tablesToMerge.size() > 0,
      "This method shouldn't be called without idTables to merge");
  size_t numColumns = tablesToMerge[0].numColumns();
  generators.reserve(tablesToMerge.size());
  for (const auto& partialResult : tablesToMerge) {
    generators.push_back(makeGenerator(partialResult));
    AD_CONTRACT_CHECK(partialResult.numColumns() == numColumns,
                      "All idTables to merge should have the same number of "
                      "columns. First table had: ",
                      numColumns, " columns. Failed table had: ",
                      partialResult.numColumns(), " columns.");
  }

  // Merge
  auto mergedRows =
      ad_utility::parallelMultiwayMerge<std::vector<ValueId>, true,
                                        decltype(sizeOfRow)>(
          memory, std::move(generators), comparator);
  IdTable output{allocator};
  output.setNumColumns(numColumns);

  if (distinct) {
    size_t numDistinctRows = 1;
    auto view = ql::views::join(mergedRows);
    auto it = view.begin();
    if (it == view.end()) {
      return output;
    }
    std::vector<ValueId> previousRow = *it;
    output.push_back(previousRow);
    ++it;
    for (; it != view.end(); ++it) {
      if (*it != previousRow) {
        output.push_back(*it);
        previousRow = *it;
        ++numDistinctRows;
      } else {
        LOG(DEBUG) << "Skipping duplicate row. Current nof distinct rows: "
                   << numDistinctRows;
      }
    }
    output.resize(numDistinctRows);
  } else {
    for (const auto& row : ql::views::join(mergedRows)) {
      output.push_back(row);
    }
  }
  return output;
}

}  // namespace SortedIdTableMerge

#endif  // QLEVER_SRC_INDEX_SORTEDIDTABLEMERGE_H
