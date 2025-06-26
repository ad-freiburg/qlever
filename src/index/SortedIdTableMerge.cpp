// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/SortedIdTableMerge.h"

#include "util/ParallelMultiwayMerge.h"

namespace SortedIdTableMerge {

// _____________________________________________________________________________
IdTable mergeIdTables(
    std::vector<IdTable>&& tablesToMerge,
    const ad_utility::AllocatorWithLimit<ValueId>& allocator,
    const ad_utility::MemorySize& memory, bool distinct,
    const std::function<bool(const std::vector<ValueId>&,
                             const std::vector<ValueId>&)>& comparator) {
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
