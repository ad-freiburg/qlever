// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/SortedIdTableMerge.h"

#include "util/ParallelMultiwayMerge.h"

namespace SortedIdTableMerge {

// _____________________________________________________________________________
IdTable mergeIdTables(
    std::vector<IdTable> tablesToMerge,
    const ad_utility::AllocatorWithLimit<Id>& allocator,
    const ad_utility::MemorySize& memory,
    std::function<bool(const columnBasedIdTable::Row<ValueId>&,
                       const columnBasedIdTable::Row<ValueId>&)>
        comparator) {
  std::vector<cppcoro::generator<columnBasedIdTable::Row<ValueId>>> generators;
  auto makeGenerator = [](const IdTable& table)
      -> cppcoro::generator<columnBasedIdTable::Row<ValueId>> {
    for (auto it = table.begin(); it != table.end(); ++it) {
      co_yield *it;
    }
  };

  auto sizeGetter = [](const columnBasedIdTable::Row<ValueId>& row) {
    return ad_utility::MemorySize::bytes(sizeof(row));
  };

  AD_CONTRACT_CHECK(
      tablesToMerge.size() > 0,
      "This method shouldn't be called without idTables to merge");
  size_t numColumns = tablesToMerge[0].numColumns();
  generators.reserve(tablesToMerge.size());
  for (const auto& partialResult : tablesToMerge) {
    generators.push_back(makeGenerator(partialResult));
    AD_CONTRACT_CHECK(
        partialResult.numColumns() == numColumns,
        "All idTables to merge should have the same number of columns");
  }

  // Merge
  auto mergedRows =
      ad_utility::parallelMultiwayMerge<columnBasedIdTable::Row<ValueId>, true,
                                        decltype(sizeGetter)>(
          memory, std::move(generators), comparator);
  IdTable output{allocator};
  output.setNumColumns(numColumns);

  for (const auto& row : ql::views::join(mergedRows)) {
    output.push_back(row);
  }
  return output;
}

}  // namespace SortedIdTableMerge
