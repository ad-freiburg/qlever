// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/SortedIdTableMerge.h"

namespace sortedIdTableMerge {

// _____________________________________________________________________________
void writeIdTableFromPermutation(
    const std::vector<IdTable>& idTablesToMerge,
    std::vector<std::vector<size_t>> permutationIdTables, IdTable& result,
    const std::vector<size_t>& sortPerm) {
  auto filteredCols = ql::views::iota(size_t{0}, result.numColumns()) |
                      ql::views::filter([&sortPerm](const auto& index) {
                        return !::ranges::contains(sortPerm, index);
                      });

  size_t resultColIndex = sortPerm.size();
  auto writeColumnToResult = [&result, &permutationIdTables, &idTablesToMerge,
                              &resultColIndex](const auto& columnIndex) {
    auto resultColumn = result.getColumn(resultColIndex);
    for (size_t i = 0; i < permutationIdTables.size(); ++i) {
      auto columnToCopyFrom = idTablesToMerge.at(i).getColumn(columnIndex);
      ql::ranges::for_each(
          ql::views::iota(size_t{0}, columnToCopyFrom.size()),
          [&permutationIdTables, &i, &resultColumn,
           &columnToCopyFrom](const size_t& rowIndex) {
            resultColumn[permutationIdTables.at(i).at(rowIndex)] =
                columnToCopyFrom[rowIndex];
          });
    }
    ++resultColIndex;
  };

  ql::ranges::for_each(filteredCols, writeColumnToResult);
}

// _____________________________________________________________________________
void checkErrors(const std::vector<IdTable>& idTablesToMerge,
                 const std::vector<size_t>& sortPerm) {
  AD_CONTRACT_CHECK(
      !idTablesToMerge.empty(),
      "mergeIdTables shouldn't be called with no idTables to merge.");

  size_t numCols = idTablesToMerge.at(0).numColumns();
  ql::ranges::for_each(idTablesToMerge, [&numCols](const auto& idTable) {
    AD_CONTRACT_CHECK(idTable.numColumns() == numCols,
                      "All idTables to merge should have the same number of "
                      "columns. First idTable has: ",
                      numCols,
                      " columns. Failed table had: ", idTable.numColumns(),
                      " columns");
  });

  AD_CONTRACT_CHECK(sortPerm.size() <= numCols,
                    "The given sortPerm does not contain less than or equal "
                    "the amount of columns the idTablesToMerge have.");
  AD_CONTRACT_CHECK(ql::ranges::all_of(
                        sortPerm, [&numCols](size_t i) { return i < numCols; }),
                    "The given sortPerm contains column indices outside of the "
                    "range of the idTablesToMerge.");
  auto set = ad_utility::HashSet<size_t>{sortPerm.begin(), sortPerm.end()};
  AD_CONTRACT_CHECK(set.size() == sortPerm.size(),
                    "The given sortPerm contains duplicate column indices.");
}

}  // namespace sortedIdTableMerge
