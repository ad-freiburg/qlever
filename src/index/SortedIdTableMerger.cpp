// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/SortedIdTableMerger.h"

// _____________________________________________________________________________
IdTable SortedIdTableMerger::mergeIdTables(
    std::vector<IdTable> idTablesToMerge,
    const ad_utility::AllocatorWithLimit<Id>& allocator) {
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

  auto sizes = idTablesToMerge | std::views::transform(&IdTable::size);
  auto sizesSum = std::accumulate(sizes.begin(), sizes.end(), size_t{0});

  IdTable result(allocator);
  result.setNumColumns(numCols);
  result.resize(sizesSum);

  std::vector<ColumnIterator> iterators;
  std::vector<ColumnSentinel> sentinels;

  auto getIteratorsForColumn = [&iterators, &sentinels,
                                &idTablesToMerge](size_t col) {
    iterators.clear();
    sentinels.clear();
    ql::ranges::for_each(idTablesToMerge,
                         [&iterators, &sentinels, &col](const auto& idTable) {
                           iterators.push_back(idTable.getColumn(col).begin());
                           sentinels.push_back(idTable.getColumn(col).end());
                         });
  };

  // Lambda to find from which idTable the next smallest element is. If all
  // iterators are at the end then return `std::nullopt`
  auto minElementIdTable = [](const std::vector<ColumnIterator>& iterators,
                              const std::vector<ColumnSentinel>& sentinels)
      -> std::optional<size_t> {
    auto indices = ql::views::iota(size_t{0}, iterators.size()) |
                   ql::views::filter(
                       [&](size_t i) { return iterators[i] != sentinels[i]; });

    if (indices.begin() == indices.end()) {
      return std::nullopt;
    }

    auto indexWithMinIt =
        ql::ranges::min_element(indices, [&](size_t lhs, size_t rhs) {
          return iterators[lhs]->compareWithoutLocalVocab(*iterators[rhs]) ==
                 std::strong_ordering::less;
        });

    return *indexWithMinIt;
  };

  // For each idTable to merge this vector contains a vector that will contain
  // the index a row belongs to in the result idTable.
  std::vector<std::vector<size_t>> permutationIdTables;
  permutationIdTables.reserve(idTablesToMerge.size());
  ql::ranges::for_each(
      ::ranges::views::enumerate(idTablesToMerge), [&](auto pair) {
        permutationIdTables.emplace_back(std::vector<size_t>{});
        permutationIdTables.at(pair.first).reserve(pair.second.size());
      });

  // Fill the first column (which is the sorted one) and track in which order
  // the `IdTable`s are accessed.
  getIteratorsForColumn(0);
  auto resultOrderedColIt = result.getColumn(0).begin();
  auto resultOrderedColEnd = result.getColumn(0).end();
  std::optional<size_t> possibleCurrentMinIdTable =
      minElementIdTable(iterators, sentinels);
  size_t rowIndex = 0;
  while (possibleCurrentMinIdTable.has_value()) {
    size_t currentMinIdTable = possibleCurrentMinIdTable.value();
    AD_CORRECTNESS_CHECK(resultOrderedColIt != resultOrderedColEnd);
    *resultOrderedColIt = *iterators[currentMinIdTable];
    permutationIdTables.at(currentMinIdTable).push_back(rowIndex);
    ++(iterators[currentMinIdTable]);
    ++resultOrderedColIt;
    ++rowIndex;
    possibleCurrentMinIdTable = minElementIdTable(iterators, sentinels);
  }

  for (size_t idTableIndex = 0; idTableIndex < idTablesToMerge.size();
       ++idTableIndex) {
    // Start at second column since first has been written
    auto& idTable = idTablesToMerge.at(idTableIndex);
    for (size_t column = 1; column < idTable.numColumns(); ++column) {
      decltype(auto) resultColumn = result.getColumn(column);
      decltype(auto) idTableColumn = idTable.getColumn(column);
      for (size_t rowIndex = 0; rowIndex < idTableColumn.size(); ++rowIndex) {
        resultColumn[permutationIdTables.at(idTableIndex).at(rowIndex)] =
            idTableColumn[rowIndex];
      }
    }
  }
  return result;
}
