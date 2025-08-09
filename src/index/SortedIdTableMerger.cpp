// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/SortedIdTableMerger.h"

// _____________________________________________________________________________
IdTable SortedIdTableMerger::mergeIdTables(
    std::vector<IdTable> idTablesToMerge,
    const ad_utility::AllocatorWithLimit<Id>& allocator) {
  // TODO Check if all have same num columns

  auto sizes = idTablesToMerge | std::views::transform(&IdTable::size);
  auto sizesSum = std::accumulate(sizes.begin(), sizes.end(), size_t{0});

  IdTable result(allocator);
  result.setNumColumns(idTablesToMerge.at(0).numColumns());
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

  // This vector saves the ordering of which IdTable was accessed to get the
  // next element
  std::vector<size_t> accessedIdTables;
  accessedIdTables.reserve(sizesSum);

  // Fill the first column (which is the sorted one) and track in which order
  // the `IdTable`s are accessed.
  getIteratorsForColumn(0);
  auto resultOrderedColIt = result.getColumn(0).begin();
  auto resultOrderedColEnd = result.getColumn(0).end();
  for (std::optional<size_t> currentMinIdTable =
           minElementIdTable(iterators, sentinels);
       currentMinIdTable.has_value();
       currentMinIdTable = minElementIdTable(iterators, sentinels)) {
    size_t currentIdTable = currentMinIdTable.value();
    AD_CORRECTNESS_CHECK(resultOrderedColIt != resultOrderedColEnd);
    *resultOrderedColIt = *iterators[currentIdTable];
    accessedIdTables.push_back(currentIdTable);
    ++(iterators[currentIdTable]);
    ++resultOrderedColIt;
  }

  for (size_t i = 1; i < result.numColumns(); ++i) {
    getIteratorsForColumn(i);
    auto resultColIt = result.getColumn(i).begin();
    auto resultColEnd = result.getColumn(i).end();
    ql::ranges::for_each(accessedIdTables, [&](size_t tableIndex) {
      AD_CORRECTNESS_CHECK(resultColIt != resultColEnd);
      *resultColIt = *iterators[tableIndex];
      ++(iterators[tableIndex]);
      ++resultColIt;
    });
  }
  return result;
}
