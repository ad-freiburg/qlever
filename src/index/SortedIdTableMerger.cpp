// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/SortedIdTableMerger.h"

namespace SortedIdTableMerge {
// _____________________________________________________________________________
IdTable mergeIdTables(std::vector<IdTable> idTablesToMerge,
                      const ad_utility::AllocatorWithLimit<Id>& allocator,
                      std::vector<size_t> sortPerm) {
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

  auto sizes = idTablesToMerge | std::views::transform(&IdTable::size);
  auto sizesSum = std::accumulate(sizes.begin(), sizes.end(), size_t{0});

  IdTable result(allocator);
  result.setNumColumns(numCols);
  result.resize(sizesSum);

  // For each idTable to merge this vector contains a vector that maps the rows
  // to the result idTable
  std::vector<std::vector<size_t>> permutationIdTables;
  permutationIdTables.reserve(idTablesToMerge.size());
  ql::ranges::for_each(idTablesToMerge,
                       [&permutationIdTables](const auto& idTable) {
                         permutationIdTables.emplace_back();
                         permutationIdTables.back().reserve(idTable.size());
                       });

  // Fill the permutationIdTables by iterating over the idTablesToMerge
  MinRowIterator iterator(idTablesToMerge, sortPerm);
  auto idTableAndResultRow = iterator.next();
  while (idTableAndResultRow != std::nullopt) {
    permutationIdTables.at(idTableAndResultRow->first)
        .push_back(idTableAndResultRow->second);
    idTableAndResultRow = iterator.next();
  }

  // Write the result from the permutation.
  writeIdTableFromPermutation(std::move(idTablesToMerge),
                              std::move(permutationIdTables), result);

  return result;
}

// _____________________________________________________________________________
void writeIdTableFromPermutation(
    std::vector<IdTable> idTablesToMerge,
    std::vector<std::vector<size_t>> permutationIdTables, IdTable& result) {
  ql::ranges::for_each(
      ::ranges::views::enumerate(idTablesToMerge),
      [&result, &permutationIdTables](auto idTablePair) {
        ql::ranges::for_each(
            ql::ranges::views::iota(size_t{0}, idTablePair.second.numColumns()),
            [&result, &idTablePair, &permutationIdTables](auto columnIndex) {
              decltype(auto) resultColumn = result.getColumn(columnIndex);
              decltype(auto) idTableColumn =
                  idTablePair.second.getColumn(columnIndex);
              ql::ranges::for_each(
                  ql::ranges::views::iota(size_t{0}, idTableColumn.size()),
                  [&resultColumn, &idTableColumn, &permutationIdTables,
                   &idTablePair](auto rowIndex) {
                    resultColumn[permutationIdTables.at(idTablePair.first)
                                     .at(rowIndex)] = idTableColumn[rowIndex];
                  });
            });
      });
}

// _____________________________________________________________________________
MinRowIterator::MinRowIterator(const std::vector<IdTable>& idTablesToMerge,
                               std::vector<size_t> sortPerm)
    : rowCounter_(size_t{0}) {
  // For all idTables
  ql::ranges::for_each(
      ::ranges::views::enumerate(idTablesToMerge),
      [this, &sortPerm](auto idTablePair) {
        // Add all relevant columns
        std::vector<ColumnRange> columnRangesForIdTable;
        ql::ranges::for_each(sortPerm, [this, &idTablePair,
                                        &columnRangesForIdTable](size_t col) {
          auto span = idTablePair.second.getColumn(col);
          columnRangesForIdTable.push_back({span.begin(), span.end()});
        });
        idTableToColumnRangesMap_.emplace(idTablePair.first,
                                          std::move(columnRangesForIdTable));
      });
  // Check if there are iterators for empty containers and if so throw
  AD_CONTRACT_CHECK(
      ql::ranges::all_of(idTableToColumnRangesMap_,
                         [](const auto& keyValuePair) {
                           return ql::ranges::all_of(
                               keyValuePair.second,
                               [](const ColumnRange& columnRange) {
                                 return columnRange.first != columnRange.second;
                               });
                         }),
      "At least one of the idTablesToMerge has an empty column.");
  // Fill heap with the first rows
  ql::ranges::for_each(
      idTableToColumnRangesMap_, [this](const auto& keyValuePair) {
        minHeap_.emplace(keyValuePair.first,
                         getRowSubsetAndAdvanceForIdTable(keyValuePair.first));
      });
}

// _____________________________________________________________________________
std::optional<IdTableAndResultRow> MinRowIterator::next() {
  if (minHeap_.empty()) {
    return std::nullopt;
  }
  // Retrieve element
  HeapElement nextMin = minHeap_.top();
  minHeap_.pop();

  // Get next row for idTable retrieved if now empty
  auto it = idTableToColumnRangesMap_.find(nextMin.first);
  if (it != idTableToColumnRangesMap_.end()) {
    minHeap_.push(HeapElement{nextMin.first,
                              getRowSubsetAndAdvanceForIdTable(nextMin.first)});
  }
  return std::make_optional(IdTableAndResultRow{nextMin.first, rowCounter_++});
};

// _____________________________________________________________________________
std::vector<const Id*> MinRowIterator::getRowSubsetAndAdvanceForIdTable(
    IdTableIndex idTableIndex) {
  auto it = idTableToColumnRangesMap_.find(idTableIndex);
  AD_CONTRACT_CHECK(
      it != idTableToColumnRangesMap_.end(),
      "The idTableIndex is not in the columnRanges_ map. The index was: ",
      idTableIndex);
  auto& columnRanges = it->second;
  std::vector<const Id*> rowSubset;
  for (auto& columnRange : columnRanges) {
    rowSubset.push_back(&(*columnRange.first));
  }
  incrementColumnRangesForIdTable(idTableIndex);
  return rowSubset;
}

// _____________________________________________________________________________
void MinRowIterator::incrementColumnRangesForIdTable(
    IdTableIndex idTableIndex) {
  auto it = idTableToColumnRangesMap_.find(idTableIndex);
  AD_CONTRACT_CHECK(
      it != idTableToColumnRangesMap_.end(),
      "The idTableIndex is not in the columnRanges_ map. The index was: ",
      idTableIndex);

  for (auto& columnRange : it->second) {
    ++columnRange.first;
    if (columnRange.first == columnRange.second) {
      idTableToColumnRangesMap_.erase(idTableIndex);
      break;
    }
  }
}

}  // namespace SortedIdTableMerge
