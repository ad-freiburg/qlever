// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)
#ifndef QLEVER_SRC_ENGINE_SORTEDIDTABLEMERGE_H
#define QLEVER_SRC_ENGINE_SORTEDIDTABLEMERGE_H

#include <queue>

#include "engine/idTable/IdTable.h"
#include "util/HashMap.h"

namespace SortedIdTableMerge {

using ColumnIterator = decltype(std::declval<const IdTable&>()
                                    .getColumn(std::declval<size_t>())
                                    .begin());
using ColumnSentinel = decltype(std::declval<const IdTable&>()
                                    .getColumn(std::declval<size_t>())
                                    .end());

using ColumnRange = std::pair<ColumnIterator, ColumnSentinel>;

using IdTableIndex = size_t;
using IdTableAndResultRow = std::pair<IdTableIndex, size_t>;

// This method takes in k `idTablesToMerge` which are pre-sorted. The way they
// are sorted has to match the `sortPerm`. The `sortPerm` contains the column
// indices in which the table is sorted. E.g. `sortPerm` = {0, 2, 1} would mean
// first compare the elements of the first column, if there is a tie then
// compare the elements of the third column and lastly of the second. The
// `sortPerm` doesn't need to include all indices.
IdTable mergeIdTables(std::vector<IdTable> idTablesToMerge,
                      const ad_utility::AllocatorWithLimit<Id>& allocator,
                      std::vector<size_t> sortPerm = {0});

// For each idTable iterate over each column. For each column iterate over the
// values and look up where the value belongs in the result table. Write the
// value to the correct place. This method ensures cache locality during the
// iteration of the idTables that have to be merged.
void writeIdTableFromPermutation(
    std::vector<IdTable> idTablesToMerge,
    std::vector<std::vector<size_t>> permutationIdTables, IdTable& result);

/**
 * @brief This class takes in a vector of pre-sorted `IdTable`s. The `IdTable`s
 *        should be sorted in ascending order. The comparison on rows should
 *        work in the order of the sortPerm, meaning when comparing two rows
 *        first compare the column given at 0 in the sortPerm then the column
 *        given at 1 in the sortPerm and so on. The sortPerm doesn't need to
 *        include all column indices since some columns don't need to be sorted.
 *        Once constructed the next() method can be called to get a pair of
 *        `IdTableIndex` and result row index. To make an example, next() is
 *        called and returns {1, 5} this means that in the result of the merged
 *        `IdTable`s the row 5 comes from idTablesToMerge[1]. This sort of
 *        mapping can be collected and later used to merge.
 */
class MinRowIterator {
 public:
  MinRowIterator(const std::vector<IdTable>& idTablesToMerge,
                 std::vector<size_t> sortPerm);

  // Returns pair idTable index, row index of the result table where the
  // element is mapped to. If all
  std::optional<IdTableAndResultRow> next();

 private:
  // A map storing all column iterators and sentinels for columns that are
  // necessary for sorting for different idTables.
  ad_utility::HashMap<IdTableIndex, std::vector<ColumnRange>>
      idTableToColumnRangesMap_;

  // Counts how many rows where returned already to track what rowIndex to map
  // to
  size_t rowCounter_;

  // Storing pointers here avoids copying the values into the heap
  using HeapElement = std::pair<IdTableIndex, std::vector<const Id*>>;

  struct MinHeapCompare {
    bool operator()(const HeapElement& lhs, const HeapElement& rhs) const {
      for (size_t i = 0; i < lhs.second.size(); ++i) {
        auto order = lhs.second[i]->compareWithoutLocalVocab(*rhs.second[i]);
        if (order != std::strong_ordering::equal) {
          // This is greater instead of less since the priority queue normally
          // works as max heap.
          return order == std::strong_ordering::greater;
        }
      }
      // If all elements are equal then the first one is smaller
      return false;
    }
  };

  std::priority_queue<HeapElement, std::vector<HeapElement>, MinHeapCompare>
      minHeap_;

  // Get the row subset of an idTable. The subset is determined by the sortPerm
  // given during construction. E.g. if {0, 2, 1} was given as perm then the row
  // returned is row[0], row[2], row[1]. Instead of the values directly only
  // pointers are returned to avoid copies.
  std::vector<const Id*> getRowSubsetAndAdvanceForIdTable(
      IdTableIndex idTableIndex);

  // Advances the column iterators for a certain idTable. If the iterators are
  // at the end then remove them from the columnRanges_
  void incrementColumnRangesForIdTable(IdTableIndex idTableIndex);
};

}  // namespace SortedIdTableMerge

#endif  // QLEVER_SRC_ENGINE_SORTEDIDTABLEMERGE_H
