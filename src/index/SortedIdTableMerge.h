// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)
#ifndef QLEVER_SRC_ENGINE_SORTEDIDTABLEMERGE_H
#define QLEVER_SRC_ENGINE_SORTEDIDTABLEMERGE_H

#include <queue>

#include "engine/idTable/IdTable.h"
#include "util/HashMap.h"

namespace sortedIdTableMerge {

// Compares rows from left to right using the underlying bits.
struct DirectComparator {
  template <typename T>
  bool operator()(const T& lhs, const T& rhs) const {
    // Project
    const auto& lhsRow = lhs.second;
    const auto& rhsRow = rhs.second;

    // Compare
    return ql::ranges::lexicographical_compare(
        lhsRow, rhsRow, [](const auto& id1, const auto& id2) {
          // Swap id1 and id2 since else the heap would be a max heap
          return id2.compareWithoutLocalVocab(id1) < 0;
        });
  }
};

// Alias to easier understand the code
using IdTableIndex = size_t;

/**
 * @brief This struct is an InputRangeMixin that can be used to retrieve
 *        rows in ascending order of the `CompType` comparator given
 *        `IdTableViews` that are pre-sorted by the same comparator.
 * @tparam CompType The type of the comparator. NOTE the comparator is used
 *                  to compare rows in the context of a priority queue. This
 *                  queue works as max heap, but in this case should work as
 *                  min heap. Therefore, the comparator has to have the
 *                  logic turned around. E.g. if a < b then comp(a,b) should
 *                  return true.
 * @tparam NumCols The number of columns the idTables in `idTableViews` have.
 *                 NOTE this is used as template parameter to let
 *                 the struct work with static idTable views instead of dynamic.
 * @tparam FetchBlockSize The number of rows put into the `priorityQueue` at
 *                        once per idTable. NOTE this is done to reduce cache
 *                        misses.
 */
template <typename CompType, size_t NumCols, size_t FetchBlockSize = 500>
struct MinRowGetter
    : ad_utility::InputRangeMixin<MinRowGetter<CompType, NumCols>> {
  // Aliases
  using RowIterator = decltype(std::declval<IdTableView<NumCols>>().begin());
  using RowSentinel = decltype(std::declval<IdTableView<NumCols>>().end());
  using RowIteratorPair = std::pair<RowIterator, RowSentinel>;
  using IdTableAndRowIteratorPair = std::pair<IdTableIndex, RowIteratorPair>;
  using IdTableAndRow =
      std::pair<IdTableIndex, columnBasedIdTable::Row<Id, NumCols>>;

  /// MEMBERS
  std::vector<IdTableAndRowIteratorPair> tableIterators_;
  std::vector<bool> tableIteratorsFinished_;
  bool allIteratorsExhausted_ = false;
  bool isFinished_ = false;

  std::vector<size_t> numberOfElementsOfIdTableInQueue_;
  std::vector<IdTableAndRow> priorityQueue_;
  IdTableAndRow currentMin_;
  CompType comp_;

  const std::vector<IdTableView<NumCols>>& idTableViews_;

  /// CONSTRUCTORS AND OPERATORS
  explicit MinRowGetter(CompType comp,
                        const std::vector<IdTableView<NumCols>>& idTables)
      : comp_{std::move(comp)}, idTableViews_(idTables) {}

  // Delete default, copy, move constructor and copy, move assignment
  MinRowGetter() = delete;
  MinRowGetter(const MinRowGetter&) = delete;
  MinRowGetter(MinRowGetter&&) = delete;
  MinRowGetter& operator=(const MinRowGetter&) = delete;
  MinRowGetter& operator=(MinRowGetter&&) = delete;

  /// FUNCTIONS
  void start() {
    priorityQueue_.reserve(idTableViews_.size() * FetchBlockSize);
    tableIterators_.reserve(idTableViews_.size());
    numberOfElementsOfIdTableInQueue_.resize(idTableViews_.size(), 0);
    tableIteratorsFinished_.resize(idTableViews_.size(), false);
    size_t index = 0;
    for (auto& idTable : idTableViews_) {
      AD_CONTRACT_CHECK(idTable.numColumns() == NumCols);
      tableIterators_.push_back({index, {idTable.begin(), idTable.end()}});
      const auto& generator = tableIterators_.back().second;
      AD_CORRECTNESS_CHECK(generator.first != generator.second);
      fetchNextEntriesFromTable(index);
      ++index;
    }
    next();
  }

  void next() {
    if (priorityQueue_.empty() && allIteratorsExhausted_) {
      isFinished_ = true;
      return;
    }
    ql::ranges::pop_heap(priorityQueue_, comp_);
    auto& [index, row] = priorityQueue_.back();
    currentMin_ = {index, std::move(row)};
    --numberOfElementsOfIdTableInQueue_[index];
    priorityQueue_.pop_back();
    if (numberOfElementsOfIdTableInQueue_[index] == 0 &&
        !allIteratorsExhausted_) {
      fetchNextEntriesFromTable(index);
    }
  }

  auto& get() { return currentMin_; }

  bool isFinished() const { return isFinished_; }

  // NOTE only call on an index that exists in the tableIterators_
  void fetchNextEntriesFromTable(IdTableIndex idTableIndex) {
    auto& iterator = tableIterators_[idTableIndex].second.first;
    auto& sentinel = tableIterators_[idTableIndex].second.second;
    size_t i = 0;
    for (; i < FetchBlockSize && iterator != sentinel; ++i) {
      priorityQueue_.push_back({idTableIndex, *iterator++});
      ql::ranges::push_heap(priorityQueue_, comp_);
    };
    numberOfElementsOfIdTableInQueue_[idTableIndex] = i;
    if (iterator == sentinel) {
      tableIteratorsFinished_[idTableIndex] = true;
      if (ql::ranges::all_of(tableIteratorsFinished_,
                             [](const auto& el) { return el; })) {
        allIteratorsExhausted_ = true;
      }
    }
  };
};

/**
 * @brief This function takes in `idTablesToMerge` and a mapping
 *        `permutationIdTables` that maps each row of each `IdTable` to a row
 *        of the result. The values from the columns of `idTablesToMerge` except
 *        the columns of `sortPerm` are written to the result using that map.
 * @param idTablesToMerge
 * @param permutationIdTables This is the pre-computed mapping. For each
 *                            `IdTable` in `idTablesToMerge` it maps the rows to
 *                            rows of the result `IdTable`. E.g. if we have
 *                            two `IdTables` in `idTablesToMerge` that only have
 *                            two rows, {0, 1} and {1, 2} for the first and
 *                            {0, 2} and {1, 3} for the second then (if rows
 *                            were sorted left to right) the
 *                            `permutationIdTables` is {{0, 2}, {1, 3}}.
 * @param result Result `IdTable` that is filled.
 * @param sortPerm
 */
void writeIdTableFromPermutation(
    const std::vector<IdTable>& idTablesToMerge,
    std::vector<std::vector<IdTableIndex>> permutationIdTables, IdTable& result,
    const std::vector<size_t>& sortPerm);

/**
 * @brief Checks if:
 *        - `idTablesToMerge` is empty
 *        - all `IdTable`s in `idTablesToMerge` have the same number of columns
 *        - the `sortPerm` has less than or equal the number of `ColumnIndex`es
 *          than the `IdTable`s
 *        - the `ColumnIndex`es of the `sortPerm` are in a valid range
 *        - there are no duplicates in the `ColumnIndex`es
 * @param idTablesToMerge
 * @param sortPerm
 */
void checkErrors(const std::vector<IdTable>& idTablesToMerge,
                 const std::vector<ColumnIndex>& sortPerm);

/**
 *
 * @tparam NumResultCols Number of columns the result has. The `idTablesToMerge`
 *                       need to have the same number of columns else
 *                       the function throws. This is a template parameter to
 *                       use `IdTableStatic` which improves the runtime of this
 *                       function.
 * @tparam NumPermCols Number of `ColumnIndex`es given by the `sortPerm`. This
 *                     is a template parameter to use static `IdTableViews`
 *                     which improves the runtime of this function.
 * @tparam CompType The type of the comparator. For details see `CompType` of
 *                  `MinRowGetter`.
 * @param idTablesToMerge A vector of pre-sorted `IdTable`s in terms of the
 *                        `comparator` that are merged into one sorted
 *                        `IdTable`.
 * @param allocator Allocator used to create the result `IdTable`.
 * @param sortPerm The `sortPerm` contains `ColumnIndex`es. The
 *                 `idTablesToMerge` are only compared on those columns, and
 *                 the result has the given columns in front. E.g. if a
 *                 `sortPerm` {1, 0} is given for `idTablesToMerge` with
 *                 three columns {0, 1, 2}, then the result has the columns
 *                 {1, 0, 2}.
 * @param comparator For details see `CompType` of `MinRowGetter`.
 * @return A dynamic `IdTable` that is the merged result of all
 *         `idTablesToMerge`. NOTE the columns in the result can be permuted.
 *         For details see `sortPerm`
 * @details This method works by first creating static `IdTableView`s for the
 *          columns given by `sortPerm`. Using those views with the
 *          `MinRowGetter` it fills the result with those columns in sorted
 *          order while creating a mapping that maps rows of the
 *          `idTablesToMerge` to the rows of the result. After this is done
 *          all columns of the results are filled using
 *          `writeIdTableFromPermutation`. This happens in a column based way
 *          reducing jumps in the input data.
 */
template <size_t NumResultCols, size_t NumPermCols, typename CompType>
IdTable mergeIdTables(const std::vector<IdTable>& idTablesToMerge,
                      const ad_utility::AllocatorWithLimit<Id>& allocator,
                      std::vector<ColumnIndex> sortPerm, CompType comparator) {
  checkErrors(idTablesToMerge, sortPerm);
  AD_CONTRACT_CHECK(NumPermCols == sortPerm.size(),
                    "The `NumPermCols` specified in the template of the "
                    "function doesn't match the number of `ColumnIndex`es "
                    "given in `sortPerm`. `NumPermCols` was: ",
                    NumPermCols, ". `sortPerm` size was: ", sortPerm.size());

  std::vector<IdTableView<NumPermCols>> idTableViews;
  std::vector<std::vector<IdTableIndex>> permutationIdTables;
  permutationIdTables.reserve(idTablesToMerge.size());
  size_t sumSize = 0;
  ql::ranges::for_each(
      idTablesToMerge, [&permutationIdTables, &sumSize, &idTableViews,
                        &sortPerm](const auto& idTable) {
        AD_CONTRACT_CHECK(NumResultCols == idTable.numColumns(),
                          "The `NumResultCols` in the template of the function "
                          "doesn't match the number of columns of at least on "
                          "given `IdTable`. `NumResultCols` was: ",
                          NumResultCols,
                          ". Number of columns of first faulty `IdTable` was: ",
                          idTable.numColumns());
        permutationIdTables.emplace_back();
        permutationIdTables.back().reserve(idTable.size());
        sumSize += idTable.size();
        idTableViews.push_back(idTable.asColumnSubsetView(sortPerm)
                                   .template asStaticView<NumPermCols>());
      });

  IdTableStatic<NumPermCols> partialResult(allocator);
  partialResult.reserve(sumSize);

  [&] {
    size_t resultIndex = 0;
    for (auto [idTableIndex, row] :
         MinRowGetter<decltype(DirectComparator{}), NumPermCols>{
             std::move(comparator), idTableViews}) {
      permutationIdTables.at(idTableIndex).push_back(resultIndex);
      partialResult.push_back(row);
      ++resultIndex;
    }
  }();

  IdTable result = std::move(partialResult).template toDynamic<>();
  if (NumResultCols == NumPermCols) {
    return result;
  }

  for (size_t i = 0; i < NumResultCols - NumPermCols; ++i) {
    result.addEmptyColumn<>();
  }

  writeIdTableFromPermutation(idTablesToMerge, std::move(permutationIdTables),
                              result, sortPerm);
  return result;
}

}  // namespace sortedIdTableMerge

#endif  // QLEVER_SRC_ENGINE_SORTEDIDTABLEMERGE_H
