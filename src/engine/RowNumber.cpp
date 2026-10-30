// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/RowNumber.h"

#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "engine/QueryExecutionTree.h"

// _____________________________________________________________________________
RowNumber::RowNumber(QueryExecutionContext* qec,
                     std::shared_ptr<QueryExecutionTree> child,
                     Variable rowNumberVariable, int64_t offset,
                     std::vector<ColumnIndex> sortedOn)
    : Operation{qec},
      child_{std::move(child)},
      rowNumberVariable_{std::move(rowNumberVariable)},
      offset_{offset},
      sortedOn_{std::move(sortedOn)} {
  // Negative row numbers would break the sorting by the row number column,
  // because `Id`s of integers are compared by their bits.
  AD_CONTRACT_CHECK(offset_ >= 0);

  // The row number variable must not shadow a variable of the child.
  AD_CONTRACT_CHECK(!child_->getVariableColumns().contains(rowNumberVariable_));

  // By default, sort by the row number column and then by the sort columns of
  // the child.
  //
  // NOTE: The row number column is unique and the child's order is preserved,
  // so the result is sorted by every column sequence that starts with the row
  // number column, and also by the sort columns of the child alone. Since
  // `resultSortedOn` can only express a single one of these orders, we report
  // the one that keeps the most information and let `makeSortedTree` provide
  // the other orders that start with the row number column for free.
  if (sortedOn_.empty()) {
    sortedOn_.push_back(0);
    for (auto col : child_->resultSortedOn()) {
      sortedOn_.push_back(col + 1);
    }
  }
  AD_CONTRACT_CHECK(sortedOn_[0] == 0);
}

// _____________________________________________________________________________
qlm::vector<QueryExecutionTree*> RowNumber::getChildrenImpl() const {
  return {{child_.get()}, allocator()};
}

// _____________________________________________________________________________
std::string RowNumber::getCacheKeyImpl() const {
  // The `sortedOn_` columns are not part of the key, because they don't change
  // the result.
  return absl::StrCat("ROW NUMBER (offset ", offset_, ") ",
                      child_->getCacheKey());
}

// _____________________________________________________________________________
std::string RowNumber::getDescriptor() const {
  return absl::StrCat("RowNumber ", rowNumberVariable_.name());
}

// _____________________________________________________________________________
size_t RowNumber::getResultWidth() const {
  return child_->getResultWidth() + 1;
}

// _____________________________________________________________________________
size_t RowNumber::getCostEstimate() {
  return child_->getCostEstimate() + child_->getSizeEstimate();
}

// _____________________________________________________________________________
uint64_t RowNumber::getSizeEstimateBeforeLimit() {
  return child_->getSizeEstimate();
}

// _____________________________________________________________________________
float RowNumber::getMultiplicity(size_t col) {
  return col == 0 ? 1.0f : child_->getMultiplicity(col - 1);
}

// _____________________________________________________________________________
bool RowNumber::knownEmptyResult() { return child_->knownEmptyResult(); }

// _____________________________________________________________________________
std::unique_ptr<Operation> RowNumber::cloneImpl() const {
  return std::make_unique<RowNumber>(getExecutionContext(), child_->clone(),
                                     rowNumberVariable_, offset_, sortedOn_);
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>> RowNumber::makeSortedTree(
    const std::vector<ColumnIndex>& sortColumns) const {
  // Any order that starts with the row number column is already satisfied
  // (the row numbers are unique and ascending, so there are no ties to be
  // broken by the remaining columns, regardless of the order of the child).
  AD_CONTRACT_CHECK(!isSortedBy(sortColumns));
  if (sortColumns.at(0) != 0) {
    return std::nullopt;
  }
  return ad_utility::makeExecutionTree<RowNumber>(
      getExecutionContext(), child_, rowNumberVariable_, offset_, sortColumns);
}

// _____________________________________________________________________________
std::vector<ColumnIndex> RowNumber::resultSortedOn() const { return sortedOn_; }

// _____________________________________________________________________________
VariableToColumnMap RowNumber::computeVariableToColumnMap() const {
  // Shift the columns of the child by one to make room for the row number
  // column.
  auto varToCol = child_->getVariableColumns();
  for (auto& [variable, info] : varToCol) {
    ++info.columnIndex_;
  }
  varToCol[rowNumberVariable_] = makeAlwaysDefinedColumn(0);
  return varToCol;
}

// _____________________________________________________________________________
Result RowNumber::computeResult(bool requestLaziness) {
  auto childResult = child_->getResult(requestLaziness);

  // Append a column with the consecutive row numbers starting at `offset_` to
  // an `IdTable` and move it to the front. The next row number persists between
  // the calls (for the blocks of a lazy result). Throw if a row number exceeds
  // the range of integer `Id`s (it would wrap around and break the sort order).
  std::vector<ColumnIndex> permutation(getResultWidth());
  permutation[0] = permutation.size() - 1;
  std::iota(permutation.begin() + 1, permutation.end(), 0);
  auto addRowNumbers = [permutation = std::move(permutation),
                        nextRowNumber = offset_](IdTable& idTable) mutable {
    if (static_cast<int64_t>(idTable.numRows()) >
        Id::maxInt - nextRowNumber + 1) {
      throw std::runtime_error(absl::StrCat(
          "The row numbers of a row number SERVICE exceed the maximum integer "
          "value ",
          Id::maxInt, ", choose a smaller `<offset>`"));
    }
    idTable.addEmptyColumn();
    ql::ranges::generate(
        idTable.getColumn(idTable.numColumns() - 1),
        [&nextRowNumber]() { return Id::makeFromInt(nextRowNumber++); });
    idTable.setColumnSubset(permutation);
  };

  // Number a copy of the materialized result.
  if (childResult->isFullyMaterialized()) {
    IdTable idTable = childResult->cloneIdTable(cancellationChecker());
    addRowNumbers(idTable);
    return {std::move(idTable), resultSortedOn(),
            childResult->getSharedLocalVocab()};
  }

  // Number the blocks of the lazy result one after the other.
  return {Result::LazyResult{ad_utility::CachingTransformInputRange(
              childResult->idTables(),
              [addRowNumbers =
                   std::move(addRowNumbers)](auto& tableAndVocab) mutable {
                addRowNumbers(tableAndVocab.idTable_);
                return std::move(tableAndVocab);
              })},
          resultSortedOn()};
}
