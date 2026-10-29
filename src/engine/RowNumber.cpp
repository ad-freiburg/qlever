// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/RowNumber.h"

#include <numeric>

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

  // By default, sort by the row number column and then by the sort columns of
  // the child.
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

  // Append a column with the row numbers starting at `nextRowNumber` to an
  // `IdTable` and move it to the front. Advance `nextRowNumber` accordingly
  // (it persists between the blocks of a lazy result).
  std::vector<ColumnIndex> permutation(getResultWidth());
  permutation[0] = permutation.size() - 1;
  std::iota(permutation.begin() + 1, permutation.end(), 0);
  auto addRowNumbers = [permutation = std::move(permutation)](
                           IdTable& idTable, int64_t& nextRowNumber) {
    idTable.addEmptyColumn();
    ql::ranges::generate(idTable.getColumn(idTable.numColumns() - 1),
                         [&]() { return Id::makeFromInt(nextRowNumber++); });
    idTable.setColumnSubset(permutation);
  };

  if (childResult->isFullyMaterialized()) {
    IdTable idTable = childResult->cloneIdTable(cancellationChecker());
    int64_t nextRowNumber = offset_;
    addRowNumbers(idTable, nextRowNumber);
    return {std::move(idTable), resultSortedOn(),
            childResult->getSharedLocalVocab()};
  }
  return {Result::LazyResult{ad_utility::CachingTransformInputRange(
              childResult->idTables(),
              [addRowNumbers = std::move(addRowNumbers),
               nextRowNumber = offset_](auto& tableAndVocab) mutable {
                addRowNumbers(tableAndVocab.idTable_, nextRowNumber);
                return std::move(tableAndVocab);
              })},
          resultSortedOn()};
}
