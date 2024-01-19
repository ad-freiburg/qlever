//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#include "./TextLimitOperation.h"

// _____________________________________________________________________________
TextLimitOperation::TextLimitOperation(
    QueryExecutionContext* qec, const size_t& n,
    std::shared_ptr<QueryExecutionTree> child,
    const ColumnIndex& textRecordColumn, const ColumnIndex& entityColumn,
    const ColumnIndex& scoreColumn)
    : Operation(qec),
      n_(n),
      child_(std::move(child)),
      textRecordColumn_(textRecordColumn),
      entityColumn_(entityColumn),
      scoreColumn_(scoreColumn) {}

// _____________________________________________________________________________
ResultTable TextLimitOperation::computeResult() {
  shared_ptr<const ResultTable> childRes = child_->getResult();
  IdTable idTable =
      childRes->idTable().clone();  // QUESTION: is there a cheaper solution?
  // Sort the table by the entity column, then the score column, then the text
  // column. Runtime: O(n log n)
  std::ranges::sort(idTable, [this](const auto& lhs, const auto& rhs) {
    return lhs[entityColumn_] < rhs[entityColumn_] ||
           (lhs[entityColumn_] == rhs[entityColumn_] &&
            (lhs[scoreColumn_] > rhs[scoreColumn_] ||
             (lhs[scoreColumn_] == rhs[scoreColumn_] &&
              (lhs[textRecordColumn_] < rhs[textRecordColumn_]))));
  });

  // Go through the table and remove all but the first n entries for each
  // entity. NOTE: This doesn't account for multiple text records with different
  // words, but it can be changed easily. Runtime: O(n)? How expensive is erase?
  Id currentEntity = idTable[0][entityColumn_];
  size_t currentEntityCount = 0;
  size_t currentIndex = 0;
  for (size_t i = 0; i < idTable.numRows(); ++i) {
    if (idTable[i][entityColumn_] != currentEntity) {
      currentEntity = idTable[i][entityColumn_];
      currentEntityCount = 0;
      currentIndex = i;
    }
    if (currentEntityCount >= n_) {
      idTable.erase(idTable.begin() + currentIndex);
      --i;
    }
    ++currentEntityCount;
  }

  return {std::move(idTable), resultSortedOn(),
          childRes->getSharedLocalVocab()};
}

// _____________________________________________________________________________
VariableToColumnMap TextLimitOperation::computeVariableToColumnMap() const {
  return child_->getVariableColumns();
}

// _____________________________________________________________________________
size_t TextLimitOperation::getResultWidth() const {
  return child_->getResultWidth();
}

// _____________________________________________________________________________
size_t TextLimitOperation::getCostEstimate() {
  size_t sizeChild = child_->getSizeEstimate();
  return sizeChild + log2(sizeChild) * sizeChild;
}

// _____________________________________________________________________________
uint64_t TextLimitOperation::getSizeEstimateBeforeLimit() {
  return child_
      ->getSizeEstimate();  // QUESTION: Why is getSizeEstimateBeforeLimit() not
                            // possible? And how come we don't have to implement
                            // getSizeEstimate() here?
}

// _____________________________________________________________________________
vector<ColumnIndex> TextLimitOperation::resultSortedOn() const {
  // NOTE: while entityColumn_ and  textRecordColumn_ are sorted ascending,
  // scoreColumn_ is sorted descending.
  return {entityColumn_, scoreColumn_, textRecordColumn_};
}

// _____________________________________________________________________________
string TextLimitOperation::getDescriptor() const {
  std::ostringstream os;
  os << "TextLimitOperation with limit n: " << n_;
  return os.str();
}

// _____________________________________________________________________________
string TextLimitOperation::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "TEXT LIMIT: "
     << " with n: " << n_ << " and child: " << child_->getCacheKey();
  return std::move(os).str();
}
