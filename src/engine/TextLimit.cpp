//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#include "./TextLimit.h"

// _____________________________________________________________________________
TextLimit::TextLimit(QueryExecutionContext* qec, const size_t& n,
                     std::shared_ptr<QueryExecutionTree> child,
                     const ColumnIndex& textRecordColumn,
                     const ColumnIndex& entityColumn,
                     const ColumnIndex& scoreColumn)
    : Operation(qec),
      n_(n),
      child_(std::move(child)),
      textRecordColumn_(textRecordColumn),
      entityColumn_(entityColumn),
      scoreColumn_(scoreColumn) {}

// _____________________________________________________________________________
ResultTable TextLimit::computeResult() {
  shared_ptr<const ResultTable> childRes = child_->getResult();

  if (n_ == 0) {
    return {IdTable(childRes->width(), getExecutionContext()->getAllocator()),
            resultSortedOn(), childRes->getSharedLocalVocab()};
  }

  IdTable idTable =
      childRes->idTable().clone();  // QUESTION: is there a cheaper solution?
  // Sort the table by the entity column, then the score column, then the text
  // column.

  // Runtime: O(n log n)
  std::ranges::sort(idTable, [this](const auto& lhs, const auto& rhs) {
    return lhs[entityColumn_] > rhs[entityColumn_] ||
           (lhs[entityColumn_] == rhs[entityColumn_] &&
            (lhs[scoreColumn_] > rhs[scoreColumn_] ||
             (lhs[scoreColumn_] == rhs[scoreColumn_] &&
              (lhs[textRecordColumn_] > rhs[textRecordColumn_]))));
  });
  // NOTE: The algorithm assumes that the scores only depends on entity and text
  // record.

  // Go through the table and remove all but the first n entries for each
  // entity.

  // Runtime: O(n)? How expensive is erase?
  Id currentEntity = idTable[0][entityColumn_];
  size_t currentEntityCount = 1;
  for (size_t i = 1; i < idTable.numRows(); ++i) {
    if (idTable[i][entityColumn_] != currentEntity) {
      // Case: new entity.
      currentEntity = idTable[i][entityColumn_];
      currentEntityCount = 1;
    } else if (idTable[i][textRecordColumn_] !=
               idTable[i - 1][textRecordColumn_]) {
      // Case: new text record.
      if (currentEntityCount >= n_) {
        // Case: new text record and reached the limit.
        idTable.erase(idTable.begin() + i);
        --i;
      } else {
        // Case: new text record but limit not reached yet.
        ++currentEntityCount;
      }
    }
    // If the we don't have a new text record we don't have to do anything.

    // NOTE: This results that for each entity there only max n texts not
    // entries!!! So there can be two entries that share the same entity and
    // text in the result. Could be confusing if only entity and text are
    // selected.
  }

  return {std::move(idTable), resultSortedOn(),
          childRes->getSharedLocalVocab()};
}

// _____________________________________________________________________________
VariableToColumnMap TextLimit::computeVariableToColumnMap() const {
  return child_->getVariableColumns();
}

// _____________________________________________________________________________
size_t TextLimit::getResultWidth() const { return child_->getResultWidth(); }

// _____________________________________________________________________________
size_t TextLimit::getCostEstimate() {
  size_t sizeChild = child_->getSizeEstimate();
  return sizeChild + log2(sizeChild) * sizeChild;
}

// _____________________________________________________________________________
uint64_t TextLimit::getSizeEstimateBeforeLimit() {
  return child_
      ->getSizeEstimate();  // QUESTION: Why is getSizeEstimateBeforeLimit() not
                            // possible? And how come we don't have to implement
                            // getSizeEstimate() here?
}

// _____________________________________________________________________________
vector<ColumnIndex> TextLimit::resultSortedOn() const {
  return {};  // NOTE: sorted on entity, then score, then text all descending.
              // But apparently this expects an ascending sort. Possible
              // solution: sort ascending but go through list from back to
              // front.
}

// _____________________________________________________________________________
string TextLimit::getDescriptor() const {
  std::ostringstream os;
  os << "TextLimit with limit n: " << n_;
  return os.str();
}

// _____________________________________________________________________________
string TextLimit::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "TEXT LIMIT: "
     << " with n: " << n_ << ", with child: " << child_->getCacheKey()
     << "and ColumnIndices: " << textRecordColumn_ << ", " << entityColumn_
     << ", " << scoreColumn_;
  return std::move(os).str();
}
