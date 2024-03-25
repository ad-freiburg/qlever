//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#include "./TextLimit.h"

// _____________________________________________________________________________
TextLimit::TextLimit(QueryExecutionContext* qec, const size_t n,
                     std::shared_ptr<QueryExecutionTree> child,
                     const ColumnIndex& textRecordColumn,
                     const vector<ColumnIndex>& entityColumns,
                     const vector<ColumnIndex>& scoreColumns)
    : Operation(qec),
      qec_(qec),
      n_(n),
      child_(std::move(child)),
      textRecordColumn_(textRecordColumn),
      entityColumns_(entityColumns),
      scoreColumns_(scoreColumns) {}

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

  auto compareScores = [this](const auto& lhs, const auto& rhs) {
    size_t lhsScore = 0;
    size_t rhsScore = 0;
    for (size_t i = 0; i < scoreColumns_.size(); ++i) {
      lhsScore += lhs[scoreColumns_[i]].getInt();
      rhsScore += rhs[scoreColumns_[i]].getInt();
    }
    if (lhsScore > rhsScore) {
      return 1;
    } else if (lhsScore < rhsScore) {
      return -1;
    } else {
      return 0;
    }
  };

  auto compareEntities = [this](const auto& lhs, const auto& rhs) {
    for (size_t i = 0; i < entityColumns_.size(); ++i) {
      if (lhs[entityColumns_[i]] > rhs[entityColumns_[i]]) {
        return 1;
      } else if (lhs[entityColumns_[i]] < rhs[entityColumns_[i]]) {
        return -1;
      }
    }
    return 0;
  };

  // Runtime: O(n log n)
  std::ranges::sort(idTable, [this, compareScores, compareEntities](
                                 const auto& lhs, const auto& rhs) {
    return compareEntities(lhs, rhs) == 1 ||
           (compareEntities(lhs, rhs) == 0 &&
            (compareScores(lhs, rhs) == 1 ||
             (compareScores(lhs, rhs) == 0 &&
              (lhs[textRecordColumn_] > rhs[textRecordColumn_]))));
  });
  // NOTE: The algorithm assumes that the scores only depends on entity and text
  // record.

  // Go through the table and remove all but the first n entries for each
  // entity.

  // Runtime: O(n)? How expensive is erase?
  size_t currentEntityIndex = 0;
  size_t currentEntityCount = 1;
  for (size_t i = 1; i < idTable.numRows(); ++i) {
    if (compareEntities(idTable[i], idTable[currentEntityIndex]) != 0) {
      // Case: new entity.
      currentEntityIndex = i;
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
     << " and ColumnIndices: " << std::to_string(textRecordColumn_) << ", {";
  for (const auto& column : entityColumns_) {
    os << std::to_string(column) << ", ";
  }
  os.seekp(-2, os.cur);
  os << "}, {";
  for (const auto& column : scoreColumns_) {
    os << std::to_string(column) << ", ";
  }
  os << "}";
  return std::move(os).str();
}
