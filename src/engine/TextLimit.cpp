//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#include "engine/TextLimit.h"

// _____________________________________________________________________________
TextLimit::TextLimit(QueryExecutionContext* qec, const size_t limit,
                     std::shared_ptr<QueryExecutionTree> child,
                     const ColumnIndex& textRecordColumn,
                     const vector<ColumnIndex>& entityColumns,
                     const vector<ColumnIndex>& scoreColumns)
    : Operation(qec),
      limit_(limit),
      child_(std::move(child)),
      textRecordColumn_(textRecordColumn),
      entityColumns_(entityColumns),
      scoreColumns_(scoreColumns) {}

// _____________________________________________________________________________
ResultTable TextLimit::computeResult() {
  shared_ptr<const ResultTable> childRes = child_->getResult();

  if (limit_ == 0) {
    return {IdTable(childRes->width(), getExecutionContext()->getAllocator()),
            resultSortedOn(), childRes->getSharedLocalVocab()};
  }

  IdTable idTable = childRes->idTable().clone();

  // TODO<joka921> Let the SORT class handle this. This requires descending
  // sorting for positive integers though.
  auto compareScores = [this](const auto& lhs, const auto& rhs) {
    size_t lhsScore = 0;
    size_t rhsScore = 0;
    std::ranges::for_each(scoreColumns_,
                          [&lhs, &rhs, &lhsScore, &rhsScore](const auto& col) {
                            lhsScore += lhs[col].getInt();
                            rhsScore += rhs[col].getInt();
                          });
    if (lhsScore > rhsScore) {
      return 1;
    } else if (lhsScore < rhsScore) {
      return -1;
    }
    return 0;
  };

  auto compareEntities = [this](const auto& lhs, const auto& rhs) {
    auto it =
        std::ranges::find_if(entityColumns_, [&lhs, &rhs](const auto& col) {
          return lhs[col] < rhs[col] || lhs[col] > rhs[col];
        });

    if (it != entityColumns_.end()) {
      if (lhs[*it] < rhs[*it]) {
        return 1;
      } else {
        return -1;
      }
    }

    return 0;
  };

  std::ranges::sort(idTable, [this, compareScores, compareEntities](
                                 const auto& lhs, const auto& rhs) {
    return compareEntities(lhs, rhs) == 1 ||
           (compareEntities(lhs, rhs) == 0 &&
            (compareScores(lhs, rhs) == 1 ||
             (compareScores(lhs, rhs) == 0 &&
              (lhs[textRecordColumn_] > rhs[textRecordColumn_]))));
  });

  // Go through the table and add the first n texts for each
  // entity to a new empty table (where n is the limit).
  IdTable resIdTable(idTable.numColumns(),
                     getExecutionContext()->getAllocator());
  size_t currentEntityIndex = 0;
  size_t currentEntityCount = 0;
  bool lastRecordAdded = false;
  for (size_t i = 0; i < idTable.numRows(); ++i) {
    if (i == 0) {
      resIdTable.push_back(idTable[i]);
      lastRecordAdded = true;
      currentEntityCount = 1;
      continue;  // handle first row separately to avoid out of bounds access
    }
    if (compareEntities(idTable[i], idTable[currentEntityIndex]) != 0) {
      // Case: new entity.
      currentEntityIndex = i;
      currentEntityCount = 1;
    } else if (idTable[i][textRecordColumn_] !=
               idTable[i - 1][textRecordColumn_]) {
      // Case: new text record.
      if (currentEntityCount >= limit_) {
        // Case: new text record and reached the limit.
        // If we reached the limit we do not add the row to the result table.
        lastRecordAdded = false;
        continue;
      } else {
        // Case: new text record but limit not reached yet.
        ++currentEntityCount;
      }
    } else if (!lastRecordAdded) {
      // Case: the text record is the same as the last one but we didn't add
      // the last one to the result table because we reached the limit.
      // So we do not add this row to the result table either.
      continue;
    }
    // If the we don't have a new text record we don't have to increase the
    // counter but we still add the row to the result table.
    resIdTable.push_back(idTable[i]);
    lastRecordAdded = true;
  }

  return {std::move(resIdTable), resultSortedOn(),
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
Variable TextLimit::getTextRecordVariable() const {
  return child_->getVariableAndInfoByColumnIndex(textRecordColumn_).first;
}

// _____________________________________________________________________________
vector<Variable> TextLimit::getEntityVariables() const {
  vector<Variable> entityVars;
  for (auto col : entityColumns_) {
    entityVars.push_back(child_->getVariableAndInfoByColumnIndex(col).first);
  }
  return entityVars;
}

// _____________________________________________________________________________
vector<Variable> TextLimit::getScoreVariables() const {
  vector<Variable> scoreVars;
  for (auto col : scoreColumns_) {
    scoreVars.push_back(child_->getVariableAndInfoByColumnIndex(col).first);
  }
  return scoreVars;
}

// _____________________________________________________________________________
uint64_t TextLimit::getSizeEstimateBeforeLimit() {
  return child_->getSizeEstimate();
}

// _____________________________________________________________________________
vector<ColumnIndex> TextLimit::resultSortedOn() const { return entityColumns_; }

// _____________________________________________________________________________
string TextLimit::getDescriptor() const {
  std::ostringstream os;
  os << "TextLimit with limit: " << limit_;
  return os.str();
}

// _____________________________________________________________________________
string TextLimit::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "TEXT LIMIT: "
     << " with n: " << limit_ << ", with child: " << child_->getCacheKey()
     << " and ColumnIndices: " << textRecordColumn_ << ", {";
  for (const auto& column : entityColumns_) {
    os << column << ", ";
  }
  os.seekp(-2, std::ostringstream::cur);
  os << "}, {";
  for (const auto& column : scoreColumns_) {
    os << column << ", ";
  }
  os << "}";
  return std::move(os).str();
}
