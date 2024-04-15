//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"

// This class implements the TextLimit operation.
// It only keeps the highest scoring results for the child for each unique
// entity combination.
class TextLimit : public Operation {
 private:
  const size_t limit_;
  std::shared_ptr<QueryExecutionTree> child_;
  const ColumnIndex textRecordColumn_;
  const vector<ColumnIndex> entityColumns_;
  const vector<ColumnIndex> scoreColumns_;

 public:
  TextLimit(QueryExecutionContext* qec, const size_t limit,
            std::shared_ptr<QueryExecutionTree> child,
            const ColumnIndex& textRecordColumn,
            const vector<ColumnIndex>& entityColumns,
            const vector<ColumnIndex>& scoreColumns);

  ~TextLimit() override = default;

  string getCacheKeyImpl() const override;

  string getDescriptor() const override;

  size_t getResultWidth() const override;

  size_t getCostEstimate() override;

  size_t getTextLimit() const { return limit_; }

  Variable getTextRecordVariable() const;

  vector<Variable> getEntityVariables() const;

  vector<Variable> getScoreVariables() const;

  uint64_t getSizeEstimateBeforeLimit() override;

  float getMultiplicity(size_t col) override {
    (void)col;
    return 1;
  }

  bool knownEmptyResult() override {
    return limit_ == 0 || child_->knownEmptyResult();
  }

  vector<ColumnIndex> resultSortedOn() const override;

  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  ResultTable computeResult() override;

  vector<QueryExecutionTree*> getChildren() override { return {child_.get()}; }
};
