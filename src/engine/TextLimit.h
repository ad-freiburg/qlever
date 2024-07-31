//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"

// This class implements the TextLimit operation. It limits the number of texts
// that are returned for each unique entity combination. The texts are selected
// based on the score columns.
// Note that this does not mean that the result will only have n entries for
// each entity combination (where n is the limit). It will have n texts for each
// entity combination. But there can be multiple entries with the same entities
// and text.
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
    return child_->getMultiplicity(col);
  }

  bool knownEmptyResult() override {
    return limit_ == 0 || child_->knownEmptyResult();
  }

  vector<ColumnIndex> resultSortedOn() const override;

  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  ProtoResult computeResult([[maybe_unused]] bool requestLaziness) override;

  vector<QueryExecutionTree*> getChildren() override { return {child_.get()}; }
};
