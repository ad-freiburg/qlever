//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#pragma once

#include "./Operation.h"

class TextLimitOperation : public Operation {
 private:
  const size_t n_;
  std::shared_ptr<QueryExecutionTree> child_;
  const ColumnIndex textRecordColumn_;
  const ColumnIndex entityColumn_;
  const ColumnIndex scoreColumn_;

 public:
  TextLimitOperation(QueryExecutionContext* qec, const size_t& n,
                     std::shared_ptr<QueryExecutionTree> child,
                     const ColumnIndex& textRecordColumn,
                     const ColumnIndex& entityColumn,
                     const ColumnIndex& scoreColumn);

  ~TextLimitOperation() override = default;

  string getCacheKeyImpl() const override;

  string getDescriptor() const override;

  size_t getResultWidth() const override;

  void setTextLimit(size_t) override {
    // QUESTION: is this function deprecated?
  }

  size_t getCostEstimate() override;

  uint64_t getSizeEstimateBeforeLimit() override;

  float getMultiplicity(size_t col) override {
    (void)col;
    return 1;
  }

  bool knownEmptyResult() override {
    return n_ == 0 || child_->knownEmptyResult();
  }

  vector<ColumnIndex> resultSortedOn() const override;

  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  ResultTable computeResult() override;

  vector<QueryExecutionTree*> getChildren() override { return {child_.get()}; }
};
