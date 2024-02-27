//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#pragma once

#include "./Operation.h"

class TextLimit : public Operation {
 private:
  const QueryExecutionContext* qec_;
  std::shared_ptr<QueryExecutionTree> child_;
  const ColumnIndex textRecordColumn_;
  const ColumnIndex entityColumn_;
  const ColumnIndex scoreColumn_;

 public:
  TextLimit(QueryExecutionContext* qec,
            std::shared_ptr<QueryExecutionTree> child,
            const ColumnIndex& textRecordColumn,
            const ColumnIndex& entityColumn, const ColumnIndex& scoreColumn);

  ~TextLimit() override = default;

  string getCacheKeyImpl() const override;

  string getDescriptor() const override;

  size_t getResultWidth() const override;

  void setTextLimit(size_t) override {
    // TODO: this is deprecated
  }

  size_t getCostEstimate() override;

  uint64_t getSizeEstimateBeforeLimit() override;

  float getMultiplicity(size_t col) override {
    (void)col;
    return 1;
  }

  bool knownEmptyResult() override {
    return qec_->_textLimit == 0 || child_->knownEmptyResult();
  }

  vector<ColumnIndex> resultSortedOn() const override;

  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  ResultTable computeResult() override;

  vector<QueryExecutionTree*> getChildren() override { return {child_.get()}; }
};
