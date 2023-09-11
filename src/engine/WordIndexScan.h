#pragma once

#include <string>

#include "./Operation.h"

class WordIndexScan : public Operation {
 private:
  Variable var_;
  string word_;
  bool isPrefix_ = false;
 public:
  WordIndexScan(QueryExecutionContext* qec, Variable var, string word);

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  string asStringImpl(size_t indent) const override;

  string getDescriptor() const override;

  size_t getResultWidth() const override;

  void setTextLimit(size_t) {}

  size_t getCostEstimate() { return 5; }

  uint64_t getSizeEstimateBeforeLimit() override { return 5; }

  float getMultiplicity(size_t col) { return 0; }

  bool knownEmptyResult() override { return false; }

  vector<ColumnIndex> resultSortedOn() const override;

  ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};
