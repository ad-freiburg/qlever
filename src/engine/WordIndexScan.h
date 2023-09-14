#pragma once

#include <string>

#include "./Operation.h"

class WordIndexScan : public Operation {
 public:
  using SetOfVariables = ad_utility::HashSet<Variable>;

 private:
  const SetOfVariables variables_;
  const Variable cvar_;
  const string word_;
  bool isPrefix_ = false;

 public:
  WordIndexScan(QueryExecutionContext* qec, SetOfVariables variables,
                Variable cvar, string word);

  virtual ~WordIndexScan() = default;

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  string asStringImpl(size_t indent) const override;

  string getDescriptor() const override;

  size_t getResultWidth() const override;

  void setTextLimit(size_t) override {}

  size_t getCostEstimate() override { return 5; }

  uint64_t getSizeEstimateBeforeLimit() override { return 5; }

  float getMultiplicity(size_t col) override { return 0; }

  bool knownEmptyResult() override { return false; }

  vector<ColumnIndex> resultSortedOn() const override;

  ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};
