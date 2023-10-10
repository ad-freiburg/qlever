#pragma once

#include <string>

#include "./Operation.h"

class EntityIndexScanForWord : public Operation {
 private:
  const Variable cvar_;
  const Variable evar_;
  const string word_;

 public:
  EntityIndexScanForWord(QueryExecutionContext* qec, Variable cvar,
                         Variable evar, string word);

  virtual ~EntityIndexScanForWord() = default;

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
