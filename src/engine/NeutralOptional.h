//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"

// Implement the case where an `OPTIONAL` clause is joined with the empty
// pattern. Conceptually this is the same as an optional join with the neutral
// element, but specialized and more efficient.
class NeutralOptional : public Operation {
  std::shared_ptr<QueryExecutionTree> tree_;

 public:
  NeutralOptional(QueryExecutionContext* qec,
                  std::shared_ptr<QueryExecutionTree> tree);

 private:
  std::string getCacheKeyImpl() const override;
  uint64_t getSizeEstimateBeforeLimit() override;
  std::unique_ptr<Operation> cloneImpl() const override;
  Result computeResult(bool requestLaziness) override;
  VariableToColumnMap computeVariableToColumnMap() const override;

  // Return true, if `_limit` is configured in a way that will prevent the
  // neutral element from ever appearing in the result.
  bool singleRowCroppedByLimit() const;

 public:
  std::vector<QueryExecutionTree*> getChildren() override;
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;
  size_t getCostEstimate() override;
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;
  bool supportsLimit() const override;

 protected:
  std::vector<ColumnIndex> resultSortedOn() const override;
};
