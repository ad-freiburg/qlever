//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"

class NeutralOptional : public Operation {
  std::shared_ptr<QueryExecutionTree> tree_;

 public:
  std::vector<QueryExecutionTree*> getChildren() override;

  NeutralOptional(QueryExecutionContext* qec,
                  std::shared_ptr<QueryExecutionTree> tree);

 private:
  std::string getCacheKeyImpl() const override;

 public:
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;
  size_t getCostEstimate() override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;
  bool supportsLimit() const override;

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

 protected:
  std::vector<ColumnIndex> resultSortedOn() const override;

 private:
  ProtoResult computeResult(bool requestLaziness) override;
  VariableToColumnMap computeVariableToColumnMap() const override;
};
