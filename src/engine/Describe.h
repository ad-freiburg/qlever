// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"

class Describe : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> subtree_;
  parsedQuery::Describe describe_;

 public:
  // TODO<joka921> The column should rather be a variable etc.
  Describe(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree,
           parsedQuery::Describe describe);

  std::vector<QueryExecutionTree*> getChildren() override;
  // The individual implementation of `getCacheKey` (see above) that has to be
  // customized by every child class.
  string getCacheKeyImpl() const override;

 public:
  // Gets a very short (one line without line ending) descriptor string for
  // this Operation.  This string is used in the RuntimeInformation
  string getDescriptor() const override;
  size_t getResultWidth() const override;

  size_t getCostEstimate() override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

 private:
  [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override;
  ProtoResult computeResult(bool requestLaziness) override;
  // Compute the variable to column index mapping. Is used internally by
  // `getInternallyVisibleVariableColumns`.
  VariableToColumnMap computeVariableToColumnMap() const override;
};
