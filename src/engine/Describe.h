// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"
#include "parser/GraphPatternOperation.h"

class Describe : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> subtree_;
  parsedQuery::Describe describe_;

 public:
  Describe(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree,
           parsedQuery::Describe describe);

  // Getter for testing.
  const auto& getDescribe() const {return describe_;}

  std::vector<QueryExecutionTree*> getChildren() override;

  string getCacheKeyImpl() const override;

 public:
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

  // TODO<joka921> Comment.
  void recursivelyAddBlankNodes(
      IdTable& finalResult, ad_utility::HashSetWithMemoryLimit<Id>& alreadySeen,
      IdTable blankNodes);
};
