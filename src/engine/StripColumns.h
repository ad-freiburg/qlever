// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR/QL
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// QL =  QLeverize AG

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_STRIPCOLUMNS_H
#define QLEVER_SRC_ENGINE_STRIPCOLUMNS_H

#include <set>

#include "engine/Operation.h"

// An Operation that returns the result of its only child operation when being
// evaluated, with only a subset of the child's variables.
class StripColumns : public Operation {
 private:
  // The child operation.
  std::shared_ptr<QueryExecutionTree> child_;
  // The subset of the childrens' columns that are to be kept.
  std::vector<ColumnIndex> subset_;
  // The mapping from variables to columns.
  VariableToColumnMap varToCol_;

 public:
  // Construct from a child operation and the set of variables that are to be
  // preserved by this operation. We deliberately use `std::set` to make the
  // order deterministic for easier testing and caching.
  StripColumns(QueryExecutionContext* ctx,
               std::shared_ptr<QueryExecutionTree> child,
               const std::set<Variable>& keepVariables);

  // The constructor needed for cloning.
  StripColumns(QueryExecutionContext* ctx,
               std::shared_ptr<QueryExecutionTree> child,
               std::vector<ColumnIndex> subset, VariableToColumnMap varToCol);

  // Member functions inherited from `Operation` that have to be implemented by
  // each child class.
  std::vector<QueryExecutionTree*> getChildren() override;
  std::string getCacheKeyImpl() const override;
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;

  size_t getCostEstimate() override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

 private:
  std::unique_ptr<Operation> cloneImpl() const override;
  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const override;
  Result computeResult(bool requestLaziness) override;
  VariableToColumnMap computeVariableToColumnMap() const override;
};

#endif  // QLEVER_SRC_ENGINE_STRIPCOLUMNS_H
