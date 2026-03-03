// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_GROUPBY_H
#define QLEVER_SRC_ENGINE_GROUPBY_H

#include <memory>
#include <optional>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "parser/Alias.h"

// This is a class that follows the PIMPL idiom and wraps the actual
// `GroupByImpl` class. It exposes only the constructors and virtual member
// functions. Forward declaration of the impl class
class GroupByImpl;
class GroupBy : public Operation {
 public:
  // Constructors.
  GroupBy(QueryExecutionContext* qec, std::vector<Variable> groupByVariables,
          std::vector<Alias> aliases,
          std::shared_ptr<QueryExecutionTree> subtree);

  ~GroupBy() override;
  GroupBy(GroupBy&&);
  GroupBy& operator=(GroupBy&&);

  // Internal constructor used for the implementation of `clone`.
  GroupBy(QueryExecutionContext* qec, std::unique_ptr<GroupByImpl>&& impl);

  // Virtual functions inherited from the `Operation` base class.
  std::string getDescriptor() const override;
  std::string getCacheKeyImpl() const override;
  size_t getResultWidth() const override;
  std::vector<ColumnIndex> resultSortedOn() const override;
  bool knownEmptyResult() override;
  float getMultiplicity(size_t col) override;
  uint64_t getSizeEstimateBeforeLimit() override;
  size_t getCostEstimate() override;
  std::vector<QueryExecutionTree*> getChildren() override;
  VariableToColumnMap computeVariableToColumnMap() const override;
  Result computeResult(bool requestLaziness) override;
  std::unique_ptr<Operation> cloneImpl() const override;

  // Getters for testing.
  const std::vector<Variable>& groupByVariables() const;
  const std::vector<Alias>& aliases() const;

  // Get access to the implementation. Can only be used if the (more expensive)
  // `GroupByImpl.h` header is included. Can be used to get access to internal
  // functions and data members, for example in unit tests.
  const GroupByImpl& getImpl() const;
  GroupByImpl& getImpl();

 private:
  std::unique_ptr<GroupByImpl> _impl;
};

#endif  // QLEVER_SRC_ENGINE_GROUPBY_H
