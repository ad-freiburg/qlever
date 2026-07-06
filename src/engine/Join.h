// Copyright 2015 - 2026 The QLever Authors, in particular:

// 2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// 2018-2026 Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de), UFR
// 2026 Mark Veser (mark.veser87@gmail.com)

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_JOIN_H
#define QLEVER_SRC_ENGINE_JOIN_H

#include <memory>

// `Join` adds no members beyond the Pimpl handle, so all dependencies are
// pulled in transitively via the `Operation` base class.
#include "engine/Operation.h"

// This Operation implements ordinary joins with a single join column. It is
// implemented via the Pimpl idiom; for details see `JoinImpl.h`.

// Forward declaration of the impl class.
class JoinImpl;
class Join : public Operation {
 public:
  // `allowSwappingChildrenOnlyForTesting` should only ever be changed by tests.
  Join(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
       std::shared_ptr<QueryExecutionTree> t2, ColumnIndex t1JoinCol,
       ColumnIndex t2JoinCol, bool keepJoinColumn = true,
       bool allowSwappingChildrenOnlyForTesting = true);

  // Internal constructor used for the implementation of `clone`.
  Join(QueryExecutionContext* qec, std::unique_ptr<JoinImpl>&& impl);

  // Destructor and move operators.
  ~Join() override;
  Join(Join&&);
  Join& operator=(Join&&);

  // Virtual functions inherited from the 'Operation' base class.
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;
  std::vector<ColumnIndex> resultSortedOn() const override;
  size_t getCostEstimate() override;
  bool knownEmptyResult() override;
  float getMultiplicity(size_t col) override;
  std::vector<QueryExecutionTree*> getChildren() override;
  bool columnOriginatesFromGraphOrUndef(
      const Variable& variable) const override;
  std::string getCacheKeyImpl() const override;
  std::unique_ptr<Operation> cloneImpl() const override;
  Result computeResult(bool requestLaziness) override;

 private:
  std::unique_ptr<JoinImpl> impl_;
  VariableToColumnMap computeVariableToColumnMap() const override;
  uint64_t getSizeEstimateBeforeLimit() override;

  std::optional<std::shared_ptr<QueryExecutionTree>>
  makeTreeWithStrippedColumns(
      const std::set<Variable>& variables) const override;

  std::optional<std::shared_ptr<QueryExecutionTree>> makeTreeWithBindColumn(
      const parsedQuery::Bind& bind) const override;
};

#endif  // QLEVER_SRC_ENGINE_JOIN_H
