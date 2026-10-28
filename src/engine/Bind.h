//  Copyright 2020, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_BIND_H
#define QLEVER_SRC_ENGINE_BIND_H

#include "engine/Operation.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/ParsedQuery.h"
#include "util/ContainersWithAllocator.h"

// BIND operation.
class Bind : public Operation {
 public:
  static constexpr size_t CHUNK_SIZE = 10'000;

  // Construct a `BIND` that evaluates `b` on the result of `subtree`. If
  // `onlyIfBound` is given, it must be a variable of `subtree`, and the target
  // of the `BIND` is UNDEF for all rows where that variable is UNDEF.
  //
  // NOTE: The query planner uses `onlyIfBound` when it moves a `BIND` from
  // the end of an OPTIONAL group above the `OptionalJoin`, see
  // `QueryPlanner::createJoinCandidates`. The variable is then one that is
  // always defined when the optional pattern matches, so the target stays
  // UNDEF exactly for the rows without a match, as if the `BIND` had been
  // evaluated inside the group.
  Bind(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree,
       parsedQuery::Bind b, std::optional<Variable> onlyIfBound = std::nullopt);

 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  parsedQuery::Bind _bind;
  std::optional<Variable> onlyIfBound_;
  // For the documentation of the overridden members, see Operation.h
 protected:
  [[nodiscard]] std::string getCacheKeyImpl() const override;

 public:
  const parsedQuery::Bind& bind() const { return _bind; }
  const std::shared_ptr<QueryExecutionTree>& getSubtree() const {
    return _subtree;
  }
  const std::optional<Variable>& onlyIfBound() const { return onlyIfBound_; }
  [[nodiscard]] std::string getDescriptor() const override;
  [[nodiscard]] size_t getResultWidth() const override;

 private:
  qlm::vector<QueryExecutionTree*> getChildrenImpl() const override;

 public:
  size_t getCostEstimate() override;
  LimitOffsetHandling handlesLimitOffset() const override;
  void onLimitOffsetChanged(const LimitOffsetClause& limitOffset,
                            bool childrenAreExclusivelyOwned) override;

  // `BIND` needs to be able to push down other `BIND`s if the query contains
  // multiple `BIND`s of which only some can be rewritten.
  std::optional<std::shared_ptr<QueryExecutionTree>> makeTreeWithBindColumn(
      const parsedQuery::Bind& bind) const override;
  std::unique_ptr<Operation> cloneWithNewChildren(
      std::vector<std::shared_ptr<QueryExecutionTree>> children) const override;

 private:
  [[nodiscard]] bool isDeterministicImpl() const override;
  std::unique_ptr<Operation> cloneImpl() const override;
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

 protected:
  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const override;

 private:
  Result computeResult(bool requestLaziness) override;

  static IdTable cloneSubView(const IdTableView<0>& idTable,
                              const std::pair<size_t, size_t>& subrange);

  // Implementation for the binding of arbitrary expressions.
  IdTable computeExpressionBind(
      LocalVocab* localVocab, IdTable idTable,
      const sparqlExpression::SparqlExpression* expression) const;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;
};

#endif  // QLEVER_SRC_ENGINE_BIND_H
