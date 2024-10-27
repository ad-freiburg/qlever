//  Copyright 2020, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/ParsedQuery.h"

/// BIND operation, currently only supports a very limited subset of expressions
class Bind : public Operation {
 public:
  static constexpr size_t CHUNK_SIZE = 10'000;

  Bind(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree,
       parsedQuery::Bind b)
      : Operation(qec), _subtree(std::move(subtree)), _bind(std::move(b)) {}

 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  parsedQuery::Bind _bind;
  // For the documentation of the overridden members, see Operation.h
 protected:
  [[nodiscard]] string getCacheKeyImpl() const override;

 public:
  const parsedQuery::Bind& bind() const { return _bind; }
  [[nodiscard]] string getDescriptor() const override;
  [[nodiscard]] size_t getResultWidth() const override;
  std::vector<QueryExecutionTree*> getChildren() override;
  size_t getCostEstimate() override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

 protected:
  [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override;

 private:
  ProtoResult computeResult(bool requestLaziness) override;

  static IdTable cloneSubView(const IdTable& idTable,
                              const std::pair<size_t, size_t>& subrange);

  // Implementation for the binding of arbitrary expressions.
  IdTable computeExpressionBind(
      LocalVocab* localVocab, IdTable idTable,
      const sparqlExpression::SparqlExpression* expression) const;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;
};
