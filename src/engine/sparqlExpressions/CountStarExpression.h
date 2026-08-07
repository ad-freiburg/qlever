//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_COUNTSTAREXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_COUNTSTAREXPRESSION_H

#include "engine/sparqlExpressions/SparqlExpression.h"

// Return a `SparqlExpression::Ptr` that implements the `COUNT(*)` and
// `COUNT(DISTINCT *)` function.
namespace sparqlExpression {
class CountStarExpression : public SparqlExpression {
 private:
  bool distinct_;

 public:
  // ___________________________________________________________________________
  explicit CountStarExpression(bool distinct);

  // ___________________________________________________________________________
  ExpressionResult evaluate(
      sparqlExpression::EvaluationContext* ctx) const override;

  // COUNT * technically is an aggregate.
  AggregateStatus isAggregate() const override;

  // A `COUNT(DISTINCT *)` implicitly reads all the visible columns of its
  // input, so the machinery that strips unused columns has to know about it.
  // A plain `COUNT(*)` only needs the number of rows, which is unaffected by
  // stripping columns.
  bool readsAllVisibleColumns() const override { return distinct_; }

  // ___________________________________________________________________________
  std::string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override;

  // ___________________________________________________________________________
  ql::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }

  [[nodiscard]] bool isDeterministic() const override { return true; }

  bool isDistinct() const { return distinct_; }
};

SparqlExpression::Ptr makeCountStarExpression(bool distinct);
}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_COUNTSTAREXPRESSION_H
