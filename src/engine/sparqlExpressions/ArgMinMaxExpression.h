// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_ARGMINMAXEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_ARGMINMAXEXPRESSION_H

#include <array>

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "global/ValueIdComparators.h"

namespace sparqlExpression {

// The aggregates `ql:argmin(?x, ?v)` and `ql:argmax(?x, ?v)`, which return the
// value of `?x` in the first row of the group where `?v` is minimal resp.
// maximal. Rows where `?v` is UNDEF are skipped.
//
// NOTE: The order of the bound keys is the one of `MIN` resp. `MAX` (see
// `MinMaxForAllTypes`).
template <valueIdComparators::Comparison comparison>
class ArgMinMaxExpression : public SparqlExpression {
 public:
  ArgMinMaxExpression(Ptr&& value, Ptr&& key)
      : children_{std::move(value), std::move(key)} {
    setIsInsideAggregate();
  }

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // __________________________________________________________________________
  std::string getCacheKey(const VariableToColumnMap& varColMap) const override;

  // __________________________________________________________________________
  ql::span<Ptr> childrenImpl() override { return children_; }

  // __________________________________________________________________________
  [[nodiscard]] bool isDeterministic() const override {
    return children_[0]->isDeterministic() && children_[1]->isDeterministic();
  }

  // __________________________________________________________________________
  AggregateStatus isAggregate() const override {
    return AggregateStatus::NonDistinctAggregate;
  }

 private:
  // The `value` (`?x`) and the `key` (`?v`).
  std::array<Ptr, 2> children_;
};

using ArgMinExpression =
    ArgMinMaxExpression<valueIdComparators::Comparison::LT>;
using ArgMaxExpression =
    ArgMinMaxExpression<valueIdComparators::Comparison::GT>;

}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_ARGMINMAXEXPRESSION_H
