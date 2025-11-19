//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SAMPLEEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SAMPLEEXPRESSION_H

#include <absl/strings/str_cat.h>

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {
// The (SAMPLE(?x) AS ?sample) expression
class SampleExpression : public SparqlExpression {
 public:
  // The first parameter is the `DISTINCT` bool that is always there on the
  // constructor for all aggregate expressions. For this particular expression
  // it doesn't make a difference, so it is ignored.
  SampleExpression(bool, Ptr&& child) : _child{std::move(child)} {
    setIsInsideAggregate();
  }

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // __________________________________________________________________________
  std::string getCacheKey(const VariableToColumnMap& varColMap) const override {
    return absl::StrCat("SAMPLE(", _child->getCacheKey(varColMap), ")");
  }

  // __________________________________________________________________________
  ql::span<Ptr> childrenImpl() override { return {&_child, 1}; }

  // SAMPLE is an aggregate, the distinctness doesn't matter, so we return "not
  // distinct", because that allows for more efficient implementations in GROUP
  // BY.
  AggregateStatus isAggregate() const override {
    return AggregateStatus::NonDistinctAggregate;
  }

 private:
  Ptr _child;
};
}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SAMPLEEXPRESSION_H
