//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SAMPLEEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SAMPLEEXPRESSION_H

#include "./SparqlExpression.h"
#include "absl/strings/str_cat.h"

namespace sparqlExpression {
/// The (SAMPLE(?x) as ?sample) expression
class SampleExpression : public SparqlExpression {
 public:
  SampleExpression([[maybe_unused]] bool distinct, Ptr&& child)
      : _child{std::move(child)} {
    setIsInsideAggregate();
  }

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // __________________________________________________________________________
  string getCacheKey(const VariableToColumnMap& varColMap) const override {
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
