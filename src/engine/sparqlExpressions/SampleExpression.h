//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 15.09.21.
//

#ifndef QLEVER_SAMPLEEXPRESSION_H
#define QLEVER_SAMPLEEXPRESSION_H

#include "./SparqlExpression.h"

namespace sparqlExpression {
/// The (SAMPLE(?x) as ?sample) expression
class SampleExpression : public SparqlExpression {
 public:
  SampleExpression([[maybe_unused]] bool distinct, Ptr&& child)
      : _child{std::move(child)} {}

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // _____________________________________________________________________
  vector<std::string> getUnaggregatedVariables() override {
    // This is an aggregation, so it never leaves any unaggregated variables.
    return {};
  }

  // __________________________________________________________________________
  string getCacheKey(const VariableToColumnMap& varColMap) const override {
    return "SAMPLE("s + _child->getCacheKey(varColMap) + ")";
  }

  // __________________________________________________________________________
  std::span<Ptr> children() override { return {&_child, 1}; }

 private:
  Ptr _child;
};
}  // namespace sparqlExpression

#endif  // QLEVER_SAMPLEEXPRESSION_H
