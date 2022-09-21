//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "gtest/gtest.h"

#pragma once

namespace sparqlExpression {
/// Dummy expression for testing, that for `evaluate` returns the `result`
/// that is specified in the constructor.
struct DummyExpression : public SparqlExpression {
  explicit DummyExpression(ExpressionResult result)
      : _result{std::move(result)} {}
  mutable ExpressionResult _result;
  ExpressionResult evaluate(EvaluationContext*) const override {
    return std::move(_result);
  }
  vector<std::string> getUnaggregatedVariables() override { return {}; }
  string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    return "DummyDummyDummDumm";
  }

  std::span<SparqlExpression::Ptr> children() override { return {}; }
};
}  // namespace sparqlExpression

// Add output for `SetOfIntervals for Gtest.
namespace ad_utility {
void PrintTo(const SetOfIntervals& set, std::ostream* os) {
  for (auto [first, second] : set._intervals) {
    *os << '{' << first << ", " << second << '}';
  }
}
}  // namespace ad_utility
