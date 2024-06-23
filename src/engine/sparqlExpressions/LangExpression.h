//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

// Test file for the LangExpression class: LanguageExpressionTests.h

#pragma once

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {
// A dummy implementation for expressions of the form `LANG(?variable)`. This
// expression cannot be evaluated (almost all methods will throw an assertion
// failure), but is only used during parsing to recognize the special pattern
// `FILTER (LANG(?variable) = "lang")` which is then transformed to QLever's
// special language filter implementation using special predicates.
class LangExpression : public SparqlExpression {
 private:
  // The stored variable.
  Variable variable_;
  SparqlExpression::Ptr child_;

 public:
  // Construct from a child expression. The child must be a single variable,
  // otherwise an exception will be thrown.
  LangExpression(SparqlExpression::Ptr child);

  const Variable& variable() const { return variable_; }

  bool containsLangExpression() const override { return true; }

  ExpressionResult evaluate(EvaluationContext* context) const override;

  std::string getCacheKey(const VariableToColumnMap&) const override;

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override;
  static void checkCancellation(
      const sparqlExpression::EvaluationContext* context,
      ad_utility::source_location location =
          ad_utility::source_location::current());
};
}  // namespace sparqlExpression
