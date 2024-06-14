//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

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

 public:
  // Construct from a child expression. The child must be a single variable,
  // otherwise an exception will be thrown.
  LangExpression(SparqlExpression::Ptr child);

  const Variable& variable() const { return variable_; }

  bool containsLangExpression() const override { return true; }

  // The following methods are required for the virtual interface of
  // `SparqlExpression`, but they will always fail at runtime when executed. All
  // occurrences of `LanguageExpression` should be detected and dealt with by
  // the parser before any of these methods is ever called.
  ExpressionResult evaluate(EvaluationContext*) const override { AD_FAIL(); }

  std::string getCacheKey(const VariableToColumnMap&) const override {
    AD_FAIL();
  }

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override { AD_FAIL(); }
};
}  // namespace sparqlExpression
