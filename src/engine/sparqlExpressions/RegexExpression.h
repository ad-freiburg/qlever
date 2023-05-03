//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include <string>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "re2/re2.h"

namespace sparqlExpression {
class RegexExpression : public SparqlExpression {
 private:
  SparqlExpression::Ptr child_;
  // If this variant holds a string, we consider this string as the prefix of a
  // prefix regex.
  std::variant<std::string, RE2> regex_;
  // The regex as a string, used for the cache key.
  std::string regexAsString_;

  // True if the STR() function is to be applied on the child before evaluating
  // the regex.
  bool childIsStrExpression_ = false;

 public:
  // `child` must be a `VariableExpression` and `regex` must be a
  // `LiteralExpression` that stores a string, else an exception will be thrown.
  RegexExpression(SparqlExpression::Ptr child, SparqlExpression::Ptr regex,
                  std::optional<SparqlExpression::Ptr> optionalFlags);

  ExpressionResult evaluate(EvaluationContext* context) const override;

  std::span<SparqlExpression::Ptr> children() override;

  // _________________________________________________________________________
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

  // _________________________________________________________________________
  [[nodiscard]] bool isPrefixExpression() const;

  // _________________________________________________________________________
  Estimates getEstimatesForFilterExpression(
      uint64_t inputSize,
      const std::optional<Variable>& firstSortedVariable) const override;

 private:
  // Internal implementations that are called by `evaluate`.
  ExpressionResult evaluatePrefixRegex(
      const Variable& variable,
      sparqlExpression::EvaluationContext* context) const;
  ExpressionResult evaluateNonPrefixRegex(
      const Variable& variable,
      sparqlExpression::EvaluationContext* context) const;
};
namespace detail {
// Check if `regex` is a prefix regex which means that it starts with `^` and
// contains no other "special" regex characters like `*` or `.`. If this check
// suceeds, the prefix is returned without the leading `^` and with all escaping
// undone. Else, `std::nullopt` is returned.
std::optional<std::string> getPrefixRegex(std::string regex);
}  // namespace detail
}  // namespace sparqlExpression
