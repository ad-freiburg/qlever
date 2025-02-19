// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include <string>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "re2/re2.h"

namespace sparqlExpression {
// Class implementing the REGEX function, which takes two mandatory arguments
// (an expression and a regex) and one optional argument (a string of flags).
class RegexExpression : public SparqlExpression {
 private:
  SparqlExpression::Ptr child_;
  // The reguar expression. It needs to be a `std::optional` because `RE2`
  // objects do not have a default constructor.
  std::optional<RE2> regex_;
  // If this `std::optional` holds a string, we have a simple prefix regex
  // (which translates to a range search) and this string holds the prefix.
  std::optional<std::string> prefixRegex_;
  // The regex as a string, used for the cache key.
  std::string regexAsString_;

  // True iff the expression is enclosed in `STR()`.
  bool childIsStrExpression_ = false;

 public:
  // The `child` must be a `VariableExpression` and `regex` must be a
  // `LiteralExpression` that stores a string, otherwise an exception will be
  // thrown.
  RegexExpression(SparqlExpression::Ptr child, SparqlExpression::Ptr regex,
                  std::optional<SparqlExpression::Ptr> optionalFlags);

  ExpressionResult evaluate(EvaluationContext* context) const override;

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
  std::span<SparqlExpression::Ptr> childrenImpl() override;

  // Evaluate for the special case, where the expression is a variable and we
  // have a simple prefix regex (in which case the regex match translates to a
  // simple range check).
  ExpressionResult evaluatePrefixRegex(
      const Variable& variable,
      sparqlExpression::EvaluationContext* context) const;

  // Evaluate for the general case.
  CPP_template(typename T)(requires SingleExpressionResult<T>) ExpressionResult
      evaluateGeneralCase(T&& input,
                          sparqlExpression::EvaluationContext* context) const;

  // Check if the `CancellationHandle` of `context` has been cancelled and throw
  // an exception if this is the case.
  static void checkCancellation(
      const sparqlExpression::EvaluationContext* context,
      ad_utility::source_location location =
          ad_utility::source_location::current());
};
namespace detail {
// Check if `regex` is a prefix regex which means that it starts with `^` and
// contains no other "special" regex characters like `*` or `.`. If this check
// succeeds, the prefix is returned without the leading `^` and with all
// escaping undone. Else, `std::nullopt` is returned.
std::optional<std::string> getPrefixRegex(std::string regex);
}  // namespace detail
}  // namespace sparqlExpression
