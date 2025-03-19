// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include <re2/re2.h>

#include <string>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {
// Class implementing the REGEX function, which takes two mandatory arguments
// (an expression and a regex) and one optional argument (a string of flags).
class RegexExpression : public SparqlExpression {
 private:
  // Array to both children of this expression. The first child represent the
  // string to be matched. The second child represents the regex, it is null if
  // the regex can be precomputed, in which case the regex is stored in the
  // member variable `regex_`.
  std::array<Ptr, 2> children_;
  // The regular expression if precomputed.
  std::optional<RE2> regex_;
  // If this `std::optional` holds a string, we have a simple prefix regex
  // (which translates to a range search) and this string holds the prefix.
  std::optional<std::string> prefixRegex_;

  // True iff the expression is enclosed in `STR()`.
  bool childIsStrExpression_ = false;

 public:
  // The `child` must be a `VariableExpression` and `regex` must be a
  // `LiteralExpression` that stores a string, otherwise an exception will be
  // thrown.
  RegexExpression(Ptr child, Ptr regex, std::optional<Ptr> optionalFlags);

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
  std::span<Ptr> childrenImpl() override;

  // Evaluate for the special case, where the expression is a variable and we
  // have a simple prefix regex (in which case the regex match translates to a
  // simple range check).
  ExpressionResult evaluatePrefixRegex(const Variable& variable,
                                       EvaluationContext* context) const;

  // Evaluate for the general case.
  CPP_template(typename T,
               typename F)(requires SingleExpressionResult<T>) ExpressionResult
      evaluateGeneralCase(T&& input, EvaluationContext* context,
                          F getNextRegex) const;

  // Check if the `CancellationHandle` of `context` has been cancelled and throw
  // an exception if this is the case.
  static void checkCancellation(const EvaluationContext* context,
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
