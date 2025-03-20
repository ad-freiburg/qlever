// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include <gtest/gtest_prod.h>
#include <re2/re2.h>

#include <string>

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {
// Class implementing the REGEX function, which takes two mandatory arguments
// (an expression and a regex) and one optional argument (a string of flags).
class PrefixRegexExpression : public SparqlExpression {
 private:
  Ptr child_;
  // A simple prefix regex (which translates to a range search) and this string
  // holds the prefix.
  std::string prefixRegex_;
  // Holds the variable over which the regex is evaluated.
  Variable variable_;
  // If the variable is wrapped inside a `STR()` function, this is set to true.
  bool childIsStrExpression_ = false;

  // The `child` must be a `VariableExpression` and `regex` must be a
  // `LiteralExpression` that stores a string, otherwise an exception will be
  // thrown.
  PrefixRegexExpression(Ptr child, std::string prefixRegex, Variable variable);

 public:
  PrefixRegexExpression(PrefixRegexExpression&&) = default;
  PrefixRegexExpression& operator=(PrefixRegexExpression&&) = default;
  PrefixRegexExpression(const PrefixRegexExpression&) = delete;
  PrefixRegexExpression& operator=(const PrefixRegexExpression&) = delete;

  // ___________________________________________________________________________
  static std::optional<PrefixRegexExpression>
  makePrefixRegexExpressionIfPossible(Ptr& string, const Ptr& regex);

  // ___________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // ___________________________________________________________________________
  [[nodiscard]] std::string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

  // ___________________________________________________________________________
  Estimates getEstimatesForFilterExpression(
      uint64_t inputSize,
      const std::optional<Variable>& firstSortedVariable) const override;

 private:
  std::span<Ptr> childrenImpl() override;

  // Check if the `CancellationHandle` of `context` has been cancelled and throw
  // an exception if this is the case.
  static void checkCancellation(const EvaluationContext* context,
                                ad_utility::source_location location =
                                    ad_utility::source_location::current());

  // Check if `regex` is a prefix regex which means that it starts with `^` and
  // contains no other "special" regex characters like `*` or `.`. If this check
  // succeeds, the prefix is returned without the leading `^` and with all
  // escaping undone. Else, `std::nullopt` is returned.
  static std::optional<std::string> getPrefixRegex(std::string regex);

  FRIEND_TEST(RegexExpression, getPrefixRegex);
};

SparqlExpression::Ptr makeRegexExpression(SparqlExpression::Ptr string,
                                          SparqlExpression::Ptr regex,
                                          SparqlExpression::Ptr flags);
}  // namespace sparqlExpression
