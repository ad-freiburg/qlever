// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_REGEXEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_REGEXEXPRESSION_H

#include <gtest/gtest_prod.h>
#include <re2/re2.h>

#include <string>

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {
// Class implementing a specialization of the REGEX function. This optimization
// is possible if the regex is known in advance and is a simple prefix regex.
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
  PrefixRegexExpression(PrefixRegexExpression&&) noexcept = default;
  PrefixRegexExpression& operator=(PrefixRegexExpression&&) noexcept = default;
  PrefixRegexExpression(const PrefixRegexExpression&) noexcept = delete;
  PrefixRegexExpression& operator=(const PrefixRegexExpression&) noexcept =
      delete;

  std::vector<PrefilterExprVariablePair> getPrefilterExpressionForMetadata(
      bool isNegated) const override;

  // Check if the children of this expression allow for the prefix regex
  // optimization. If this is the case, a `PrefixRegexExpression` is returned,
  // otherwise `std::nullopt`.
  static std::optional<PrefixRegexExpression>
  makePrefixRegexExpressionIfPossible(Ptr& string,
                                      const SparqlExpression& regex);

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
  ql::span<Ptr> childrenImpl() override;

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

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_REGEXEXPRESSION_H
