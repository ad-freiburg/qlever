// Copyright 2022 - 2026 The QLever Authors, in particular:
//
// 2022 - 2024 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2024 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_PREFIXMATCHEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_PREFIXMATCHEXPRESSION_H

#include <gtest/gtest_prod.h>

#include <string>

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {
// Class implementing the fast, but inaccurate `ql:prefix-match` function. It
// does not evaluate an actual regex, but performs a range search on the sorted
// vocabulary (which is case-insensitive and works on the primary collation
// level). This is only used for the dedicated `ql:prefix-match` function; the
// standard `REGEX` function always evaluates the real regex (see
// `makeRegexExpression`).
class PrefixMatchExpression : public SparqlExpression {
 private:
  Ptr child_;
  // The prefix that the value of `child_` has to start with.
  std::string prefix_;
  // Holds the variable over which the prefix match is evaluated.
  Variable variable_;
  // If the variable is wrapped inside a `STR()` function, this is set to true.
  bool childIsStrExpression_ = false;

 public:
  // The `child` must be a `VariableExpression` (optionally wrapped in `STR()`).
  PrefixMatchExpression(Ptr child, std::string prefix, Variable variable);
  PrefixMatchExpression(PrefixMatchExpression&&) = default;
  PrefixMatchExpression& operator=(PrefixMatchExpression&&) = default;
  PrefixMatchExpression(const PrefixMatchExpression&) = delete;
  PrefixMatchExpression& operator=(const PrefixMatchExpression&) = delete;

  std::vector<PrefilterExprVariablePair> getPrefilterExpressionForMetadata(
      const LocalVocabContext& context, bool isNegated) const override;

  // ___________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // ___________________________________________________________________________
  [[nodiscard]] std::string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

  // ___________________________________________________________________________
  Estimates getEstimatesForFilterExpression(
      uint64_t inputSize,
      const std::optional<Variable>& firstSortedVariable) const override;

  [[nodiscard]] bool isDeterministic() const override {
    return child_->isDeterministic();
  }

 private:
  ql::span<Ptr> childrenImpl() override;

  // Check if the `CancellationHandle` of `context` has been cancelled and throw
  // an exception if this is the case.
  static void checkCancellation(
      const EvaluationContext* context,
      ad_utility::source_location location = AD_CURRENT_SOURCE_LOC());

  FRIEND_TEST(PrefixMatchExpression, makePrefixMatchExpression);
};

// Make a custom `ql:prefix-match` expression which allows for efficient
// prefix search.
SparqlExpression::Ptr makePrefixMatchExpression(
    SparqlExpression::Ptr string, const SparqlExpression::Ptr& prefix);
}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_PREFIXMATCHEXPRESSION_H
