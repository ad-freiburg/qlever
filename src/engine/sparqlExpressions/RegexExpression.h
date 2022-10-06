//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include <string>

#include "LiteralExpression.h"
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

  Estimates getEstimatesForFilterExpression(
      uint64_t inputSize,
      const std::optional<Variable>& firstSortedVariable) const override {
    if (isPrefixExpression()) {
      // Assume that only 10^-k entries remain, where k is the length of the
      // prefix. The reason for the -2 is that at this point, _rhs always
      // starts with ^"
      double reductionFactor = std::pow(
          10,
          std::max(0,
                   static_cast<int>(std::get<std::string>(regex_).size()) - 2));
      // Cap to reasonable minimal and maximal values to prevent numerical
      // stability problems.
      reductionFactor = std::min(100000000.0, reductionFactor);
      reductionFactor = std::max(1.0, reductionFactor);
      size_t sizeEstimate = inputSize / static_cast<size_t>(reductionFactor);
      auto varPtr = dynamic_cast<VariableExpression*>(child_.get());
      AD_CHECK(varPtr);
      size_t costEstimate = firstSortedVariable == varPtr->value()
                                ? sizeEstimate
                                : sizeEstimate + inputSize;

      return {sizeEstimate, costEstimate};
    }
    // TODO<joka921> Discuss with Hannah. Previously was "max()" which
    // definitely is not correct.
    size_t sizeEstimate = inputSize / 2;
    // We assume that checking a REGEX for an element is 10 times more
    // expensive than an "ordinary" filter check.
    size_t costEstimate = sizeEstimate + 10 * inputSize;

    return {sizeEstimate, costEstimate};
  }
};
namespace detail {
// Check if `regex` is a prefix regex which means that it starts with `^` and
// contains no other "special" regex characters like `*` or `.`. If this check
// suceeds, the prefix is returned without the leading `^` and with all escaping
// undone. Else, `std::nullopt` is returned.
std::optional<std::string> getPrefixRegex(std::string regex);
}  // namespace detail
}  // namespace sparqlExpression
