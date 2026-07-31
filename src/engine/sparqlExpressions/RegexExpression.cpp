// Copyright 2022 - 2026 The QLever Authors, in particular:
//
// 2022 - 2024 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2024 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/sparqlExpressions/RegexExpression.h"

#include <re2/re2.h>

#include <algorithm>
#include <cmath>
#include <utility>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/RegexHelpers.h"
#include "engine/sparqlExpressions/SimpleLiteralHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "engine/sparqlExpressions/StringExpressionsHelper.h"

using namespace std::literals;

namespace sparqlExpression::detail {

// _____________________________________________________________________________
void ensureIsValidRegexIfConstant(const SparqlExpression& expression) {
  const auto* stringLiteralExpression =
      dynamic_cast<const StringLiteralExpression*>(&expression);
  if (stringLiteralExpression) {
    const auto& literal = stringLiteralExpression->value();
    const auto& string = asStringViewUnsafe(literal.getContent());
    RE2 regex{string, RE2::Quiet};
    if (!regex.ok()) {
      throw std::runtime_error{absl::StrCat(
          "The regex \"", string,
          "\" is not supported by QLever (which uses Google's RE2 library); "
          "the error from RE2 is: ",
          regex.error())};
    }
  }
}

// _____________________________________________________________________________
void ensureIsValidFlagIfConstant(const SparqlExpression& expression) {
  const auto* stringLiteralExpression =
      dynamic_cast<const StringLiteralExpression*>(&expression);
  if (stringLiteralExpression) {
    const auto& literal = stringLiteralExpression->value();
    const auto& string = asStringViewUnsafe(literal.getContent());
    auto firstInvalidFlag = string.find_first_not_of("imsU");
    if (firstInvalidFlag != std::string::npos) {
      throw std::runtime_error{absl::StrCat(
          "Invalid regex flag '", string.substr(firstInvalidFlag, 1),
          "' found in \"", string,
          "\". The only supported flags are 'i', 'm', 's', 'U', and any "
          "combination of them")};
    }
  }
}

// _____________________________________________________________________________
struct RegexImpl {
  Id operator()(const std::optional<std::string>& input,
                const std::shared_ptr<RE2>& pattern) const {
    if (!input.has_value() || !pattern) {
      return Id::makeUndefined();
    }
    // Check for invalid regexes.
    if (!pattern->ok()) {
      return Id::makeUndefined();
    }
    return Id::makeFromBool(RE2::PartialMatch(input.value(), *pattern));
  }
};

// The standard `REGEX` expression. It always evaluates the actual regex (via
// Google's RE2 library) by delegating to the string-expression machinery.
using RegexExpressionBase =
    string_expressions::StringExpressionImpl<2, RegexImpl, RegexValueGetter>;

// A `RegexExpressionBase` that additionally supports prefiltering when the
// regex is a prefix regex (e.g. `^prefix`) applied to a plain variable. The
// prefilter only restricts the blocks that are scanned; the actual regex is
// still evaluated on the remaining rows by the base class.
class RegexExpression : public RegexExpressionBase {
 private:
  // The variable and the guaranteed literal prefix (see
  // `getLiteralPrefixOfRegex`) if the regex is a prefix regex on a plain
  // variable, `std::nullopt` otherwise.
  std::optional<std::pair<Variable, std::string>> prefix_;

 public:
  RegexExpression(Ptr child, Ptr regex,
                  std::optional<std::pair<Variable, std::string>> prefix)
      : RegexExpressionBase(std::move(child), std::move(regex)),
        prefix_{std::move(prefix)} {}

  std::vector<PrefilterExprVariablePair> getPrefilterExpressionForMetadata(
      [[maybe_unused]] const LocalVocabContext& context,
      [[maybe_unused]] bool isNegated) const override {
    if (!prefix_.has_value()) {
      return {};
    }
    std::vector<PrefilterExprVariablePair> prefilterVec;
    prefilterVec.emplace_back(
        std::make_unique<prefilterExpressions::PrefixRegexExpression>(
            TripleComponent::Literal::literalWithNormalizedContent(
                asNormalizedStringViewUnsafe(prefix_->second))),
        prefix_->first);
    return prefilterVec;
  }

  // If we know a guaranteed prefix, assume that only 10^-k entries remain,
  // where k is the length of that prefix, and cap to reasonable maximal values
  // to prevent numerical stability problems. Note that unlike for
  // `PrefixMatchExpression`, the actual regex has to be evaluated for each of
  // the input rows, so the cost always includes the full input size (even if
  // the input is sorted by the variable).
  Estimates getEstimatesForFilterExpression(
      uint64_t inputSize,
      const std::optional<Variable>& firstSortedVariable) const override {
    if (!prefix_.has_value()) {
      return RegexExpressionBase::getEstimatesForFilterExpression(
          inputSize, firstSortedVariable);
    }
    double reductionFactor =
        std::pow(10, std::min<size_t>(8, prefix_->second.size()));
    size_t sizeEstimate = inputSize / static_cast<size_t>(reductionFactor);
    return {sizeEstimate, sizeEstimate + inputSize};
  }
};

// If `string` is a plain variable and `regex` is a constant regex with a
// guaranteed literal prefix (see `getLiteralPrefixOfRegex`), return the
// variable and that prefix, which enables prefiltering. Return `std::nullopt`
// otherwise.
//
// Note: Prefiltering `STR(?var)` is deliberately not supported, since we would
// not only have to match "Bob", but also "Bob"@en, "Bob"^^<iri>, and so on. The
// current prefilter expressions do not consider this matching logic.
std::optional<std::pair<Variable, std::string>> getRegexPrefilterInfo(
    const SparqlExpression& string, const SparqlExpression& regex) {
  bool childIsStrExpression = string.isStrExpression();
  const auto* variableExpression = dynamic_cast<const VariableExpression*>(
      childIsStrExpression ? string.children()[0].get() : &string);
  const auto* stringLiteralExpression =
      dynamic_cast<const StringLiteralExpression*>(&regex);
  if (!variableExpression || !stringLiteralExpression || childIsStrExpression) {
    return std::nullopt;
  }
  std::string prefix = getLiteralPrefixOfRegex(
      asStringViewUnsafe(stringLiteralExpression->value().getContent()));
  // An empty prefix would not restrict the scanned blocks at all, so there is
  // no point in adding a prefilter for it.
  if (prefix.empty()) {
    return std::nullopt;
  }
  return std::pair{variableExpression->value(), std::move(prefix)};
}

}  // namespace sparqlExpression::detail

namespace sparqlExpression {

// _____________________________________________________________________________
SparqlExpression::Ptr makeRegexExpression(SparqlExpression::Ptr string,
                                          SparqlExpression::Ptr regex,
                                          SparqlExpression::Ptr flags) {
  // The pattern has to be a simple literal, no matter what the other arguments
  // look like.
  if (const auto* regexLiteralExpression =
          dynamic_cast<const StringLiteralExpression*>(regex.get())) {
    detail::ensureIsSimpleLiteral(regexLiteralExpression->value(), "REGEX");
  }
  detail::ensureIsValidRegexIfConstant(*regex);
  if (flags) {
    if (auto* stringLiteralExpression =
            dynamic_cast<const StringLiteralExpression*>(flags.get())) {
      detail::ensureIsSimpleLiteral(stringLiteralExpression->value(), "REGEX");
    }
    detail::ensureIsValidFlagIfConstant(*flags);
    // Merge the flags into the regex. The result is no longer a plain string
    // literal, so a regex with flags will never allow prefiltering (see
    // `getRegexPrefilterInfo`).
    regex = makeMergeRegexPatternAndFlagsExpression(std::move(regex),
                                                    std::move(flags));
  }
  // Compute the prefilter information (if the regex is a prefix regex) before
  // moving the arguments into the expression. The actual regex is always
  // evaluated by `RegexExpression`; the prefilter only restricts the scanned
  // blocks.
  auto prefilterInfo = detail::getRegexPrefilterInfo(*string, *regex);
  return std::make_unique<detail::RegexExpression>(
      std::move(string), std::move(regex), std::move(prefilterInfo));
}

}  // namespace sparqlExpression
