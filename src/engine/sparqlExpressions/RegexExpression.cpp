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

#include <absl/strings/str_replace.h>
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

// Return the content of `expression` if it is a constant string literal, and
// `std::nullopt` otherwise. Note that the returned view points into the literal
// that is owned by `expression`, which therefore has to outlive it. In
// particular this must not be implemented via
// `getLiteralFromLiteralExpression`, which returns a copy of the literal.
std::optional<std::string_view> getConstantLiteralContent(
    const SparqlExpression& expression) {
  const auto* literalExpression =
      dynamic_cast<const StringLiteralExpression*>(&expression);
  if (!literalExpression) {
    return std::nullopt;
  }
  return asStringViewUnsafe(literalExpression->value().getContent());
}

// _____________________________________________________________________________
void ensureIsValidRegexIfConstant(const SparqlExpression& expression) {
  auto string = getConstantLiteralContent(expression);
  if (!string.has_value()) {
    return;
  }
  RE2 regex{string.value(), RE2::Quiet};
  if (!regex.ok()) {
    throw std::runtime_error{absl::StrCat(
        "The regex \"", string.value(),
        "\" is not supported by QLever (which uses Google's RE2 library); "
        "the error from RE2 is: ",
        regex.error())};
  }
}

// _____________________________________________________________________________
void ensureIsValidFlagIfConstant(const SparqlExpression& expression) {
  auto string = getConstantLiteralContent(expression);
  if (!string.has_value()) {
    return;
  }
  auto firstInvalidFlag = string.value().find_first_not_of(supportedRegexFlags);
  if (firstInvalidFlag != std::string::npos) {
    throw std::runtime_error{absl::StrCat(
        "Invalid regex flag '", string.value().substr(firstInvalidFlag, 1),
        "' found in \"", string.value(),
        "\". The only supported flags are 'i', 'm', 's', 'U', and any "
        "combination of them")};
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
      bool isNegated) const override {
    if (!prefix_.has_value()) {
      return {};
    }
    // A `PrefixRegexExpression` keeps every block that may contain a value in
    // the range of the prefix, which is exactly what we need here: the prefix
    // is guaranteed for every match, so a block outside of that range cannot
    // contain a match. For a negated `REGEX` the requirement is the opposite
    // one, namely that no block which may contain a *non*-match is dropped, and
    // that does not hold: the range of the prefix is computed on the PRIMARY
    // level of the collation, so it also contains values that the regex does
    // not match (e.g. "ÄBC" is in the range of the prefix "abc"). Those values
    // satisfy the negated `REGEX`, but their block would be dropped.
    //
    // Note that this is different for `ql:prefix-match` (and `STRSTARTS`),
    // which is *defined* via the very same range and can therefore be
    // prefiltered in both directions.
    if (isNegated) {
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

// Return the regex that the `regex` expression will evaluate to, with the
// `flags` (which may be `nullptr` if the `REGEX` call has no third argument)
// merged into it, or `std::nullopt` if either of them is not a constant
// literal.
//
// Merging the flags exactly as `MergeFlagsIntoRegex` does at runtime means that
// RE2, and not this code, decides what they mean. In particular the `m` flag,
// which lets `^` match after every newline and hence makes prefiltering
// unsound, is handled correctly for free.
//
// The only flag that is treated specially is `i`, which is dropped: every value
// that the case-insensitive regex matches is a case variant of a value that the
// case-sensitive regex matches, and case variants all lie in the same prefix
// range, because that range is computed on the PRIMARY level of the collation
// (see `UnicodeVocabulary::prefix_range`). Keeping the flag would yield no
// prefix at all, as the bounds that RE2 reports for `(?i:^abc)` are "ABC" and
// "abc", which have no common prefix.
std::optional<std::string> getConstantRegexWithFlags(
    const SparqlExpression& regex, const SparqlExpression* flags) {
  auto regexString = getConstantLiteralContent(regex);
  if (!regexString.has_value()) {
    return std::nullopt;
  }
  if (flags == nullptr) {
    return std::string{regexString.value()};
  }
  auto flagsString = getConstantLiteralContent(*flags);
  if (!flagsString.has_value()) {
    return std::nullopt;
  }
  std::string flagsWithoutIgnoreCase =
      absl::StrReplaceAll(flagsString.value(), {{"i", ""}});
  return mergeFlagsIntoRegex(std::string{regexString.value()},
                             flagsWithoutIgnoreCase);
}

// If `string` is a plain variable and `regex` is a constant regex (see
// `getConstantRegexWithFlags`) with a guaranteed literal prefix (see
// `getLiteralPrefixOfRegex`), return the variable and that prefix, which
// enables prefiltering. Return `std::nullopt` otherwise.
//
// Note: Prefiltering `STR(?var)` is deliberately not supported, since we would
// not only have to match "Bob", but also "Bob"@en, "Bob"^^<iri>, and so on. The
// current prefilter expressions do not consider this matching logic.
std::optional<std::pair<Variable, std::string>> getRegexPrefilterInfo(
    const SparqlExpression& string, const SparqlExpression& regex,
    const SparqlExpression* flags) {
  bool childIsStrExpression = string.isStrExpression();
  const auto* variableExpression = dynamic_cast<const VariableExpression*>(
      childIsStrExpression ? string.children()[0].get() : &string);
  auto constantRegex = getConstantRegexWithFlags(regex, flags);
  if (!variableExpression || !constantRegex.has_value() ||
      childIsStrExpression) {
    return std::nullopt;
  }
  std::string prefix = getLiteralPrefixOfRegex(constantRegex.value());
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
  if (auto literal = detail::getLiteralFromLiteralExpression(regex.get())) {
    detail::ensureIsSimpleLiteral(literal.value(), "REGEX");
  }
  detail::ensureIsValidRegexIfConstant(*regex);
  // Compute the prefilter information (if the regex is a prefix regex) before
  // the arguments are moved into the expression below. The actual regex is
  // always evaluated by `RegexExpression`; the prefilter only restricts the
  // scanned blocks.
  auto prefilterInfo =
      detail::getRegexPrefilterInfo(*string, *regex, flags.get());
  if (flags) {
    if (auto literal = detail::getLiteralFromLiteralExpression(flags.get())) {
      detail::ensureIsSimpleLiteral(literal.value(), "REGEX");
    }
    detail::ensureIsValidFlagIfConstant(*flags);
    // Merge the flags into the regex, which turns it into an expression that is
    // no longer a plain string literal (hence the prefiltering above).
    regex = makeMergeRegexPatternAndFlagsExpression(std::move(regex),
                                                    std::move(flags));
  }
  return std::make_unique<detail::RegexExpression>(
      std::move(string), std::move(regex), std::move(prefilterInfo));
}

}  // namespace sparqlExpression
