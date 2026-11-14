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

#include <absl/strings/str_cat.h>
#include <re2/re2.h>

#include <algorithm>
#include <cmath>
#include <memory>
#include <string_view>
#include <utility>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/RegexHelpers.h"
#include "engine/sparqlExpressions/SimpleLiteralHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
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
  return literalExpression->value().getContent();
}

// _____________________________________________________________________________
void ensureIsValidRegexIfConstant(const SparqlExpression& expression) {
  if (auto regex = getConstantLiteralContent(expression)) {
    ensureIsValidRegex(regex.value());
  }
}

// _____________________________________________________________________________
void ensureIsValidFlagIfConstant(const SparqlExpression& expression) {
  if (auto flags = getConstantLiteralContent(expression)) {
    ensureIsValidRegexFlags(flags.value());
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

// A constant regex with a guaranteed literal prefix (see
// `getLiteralPrefixOfRegex`), applied to a plain variable, optionally wrapped
// in `STR()` (see `getRegexPrefixInfo`).
struct RegexPrefixInfo {
  Variable variable_;
  std::string prefix_;
  // If true, IRIs are matched as well (without the angle brackets).
  bool childIsStrExpression_;
  // The regex as evaluated at runtime, with all flags (including `i`).
  std::shared_ptr<RE2> regex_;
};

// A `RegexExpressionBase` that additionally exploits a guaranteed literal
// prefix of the regex (e.g. `^prefix`) on a plain variable in two ways:
//
// 1. Prefiltering: blocks of an index scan whose values all lie outside the
//    range of the prefix are skipped (see `getPrefilterExpressionForMetadata`).
//
// 2. Vocabulary entries outside the range of the prefix are rejected via their
//    `Id`, without looking up their string (see `evaluate`). This matters when
//    the input is not sorted by the variable, because the lookups are then
//    random accesses, which are expensive for a vocabulary on disk.
//
// In both cases, the actual regex is still evaluated on all remaining values.
class RegexExpression : public RegexExpressionBase {
 private:
  // See `RegexPrefixInfo`, `std::nullopt` if the regex has no guaranteed prefix
  // or is not applied to a plain variable.
  std::optional<RegexPrefixInfo> prefixInfo_;

 public:
  RegexExpression(Ptr child, Ptr regex,
                  std::optional<RegexPrefixInfo> prefixInfo)
      : RegexExpressionBase(std::move(child), std::move(regex)),
        prefixInfo_{std::move(prefixInfo)} {}

  // Reject the literals (and, inside of `STR()`, the IRIs) of the vocabulary
  // outside the range of the prefix via their `Id`, and evaluate the actual
  // regex on all other values. This is correct because the range is computed on
  // the PRIMARY level of the collation, so it contains every entry that starts
  // with the prefix (and more, e.g. "ABC" for "abc"). All values that are not
  // in the vocabulary (local vocab entries, the WKT literals of a geo-split
  // vocabulary, encoded IRIs, numbers, ...) are evaluated by the actual regex.
  ExpressionResult evaluate(EvaluationContext* context) const override {
    // Leave grouped data and unbound variables to the base class.
    if (!prefixInfo_.has_value() || worksOnAggregatedData(context) ||
        !context->getColumnIndexForVariable(prefixInfo_->variable_)
             .has_value()) {
      return RegexExpressionBase::evaluate(context);
    }
    const RegexPrefixInfo& info = prefixInfo_.value();
    const bool childIsStrExpression = info.childIsStrExpression_;

    const auto& vocab = context->_qec.getIndex().getVocab();
    auto literalRange = vocab.prefixRanges(absl::StrCat("\"", info.prefix_));
    auto iriRange = vocab.prefixRanges(absl::StrCat("<", info.prefix_));
    // Without `STR()`, the regex yields UNDEF (not `false`) for an IRI.
    auto isOutsideOfPrefixRange = [&vocab, &literalRange, &iriRange,
                                   childIsStrExpression](Id id) {
      // TODO<RobinTF> Handle encoded IRIs more efficiently
      if (id.getDatatype() != Datatype::VocabIndex) {
        return false;
      }
      auto index = id.getVocabIndex();
      if (vocab.isLiteral(index)) {
        return !literalRange.contain(index);
      }
      return childIsStrExpression && vocab.isIri(index) &&
             !iriRange.contain(index);
    };

    // The same value getters as in `StringExpressionImpl`.
    auto getString = [context, childIsStrExpression](Id id) {
      return childIsStrExpression ? StringValueGetter{}(id, context)
                                  : LiteralFromIdGetter{}(id, context);
    };

    VectorWithMemoryLimit<Id> result{context->_allocator};
    result.reserve(context->size());
    for (Id id : makeGenerator(info.variable_, context->size(), context)) {
      result.push_back(isOutsideOfPrefixRange(id)
                           ? Id::makeFromBool(false)
                           : RegexImpl{}(getString(id), info.regex_));
      context->cancellationHandle_->throwIfCancelled();
    }
    return result;
  }

  std::vector<PrefilterExprVariablePair> getPrefilterExpressionForMetadata(
      [[maybe_unused]] const LocalVocabContext& context,
      bool isNegated) const override {
    // Prefiltering `STR(?var)` is deliberately not supported, since we would
    // also have to match "Bob"@en, "Bob"^^<iri>, and so on for "Bob", which
    // the current prefilter expressions do not consider.
    if (!prefixInfo_.has_value() || prefixInfo_->childIsStrExpression_) {
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
                prefixInfo_->prefix_)),
        prefixInfo_->variable_);
    return prefilterVec;
  }

  // If we know a guaranteed prefix, assume that only 10^-k entries remain,
  // where k is the length of that prefix, and cap to reasonable maximal values
  // to prevent numerical stability problems. Note that unlike for
  // `PrefixMatchExpression`, each input row has to be looked at, so the cost
  // always includes the full input size (even if the input is sorted by the
  // variable).
  Estimates getEstimatesForFilterExpression(
      uint64_t inputSize,
      const std::optional<Variable>& firstSortedVariable) const override {
    if (!prefixInfo_.has_value()) {
      return RegexExpressionBase::getEstimatesForFilterExpression(
          inputSize, firstSortedVariable);
    }
    double reductionFactor =
        std::pow(10, std::min<size_t>(8, prefixInfo_->prefix_.size()));
    size_t sizeEstimate = inputSize / static_cast<size_t>(reductionFactor);
    return {sizeEstimate, sizeEstimate + inputSize};
  }
};

// Return the regex that the `regex` expression will evaluate to, with the
// `flags` (which may be `nullptr` if the `REGEX` call has no third argument)
// merged into it as at runtime, but without the flags in `flagsToDrop`. Return
// `std::nullopt` if either of them is not a constant literal or if the flags
// are invalid.
std::optional<std::string> getConstantRegexWithFlags(
    const SparqlExpression& regex, const SparqlExpression* flags,
    std::string_view flagsToDrop = "") {
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
  std::string keptFlags;
  for (char flag : flagsString.value()) {
    if (flagsToDrop.find(flag) == std::string_view::npos) {
      keptFlags.push_back(flag);
    }
  }
  return mergeFlagsIntoRegex(std::string{regexString.value()}, keptFlags);
}

// Return the regex from which the prefix for the prefiltering is derived (see
// `getConstantRegexWithFlags`). The result must only be used for that purpose,
// as the case-insensitivity flag is deliberately dropped from it (see below),
// so it is *not* equivalent to the regex that is evaluated at runtime.
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
std::optional<std::string> getConstantRegexForPrefiltering(
    const SparqlExpression& regex, const SparqlExpression* flags) {
  return getConstantRegexWithFlags(regex, flags, "i");
}

// If `string` is a plain variable (optionally wrapped in `STR()`) and `regex`
// is a constant regex (see `getConstantRegexForPrefiltering`) with a guaranteed
// literal prefix (see `getLiteralPrefixOfRegex`), return the corresponding
// `RegexPrefixInfo`. Return `std::nullopt` otherwise.
std::optional<RegexPrefixInfo> getRegexPrefixInfo(
    const SparqlExpression& string, const SparqlExpression& regex,
    const SparqlExpression* flags) {
  bool childIsStrExpression = string.isStrExpression();
  const auto* variableExpression = dynamic_cast<const VariableExpression*>(
      childIsStrExpression ? string.children()[0].get() : &string);
  if (!variableExpression) {
    return std::nullopt;
  }
  auto constantRegex = getConstantRegexForPrefiltering(regex, flags);
  auto runtimeRegex = getConstantRegexWithFlags(regex, flags);
  if (!constantRegex.has_value() || !runtimeRegex.has_value()) {
    return std::nullopt;
  }
  std::string prefix = getLiteralPrefixOfRegex(constantRegex.value());
  // An empty prefix would not restrict anything.
  if (prefix.empty()) {
    return std::nullopt;
  }
  // Compile the regex the same way as `RegexValueGetter`.
  return RegexPrefixInfo{
      variableExpression->value(), std::move(prefix), childIsStrExpression,
      std::make_shared<RE2>(runtimeRegex.value(), RE2::Quiet)};
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
  // Compute the prefix information (if the regex is a prefix regex) before the
  // arguments are moved into the expression below. The actual regex is always
  // evaluated by `RegexExpression`; the prefix only restricts which blocks and
  // values it is evaluated on.
  auto prefixInfo = detail::getRegexPrefixInfo(*string, *regex, flags.get());
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
      std::move(string), std::move(regex), std::move(prefixInfo));
}

}  // namespace sparqlExpression
