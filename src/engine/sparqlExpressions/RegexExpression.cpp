// Copyright 2022 - 2024
// University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/RegexExpression.h"

#include <re2/re2.h>

#include "NaryExpressionImpl.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "engine/sparqlExpressions/StringExpressionsHelper.h"
#include "global/ValueIdComparators.h"

using namespace std::literals;

namespace sparqlExpression::detail {

void ensureIsSimpleLiteral(
    const ad_utility::triple_component::Literal& literal) {
  if (literal.hasDatatype() || literal.hasLanguageTag()) {
    throw std::runtime_error{
        "The REGEX function only accepts simple literals (literals without a "
        "language tag or a datatype)"};
  }
}

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
[[maybe_unused]] auto regexImpl = [](const std::optional<std::string>& input,
                                     const std::shared_ptr<RE2>& pattern) {
  if (!input.has_value() || !pattern) {
    return Id::makeUndefined();
  }
  // Check for invalid regexes.
  if (!pattern->ok()) {
    return Id::makeUndefined();
  }
  return Id::makeFromBool(RE2::PartialMatch(input.value(), *pattern));
};

using RegexExpression =
    string_expressions::StringExpressionImpl<2, decltype(regexImpl),
                                             RegexValueGetter>;

}  // namespace sparqlExpression::detail

namespace sparqlExpression {

// _____________________________________________________________________________
std::optional<std::string> PrefixRegexExpression::getPrefixRegex(
    std::string regex) {
  if (!regex.starts_with('^')) {
    return std::nullopt;
  }
  // Check if we can use the more efficient prefix filter instead
  // of an expensive regex filter.
  bool escaped = false;

  // Positions of backslashes that are used for escaping within the regex. These
  // have to be removed if the regex is simply a prefix filter.
  std::vector<size_t> escapePositions;

  // Check if the regex is only a prefix regex or also contains other special
  // regex characters that are not properly escaped.
  for (size_t i = 1; i < regex.size(); i++) {
    if (regex[i] == '\\') {
      if (!escaped) {
        escapePositions.push_back(i);
      }
      escaped = !escaped;  // correctly deal with consecutive backslashes
      continue;
    }
    char c = regex[i];
    const static string regexSpecialChars = "[]^$.|?*+()";
    bool isControlChar = regexSpecialChars.find(c) != string::npos;
    if (!escaped && isControlChar) {
      return std::nullopt;
    } else if (escaped && !isControlChar) {
      const std::string error =
          "Escaping the character "s + c +
          " is not allowed in QLever's regex filters. (Regex was " + regex +
          ") Please note that "
          "there are two levels of escaping in place here: One for SPARQL "
          "and one for the regex engine";
      throw std::runtime_error(error);
    }
    escaped = false;
  }
  // There are no regex special chars apart from the leading '^'
  // so we can use a prefix filter.

  // we have to remove the escaping backslashes
  for (auto it = escapePositions.rbegin(); it != escapePositions.rend(); ++it) {
    regex.erase(regex.begin() + *it);
  }
  // also remove the leading "^".
  regex.erase(regex.begin());
  return regex;
}

// _____________________________________________________________________________
PrefixRegexExpression::PrefixRegexExpression(Ptr child, std::string prefixRegex,
                                             Variable variable)
    : child_{std::move(child)},
      prefixRegex_{std::move(prefixRegex)},
      variable_{std::move(variable)} {
  // If we have a `STR()` expression, remove the `STR()` and remember that it
  // was there.
  if (child_->isStrExpression()) {
    child_ = std::move(std::move(*child_).moveChildrenOut().at(0));
    childIsStrExpression_ = true;
  }
}

// _____________________________________________________________________________
string PrefixRegexExpression::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  return absl::StrCat("Prefix REGEX expression: ", prefixRegex_,
                      " child:", child_->getCacheKey(varColMap),
                      " str:", childIsStrExpression_);
}

// _____________________________________________________________________________
ql::span<SparqlExpression::Ptr> PrefixRegexExpression::childrenImpl() {
  return {&child_, 1};
}

// _____________________________________________________________________________
ExpressionResult PrefixRegexExpression::evaluate(
    EvaluationContext* context) const {
  // This function must only be called if we have a simple prefix regex.

  // If the expression is enclosed in `STR()`, we have two ranges: for the
  // prefix with and without leading "<".
  //
  // TODO<joka921> prefix filters currently have false negatives when the prefix
  // is not in the vocabulary, and there exist local vocab entries in the input
  // that are between the prefix and the next local vocab entry. This is
  // non-trivial to fix as it involves fiddling with Unicode prefix encodings.
  //
  // TODO<joka921> prefix filters currently never find numbers or other
  // datatypes that are encoded directly inside the IDs.
  std::vector<std::string> actualPrefixes;
  actualPrefixes.push_back("\"" + prefixRegex_);
  if (childIsStrExpression_) {
    actualPrefixes.push_back("<" + prefixRegex_);
  }

  // Compute the (one or two) ranges.
  std::vector<std::pair<Id, Id>> lowerAndUpperIds;
  lowerAndUpperIds.reserve(actualPrefixes.size());
  for (const auto& prefix : actualPrefixes) {
    const auto& ranges = context->_qec.getIndex().prefixRanges(prefix);
    for (const auto& [begin, end] : ranges.ranges()) {
      lowerAndUpperIds.emplace_back(Id::makeFromVocabIndex(begin),
                                    Id::makeFromVocabIndex(end));
    }
  }
  checkCancellation(context);

  // Begin and end of the input (for each row of which we want to
  // evaluate the regex).
  auto beg = context->_inputTable.begin() + context->_beginIndex;
  auto end = context->_inputTable.begin() + context->_endIndex;
  AD_CONTRACT_CHECK(end <= context->_inputTable.end());

  // In this function, the expression is a simple variable. If the input is
  // sorted by that variable, the result can be computed by a constant number
  // of binary searches and the result is a set of intervals.
  if (context->isResultSortedBy(variable_)) {
    std::vector<ad_utility::SetOfIntervals> resultSetOfIntervals;
    auto optColumn = context->getColumnIndexForVariable(variable_);
    AD_CORRECTNESS_CHECK(optColumn.has_value(),
                         "We have previously asserted that the input is sorted "
                         "by the variable, so we expect it to exist");
    const auto& column = optColumn.value();
    for (auto [lowerId, upperId] : lowerAndUpperIds) {
      // Two binary searches to find the lower and upper bounds of the range.
      auto lower = std::lower_bound(
          beg, end, nullptr,
          [column, lowerId = lowerId](const auto& l, const auto&) {
            return l[column] < lowerId;
          });
      auto upper = std::lower_bound(
          beg, end, nullptr,
          [column, upperId = upperId](const auto& l, const auto&) {
            return l[column] < upperId;
          });
      // Return the empty result as an empty `SetOfIntervals` instead of as an
      // empty range.
      if (lower != upper) {
        resultSetOfIntervals.push_back(
            ad_utility::SetOfIntervals{{{lower - beg, upper - beg}}});
      }
      checkCancellation(context);
    }
    return std::reduce(resultSetOfIntervals.begin(), resultSetOfIntervals.end(),
                       ad_utility::SetOfIntervals{},
                       ad_utility::SetOfIntervals::Union{});
  }

  // If the input is not sorted by the variable, we have to check each row
  // individually (by checking inclusion in the ranges).
  auto resultSize = context->size();
  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(resultSize);
  for (auto id : detail::makeGenerator(variable_, resultSize, context)) {
    result.push_back(Id::makeFromBool(
        ql::ranges::any_of(lowerAndUpperIds, [&](const auto& lowerUpper) {
          return !valueIdComparators::compareByBits(id, lowerUpper.first) &&
                 valueIdComparators::compareByBits(id, lowerUpper.second);
        })));
    checkCancellation(context);
  }
  return result;
}

// _____________________________________________________________________________
auto PrefixRegexExpression::getEstimatesForFilterExpression(
    uint64_t inputSize,
    const std::optional<Variable>& firstSortedVariable) const -> Estimates {
  // Assume that only 10^-k entries remain, where k is the length of the prefix
  // and cap to reasonable maximal values to prevent numerical stability
  // problems.
  double reductionFactor =
      std::pow(10, std::min<size_t>(8, prefixRegex_.size()));
  size_t sizeEstimate = inputSize / static_cast<size_t>(reductionFactor);
  size_t costEstimate = firstSortedVariable == variable_
                            ? sizeEstimate
                            : sizeEstimate + inputSize;

  return {sizeEstimate, costEstimate};
}

// _____________________________________________________________________________
void PrefixRegexExpression::checkCancellation(
    const EvaluationContext* context, ad_utility::source_location location) {
  context->cancellationHandle_->throwIfCancelled(location);
}

// _____________________________________________________________________________
std::vector<PrefilterExprVariablePair>
PrefixRegexExpression::getPrefilterExpressionForMetadata(
    [[maybe_unused]] bool isNegated) const {
  // It is currently not possible to prefilter PREFIX expressions involving
  // STR(?var), since we not only have to match "Bob", but also "Bob"@en,
  // "Bob"^^<iri>, and so on. The current prefilter expressions do not consider
  // this matching logic.
  if (childIsStrExpression_) {
    return {};
  }
  std::vector<PrefilterExprVariablePair> prefilterVec;
  prefilterVec.emplace_back(
      std::make_unique<prefilterExpressions::PrefixRegexExpression>(
          TripleComponent::Literal::literalWithNormalizedContent(
              asNormalizedStringViewUnsafe(prefixRegex_))),
      variable_);
  return prefilterVec;
}

// _____________________________________________________________________________
std::optional<PrefixRegexExpression>
PrefixRegexExpression::makePrefixRegexExpressionIfPossible(
    Ptr& string, const SparqlExpression& regex) {
  detail::ensureIsValidRegexIfConstant(regex);
  const auto* variableExpression = dynamic_cast<const VariableExpression*>(
      string->isStrExpression() ? string->children()[0].get() : string.get());
  if (!variableExpression) {
    return std::nullopt;
  }
  const auto* stringLiteralExpression =
      dynamic_cast<const StringLiteralExpression*>(&regex);
  if (!stringLiteralExpression) {
    return std::nullopt;
  }
  const auto& stringLiteral = stringLiteralExpression->value();
  detail::ensureIsSimpleLiteral(stringLiteral);
  if (std::optional<std::string> prefixRegex = getPrefixRegex(
          std::string{asStringViewUnsafe(stringLiteral.getContent())})) {
    return PrefixRegexExpression{std::move(string),
                                 std::move(prefixRegex.value()),
                                 variableExpression->value()};
  }
  return std::nullopt;
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeRegexExpression(SparqlExpression::Ptr string,
                                          SparqlExpression::Ptr regex,
                                          SparqlExpression::Ptr flags) {
  if (flags) {
    if (auto* stringLiteralExpression =
            dynamic_cast<const StringLiteralExpression*>(flags.get())) {
      detail::ensureIsSimpleLiteral(stringLiteralExpression->value());
    }
    detail::ensureIsValidRegexIfConstant(*regex);
    detail::ensureIsValidFlagIfConstant(*flags);
    regex = makeMergeRegexPatternAndFlagsExpression(std::move(regex),
                                                    std::move(flags));
  } else if (auto prefixExpression =
                 PrefixRegexExpression::makePrefixRegexExpressionIfPossible(
                     string, *regex)) {
    return std::make_unique<PrefixRegexExpression>(
        std::move(prefixExpression.value()));
  } else {
    detail::ensureIsValidRegexIfConstant(*regex);
  }
  return std::make_unique<detail::RegexExpression>(std::move(string),
                                                   std::move(regex));
}

}  // namespace sparqlExpression
