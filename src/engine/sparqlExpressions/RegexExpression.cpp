// Copyright 2022 - 2024
// University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "./RegexExpression.h"

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "global/ValueIdComparators.h"
#include "re2/re2.h"

using namespace std::literals;

namespace sparqlExpression::detail {

// ____________________________________________________________________________
std::optional<std::string> getPrefixRegex(std::string regex) {
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

}  // namespace sparqlExpression::detail

namespace sparqlExpression {
// ___________________________________________________________________________
RegexExpression::RegexExpression(
    SparqlExpression::Ptr child, SparqlExpression::Ptr regex,
    std::optional<SparqlExpression::Ptr> optionalFlags)
    : child_{std::move(child)} {
  if (child_->isStrExpression()) {
    child_ = std::move(std::move(*child_).moveChildrenOut().at(0));
    childIsStrExpression_ = true;
  }
  if (!dynamic_cast<const VariableExpression*>(child_.get())) {
    throw std::runtime_error(
        "REGEX expressions are currently supported only on variables.");
  }
  std::string regexString;
  if (auto regexPtr =
          dynamic_cast<const StringLiteralExpression*>(regex.get())) {
    const auto& regexLiteral = regexPtr->value();
    regexString = asStringViewUnsafe(regexLiteral.getContent());
    if (regexLiteral.hasDatatype() || regexLiteral.hasLanguageTag()) {
      throw std::runtime_error(
          "The second argument to the REGEX function (which contains the "
          "regular expression) must not contain a language tag or a datatype");
    }
  } else {
    throw std::runtime_error(
        "The second argument to the REGEX function must be a "
        "string literal (which contains the regular expression)");
  }
  if (optionalFlags.has_value()) {
    if (auto flagsPtr = dynamic_cast<const StringLiteralExpression*>(
            optionalFlags.value().get())) {
      const auto& flagsLiteral = flagsPtr->value();
      std::string_view flags = asStringViewUnsafe(flagsLiteral.getContent());
      if (flagsLiteral.hasDatatype() || flagsLiteral.hasLanguageTag()) {
        throw std::runtime_error(
            "The third argument to the REGEX function (which contains optional "
            "flags to configure the evaluation) must not contain a language "
            "tag or a datatype");
      }
      auto firstInvalidFlag = flags.find_first_not_of("imsu");
      if (firstInvalidFlag != std::string::npos) {
        throw std::runtime_error{absl::StrCat(
            "Invalid regex flag '", std::string{flags[firstInvalidFlag]},
            "' found in \"", flags,
            "\". The only supported flags are 'i', 's', 'm', 's', 'u', and any "
            "combination of them")};
      }

      // In Google RE2 the flags are directly part of the regex.
      if (!flags.empty()) {
        regexString = absl::StrCat("(?", flags, ":", regexString + ")");
      }
    } else {
      throw std::runtime_error(
          "The optional third argument to the REGEX function must be a string "
          "literal (which contains the configuration flags)");
    }
  }

  regexAsString_ = regexString;
  if (auto opt = detail::getPrefixRegex(regexString)) {
    regex_ = std::move(opt.value());
  } else {
    regex_.emplace<RE2>(regexString, RE2::Quiet);
    const auto& r = std::get<RE2>(regex_);
    if (r.error_code() != RE2::NoError) {
      throw std::runtime_error{absl::StrCat(
          "The regex \"", regexString,
          "\" is not supported by QLever (which uses Google's RE2 library). "
          "Error from RE2 is: ",
          r.error())};
    }
  }
}

// ___________________________________________________________________________
string RegexExpression::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  return absl::StrCat("REGEX expression ", child_->getCacheKey(varColMap),
                      " with ", regexAsString_, "str:", childIsStrExpression_);
}

// ___________________________________________________________________________
std::span<SparqlExpression::Ptr> RegexExpression::childrenImpl() {
  return {&child_, 1};
}

// ___________________________________________________________________________
ExpressionResult RegexExpression::evaluatePrefixRegex(
    const Variable& variable,
    sparqlExpression::EvaluationContext* context) const {
  std::string prefixRegex = std::get<std::string>(regex_);
  std::vector<std::string> actualPrefixes;
  actualPrefixes.push_back("\"" + prefixRegex);
  // If the STR function was applied, we also look for prefix matches for IRIs.
  // TODO<joka921> prefix filters currently never find numbers or local vocab
  // entries, numbers, or other datatypes that are encoded directly inside the
  // IDs.
  if (childIsStrExpression_) {
    actualPrefixes.push_back("<" + prefixRegex);
  }
  std::vector<ad_utility::SetOfIntervals> resultSetOfIntervals;
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
  auto beg = context->_inputTable.begin() + context->_beginIndex;
  auto end = context->_inputTable.begin() + context->_endIndex;
  AD_CONTRACT_CHECK(end <= context->_inputTable.end());
  if (context->isResultSortedBy(variable)) {
    auto column = context->getColumnIndexForVariable(variable);
    for (auto [lowerId, upperId] : lowerAndUpperIds) {
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
  } else {
    auto resultSize = context->size();
    VectorWithMemoryLimit<Id> result{context->_allocator};
    result.reserve(resultSize);
    for (auto id : detail::makeGenerator(variable, resultSize, context)) {
      result.push_back(Id::makeFromBool(
          std::ranges::any_of(lowerAndUpperIds, [&](const auto& lowerUpper) {
            return !valueIdComparators::compareByBits(id, lowerUpper.first) &&
                   valueIdComparators::compareByBits(id, lowerUpper.second);
          })));
      checkCancellation(context);
    }
    return result;
  }
}

// ___________________________________________________________________________
ExpressionResult RegexExpression::evaluateNonPrefixRegex(
    const Variable& variable,
    sparqlExpression::EvaluationContext* context) const {
  AD_CONTRACT_CHECK(std::holds_alternative<RE2>(regex_));
  auto resultSize = context->size();
  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(resultSize);

  auto impl = [&]<typename ValueGetter>(const ValueGetter& getter) {
    for (auto id : detail::makeGenerator(variable, resultSize, context)) {
      auto str = getter(id, context);
      if (!str.has_value()) {
        result.push_back(Id::makeUndefined());
      } else {
        result.push_back(Id::makeFromBool(
            RE2::PartialMatch(str.value(), std::get<RE2>(regex_))));
      }
      checkCancellation(context);
    }
  };
  if (childIsStrExpression_) {
    impl(detail::StringValueGetter{});
  } else {
    impl(detail::LiteralFromIdGetter{});
  }
  return result;
}

// ___________________________________________________________________________
ExpressionResult RegexExpression::evaluate(
    sparqlExpression::EvaluationContext* context) const {
  auto resultAsVariant = child_->evaluate(context);
  auto variablePtr = std::get_if<Variable>(&resultAsVariant);
  AD_CONTRACT_CHECK(variablePtr);

  if (std::holds_alternative<std::string>(regex_)) {
    return evaluatePrefixRegex(*variablePtr, context);
  } else {
    return evaluateNonPrefixRegex(*variablePtr, context);
  }
}

// ____________________________________________________________________________
bool RegexExpression::isPrefixExpression() const {
  return std::holds_alternative<std::string>(regex_);
}

// ____________________________________________________________________________
auto RegexExpression::getEstimatesForFilterExpression(
    uint64_t inputSize,
    const std::optional<Variable>& firstSortedVariable) const -> Estimates {
  if (isPrefixExpression()) {
    // Assume that only 10^-k entries remain, where k is the length of the
    // prefix. The reason for the -2 is that at this point, _rhs always
    // starts with ^"
    double reductionFactor = std::pow(
        10, std::max(
                0, static_cast<int>(std::get<std::string>(regex_).size()) - 2));
    // Cap to reasonable minimal and maximal values to prevent numerical
    // stability problems.
    reductionFactor = std::min(100000000.0, reductionFactor);
    reductionFactor = std::max(1.0, reductionFactor);
    size_t sizeEstimate = inputSize / static_cast<size_t>(reductionFactor);
    auto varPtr = dynamic_cast<VariableExpression*>(child_.get());
    AD_CONTRACT_CHECK(varPtr);
    size_t costEstimate = firstSortedVariable == varPtr->value()
                              ? sizeEstimate
                              : sizeEstimate + inputSize;

    return {sizeEstimate, costEstimate};
  } else {  // Not a prefix filter.
    size_t sizeEstimate = inputSize / 2;
    // We assume that checking a REGEX for an element is 10 times more
    // expensive than an "ordinary" filter check.
    size_t costEstimate = sizeEstimate + 10 * inputSize;

    return {sizeEstimate, costEstimate};
  }
}

// ____________________________________________________________________________
void RegexExpression::checkCancellation(
    const sparqlExpression::EvaluationContext* context,
    ad_utility::source_location location) {
  context->cancellationHandle_->throwIfCancelled(location);
}

}  // namespace sparqlExpression
