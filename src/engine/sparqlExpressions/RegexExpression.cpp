//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "./RegexExpression.h"

#include <regex>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "global/ValueIdComparators.h"

using namespace std::literals;

namespace {

// Check if `regex` is a prefix regex which means that it starts with `^` and
// contains no other "special" regex characters like `*` or `.`. If this check
// suceeds, the prefix is returned without the leading `^` and with all escaping
// undone. Else, `std::nullopt` is returned.
std::optional<std::string> getPrefixRegex(std::string regex) {
  if (!regex.starts_with('^')) {
    return std::nullopt;
  }
  // Check if we can use the more efficient prefix filter instead
  // of an expensive regex filter.
  bool escaped = false;

  // Positions of backslashes that are used for
  // escaping within the regex
  // these have to be removed if the regex is simply a prefix filter.
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
}  // namespace

namespace sparqlExpression {
// ___________________________________________________________________________
RegexExpression::RegexExpression(SparqlExpression::Ptr child,
                                 SparqlExpression::Ptr regex)
    : child_{std::move(child)} {
  if (!dynamic_cast<const VariableExpression*>(child_.get())) {
    throw std::runtime_error(
        "REGEX epxressions are currently supported on variables.");
  }
  if (auto regexPtr = dynamic_cast<const StringOrIriExpression*>(regex.get())) {
    regex_ = regexPtr->value();
    // TODO<joka921> Throw an error message if the regex is not quoted.
    // TODO<joka921> Check the paths for the StringExpressions, whether this
    // is really an AD-CHECK.
    AD_CHECK(regex_.size() >= 2);
    regex_ = regex_.substr(1, regex_.size() - 2);
  } else {
    throw std::runtime_error(
        "The second argument to the REGEX function must be a string literal");
  }
  if (auto opt = getPrefixRegex(regex_)) {
    isPrefixRegex_ = true;
    regex_ = std::move(opt.value());
  }
}

// ___________________________________________________________________________
string RegexExpression::getCacheKey(
    const sparqlExpression::VariableToColumnMap& varColMap) const {
  return "REGEX expression " + child_->getCacheKey(varColMap);
}

// ___________________________________________________________________________
std::span<SparqlExpression::Ptr> RegexExpression::children() {
  return {&child_, 1};
}

ExpressionResult RegexExpression::evaluate(
    sparqlExpression::EvaluationContext* context) const {
  auto resultAsVariant = child_->evaluate(context);
  auto variablePtr = std::get_if<Variable>(&resultAsVariant);
  AD_CHECK(variablePtr);

  if (isPrefixRegex_) {
    // TODO<joka921> Make this a function of the context
    auto prefixRange = context->_qec.getIndex().getVocab().prefix_range(regex_);
    Id lowerId = Id::makeFromVocabIndex(prefixRange.first);
    Id upperId = Id::makeFromVocabIndex(prefixRange.second);
    auto beg = context->_inputTable.begin() + context->_beginIndex;
    auto end = context->_inputTable.begin() + context->_endIndex;
    AD_CHECK(end <= context->_inputTable.end());
    if (context->isResultSortedBy(*variablePtr)) {
      auto column =
          context->_variableToColumnAndResultTypeMap.at(variablePtr->name())
              .first;
      auto lower = std::lower_bound(
          beg, end, nullptr,
          [&, lowerId = Id::makeFromVocabIndex(prefixRange.first)](
              const auto& l, const auto&) { return l[column] < lowerId; });
      auto upper = std::lower_bound(
          beg, end, nullptr,
          [&, lowerId = Id::makeFromVocabIndex(prefixRange.first)](
              const auto& l, const auto&) { return l[column] < lowerId; });

      return ad_utility::SetOfIntervals{{{lower - beg, upper - beg}}};
    } else {
      auto resultSize = context->_endIndex - context->_beginIndex;
      VectorWithMemoryLimit<Bool> result{context->_allocator};
      result.reserve(resultSize);
      for (auto id : detail::makeGenerator(
               *variablePtr, context->_endIndex - context->_beginIndex,
               context)) {
        result.push_back(
            !valueIdComparators::compareByBits(id, lowerId) &&
            valueIdComparators::compareByBits(id, upperId));
      }
      return result;
    }
  } else {
    // TODO<joka921> don't use std::regex...
    std::regex regex;
    try {
      regex.assign(regex_, std::regex_constants::ECMAScript);
    } catch (const std::regex_error& e) {
      // Rethrow the regex error with more information. Can't use the
      // regex_error here as the constructor does not allow setting the
      // error message.
      throw std::runtime_error(
          "The regex '" + regex_ +
          "'is not an ECMAScript regex: " + std::string(e.what()));
    }
    auto resultSize = context->_endIndex - context->_beginIndex;
    VectorWithMemoryLimit<Bool> result{context->_allocator};
    result.reserve(resultSize);
    // TODO<joka921> is the second argument not ALWAYS and BY definition
    // endIndex - beginIndex?
    for (auto id : detail::makeGenerator(
             *variablePtr, context->_endIndex - context->_beginIndex,
             context)) {
      result.push_back(
          std::regex_search(detail::StringValueGetter{}(id, context), regex));
    }
    return result;
  }
}

}  // namespace sparqlExpression
