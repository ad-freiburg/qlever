// Copyright 2022 - 2024
// University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "./RegexExpression.h"

#include <absl/container/flat_hash_map.h>
#include <re2/re2.h>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/ValueIdComparators.h"

using namespace std::literals;

namespace sparqlExpression::detail {

template <typename K, typename V>
class LRUCache {
 private:
  size_t capacity_;
  // Stores keys in order of usage (MRU at front)
  std::list<K> keys_;
  absl::flat_hash_map<K, std::pair<V, typename std::list<K>::iterator>> cache_;

 public:
  explicit LRUCache(size_t capacity) : capacity_{capacity} {}

  template <typename Func>
  const V& getOrCompute(const K& key, Func computeFunction) {
    auto it = cache_.find(key);
    if (it != cache_.end()) {
      // Move accessed key to front (most recently used)
      keys_.erase(it->second.second);
      keys_.push_front(key);
      it->second.second = keys_.begin();

      return it->second.first;
    }
    // Evict LRU if cache is full
    if (cache_.size() >= capacity_) {
      K lruKey = keys_.back();
      keys_.pop_back();
      cache_.erase(lruKey);
    }
    // Insert new element
    keys_.push_front(key);
    auto result = cache_.try_emplace(key, computeFunction(key), keys_.begin());
    AD_CORRECTNESS_CHECK(result.second);
    return result.first->second.first;
  }
};

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
RegexExpression::RegexExpression(Ptr child, Ptr regex,
                                 std::optional<Ptr> optionalFlags)
    : children_{std::move(child), nullptr} {
  // If we have a `STR()` expression, remove the `STR()` and remember that it
  // was there.
  auto& firstChild = children_.at(0);
  if (firstChild->isStrExpression()) {
    firstChild = std::move(std::move(*firstChild).moveChildrenOut().at(0));
    childIsStrExpression_ = true;
  }

  // Get the regex string, which must be a string literal without a datatype or
  // language tag.
  std::string regexString;
  if (auto regexPtr =
          dynamic_cast<const StringLiteralExpression*>(regex.get())) {
    const auto& regexLiteral = regexPtr->value();
    if (regexLiteral.hasDatatype() || regexLiteral.hasLanguageTag()) {
      throw std::runtime_error(
          "The second argument to the REGEX function (which contains the "
          "regular expression) must not contain a language tag or a datatype");
    }
    regexString = asStringViewUnsafe(regexLiteral.getContent());
  }

  // Parse the flags. The optional argument for that must, again, be a
  // string literal without a datatype or language tag.
  if (!regexString.empty() && optionalFlags.has_value()) {
    if (auto flagsPtr = dynamic_cast<const StringLiteralExpression*>(
            optionalFlags.value().get())) {
      const auto& flagsLiteral = flagsPtr->value();
      if (flagsLiteral.hasDatatype() || flagsLiteral.hasLanguageTag()) {
        throw std::runtime_error(
            "The third argument to the REGEX function (which contains optional "
            "flags to configure the evaluation) must not contain a language "
            "tag or a datatype");
      }
      std::string_view flags = asStringViewUnsafe(flagsLiteral.getContent());
      auto firstInvalidFlag = flags.find_first_not_of("imsu");
      if (firstInvalidFlag != std::string::npos) {
        throw std::runtime_error{absl::StrCat(
            "Invalid regex flag '", std::string{flags[firstInvalidFlag]},
            "' found in \"", flags,
            "\". The only supported flags are 'i', 'm', 's', 'u', and any "
            "combination of them")};
      }

      // In Google RE2 the flags are directly part of the regex.
      if (!flags.empty()) {
        regexString = absl::StrCat("(?", flags, ":", regexString + ")");
      }
    }
  }

  // Create RE2 object from the regex string. If it is a simple prefix regex,
  // store the prefix in `prefixRegex_` (otherwise that becomes `std::nullopt`).
  prefixRegex_ = detail::getPrefixRegex(regexString);
  if (!regexString.empty()) {
    const auto& compiledRegex = regex_.emplace(regexString, RE2::Quiet);
    if (!compiledRegex.ok()) {
      throw std::runtime_error{absl::StrCat(
          "The regex \"", regexString,
          "\" is not supported by QLever (which uses Google's RE2 library); "
          "the error from RE2 is: ",
          compiledRegex.error())};
    }
  } else {
    if (optionalFlags.has_value()) {
      children_.at(1) = makeMergeRegexPatternAndFlagsExpression(
          std::move(regex), std::move(optionalFlags.value()));
    } else {
      children_.at(1) = std::move(regex);
    }
  }
}

// ___________________________________________________________________________
string RegexExpression::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  return absl::StrCat(
      "REGEX expression ", children_.at(0)->getCacheKey(varColMap), " with ",
      regex_.has_value() ? regex_.value().pattern()
                         : children_.at(1)->getCacheKey(varColMap),
      "str:", childIsStrExpression_);
}

// ___________________________________________________________________________
std::span<SparqlExpression::Ptr> RegexExpression::childrenImpl() {
  if (children_.at(1) == nullptr) {
    return {&children_.at(0), 1};
  }
  return children_;
}

// ___________________________________________________________________________
ExpressionResult RegexExpression::evaluatePrefixRegex(
    const Variable& variable, EvaluationContext* context) const {
  // This function must only be called if we have a simple prefix regex.
  AD_CORRECTNESS_CHECK(prefixRegex_.has_value());
  const std::string& prefixRegex = prefixRegex_.value();

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
  actualPrefixes.push_back("\"" + prefixRegex);
  if (childIsStrExpression_) {
    actualPrefixes.push_back("<" + prefixRegex);
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
  std::vector<ad_utility::SetOfIntervals> resultSetOfIntervals;
  if (context->isResultSortedBy(variable)) {
    auto optColumn = context->getColumnIndexForVariable(variable);
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
  for (auto id : detail::makeGenerator(variable, resultSize, context)) {
    result.push_back(Id::makeFromBool(
        ql::ranges::any_of(lowerAndUpperIds, [&](const auto& lowerUpper) {
          return !valueIdComparators::compareByBits(id, lowerUpper.first) &&
                 valueIdComparators::compareByBits(id, lowerUpper.second);
        })));
    checkCancellation(context);
  }
  return result;
}

// ___________________________________________________________________________
CPP_template_def(typename T, typename F)(requires SingleExpressionResult<T>)
    ExpressionResult RegexExpression::evaluateGeneralCase(
        T&& input, EvaluationContext* context, F getNextRegex) const {
  // We have one result for each row of the input.
  auto resultSize = context->size();
  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(resultSize);

  // Compute the result using the given value getter. If the getter returns
  // `std::nullopt` for a row, the result is `UNDEF`. Otherwise, we have a
  // string and evaluate the regex on it.
  auto computeResult = [&]<typename ValueGetter>(const ValueGetter& getter) {
    ql::ranges::for_each(
        detail::makeGenerator(AD_FWD(input), resultSize, context),
        [&getter, &context, &result, &getNextRegex](const auto& id) {
          auto str = getter(id, context);
          auto* nextRegex = getNextRegex();
          if (!str.has_value() || !nextRegex) {
            result.push_back(Id::makeUndefined());
          } else {
            result.push_back(
                Id::makeFromBool(RE2::PartialMatch(str.value(), *nextRegex)));
          }
          checkCancellation(context);
        });
  };

  // Compute the result with the correct value getter (depending on whether the
  // expression is enclosed in `STR()` or not), and return it.
  if (childIsStrExpression_) {
    computeResult(detail::StringValueGetter{});
  } else {
    computeResult(detail::LiteralFromIdGetter{});
  }
  return result;
}

// ___________________________________________________________________________
ExpressionResult RegexExpression::evaluate(EvaluationContext* context) const {
  auto resultAsVariant = children_.at(0)->evaluate(context);
  auto variablePtr = std::get_if<Variable>(&resultAsVariant);

  if (prefixRegex_.has_value() && variablePtr != nullptr) {
    return evaluatePrefixRegex(*variablePtr, context);
  }
  AD_CORRECTNESS_CHECK(regex_.has_value() || children_.at(1));

  return std::visit(
      [this, context](auto&& input) {
        if (regex_.has_value()) {
          return evaluateGeneralCase(AD_FWD(input), context,
                                     [this]() { return &regex_.value(); });
        }
        return std::visit(
            [this, context, &input](auto&& regexInput) {
              auto generator = detail::makeGenerator(AD_FWD(regexInput),
                                                     context->size(), context);

              std::optional<ql::ranges::iterator_t<decltype(generator)>>
                  resultIterator = std::nullopt;

              detail::LRUCache<std::string, std::unique_ptr<RE2>> regexCache{
                  100};

              auto getNextRegex = [&generator, &resultIterator, context,
                                   &regexCache]() -> const RE2* {
                if (resultIterator.has_value()) {
                  AD_CORRECTNESS_CHECK(resultIterator.value() !=
                                       generator.end());
                  ++resultIterator.value();
                } else {
                  resultIterator = generator.begin();
                  AD_CORRECTNESS_CHECK(resultIterator.value() !=
                                       generator.end());
                }
                auto regexString = detail::LiteralFromIdGetter{}(
                    *resultIterator.value(), context);
                if (regexString.has_value()) {
                  const auto& regex = regexCache.getOrCompute(
                      regexString.value(), [](const std::string& string) {
                        return std::make_unique<RE2>(string, RE2::Quiet);
                      });
                  if (regex->ok()) {
                    return regex.get();
                  }
                }
                return nullptr;
              };
              return this->evaluateGeneralCase(AD_FWD(input), context,
                                               std::move(getNextRegex));
            },
            children_.at(1)->evaluate(context));
      },
      std::move(resultAsVariant));
}

// ____________________________________________________________________________
bool RegexExpression::isPrefixExpression() const {
  return prefixRegex_.has_value();
}

// ____________________________________________________________________________
auto RegexExpression::getEstimatesForFilterExpression(
    uint64_t inputSize,
    const std::optional<Variable>& firstSortedVariable) const -> Estimates {
  // If we have a simple prefix regex, assume that only 10^-k entries remain,
  // where k is the length of the prefix.
  if (isPrefixExpression()) {
    double reductionFactor = std::pow(
        10, std::max(0, static_cast<int>(prefixRegex_.value().size())));
    // Cap to reasonable minimal and maximal values to prevent numerical
    // stability problems.
    reductionFactor = std::min(100000000.0, reductionFactor);
    reductionFactor = std::max(1.0, reductionFactor);
    size_t sizeEstimate = inputSize / static_cast<size_t>(reductionFactor);
    auto varPtr = dynamic_cast<VariableExpression*>(children_.at(0).get());
    size_t costEstimate = (varPtr && firstSortedVariable == varPtr->value())
                              ? sizeEstimate
                              : sizeEstimate + inputSize;

    return {sizeEstimate, costEstimate};
  }

  // For the general case, we make two assumptions.
  //
  // 1. Half of the entries remain after the filter. This is a very simple
  // and arbitrary heuristic.
  //
  // 2. Checking a REGEX for an element is 10 times more expensive than a
  // "simple" filter check. This is reasonable because regex evaluations are
  // expensive, but the fixed factor disregard that it depends on the
  // complexity of the regex how expensive it is.
  size_t sizeEstimate = inputSize / 2;
  size_t costEstimate = sizeEstimate + 10 * inputSize;
  return {sizeEstimate, costEstimate};
}

// ____________________________________________________________________________
void RegexExpression::checkCancellation(const EvaluationContext* context,
                                        ad_utility::source_location location) {
  context->cancellationHandle_->throwIfCancelled(location);
}

}  // namespace sparqlExpression
