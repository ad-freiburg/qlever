// Copyright 2022 - 2026 The QLever Authors, in particular:
//
// 2022 - 2024 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2024 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/sparqlExpressions/PrefixMatchExpression.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cmath>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/RegexExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "global/ValueIdComparators.h"

namespace sparqlExpression {

// _____________________________________________________________________________
PrefixMatchExpression::PrefixMatchExpression(Ptr child, std::string prefix,
                                             Variable variable)
    : child_{std::move(child)},
      prefix_{std::move(prefix)},
      variable_{std::move(variable)} {
  // If we have a `STR()` expression, remove the `STR()` and remember that it
  // was there.
  if (child_->isStrExpression()) {
    child_ = std::move(std::move(*child_).moveChildrenOut().at(0));
    childIsStrExpression_ = true;
  }
}

// _____________________________________________________________________________
std::string PrefixMatchExpression::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  return absl::StrCat("Prefix match expression: ", prefix_,
                      " child:", child_->getCacheKey(varColMap),
                      " str:", childIsStrExpression_);
}

// _____________________________________________________________________________
ql::span<SparqlExpression::Ptr> PrefixMatchExpression::childrenImpl() {
  return {&child_, 1};
}

// _____________________________________________________________________________
ExpressionResult PrefixMatchExpression::evaluate(
    EvaluationContext* context) const {
  auto optColumn = context->getColumnIndexForVariable(variable_);
  if (!optColumn.has_value()) {
    return Id::makeUndefined();
  }

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
  actualPrefixes.push_back("\"" + prefix_);
  if (childIsStrExpression_) {
    actualPrefixes.push_back("<" + prefix_);
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

  // Helper that checks whether a single `Id` lies in (at least) one of the
  // prefix ranges.
  auto matchesPrefix = [&lowerAndUpperIds](Id id) {
    if (id.isUndefined()) {
      return Id::makeUndefined();
    }
    return Id::makeFromBool(
        ql::ranges::any_of(lowerAndUpperIds, [&](const auto& lowerUpper) {
          return !valueIdComparators::compareByBits(id, lowerUpper.first) &&
                 valueIdComparators::compareByBits(id, lowerUpper.second);
        }));
  };

  // When we work on aggregated data (i.e. as part of a GROUP BY, but outside of
  // an aggregate), the variable is grouped and thus constant within each group.
  if (worksOnAggregatedData(context)) {
    AD_CORRECTNESS_CHECK(
        context->_groupedVariables.contains(variable_),
        "A non-grouped variable outside of an aggregate should have been "
        "rejected by the parser");
    return std::visit(
        [context, &matchesPrefix](const auto& childResult) -> ExpressionResult {
          using T = std::decay_t<decltype(childResult)>;
          // Usually the child of a prefix-match expression is a
          // `VariableExpression`, so the result is a single `ValueId`.
          if constexpr (ad_utility::isSimilar<T, ValueId>) {
            return matchesPrefix(childResult);
            // Hash-map based or lazy GROUP BY implementations can lead to the
            // child being replaced by `sparqlExpression::VectorIdExpression`.
            // In this case, we have to apply the prefix check to each value in
            // the vector.
          } else if constexpr (ad_utility::isSimilar<
                                   T, VectorWithMemoryLimit<ValueId>>) {
            VectorWithMemoryLimit<Id> result{context->_allocator};
            result.reserve(childResult.size());
            for (Id id : childResult) {
              result.push_back(matchesPrefix(id));
              checkCancellation(context);
            }
            return ExpressionResult{std::move(result)};
          } else {
            // The child of a prefix-match expression is always a single
            // variable, so this is unreachable.
            AD_FAIL();
          }
        },
        child_->evaluate(context));
  }

  // Begin and end of the input (for each row of which we want to
  // evaluate the prefix match).
  auto beg = context->_inputTable.begin() + context->_beginIndex;
  auto end = context->_inputTable.begin() + context->_endIndex;
  AD_CONTRACT_CHECK(end <= context->_inputTable.end());

  // In this function, the expression is a simple variable. If the input is
  // sorted by that variable, the result can be computed by a constant number
  // of binary searches and the result is a set of intervals.
  const auto& column = optColumn.value();
  if (context->isResultSortedBy(variable_) &&
      (beg == end || !(*beg)[column].isUndefined())) {
    std::vector<ad_utility::SetOfIntervals> resultSetOfIntervals;
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
    return ::ranges::accumulate(resultSetOfIntervals,
                                ad_utility::SetOfIntervals{},
                                ad_utility::SetOfIntervals::Union{});
  }

  // If the input is not sorted by the variable, we have to check each row
  // individually (by checking inclusion in the ranges).
  auto resultSize = context->size();
  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(resultSize);
  for (auto id : detail::makeGenerator(variable_, resultSize, context)) {
    result.push_back(matchesPrefix(id));
    checkCancellation(context);
  }
  return result;
}

// _____________________________________________________________________________
auto PrefixMatchExpression::getEstimatesForFilterExpression(
    uint64_t inputSize,
    const std::optional<Variable>& firstSortedVariable) const -> Estimates {
  // Assume that only 10^-k entries remain, where k is the length of the prefix
  // and cap to reasonable maximal values to prevent numerical stability
  // problems.
  double reductionFactor = std::pow(10, std::min<size_t>(8, prefix_.size()));
  size_t sizeEstimate = inputSize / static_cast<size_t>(reductionFactor);
  size_t costEstimate = firstSortedVariable == variable_
                            ? sizeEstimate
                            : sizeEstimate + inputSize;

  return {sizeEstimate, costEstimate};
}

// _____________________________________________________________________________
void PrefixMatchExpression::checkCancellation(
    const EvaluationContext* context, ad_utility::source_location location) {
  context->cancellationHandle_->throwIfCancelled(location);
}

// _____________________________________________________________________________
std::vector<PrefilterExprVariablePair>
PrefixMatchExpression::getPrefilterExpressionForMetadata(
    [[maybe_unused]] const LocalVocabContext& context,
    [[maybe_unused]] bool isNegated) const {
  // It is currently not possible to prefilter prefix matches involving
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
              asNormalizedStringViewUnsafe(prefix_))),
      variable_);
  return prefilterVec;
}

// _____________________________________________________________________________
SparqlExpression::Ptr makePrefixMatchExpression(
    SparqlExpression::Ptr string, const SparqlExpression::Ptr& prefix) {
  const auto* variableExpression = dynamic_cast<const VariableExpression*>(
      string->isStrExpression() ? string->children()[0].get() : string.get());
  if (!variableExpression) {
    throw std::runtime_error{
        "ql:prefix-match does only support STR(?var) or ?var as the first "
        "argument"};
  }
  auto stringLiteralExpression =
      dynamic_cast<const StringLiteralExpression*>(&*prefix);
  if (!stringLiteralExpression) {
    throw std::runtime_error{
        "ql:prefix-match does only support static string literals as the "
        "second argument"};
  }
  const auto& stringLiteral = stringLiteralExpression->value();
  detail::ensureIsSimpleLiteral(stringLiteral);
  return std::make_unique<PrefixMatchExpression>(
      std::move(string),
      std::string{asStringViewUnsafe(stringLiteral.getContent())},
      variableExpression->value());
}

}  // namespace sparqlExpression
