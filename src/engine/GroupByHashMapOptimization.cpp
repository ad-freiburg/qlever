//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "engine/GroupByHashMapOptimization.h"

// _____________________________________________________________________________
[[nodiscard]] ValueId AvgAggregationData::calculateResult(
    [[maybe_unused]] const LocalVocab* localVocab) const {
  if (error_) {
    return ValueId::makeUndefined();
  }
  // AVG(empty group) = 0, this is mandated by the SPARQL 1.1 standard.
  if (count_ == 0) {
    return ValueId::makeFromInt(0);
  }

  return ValueId::makeFromDouble(sum_ / static_cast<double>(count_));
}

// _____________________________________________________________________________
[[nodiscard]] ValueId CountAggregationData::calculateResult(
    [[maybe_unused]] const LocalVocab* localVocab) const {
  return ValueId::makeFromInt(count_);
}

// _____________________________________________________________________________
template <valueIdComparators::Comparison Comp>
[[nodiscard]] ValueId ExtremumAggregationData<Comp>::calculateResult(
    LocalVocab* localVocab) const {
  return sparqlExpression::detail::idOrLiteralOrIriToId(currentValue_,
                                                        localVocab);
}

template struct ExtremumAggregationData<valueIdComparators::Comparison::LT>;
template struct ExtremumAggregationData<valueIdComparators::Comparison::GT>;

// _____________________________________________________________________________
[[nodiscard]] ValueId SumAggregationData::calculateResult(
    [[maybe_unused]] const LocalVocab* localVocab) const {
  if (error_) {
    return ValueId::makeUndefined();
  }
  if (intSumValid_) {
    return ValueId::makeFromInt(intSum_);
  }
  return ValueId::makeFromDouble(sum_);
}

// _____________________________________________________________________________
[[nodiscard]] ValueId GroupConcatAggregationData::calculateResult(
    LocalVocab* localVocab) const {
  using namespace ad_utility::triple_component;
  using Lit = ad_utility::triple_component::Literal;
  auto localVocabIndex = localVocab->getIndexAndAddIfNotContained(
      LiteralOrIri{Lit::literalWithNormalizedContent(
          asNormalizedStringViewUnsafe(currentValue_))});
  return ValueId::makeFromLocalVocabIndex(localVocabIndex);
}

// _____________________________________________________________________________
[[nodiscard]] ValueId SampleAggregationData::calculateResult(
    LocalVocab* localVocab) const {
  if (!value_.has_value()) {
    return Id::makeUndefined();
  }

  return sparqlExpression::detail::idOrLiteralOrIriToId(value_.value(),
                                                        localVocab);
}
