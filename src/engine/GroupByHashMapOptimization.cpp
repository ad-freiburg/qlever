//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "engine/GroupByHashMapOptimization.h"

// _____________________________________________________________________________
[[nodiscard]] ValueId AvgAggregationData::calculateResult(
    [[maybe_unused]] const IndexImpl& index,
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
    [[maybe_unused]] const IndexImpl& index,
    [[maybe_unused]] const LocalVocab* localVocab) const {
  return ValueId::makeFromInt(count_);
}

// _____________________________________________________________________________
template <valueIdComparators::Comparison Comp>
[[nodiscard]] ValueId ExtremumAggregationData<Comp>::calculateResult(
    [[maybe_unused]] const IndexImpl& index, LocalVocab* localVocab) const {
  return sparqlExpression::detail::idOrLiteralOrIriToId(currentValue_,
                                                        localVocab);
}

template struct ExtremumAggregationData<valueIdComparators::Comparison::LT>;
template struct ExtremumAggregationData<valueIdComparators::Comparison::GT>;

// _____________________________________________________________________________
[[nodiscard]] ValueId SumAggregationData::calculateResult(
    [[maybe_unused]] const IndexImpl& index,
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
void GroupConcatAggregationData::addValueImpl(
    const std::optional<ad_utility::triple_component::Literal>& val) {
  if (first_) {
    first_ = false;
  } else {
    currentValue_.append(separator_);
  }
  if (val.has_value()) {
    currentValue_.append(asStringViewUnsafe(val.value().getContent()));
  } else {
    undefined_ = true;
  }
}

// _____________________________________________________________________________
[[nodiscard]] ValueId GroupConcatAggregationData::calculateResult(
    const IndexImpl& index, LocalVocab* localVocab) const {
  if (undefined_) {
    return ValueId::makeUndefined();
  }
  using namespace ad_utility::triple_component;
  auto localVocabIndex =
      localVocab->getIndexAndAddIfNotContained(LocalVocabEntry{
          ad_utility::triple_component::Literal::literalWithNormalizedContent(
              asNormalizedStringViewUnsafe(currentValue_)),
          index});
  return ValueId::makeFromLocalVocabIndex(localVocabIndex);
}

// _____________________________________________________________________________
GroupConcatAggregationData::GroupConcatAggregationData(
    std::string_view separator)
    : separator_{separator} {
  currentValue_.reserve(20000);
}

// _____________________________________________________________________________
void GroupConcatAggregationData::reset() {
  undefined_ = false;
  first_ = true;
  currentValue_.clear();
}

// _____________________________________________________________________________
[[nodiscard]] ValueId SampleAggregationData::calculateResult(
    [[maybe_unused]] const IndexImpl& index, LocalVocab* localVocab) const {
  if (!value_.has_value()) {
    return Id::makeUndefined();
  }

  return sparqlExpression::detail::idOrLiteralOrIriToId(value_.value(),
                                                        localVocab);
}
