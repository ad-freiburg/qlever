// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#include "engine/tensor/aggregate/TensorAggregate.h"

#include <absl/strings/str_cat.h>

#include <cmath>

#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "index/TripleComponentConversions.h"

namespace sparqlExpression::detail {

using ad_utility::TensorData;

// _____________________________________________________________________________
void TensorAccumulator::add(const std::optional<TensorData>& tensor) {
  if (!tensor.has_value()) {
    invalidate();
    return;
  }
  add(tensor.value());
}

// _____________________________________________________________________________
void TensorAccumulator::add(const TensorData& tensor) {
  if (!valid_) {
    return;
  }
  const auto& data = tensor.tensorData();
  if (count_ == 0) {
    shape_ = tensor.shape();
    dtype_ = tensor.dtype();
    sumOrMean_.assign(data.size(), 0.0);
    if (needsVariance()) {
      m2_.assign(data.size(), 0.0);
    }
  } else if (tensor.shape() != shape_ || data.size() != sumOrMean_.size()) {
    // Tensors of different shapes cannot be aggregated elementwise.
    invalidate();
    return;
  }
  ++count_;

  if (!needsVariance()) {
    for (size_t i = 0; i < data.size(); ++i) {
      sumOrMean_[i] += static_cast<double>(data[i]);
    }
    return;
  }

  // Welford's online algorithm.
  const auto n = static_cast<double>(count_);
  for (size_t i = 0; i < data.size(); ++i) {
    const auto x = static_cast<double>(data[i]);
    const double delta = x - sumOrMean_[i];
    sumOrMean_[i] += delta / n;
    m2_[i] += delta * (x - sumOrMean_[i]);
  }
}

// _____________________________________________________________________________
std::optional<TensorData> TensorAccumulator::finish() const {
  if (!valid_ || count_ == 0) {
    return std::nullopt;
  }
  const auto n = static_cast<double>(count_);
  std::vector<float> result(sumOrMean_.size());
  auto transform = [&](auto f) {
    for (size_t i = 0; i < result.size(); ++i) {
      result[i] = static_cast<float>(f(i));
    }
  };
  // The sample variance of element `i`, zero if there is only one value.
  auto variance = [&](size_t i) { return count_ > 1 ? m2_[i] / (n - 1) : 0.0; };
  switch (kind_) {
    case TensorAggregateKind::Sum:
      transform([&](size_t i) { return sumOrMean_[i]; });
      break;
    case TensorAggregateKind::Avg:
      transform([&](size_t i) { return sumOrMean_[i] / n; });
      break;
    case TensorAggregateKind::Var:
      transform(variance);
      break;
    case TensorAggregateKind::Std:
      transform([&](size_t i) { return std::sqrt(variance(i)); });
      break;
  }
  // The sum of integer/bool tensors keeps the dtype, all other aggregates of
  // non-floating-point tensors are floating point values.
  using enum TensorData::DType;
  auto dtype = dtype_;
  if (kind_ != TensorAggregateKind::Sum && dtype != FLOAT && dtype != DOUBLE) {
    dtype = FLOAT;
  }
  return TensorData(std::move(result), shape_, dtype);
}

// _____________________________________________________________________________
TensorAggregateExpressionBase::TensorAggregateExpressionBase(
    bool distinct, Ptr&& child, TensorAggregateKind kind)
    : distinct_{distinct}, child_{std::move(child)}, kind_{kind} {
  setIsInsideAggregate();
}

// _____________________________________________________________________________
ExpressionResult TensorAggregateExpressionBase::evaluate(
    EvaluationContext* context) const {
  auto impl = [this, context](auto&& el)
      -> CPP_ret(ExpressionResult)(
          requires SingleExpressionResult<decltype(el)>) {
    using T = std::decay_t<decltype(el)>;
    const size_t inputSize = context->size();
    if (inputSize == 0) {
      return resultForEmptyGroup();
    }
    TensorAccumulator accumulator{kind_};
    if constexpr (isConstantResult<T>) {
      // The same value for every row: fetch (and parse) the tensor only once.
      auto tensor = TensorValueGetter{}(AD_FWD(el), context);
      const size_t numValues = distinct_ ? 1 : inputSize;
      for (size_t i = 0; i < numValues && accumulator.isValid(); ++i) {
        accumulator.add(tensor);
      }
    } else {
      auto accumulate = [&accumulator, context](auto&& operands) {
        for (auto& operand : operands) {
          accumulator.add(TensorValueGetter{}(std::move(operand), context));
          if (!accumulator.isValid()) {
            return;
          }
          context->cancellationHandle_->throwIfCancelled();
        }
      };
      auto generator = makeGenerator(AD_FWD(el), inputSize, context);
      if (distinct_) {
        accumulate(
            getUniqueElements(context, inputSize, std::move(generator)));
      } else {
        accumulate(std::move(generator));
      }
    }
    auto result = accumulator.finish();
    if (!result.has_value()) {
      return Id::makeUndefined();
    }
    return toIdOrLocalVocabEntry(result.value().toLiteral(),
                                 context->getLocalVocabContext());
  };

  return std::visit(impl, child_->evaluate(context));
}

// _____________________________________________________________________________
std::string TensorAggregateExpressionBase::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  return absl::StrCat("[ TENSOR_AGGREGATE ", static_cast<int>(kind_),
                      distinct_ ? " DISTINCT" : "", " ]",
                      child_->getCacheKey(varColMap));
}

}  // namespace sparqlExpression::detail
