// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#ifndef QLEVER_SRC_ENGINE_TENSOR_AGGREGATE_TENSORAGGREGATE_H
#define QLEVER_SRC_ENGINE_TENSOR_AGGREGATE_TENSORAGGREGATE_H

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "rdfTypes/TensorData.h"

namespace sparqlExpression {
namespace detail {

// The elementwise tensor aggregates `dta:sum`, `dta:avg`, `dta:var`, `dta:std`.
enum class TensorAggregateKind { Sum, Avg, Var, Std };

// Accumulates tensors of identical shape elementwise and in place (no
// intermediate `TensorData` objects are created per value). The accumulation is
// done in `double` for precision; the result is converted back to a `float`
// tensor with the shape and dtype of the first accumulated tensor.
//
// For `Var` and `Std`, Welford's online algorithm is used (numerically stable
// single pass). The variance is the SAMPLE variance (division by `n - 1`),
// consistent with the `STDEV` aggregate of QLever, and is defined as zero for a
// single value (no degrees of freedom). `Std` is the square root of `Var`.
//
// The accumulator becomes invalid (and `finish` returns `std::nullopt`) as soon
// as a value is not a tensor (including UNDEF) or has a shape that differs
// from the first tensor. This mirrors the semantics of `SUM`/`AVG` for
// non-numeric values. An empty input also yields `std::nullopt`.
class TensorAccumulator {
 public:
  explicit TensorAccumulator(TensorAggregateKind kind) : kind_{kind} {}

  // Add the next value. `std::nullopt` (not a tensor) invalidates the result.
  void add(const std::optional<ad_utility::TensorData>& tensor);
  void add(const ad_utility::TensorData& tensor);

  // Mark the result as undefined.
  void invalidate() { valid_ = false; }

  bool isValid() const { return valid_; }
  size_t count() const { return count_; }

  // The aggregated tensor, or `std::nullopt` if the input was empty or
  // invalid.
  std::optional<ad_utility::TensorData> finish() const;

 private:
  bool needsVariance() const {
    return kind_ == TensorAggregateKind::Var ||
           kind_ == TensorAggregateKind::Std;
  }

  TensorAggregateKind kind_;
  bool valid_ = true;
  size_t count_ = 0;
  std::vector<size_t> shape_;
  ad_utility::TensorData::DType dtype_ = ad_utility::TensorData::DType::FLOAT;
  // The running sum (`Sum`, `Avg`) or the running mean (`Var`, `Std`).
  std::vector<double> sumOrMean_;
  // The running sum of squared deviations from the mean (`Var`, `Std` only).
  std::vector<double> m2_;
};

// Common implementation of the tensor aggregate expressions.
class TensorAggregateExpressionBase : public SparqlExpression {
 public:
  TensorAggregateExpressionBase(bool distinct, Ptr&& child,
                                TensorAggregateKind kind);

  ExpressionResult evaluate(EvaluationContext* context) const override;

  AggregateStatus isAggregate() const override {
    return distinct_ ? AggregateStatus::DistinctAggregate
                     : AggregateStatus::NonDistinctAggregate;
  }

  [[nodiscard]] std::string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

  [[nodiscard]] bool isDeterministic() const override {
    return child_->isDeterministic();
  }

  // The result for an empty group (needed for implicit GROUP BY), which is
  // always UNDEF for the tensor aggregates.
  ValueId resultForEmptyGroup() const { return Id::makeUndefined(); }

  TensorAggregateKind kind() const { return kind_; }

 private:
  ql::span<SparqlExpression::Ptr> childrenImpl() override {
    return {&child_, 1};
  }

  bool distinct_;
  Ptr child_;
  TensorAggregateKind kind_;
};

// One concrete (and therefore distinguishable via `typeid`/`dynamic_cast`)
// class per aggregate.
template <TensorAggregateKind Kind>
class TensorAggregateExpression : public TensorAggregateExpressionBase {
 public:
  TensorAggregateExpression(bool distinct, Ptr&& child)
      : TensorAggregateExpressionBase(distinct, std::move(child), Kind) {}
};

}  // namespace detail

using detail::TensorAggregateKind;
// dta:sum
using TensorSumExpression =
    detail::TensorAggregateExpression<TensorAggregateKind::Sum>;
// dta:avg
using TensorAvgExpression =
    detail::TensorAggregateExpression<TensorAggregateKind::Avg>;
// dta:var (sample variance)
using TensorVarExpression =
    detail::TensorAggregateExpression<TensorAggregateKind::Var>;
// dta:std (sample standard deviation)
using TensorStdExpression =
    detail::TensorAggregateExpression<TensorAggregateKind::Std>;

}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_TENSOR_AGGREGATE_TENSORAGGREGATE_H
