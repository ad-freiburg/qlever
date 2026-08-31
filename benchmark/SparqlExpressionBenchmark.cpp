#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "../test/util/IndexTestHelpers.h"
#include "engine/sparqlExpressions/BinaryExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "infrastructure/Benchmark.h"
#include "infrastructure/BenchmarkMeasurementContainer.h"
#include "util/ChunkedForLoop.h"

namespace sparqlExpression::detail {

// The previous generator-based multiplication implementation. This exists only
// for comparison with the new production implementation in this benchmark.
using LegacyMultiply = MakeNumericExpression<std::multiplies<>>;
NARY_EXPRESSION(LegacyMultiplyExpression, 2,
                FV<LegacyMultiply, NumericValueGetter>);

// Addition used only to benchmark the core `BinaryExpression` evaluation
// without child-expression evaluation overhead.
struct BenchmarkAdd {
  ValueId operator()(NumericOrDateValue lhs, NumericOrDateValue rhs) const {
    return std::visit(BenchmarkAdd{}, lhs, rhs);
  }

  ValueId operator()(int64_t lhs, int64_t rhs) const {
    return Id::makeFromInt(lhs + rhs);
  }

  ValueId operator()(int64_t lhs, double rhs) const {
    return Id::makeFromDouble(static_cast<double>(lhs) + rhs);
  }

  ValueId operator()(double lhs, int64_t rhs) const {
    return Id::makeFromDouble(lhs + static_cast<double>(rhs));
  }

  ValueId operator()(double lhs, double rhs) const {
    return Id::makeFromDouble(lhs + rhs);
  }

  template <typename L, typename R>
  ValueId operator()(L, R) const {
    return Id::makeUndefined();
  }
};

}  // namespace sparqlExpression::detail

namespace ad_benchmark {
namespace {

using sparqlExpression::EvaluationContext;
using sparqlExpression::ExpressionResult;
using sparqlExpression::IdExpression;
using sparqlExpression::SparqlExpression;
using sparqlExpression::VariableExpression;
using sparqlExpression::VectorWithMemoryLimit;

// Owns all data referenced by the evaluation context.
struct NumericExpressionBenchmarkContext {
  QueryExecutionContext* qec = ad_utility::testing::getQec("");
  VariableToColumnMap variableToColumnMap;
  LocalVocab localVocab;
  IdTable table{qec->getAllocator()};

  EvaluationContext context{
      *qec,
      variableToColumnMap,
      table.asStaticView<0>(),
      qec->getAllocator(),
      localVocab,
      std::make_shared<ad_utility::CancellationHandle<>>(),
      EvaluationContext::TimePoint::max()};

  explicit NumericExpressionBenchmarkContext(size_t numRows) {
    table.setNumColumns(2);

    for (size_t i = 0; i < numRows; ++i) {
      table.push_back({Id::makeFromInt(static_cast<int64_t>(i)),
                       Id::makeFromInt(static_cast<int64_t>(i + 1))});
    }

    // Refresh the view after populating the table.
    context._inputTable = table.asStaticView<0>();
    context._beginIndex = 0;
    context._endIndex = table.size();

    variableToColumnMap[Variable{"?left"}] = makeAlwaysDefinedColumn(0);
    variableToColumnMap[Variable{"?right"}] = makeAlwaysDefinedColumn(1);
  }
};

// Create the old generator-based vector–vector multiplication expression.
SparqlExpression::Ptr makeLegacyVectorVectorExpression() {
  return std::make_unique<sparqlExpression::detail::LegacyMultiplyExpression>(
      std::make_unique<VariableExpression>(Variable{"?left"}),
      std::make_unique<VariableExpression>(Variable{"?right"}));
}

// Create the old generator-based vector–constant multiplication expression.
SparqlExpression::Ptr makeLegacyVectorConstantExpression() {
  return std::make_unique<sparqlExpression::detail::LegacyMultiplyExpression>(
      std::make_unique<VariableExpression>(Variable{"?left"}),
      std::make_unique<IdExpression>(Id::makeFromInt(2)));
}

// Create the new vector–vector multiplication expression.
SparqlExpression::Ptr makeNewVectorVectorExpression() {
  return sparqlExpression::makeMultiplyExpression(
      std::make_unique<VariableExpression>(Variable{"?left"}),
      std::make_unique<VariableExpression>(Variable{"?right"}));
}

// Create the new vector–constant multiplication expression.
SparqlExpression::Ptr makeNewVectorConstantExpression() {
  return sparqlExpression::makeMultiplyExpression(
      std::make_unique<VariableExpression>(Variable{"?left"}),
      std::make_unique<IdExpression>(Id::makeFromInt(2)));
}

// Evaluate once and verify that the expected result type and size are produced.
void validateResult(SparqlExpression& expression, EvaluationContext& context,
                    size_t expectedSize) {
  ExpressionResult result = expression.evaluate(&context);

  const auto* resultVector = std::get_if<VectorWithMemoryLimit<Id>>(&result);

  AD_CONTRACT_CHECK(resultVector != nullptr);
  AD_CONTRACT_CHECK(resultVector->size() == expectedSize);
}

// Evaluate repeatedly without additional validation inside the timed loop.
void evaluateRepeatedly(SparqlExpression& expression,
                        EvaluationContext& context, size_t repetitions) {
  for (size_t repetition = 0; repetition < repetitions; ++repetition) {
    auto result = expression.evaluate(&context);
    (void)result;
  }
}

void evaluateBinaryAddCoreRepeatedly(ql::span<const ValueId> left,
                                     ql::span<const ValueId> right,
                                     EvaluationContext& context,
                                     size_t repetitions) {
  for (size_t repetition = 0; repetition < repetitions; ++repetition) {
    auto result = sparqlExpression::detail::evaluateBinaryOperation<
        sparqlExpression::detail::BenchmarkAdd,
        sparqlExpression::detail::NumericOrDateValueGetter,
        sparqlExpression::detail::NumericOrDateValueGetter>(left, right,
                                                            &context);
    (void)result;
  }
}

template <typename Function>
void evaluateDirectRepeatedly(Function&& function, size_t repetitions) {
  for (size_t repetition = 0; repetition < repetitions; ++repetition) {
    auto result = function();
    (void)result;
  }
}

void evaluateBinaryAddVectorConstantCoreRepeatedly(ql::span<const ValueId> left,
                                                   ValueId right,
                                                   EvaluationContext& context,
                                                   size_t repetitions) {
  for (size_t repetition = 0; repetition < repetitions; ++repetition) {
    auto result = sparqlExpression::detail::evaluateBinaryOperation<
        sparqlExpression::detail::BenchmarkAdd,
        sparqlExpression::detail::NumericOrDateValueGetter,
        sparqlExpression::detail::NumericOrDateValueGetter>(left, right,
                                                            &context);
    (void)result;
  }
}

bool areAllIntegerPairs(ql::span<const ValueId> left,
                        ql::span<const ValueId> right,
                        EvaluationContext* context) {
  AD_CORRECTNESS_CHECK(left.size() == right.size());

  bool allIntegers = true;

  ad_utility::chunkedForLoop<1000>(
      0, left.size(),
      [&](size_t i, auto breakLoop) {
        if (left[i].getDatatype() != Datatype::Int ||
            right[i].getDatatype() != Datatype::Int) {
          allIntegers = false;
          breakLoop();
        }
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  return allIntegers;
}

bool areAllIntegers(ql::span<const ValueId> values,
                    EvaluationContext* context) {
  bool allIntegers = true;

  ad_utility::chunkedForLoop<1000>(
      0, values.size(),
      [&](size_t i, auto breakLoop) {
        if (values[i].getDatatype() != Datatype::Int) {
          allIntegers = false;
          breakLoop();
        }
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  return allIntegers;
}

VectorWithMemoryLimit<Id> addIntegerVectorConstantUnchecked(
    ql::span<const ValueId> left, ValueId right, EvaluationContext* context) {
  const auto rightValue = right.getInt();

  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(left.size());

  ad_utility::chunkedForLoop<1000>(
      0, left.size(),
      [&](size_t i) {
        result.push_back(Id::makeFromInt(left[i].getInt() + rightValue));
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  return result;
}

std::optional<VectorWithMemoryLimit<Id>> addIntegerVectorConstantChecked(
    ql::span<const ValueId> left, ValueId right, EvaluationContext* context) {
  if (right.getDatatype() != Datatype::Int || !areAllIntegers(left, context)) {
    return std::nullopt;
  }

  return addIntegerVectorConstantUnchecked(left, right, context);
}

std::optional<VectorWithMemoryLimit<Id>> addIntegerVectorConstantSpeculative(
    ql::span<const ValueId> left, ValueId right, EvaluationContext* context) {
  if (right.getDatatype() != Datatype::Int) {
    return std::nullopt;
  }

  const auto rightValue = right.getInt();

  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(left.size());

  bool success = true;

  ad_utility::chunkedForLoop<1000>(
      0, left.size(),
      [&](size_t i, auto breakLoop) {
        if (left[i].getDatatype() != Datatype::Int) {
          success = false;
          breakLoop();
          return;
        }

        result.push_back(Id::makeFromInt(left[i].getInt() + rightValue));
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  if (!success) {
    return std::nullopt;
  }

  return result;
}

VectorWithMemoryLimit<Id> addIntegerVectorsUnchecked(
    ql::span<const ValueId> left, ql::span<const ValueId> right,
    EvaluationContext* context) {
  AD_CORRECTNESS_CHECK(left.size() == right.size());

  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(left.size());

  ad_utility::chunkedForLoop<1000>(
      0, left.size(),
      [&](size_t i) {
        result.push_back(Id::makeFromInt(left[i].getInt() + right[i].getInt()));
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  return result;
}

std::optional<VectorWithMemoryLimit<Id>> addIntegerVectorsChecked(
    ql::span<const ValueId> left, ql::span<const ValueId> right,
    EvaluationContext* context) {
  if (!areAllIntegerPairs(left, right, context)) {
    return std::nullopt;
  }

  return addIntegerVectorsUnchecked(left, right, context);
}

std::optional<VectorWithMemoryLimit<Id>> addIntegerVectorsSpeculative(
    ql::span<const ValueId> left, ql::span<const ValueId> right,
    EvaluationContext* context) {
  AD_CORRECTNESS_CHECK(left.size() == right.size());

  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(left.size());

  bool success = true;

  ad_utility::chunkedForLoop<1000>(
      0, left.size(),
      [&](size_t i, auto breakLoop) {
        if (left[i].getDatatype() != Datatype::Int ||
            right[i].getDatatype() != Datatype::Int) {
          success = false;
          breakLoop();
          return;
        }

        result.push_back(Id::makeFromInt(left[i].getInt() + right[i].getInt()));
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  if (!success) {
    return std::nullopt;
  }

  return result;
}

VectorWithMemoryLimit<Id> makeVectorWithDoubleAt(ql::span<const ValueId> input,
                                                 size_t mismatchIndex,
                                                 EvaluationContext* context) {
  AD_CORRECTNESS_CHECK(mismatchIndex < input.size());

  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(input.size());

  for (size_t i = 0; i < input.size(); ++i) {
    if (i == mismatchIndex) {
      result.push_back(
          Id::makeFromDouble(static_cast<double>(input[i].getInt())));
    } else {
      result.push_back(input[i]);
    }
  }

  return result;
}

void evaluateCheckedWithFallbackRepeatedly(ql::span<const ValueId> left,
                                           ql::span<const ValueId> right,
                                           EvaluationContext& context,
                                           size_t repetitions) {
  for (size_t repetition = 0; repetition < repetitions; ++repetition) {
    auto direct = addIntegerVectorsChecked(left, right, &context);

    if (!direct.has_value()) {
      auto fallback = sparqlExpression::detail::evaluateBinaryOperation<
          sparqlExpression::detail::BenchmarkAdd,
          sparqlExpression::detail::NumericOrDateValueGetter,
          sparqlExpression::detail::NumericOrDateValueGetter>(left, right,
                                                              &context);
      (void)fallback;
    }
  }
}

void evaluateSpeculativeWithFallbackRepeatedly(ql::span<const ValueId> left,
                                               ql::span<const ValueId> right,
                                               EvaluationContext& context,
                                               size_t repetitions) {
  for (size_t repetition = 0; repetition < repetitions; ++repetition) {
    auto direct = addIntegerVectorsSpeculative(left, right, &context);

    if (!direct.has_value()) {
      auto fallback = sparqlExpression::detail::evaluateBinaryOperation<
          sparqlExpression::detail::BenchmarkAdd,
          sparqlExpression::detail::NumericOrDateValueGetter,
          sparqlExpression::detail::NumericOrDateValueGetter>(left, right,
                                                              &context);
      (void)fallback;
    }
  }
}

}  // namespace

class SparqlExpressionBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "SPARQL numeric binary expression benchmark";
  }

  BenchmarkResults runAllBenchmarks() final {
    constexpr size_t numRows = 100'000;
    constexpr size_t repetitions = 50;

    NumericExpressionBenchmarkContext benchmarkContext{numRows};

    auto leftIds = sparqlExpression::detail::getIdsFromVariable(
        Variable{"?left"}, &benchmarkContext.context);
    auto rightIds = sparqlExpression::detail::getIdsFromVariable(
        Variable{"?right"}, &benchmarkContext.context);
    const ValueId constantTwo = Id::makeFromInt(2);
    auto mismatchEarlyStorage =
        makeVectorWithDoubleAt(leftIds, 0, &benchmarkContext.context);
    auto mismatchMiddleStorage =
        makeVectorWithDoubleAt(leftIds, numRows / 2, &benchmarkContext.context);
    auto mismatchLateStorage =
        makeVectorWithDoubleAt(leftIds, numRows - 1, &benchmarkContext.context);

    ql::span<const ValueId> mismatchEarly{mismatchEarlyStorage.data(),
                                          mismatchEarlyStorage.size()};
    ql::span<const ValueId> mismatchMiddle{mismatchMiddleStorage.data(),
                                           mismatchMiddleStorage.size()};
    ql::span<const ValueId> mismatchLate{mismatchLateStorage.data(),
                                         mismatchLateStorage.size()};

    auto legacyVectorVector = makeLegacyVectorVectorExpression();
    auto newVectorVector = makeNewVectorVectorExpression();
    auto legacyVectorConstant = makeLegacyVectorConstantExpression();
    auto newVectorConstant = makeNewVectorConstantExpression();

    // Warm up the multiplication implementations and validate their results.
    validateResult(*legacyVectorVector, benchmarkContext.context, numRows);
    validateResult(*newVectorVector, benchmarkContext.context, numRows);
    validateResult(*legacyVectorConstant, benchmarkContext.context, numRows);
    validateResult(*newVectorConstant, benchmarkContext.context, numRows);

    auto checkedResult =
        addIntegerVectorsChecked(leftIds, rightIds, &benchmarkContext.context);
    auto speculativeResult = addIntegerVectorsSpeculative(
        leftIds, rightIds, &benchmarkContext.context);
    auto uncheckedResult = addIntegerVectorsUnchecked(
        leftIds, rightIds, &benchmarkContext.context);

    auto binaryAddResult = sparqlExpression::detail::evaluateBinaryOperation<
        sparqlExpression::detail::BenchmarkAdd,
        sparqlExpression::detail::NumericOrDateValueGetter,
        sparqlExpression::detail::NumericOrDateValueGetter>(
        leftIds, rightIds, &benchmarkContext.context);

    const auto* binaryAddVector =
        std::get_if<VectorWithMemoryLimit<Id>>(&binaryAddResult);

    AD_CORRECTNESS_CHECK(binaryAddVector != nullptr);
    AD_CORRECTNESS_CHECK(binaryAddVector->size() == numRows);

    AD_CORRECTNESS_CHECK(checkedResult.has_value());
    AD_CORRECTNESS_CHECK(speculativeResult.has_value());
    AD_CORRECTNESS_CHECK(checkedResult->size() == numRows);
    AD_CORRECTNESS_CHECK(speculativeResult->size() == numRows);
    AD_CORRECTNESS_CHECK(uncheckedResult.size() == numRows);

    for (size_t i = 0; i < numRows; ++i) {
      const auto expected = Id::makeFromInt(static_cast<int64_t>(i) +
                                            static_cast<int64_t>(i + 1));
      AD_CORRECTNESS_CHECK((*binaryAddVector)[i] == expected);
      AD_CORRECTNESS_CHECK((*checkedResult)[i] == expected);
      AD_CORRECTNESS_CHECK((*speculativeResult)[i] == expected);
      AD_CORRECTNESS_CHECK(uncheckedResult[i] == expected);
    }

    auto checkedVc = addIntegerVectorConstantChecked(leftIds, constantTwo,
                                                     &benchmarkContext.context);
    auto speculativeVc = addIntegerVectorConstantSpeculative(
        leftIds, constantTwo, &benchmarkContext.context);
    auto uncheckedVc = addIntegerVectorConstantUnchecked(
        leftIds, constantTwo, &benchmarkContext.context);

    AD_CORRECTNESS_CHECK(checkedVc.has_value());
    AD_CORRECTNESS_CHECK(speculativeVc.has_value());
    auto binaryAddVcResult = sparqlExpression::detail::evaluateBinaryOperation<
        sparqlExpression::detail::BenchmarkAdd,
        sparqlExpression::detail::NumericOrDateValueGetter,
        sparqlExpression::detail::NumericOrDateValueGetter>(
        leftIds, constantTwo, &benchmarkContext.context);

    const auto* binaryAddVcVector =
        std::get_if<VectorWithMemoryLimit<Id>>(&binaryAddVcResult);

    AD_CORRECTNESS_CHECK(binaryAddVcVector != nullptr);
    AD_CORRECTNESS_CHECK(binaryAddVcVector->size() == numRows);
    AD_CORRECTNESS_CHECK(checkedVc->size() == numRows);
    AD_CORRECTNESS_CHECK(speculativeVc->size() == numRows);
    AD_CORRECTNESS_CHECK(uncheckedVc.size() == numRows);

    for (size_t i = 0; i < numRows; ++i) {
      const auto expected = Id::makeFromInt(static_cast<int64_t>(i) + 2);
      AD_CORRECTNESS_CHECK((*binaryAddVcVector)[i] == expected);
      AD_CORRECTNESS_CHECK((*checkedVc)[i] == expected);
      AD_CORRECTNESS_CHECK((*speculativeVc)[i] == expected);
      AD_CORRECTNESS_CHECK(uncheckedVc[i] == expected);
    }

    AD_CORRECTNESS_CHECK(!addIntegerVectorsChecked(mismatchEarly, rightIds,
                                                   &benchmarkContext.context)
                              .has_value());
    AD_CORRECTNESS_CHECK(
        !addIntegerVectorsSpeculative(mismatchEarly, rightIds,
                                      &benchmarkContext.context)
             .has_value());

    AD_CORRECTNESS_CHECK(!addIntegerVectorsChecked(mismatchMiddle, rightIds,
                                                   &benchmarkContext.context)
                              .has_value());
    AD_CORRECTNESS_CHECK(
        !addIntegerVectorsSpeculative(mismatchMiddle, rightIds,
                                      &benchmarkContext.context)
             .has_value());

    AD_CORRECTNESS_CHECK(!addIntegerVectorsChecked(mismatchLate, rightIds,
                                                   &benchmarkContext.context)
                              .has_value());
    AD_CORRECTNESS_CHECK(!addIntegerVectorsSpeculative(
                              mismatchLate, rightIds, &benchmarkContext.context)
                              .has_value());

    BenchmarkResults results{};

    results.addMeasurement(
        "Legacy multiplication: vector-vector, 100k rows x 50", [&]() {
          evaluateRepeatedly(*legacyVectorVector, benchmarkContext.context,
                             repetitions);
        });

    results.addMeasurement(
        "BinaryExpression multiplication: vector-vector, 100k rows x 50",
        [&]() {
          evaluateRepeatedly(*newVectorVector, benchmarkContext.context,
                             repetitions);
        });

    results.addMeasurement(
        "Legacy multiplication: vector-constant, 100k rows x 50", [&]() {
          evaluateRepeatedly(*legacyVectorConstant, benchmarkContext.context,
                             repetitions);
        });

    results.addMeasurement(
        "BinaryExpression multiplication: vector-constant, 100k rows x 50",
        [&]() {
          evaluateRepeatedly(*newVectorConstant, benchmarkContext.context,
                             repetitions);
        });

    results.addMeasurement(
        "BinaryExpression add core: vector-vector, 100k rows x 50", [&]() {
          evaluateBinaryAddCoreRepeatedly(
              leftIds, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Checked direct integer add: vector-vector, 100k rows x 50", [&]() {
          evaluateDirectRepeatedly(
              [&]() {
                return addIntegerVectorsChecked(leftIds, rightIds,
                                                &benchmarkContext.context);
              },
              repetitions);
        });

    results.addMeasurement(
        "Speculative direct integer add: vector-vector, 100k rows x 50", [&]() {
          evaluateDirectRepeatedly(
              [&]() {
                return addIntegerVectorsSpeculative(leftIds, rightIds,
                                                    &benchmarkContext.context);
              },
              repetitions);
        });

    results.addMeasurement(
        "Unchecked direct integer add: vector-vector, 100k rows x 50", [&]() {
          evaluateDirectRepeatedly(
              [&]() {
                return addIntegerVectorsUnchecked(leftIds, rightIds,
                                                  &benchmarkContext.context);
              },
              repetitions);
        });

    results.addMeasurement(
        "BinaryExpression add core: vector-constant, 100k rows x 50", [&]() {
          evaluateBinaryAddVectorConstantCoreRepeatedly(
              leftIds, constantTwo, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Checked direct integer add: vector-constant, 100k rows x 50", [&]() {
          evaluateDirectRepeatedly(
              [&]() {
                return addIntegerVectorConstantChecked(
                    leftIds, constantTwo, &benchmarkContext.context);
              },
              repetitions);
        });

    results.addMeasurement(
        "Speculative direct integer add: vector-constant, 100k rows x 50",
        [&]() {
          evaluateDirectRepeatedly(
              [&]() {
                return addIntegerVectorConstantSpeculative(
                    leftIds, constantTwo, &benchmarkContext.context);
              },
              repetitions);
        });

    results.addMeasurement(
        "Unchecked direct integer add: vector-constant, 100k rows x 50", [&]() {
          evaluateDirectRepeatedly(
              [&]() {
                return addIntegerVectorConstantUnchecked(
                    leftIds, constantTwo, &benchmarkContext.context);
              },
              repetitions);
        });

    results.addMeasurement(
        "Mixed baseline: mismatch at 0, 100k rows x 50", [&]() {
          evaluateBinaryAddCoreRepeatedly(
              mismatchEarly, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Checked fallback: mismatch at 0, 100k rows x 50", [&]() {
          evaluateCheckedWithFallbackRepeatedly(
              mismatchEarly, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Speculative fallback: mismatch at 0, 100k rows x 50", [&]() {
          evaluateSpeculativeWithFallbackRepeatedly(
              mismatchEarly, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Mixed baseline: mismatch at 50000, 100k rows x 50", [&]() {
          evaluateBinaryAddCoreRepeatedly(
              mismatchMiddle, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Checked fallback: mismatch at 50000, 100k rows x 50", [&]() {
          evaluateCheckedWithFallbackRepeatedly(
              mismatchMiddle, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Speculative fallback: mismatch at 50000, 100k rows x 50", [&]() {
          evaluateSpeculativeWithFallbackRepeatedly(
              mismatchMiddle, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Mixed baseline: mismatch at 99999, 100k rows x 50", [&]() {
          evaluateBinaryAddCoreRepeatedly(
              mismatchLate, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Checked fallback: mismatch at 99999, 100k rows x 50", [&]() {
          evaluateCheckedWithFallbackRepeatedly(
              mismatchLate, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Speculative fallback: mismatch at 99999, 100k rows x 50", [&]() {
          evaluateSpeculativeWithFallbackRepeatedly(
              mismatchLate, rightIds, benchmarkContext.context, repetitions);
        });

    return results;
  }
};

AD_REGISTER_BENCHMARK(SparqlExpressionBenchmark);

}  // namespace ad_benchmark
