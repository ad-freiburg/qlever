#include <cstddef>
#include <functional>
#include <memory>
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

template <typename Left, typename Right>
void evaluateBinaryAddCoreRepeatedly(const Left& left, const Right& right,
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

template <typename Left, typename Right>
void evaluateGenericBinaryAddCoreRepeatedly(const Left& left,
                                            const Right& right,
                                            EvaluationContext& context,
                                            size_t repetitions) {
  for (size_t repetition = 0; repetition < repetitions; ++repetition) {
    auto getLeft = sparqlExpression::detail::makeIndexedValueGetter<
        sparqlExpression::detail::NumericOrDateValueGetter>(left, &context);
    auto getRight = sparqlExpression::detail::makeIndexedValueGetter<
        sparqlExpression::detail::NumericOrDateValueGetter>(right, &context);

    sparqlExpression::detail::BenchmarkAdd function;

    VectorWithMemoryLimit<Id> result{context._allocator};
    result.reserve(context.size());

    ad_utility::chunkedForLoop<1000>(
        0, context.size(),
        [&](size_t i) { result.push_back(function(getLeft(i), getRight(i))); },
        [&context]() { context.cancellationHandle_->throwIfCancelled(); });
  }
}

using NumericType = sparqlExpression::detail::HomogeneousNumericType;

template <typename Left, typename Right>
void classifyRepeatedly(const Left& left, const Right& right,
                        EvaluationContext& context, size_t repetitions,
                        NumericType expectedLeft, NumericType expectedRight) {
  for (size_t repetition = 0; repetition < repetitions; ++repetition) {
    const auto classification =
        sparqlExpression::detail::classifyNumericOperands(left, right,
                                                          &context);

    AD_CORRECTNESS_CHECK(classification.left == expectedLeft);
    AD_CORRECTNESS_CHECK(classification.right == expectedRight);
  }
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

VectorWithMemoryLimit<Id> makeDoubleVector(ql::span<const ValueId> input,
                                           EvaluationContext* context) {
  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(input.size());

  for (const auto& id : input) {
    result.push_back(Id::makeFromDouble(static_cast<double>(id.getInt())));
  }

  return result;
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
    auto doubleLeftStorage =
        makeDoubleVector(leftIds, &benchmarkContext.context);
    auto doubleRightStorage =
        makeDoubleVector(rightIds, &benchmarkContext.context);

    ql::span<const ValueId> doubleLeft{doubleLeftStorage.data(),
                                       doubleLeftStorage.size()};
    ql::span<const ValueId> doubleRight{doubleRightStorage.data(),
                                        doubleRightStorage.size()};
    const ValueId constantTwo = Id::makeFromInt(2);
    const ValueId constantTwoDouble = Id::makeFromDouble(2.0);
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

    // Integer vector-vector.
    results.addMeasurement(
        "Generic add: integer vector-vector, 100k rows x 50", [&]() {
          evaluateGenericBinaryAddCoreRepeatedly(
              leftIds, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "BinaryExpression add: integer vector-vector, 100k rows x 50", [&]() {
          evaluateBinaryAddCoreRepeatedly(
              leftIds, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Classification only: integer vector-vector, 100k rows x 50", [&]() {
          classifyRepeatedly(leftIds, rightIds, benchmarkContext.context,
                             repetitions, NumericType::Int, NumericType::Int);
        });

    // Double vector-vector.
    results.addMeasurement(
        "Generic add: double vector-vector, 100k rows x 50", [&]() {
          evaluateGenericBinaryAddCoreRepeatedly(
              doubleLeft, doubleRight, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "BinaryExpression add: double vector-vector, 100k rows x 50", [&]() {
          evaluateBinaryAddCoreRepeatedly(
              doubleLeft, doubleRight, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Classification only: double vector-vector, 100k rows x 50", [&]() {
          classifyRepeatedly(doubleLeft, doubleRight, benchmarkContext.context,
                             repetitions, NumericType::Double,
                             NumericType::Double);
        });

    // Integer vector-constant.
    results.addMeasurement(
        "Generic add: integer vector-constant, 100k rows x 50", [&]() {
          evaluateGenericBinaryAddCoreRepeatedly(
              leftIds, constantTwo, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "BinaryExpression add: integer vector-constant, 100k rows x 50", [&]() {
          evaluateBinaryAddCoreRepeatedly(
              leftIds, constantTwo, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Classification only: integer vector-constant, 100k rows x 50", [&]() {
          classifyRepeatedly(leftIds, constantTwo, benchmarkContext.context,
                             repetitions, NumericType::Int, NumericType::Int);
        });

    // Double vector-constant.
    results.addMeasurement(
        "Generic add: double vector-constant, 100k rows x 50", [&]() {
          evaluateGenericBinaryAddCoreRepeatedly(doubleLeft, constantTwoDouble,
                                                 benchmarkContext.context,
                                                 repetitions);
        });

    results.addMeasurement(
        "BinaryExpression add: double vector-constant, 100k rows x 50", [&]() {
          evaluateBinaryAddCoreRepeatedly(doubleLeft, constantTwoDouble,
                                          benchmarkContext.context,
                                          repetitions);
        });

    results.addMeasurement(
        "Classification only: double vector-constant, 100k rows x 50", [&]() {
          classifyRepeatedly(doubleLeft, constantTwoDouble,
                             benchmarkContext.context, repetitions,
                             NumericType::Double, NumericType::Double);
        });

    // Mixed input and generic fallback.
    results.addMeasurement(
        "Generic mixed add: mismatch at 0, 100k rows x 50", [&]() {
          evaluateGenericBinaryAddCoreRepeatedly(
              mismatchEarly, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "BinaryExpression mixed add: mismatch at 0, 100k rows x 50", [&]() {
          evaluateBinaryAddCoreRepeatedly(
              mismatchEarly, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Generic mixed add: mismatch at 50000, 100k rows x 50", [&]() {
          evaluateGenericBinaryAddCoreRepeatedly(
              mismatchMiddle, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "BinaryExpression mixed add: mismatch at 50000, 100k rows x 50", [&]() {
          evaluateBinaryAddCoreRepeatedly(
              mismatchMiddle, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "Generic mixed add: mismatch at 99999, 100k rows x 50", [&]() {
          evaluateGenericBinaryAddCoreRepeatedly(
              mismatchLate, rightIds, benchmarkContext.context, repetitions);
        });

    results.addMeasurement(
        "BinaryExpression mixed add: mismatch at 99999, 100k rows x 50", [&]() {
          evaluateBinaryAddCoreRepeatedly(
              mismatchLate, rightIds, benchmarkContext.context, repetitions);
        });

    return results;
  }
};

AD_REGISTER_BENCHMARK(SparqlExpressionBenchmark);

}  // namespace ad_benchmark
