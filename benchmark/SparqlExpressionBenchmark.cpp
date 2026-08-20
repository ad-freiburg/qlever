#include <cstddef>
#include <functional>
#include <memory>
#include <string>

#include "../test/util/IndexTestHelpers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "infrastructure/Benchmark.h"
#include "infrastructure/BenchmarkMeasurementContainer.h"

namespace sparqlExpression::detail {

// The previous generator-based multiplication implementation. This exists only
// for comparison with the new production implementation in this benchmark.
using LegacyMultiply = MakeNumericExpression<std::multiplies<>>;
NARY_EXPRESSION(LegacyMultiplyExpression, 2,
                FV<LegacyMultiply, NumericValueGetter>);

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

    auto legacyVectorVector = makeLegacyVectorVectorExpression();
    auto newVectorVector = makeNewVectorVectorExpression();
    auto legacyVectorConstant = makeLegacyVectorConstantExpression();
    auto newVectorConstant = makeNewVectorConstantExpression();

    // Warm up all implementations and validate their result types and sizes.
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

    return results;
  }
};

AD_REGISTER_BENCHMARK(SparqlExpressionBenchmark);

}  // namespace ad_benchmark
