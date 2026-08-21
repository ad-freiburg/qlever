#include "../test/SparqlExpressionTestHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "engine/sparqlExpressions/NaryExpression.h"

namespace ad_benchmark {

class SparqlExpressionBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "SPARQL expression benchmark";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    results.addMeasurement("BatchedAddExpression 100k integer vectors", []() {
      sparqlExpression::TestContext outerContext;
      auto& context = outerContext.context;

      constexpr size_t N = 100'000;
      constexpr int repetitions = 100;

      context._beginIndex = 0;
      context._endIndex = N;

      for (int rep = 0; rep < repetitions; ++rep) {
        sparqlExpression::VectorWithMemoryLimit<ValueId> left{context._allocator};
        sparqlExpression::VectorWithMemoryLimit<ValueId> right{context._allocator};

        left.reserve(N);
        right.reserve(N);

        for (size_t i = 0; i < N; ++i) {
          left.push_back(Id::makeFromInt(static_cast<int64_t>(i)));
          right.push_back(Id::makeFromInt(static_cast<int64_t>(i)));
        }

        auto leftChild = std::make_unique<sparqlExpression::SingleUseExpression>(
            sparqlExpression::ExpressionResult{std::move(left)});
        auto rightChild = std::make_unique<sparqlExpression::SingleUseExpression>(
            sparqlExpression::ExpressionResult{std::move(right)});

        auto expr = sparqlExpression::makeAddExpression(
            std::move(leftChild), std::move(rightChild));

        auto result = expr->evaluate(&context);

        auto* resultVec =
          std::get_if<sparqlExpression::VectorWithMemoryLimit<ValueId>>(&result);

        AD_CONTRACT_CHECK(resultVec != nullptr);
        AD_CONTRACT_CHECK(resultVec->size() == N);
        
        
        (void)result;
      }
    });

    return results;
  }
};

AD_REGISTER_BENCHMARK(SparqlExpressionBenchmark);

}  // namespace ad_benchmark