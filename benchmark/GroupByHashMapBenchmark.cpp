// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2024      Fabian Krause (fabian.krause@students.uni-freiburg.de)

#include <random>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../test/engine/ValuesForTesting.h"
#include "../test/util/IdTableHelpers.h"
#include "../test/util/IndexTestHelpers.h"
#include "engine/GroupBy.h"
#include "engine/Sort.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "global/RuntimeParameters.h"
#include "util/Log.h"
#include "util/Random.h"
#include "util/TypeIdentity.h"

namespace ad_benchmark {
using namespace sparqlExpression;
using namespace ad_utility::use_type_identity;

static SparqlExpression::Ptr makeVariableExpression(const Variable& var) {
  return std::make_unique<VariableExpression>(var);
}

auto generateRandomDoubleVec =
    [gen = ad_utility::RandomDoubleGenerator{}](size_t n) mutable {
      std::vector<double> res;
      for ([[maybe_unused]] auto i : ad_utility::integerRange(n)) {
        res.push_back(gen());
      }
      return res;
    };

// Create a vector filled with `n` values in [0...g) with random permutation.
auto generateRandomGroupVec =
    [](size_t n, size_t g,
       ad_utility::RandomSeed seed =
           ad_utility::RandomSeed::make(std::random_device{}())) {
      std::mt19937_64 randomEngine{seed.get()};

      std::vector<size_t> result(n);

      for (size_t i = 0; i < n; ++i) {
        result[i] = i % g;
      }
      std::shuffle(result.begin(), result.end(), randomEngine);

      return result;
    };

// Create a sorted vector filled with `n` values in [0...g).
auto generateSortedGroupVec = [](size_t n, size_t g) {
  std::vector<size_t> result(n);

  size_t rowsPerGroup = std::ceil(n / g);
  for (size_t i = 0; i < n; i++) {
    result[i] = std::floor(i / rowsPerGroup);
  }

  return result;
};

// Create a local vocab of random strings and a vector of the local vocab
// indices.
auto generateRandomLocalVocabAndIndicesVec = [](size_t n, size_t m) {
  LocalVocab localVocab;
  std::vector<LocalVocabIndex> indices;

  std::string alphanum =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  auto gen = ad_utility::SlowRandomIntGenerator<size_t>{0, alphanum.size() - 1};
  for ([[maybe_unused]] auto i : ad_utility::integerRange(n)) {
    std::string str;
    str.reserve(m);
    for (size_t j = 0; j < m; j++) {
      str += alphanum.at(gen());
    }
    indices.push_back(localVocab.getIndexAndAddIfNotContained(str));
  }

  return std::make_pair(std::move(localVocab), indices);
};

enum class ValueIdType { OnlyInt, OnlyDouble, RandomlyMixed, Strings };

constexpr std::array<ValueIdType, 3> numericValueIdTypes = {
    ValueIdType::OnlyInt, ValueIdType::OnlyDouble, ValueIdType::RandomlyMixed};

auto determineTypeString = [](ValueIdType type) {
  if (type == ValueIdType::OnlyDouble)
    return "Double";
  else if (type == ValueIdType::OnlyInt)
    return "Int";
  else if (type == ValueIdType::RandomlyMixed)
    return "Double & Int";
  else if (type == ValueIdType::Strings)
    return "String";
  else
    AD_THROW("ValueIdType not found.");
};

auto determineAggregateString = []<typename T>(TI<T>) {
  if constexpr (std::same_as<T, MinExpression>)
    return "MIN";
  else if constexpr (std::same_as<T, MaxExpression>)
    return "MAX";
  else if constexpr (std::same_as<T, AvgExpression>)
    return "AVG";
  else if constexpr (std::same_as<T, SumExpression>)
    return "SUM";
  else if constexpr (std::same_as<T, CountExpression>)
    return "COUNT";
  else if constexpr (std::same_as<T, GroupConcatExpression>)
    return "GROUP_CONCAT";
  else
    AD_THROW("Unsupported expression. Is this an aggregate?");
};

class GroupByHashMapBenchmark : public BenchmarkInterface {
  [[nodiscard]] std::string name() const final {
    return "Benchmarks for Group By using Hash Maps";
  }

  void runNumericBenchmarks(BenchmarkResults& results) {
    for (auto multiplicity : multiplicities) {
      for (auto valueIdType : numericValueIdTypes) {
        //-----------------------------------------------------------------------------------------------------
        runTests<AvgExpression>(results, multiplicity, valueIdType, false,
                                false);

        runTests<AvgExpression>(results, multiplicity, valueIdType, true,
                                false);

        //-----------------------------------------------------------------------------------------------------
        runTests<SumExpression>(results, multiplicity, valueIdType, false,
                                false);

        runTests<SumExpression>(results, multiplicity, valueIdType, true,
                                false);

        //-----------------------------------------------------------------------------------------------------
        runTests<CountExpression>(results, multiplicity, valueIdType, false,
                                  false);

        runTests<CountExpression>(results, multiplicity, valueIdType, true,
                                  false);

        //-----------------------------------------------------------------------------------------------------
        runTests<MinExpression>(results, multiplicity, valueIdType, false,
                                false);

        runTests<MinExpression>(results, multiplicity, valueIdType, true,
                                false);

        //-----------------------------------------------------------------------------------------------------
        runTests<MaxExpression>(results, multiplicity, valueIdType, false,
                                false);

        runTests<MaxExpression>(results, multiplicity, valueIdType, true,
                                false);
      }
    }
  }

  void runTwoAggregateBenchmarks(BenchmarkResults& results) {
    for (auto multiplicity : multiplicities) {
      for (auto valueIdType : numericValueIdTypes) {
        //-----------------------------------------------------------------------------------------------------
        runTests<AvgExpression, SumExpression>(results, multiplicity,
                                               valueIdType, false, false);

        runTests<AvgExpression, SumExpression>(results, multiplicity,
                                               valueIdType, true, false);

        //-----------------------------------------------------------------------------------------------------
        runTests<AvgExpression, MaxExpression>(results, multiplicity,
                                               valueIdType, false, false);

        runTests<AvgExpression, MaxExpression>(results, multiplicity,
                                               valueIdType, true, false);

        //-----------------------------------------------------------------------------------------------------
        runTests<AvgExpression, MinExpression>(results, multiplicity,
                                               valueIdType, false, false);

        runTests<AvgExpression, MinExpression>(results, multiplicity,
                                               valueIdType, true, false);

        //-----------------------------------------------------------------------------------------------------
        runTests<AvgExpression, CountExpression>(results, multiplicity,
                                                 valueIdType, false, false);

        runTests<AvgExpression, CountExpression>(results, multiplicity,
                                                 valueIdType, true, false);
      }
    }
  }

  void runStringBenchmarks(BenchmarkResults& results) {
    for (auto multiplicity : multiplicities) {
      runTests<GroupConcatExpression>(results, multiplicity,
                                      ValueIdType::Strings, false, false);
      runTests<GroupConcatExpression>(results, multiplicity,
                                      ValueIdType::Strings, true, false);
    }
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    // runTwoAggregateBenchmarks(results);
    // runStringBenchmarks(results);
    runNumericBenchmarks(results);

    return results;
  }

 private:
  std::random_device rd;
  std::mt19937_64 randomEngine{rd()};
  static constexpr size_t numInputRows = 10'000'000;
  static constexpr size_t numMeasurements = 4;
  static constexpr size_t multiplicities[] = {
      5'000'000, 500'000, 50'000, 5'000, 500, 50, 5, 3, 1};
  static constexpr size_t randomStringLength = 3;

  template <typename T>
  static void computeGroupBy(QueryExecutionContext* qec,
                             std::shared_ptr<QueryExecutionTree> subtree,
                             bool useOptimization) {
    RuntimeParameters().set<"group-by-hash-map-enabled">(useOptimization);

    using namespace sparqlExpression;

    auto createExpression = []<typename A>(TI<A>) {
      if constexpr (std::same_as<A, GroupConcatExpression>)
        return std::make_unique<T>(false,
                                   makeVariableExpression(Variable{"?b"}), "'");
      else
        return std::make_unique<T>(false,
                                   makeVariableExpression(Variable{"?b"}));
    };

    // Create `Alias` object
    auto expr1 = createExpression(ti<T>);
    auto alias1 =
        Alias{SparqlExpressionPimpl{std::move(expr1), "AGGREGATE(?b)"},
              Variable{"?x"}};

    // SELECT (AGGREGATE(?b) AS ?x) WHERE {
    //   VALUES ...
    // } GROUP BY ?a
    GroupBy groupBy{ad_utility::testing::getQec(),
                    {Variable{"?a"}},
                    {std::move(alias1)},
                    std::move(subtree)};
    auto result = groupBy.getResult();
    (void)result->idTable();

    qec->clearCacheUnpinnedOnly();
  };

  template <typename T1, typename T2>
  static void computeGroupByTwoAggregates(
      QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree,
      bool useOptimization) {
    RuntimeParameters().set<"group-by-hash-map-enabled">(useOptimization);

    using namespace sparqlExpression;

    auto createExpression1 = []<typename A>(TI<A>) {
      if constexpr (std::same_as<A, GroupConcatExpression>)
        return std::make_unique<T1>(
            false, makeVariableExpression(Variable{"?b"}), "'");
      else
        return std::make_unique<T1>(false,
                                    makeVariableExpression(Variable{"?b"}));
    };

    auto createExpression2 = []<typename A>(TI<A>) {
      if constexpr (std::same_as<A, GroupConcatExpression>)
        return std::make_unique<T2>(
            false, makeVariableExpression(Variable{"?b"}), "'");
      else
        return std::make_unique<T2>(false,
                                    makeVariableExpression(Variable{"?b"}));
    };

    // Create `Alias` object
    auto expr1 = createExpression1(ti<T1>);
    auto alias1 =
        Alias{SparqlExpressionPimpl{std::move(expr1), "AGGREGATE1(?b)"},
              Variable{"?x"}};

    // Create `Alias` object
    auto expr2 = createExpression2(ti<T2>);
    auto alias2 =
        Alias{SparqlExpressionPimpl{std::move(expr2), "AGGREGATE2(?b)"},
              Variable{"?y"}};

    // SELECT (AGGREGATE1(?b) AS ?x) (AGGREGATE2(?b) AS ?y) WHERE {
    //   VALUES ...
    // } GROUP BY ?a
    GroupBy groupBy{ad_utility::testing::getQec(),
                    {Variable{"?a"}},
                    {std::move(alias1), std::move(alias2)},
                    std::move(subtree)};
    auto result = groupBy.getResult();
    (void)result->idTable();

    qec->clearCacheUnpinnedOnly();
  };

  template <typename T1, typename T2 = std::nullopt_t>
  void runTests(BenchmarkResults& results, size_t multiplicity,
                ValueIdType valueTypes, bool optimizationEnabled, bool sorted) {
    // For coin flipping if `ValueIdType` is `RandomlyMixed`
    std::uniform_int_distribution<uint8_t> distribution(0, 1);

    // Initialize benchmark results group
    std::ostringstream buffer;
    std::ostringstream opString;
    if constexpr (std::same_as<T2, std::nullopt_t>) {
      opString << determineAggregateString(ti<T1>);
    } else {
      opString << determineAggregateString(ti<T1>) << ", "
               << determineAggregateString(ti<T2>);
    }
    buffer << "M: " << multiplicity
           << ", T: " << determineTypeString(valueTypes)
           << ", OP: " << opString.str() << ", MAP: " << std::boolalpha
           << optimizationEnabled << ", SORTED: " << sorted;
    auto& group = results.addGroup(buffer.str());
    group.metadata().addKeyValuePair("Rows", numInputRows);
    group.metadata().addKeyValuePair("Multiplicity", multiplicity);
    group.metadata().addKeyValuePair("Type", determineTypeString(valueTypes));
    group.metadata().addKeyValuePair("Sorted", sorted);
    group.metadata().addKeyValuePair("HashMap", optimizationEnabled);
    group.metadata().addKeyValuePair("Operation", opString.str());

    // Create `ValuesForTesting` object
    auto qec = ad_utility::testing::getQec();
    IdTable table{qec->getAllocator()};
    table.setNumColumns(2);
    table.resize(numInputRows);

    // Create entries of first column
    decltype(auto) groupValues = table.getColumn(0);
    size_t numGroups = numInputRows / multiplicity;
    std::vector<unsigned long> firstColumn;
    if (sorted) {
      firstColumn = generateSortedGroupVec(numInputRows, numGroups);
    } else {
      firstColumn = generateRandomGroupVec(numInputRows, numGroups);
    }
    std::ranges::transform(
        firstColumn.begin(), firstColumn.end(), groupValues.begin(),
        [](size_t value) {
          return ValueId::makeFromInt(static_cast<int64_t>(value));
        });

    // Create entries of second column
    decltype(auto) otherValues = table.getColumn(1);
    auto localVocab = LocalVocab{};
    if (valueTypes != ValueIdType::Strings) {
      auto secondColumn = generateRandomDoubleVec(numInputRows);
      std::ranges::transform(
          secondColumn.begin(), secondColumn.end(), otherValues.begin(),
          [&](double value) {
            if (valueTypes == ValueIdType::OnlyDouble)
              return ValueId::makeFromDouble(value);
            else if (valueTypes == ValueIdType::OnlyInt)
              return ValueId::makeFromInt(std::ceil(value));
            else if (valueTypes == ValueIdType::RandomlyMixed) {
              // Toss a coin to determine whether the number is a double or int
              if (distribution(randomEngine))
                return ValueId::makeFromDouble(value);
              else
                return ValueId::makeFromInt(std::ceil(value));
            } else
              AD_THROW("ValueIdType not supported.");
          });
    } else {
      auto [newLocalVocab, indices] = generateRandomLocalVocabAndIndicesVec(
          numInputRows, randomStringLength);
      localVocab = std::move(newLocalVocab);

      std::ranges::transform(indices.begin(), indices.end(),
                             otherValues.begin(), [&](LocalVocabIndex idx) {
                               return ValueId::makeFromLocalVocabIndex(idx);
                             });
    }

    std::vector<std::optional<Variable>> variables = {Variable{"?a"},
                                                      Variable{"?b"}};
    std::vector<ColumnIndex> sortedColumns = {};
    if (sorted) {
      sortedColumns = {0};
    }
    auto valueTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(table), variables, false, sortedColumns,
        std::move(localVocab));

    for (size_t i = 0; i < numMeasurements; i++)
      group.addMeasurement(std::to_string(i), [&]() {
        if constexpr (std::same_as<T2, std::nullopt_t>) {
          computeGroupBy<T1>(qec, valueTree, optimizationEnabled);
        } else {
          computeGroupByTwoAggregates<T1, T2>(qec, valueTree,
                                              optimizationEnabled);
        }
      });
  };
};
AD_REGISTER_BENCHMARK(GroupByHashMapBenchmark);
}  // namespace ad_benchmark
