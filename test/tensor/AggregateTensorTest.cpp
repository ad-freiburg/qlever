// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>
#include <gmock/gmock.h>

#include <cmath>

#include "../SparqlExpressionTestHelpers.h"
#include "TensorTestHelpers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "engine/tensor/aggregate/TensorAggregate.h"
#include "index/TripleComponentConversions.h"

namespace {
using ad_utility::TensorData;
using sparqlExpression::TensorAggregateKind;
using sparqlExpression::detail::TensorAccumulator;
using VocabularyType = ad_utility::VocabularyType;
using namespace std::literals;

// Shortcut for a one-dimensional float tensor.
TensorData T(std::vector<float> data) { return TensorData(std::move(data)); }

// Run a `TensorAccumulator` of the given `kind` on the `inputs`.
std::optional<TensorData> accumulate(
    TensorAggregateKind kind,
    const std::vector<std::optional<TensorData>>& inputs) {
  TensorAccumulator accumulator{kind};
  for (const auto& input : inputs) {
    accumulator.add(input);
  }
  return accumulator.finish();
}

// Matcher for a tensor with the given `shape` whose elements are near the
// `expected` values.
auto tensorNear(std::vector<float> expected,
                std::optional<std::vector<size_t>> shape = std::nullopt) {
  using namespace ::testing;
  std::vector<Matcher<float>> elementMatchers;
  for (float f : expected) {
    elementMatchers.push_back(FloatNear(f, 1e-5f));
  }
  auto s = shape.value_or(std::vector<size_t>{expected.size()});
  return Optional(
      AllOf(Property(&TensorData::shape, Eq(s)),
            Property(&TensorData::tensorData, ElementsAreArray(elementMatchers))));
}

// _____________________________________________________________________________
TEST(TensorAccumulator, sumAndAvg) {
  using enum TensorAggregateKind;
  std::vector<std::optional<TensorData>> in{T({1, 2, 3}), T({3, 2, 1}),
                                            T({-1, 5, 2})};
  EXPECT_THAT(accumulate(Sum, in), tensorNear({3, 9, 6}));
  EXPECT_THAT(accumulate(Avg, in), tensorNear({1, 3, 2}));
  // Single element.
  EXPECT_THAT(accumulate(Sum, {T({1, 2})}), tensorNear({1, 2}));
  EXPECT_THAT(accumulate(Avg, {T({1, 2})}), tensorNear({1, 2}));
  // Multi-dimensional tensors keep their shape.
  TensorData m{{1, 2, 3, 4}, {2, 2}, TensorData::DType::FLOAT};
  EXPECT_THAT(accumulate(Sum, {m, m}), tensorNear({2, 4, 6, 8}, {{2, 2}}));
}

// _____________________________________________________________________________
TEST(TensorAccumulator, varianceAndStd) {
  using enum TensorAggregateKind;
  std::vector<std::optional<TensorData>> in{T({1, 2, -2}), T({1, 1, 1}),
                                            T({1, -1, 2})};
  // Sample variance (division by `n - 1`).
  EXPECT_THAT(accumulate(Var, in), tensorNear({0, 7.f / 3, 13.f / 3}));
  EXPECT_THAT(accumulate(Std, in),
              tensorNear({0, std::sqrt(7.f / 3), std::sqrt(13.f / 3)}));
  // The variance of a single value is zero (no degrees of freedom).
  EXPECT_THAT(accumulate(Var, {T({4, 5})}), tensorNear({0, 0}));
  EXPECT_THAT(accumulate(Std, {T({4, 5})}), tensorNear({0, 0}));
  // Welford's algorithm is numerically stable for values with a large mean.
  EXPECT_THAT(accumulate(Var, {T({1e7f + 1}), T({1e7f + 2}), T({1e7f + 3})}),
              tensorNear({1}));
}

// _____________________________________________________________________________
TEST(TensorAccumulator, undefinedResults) {
  for (auto kind : {TensorAggregateKind::Sum, TensorAggregateKind::Avg,
                    TensorAggregateKind::Var, TensorAggregateKind::Std}) {
    // Empty input.
    EXPECT_EQ(accumulate(kind, {}), std::nullopt);
    // A value that is not a tensor.
    EXPECT_EQ(accumulate(kind, {T({1}), std::nullopt, T({2})}), std::nullopt);
    // Different shapes.
    EXPECT_EQ(accumulate(kind, {T({1, 2}), T({1, 2, 3})}), std::nullopt);
    TensorData m{{1, 2, 3, 4}, {2, 2}, TensorData::DType::FLOAT};
    EXPECT_EQ(accumulate(kind, {m, T({1, 2, 3, 4})}), std::nullopt);
  }
  TensorAccumulator accumulator{TensorAggregateKind::Sum};
  accumulator.add(T({1}));
  EXPECT_TRUE(accumulator.isValid());
  EXPECT_EQ(accumulator.count(), 1u);
  accumulator.invalidate();
  EXPECT_FALSE(accumulator.isValid());
  EXPECT_EQ(accumulator.finish(), std::nullopt);
}

// _____________________________________________________________________________
TEST(TensorAccumulator, dtype) {
  using enum TensorAggregateKind;
  TensorData ints{{1, 2}, {2}, TensorData::DType::INT};
  EXPECT_EQ(accumulate(Sum, {ints, ints})->dtype(), TensorData::DType::INT);
  EXPECT_EQ(accumulate(Avg, {ints, ints})->dtype(), TensorData::DType::FLOAT);
  TensorData doubles{{1, 2}, {2}, TensorData::DType::DOUBLE};
  EXPECT_EQ(accumulate(Var, {doubles, doubles})->dtype(),
            TensorData::DType::DOUBLE);
}

// _____________________________________________________________________________
// Evaluate the aggregate expression of the given `kind` on the `inputs` (one
// row per input) and return the result as a tensor (`std::nullopt` if the
// result is UNDEF).
std::optional<TensorData> evaluateAggregate(
    TensorAggregateKind kind, std::vector<std::optional<TensorData>> inputs,
    bool distinct = false) {
  using namespace sparqlExpression;
  TestContext t;
  VectorWithMemoryLimit<IdOrLocalVocabEntry> input(
      ad_utility::testing::makeAllocator());
  for (auto& value : inputs) {
    if (value.has_value()) {
      input.push_back(toIdOrLocalVocabEntry(
          value->toLiteral(), t.context._qec.getLocalVocabContext()));
    } else {
      input.push_back(Id::makeUndefined());
    }
  }
  t.context._endIndex = input.size();
  auto child = std::make_unique<SingleUseExpression>(std::move(input));
  auto expression = [&]() -> SparqlExpression::Ptr {
    switch (kind) {
      case TensorAggregateKind::Sum:
        return std::make_unique<TensorSumExpression>(distinct,
                                                     std::move(child));
      case TensorAggregateKind::Avg:
        return std::make_unique<TensorAvgExpression>(distinct,
                                                     std::move(child));
      case TensorAggregateKind::Var:
        return std::make_unique<TensorVarExpression>(distinct,
                                                     std::move(child));
      case TensorAggregateKind::Std:
        return std::make_unique<TensorStdExpression>(distinct,
                                                     std::move(child));
    }
    AD_FAIL();
  }();
  auto result = expression->evaluate(&t.context);
  if (auto id = std::get_if<Id>(&result)) {
    EXPECT_TRUE(id->isUndefined());
    return std::nullopt;
  }
  auto value = std::get<IdOrLocalVocabEntry>(std::move(result));
  return sparqlExpression::detail::TensorValueGetter{}(std::move(value), &t.context);
}

// _____________________________________________________________________________
TEST(TensorAggregateExpression, evaluate) {
  using enum TensorAggregateKind;
  std::vector<std::optional<TensorData>> in{T({1, 2}), T({3, 2}), T({1, 2})};
  EXPECT_THAT(evaluateAggregate(Sum, in), tensorNear({5, 6}));
  EXPECT_THAT(evaluateAggregate(Avg, in), tensorNear({5.f / 3, 2}));
  EXPECT_THAT(evaluateAggregate(Var, in), tensorNear({4.f / 3, 0}));
  EXPECT_THAT(evaluateAggregate(Std, in),
              tensorNear({std::sqrt(4.f / 3), 0}));
  // DISTINCT removes the duplicate `[1, 2]`.
  EXPECT_THAT(evaluateAggregate(Sum, in, true), tensorNear({4, 4}));
  EXPECT_THAT(evaluateAggregate(Var, in, true), tensorNear({2, 0}));
  // Single element.
  EXPECT_THAT(evaluateAggregate(Var, {T({1, 2})}), tensorNear({0, 0}));
  // Empty group.
  EXPECT_EQ(evaluateAggregate(Sum, {}), std::nullopt);
  EXPECT_EQ(evaluateAggregate(Std, {}), std::nullopt);
  // UNDEF values and shape mismatches make the result UNDEF.
  EXPECT_EQ(evaluateAggregate(Avg, {T({1}), std::nullopt}), std::nullopt);
  EXPECT_EQ(evaluateAggregate(Sum, {T({1}), T({1, 2})}), std::nullopt);
}

// _____________________________________________________________________________
TEST(TensorAggregateExpression, constantInput) {
  using namespace sparqlExpression;
  TestContext t;
  auto tensor = toIdOrLocalVocabEntry(T({1, 2}).toLiteral(),
                                      t.context._qec.getLocalVocabContext());
  auto evaluate = [&](auto expression, size_t numRows) {
    t.context._endIndex = numRows;
    return expression.evaluate(&t.context);
  };
  auto child = [&]() { return std::make_unique<SingleUseExpression>(tensor); };
  // One copy of the constant per row.
  auto result = evaluate(TensorSumExpression{false, child()}, 3);
  EXPECT_THAT(sparqlExpression::detail::TensorValueGetter{}(
                  std::get<IdOrLocalVocabEntry>(result), &t.context),
              tensorNear({3, 6}));
  // With DISTINCT only once.
  result = evaluate(TensorSumExpression{true, child()}, 3);
  EXPECT_THAT(sparqlExpression::detail::TensorValueGetter{}(
                  std::get<IdOrLocalVocabEntry>(result), &t.context),
              tensorNear({1, 2}));
  // No rows.
  result = evaluate(TensorAvgExpression{false, child()}, 0);
  EXPECT_EQ(std::get<Id>(result), Id::makeUndefined());
  // A constant that is not a tensor.
  result = evaluate(
      TensorAvgExpression{
          false, std::make_unique<SingleUseExpression>(Id::makeFromInt(3))},
      2);
  EXPECT_EQ(std::get<Id>(result), Id::makeUndefined());
}

// _____________________________________________________________________________
// End-to-end tests of the aggregates in a `GROUP BY` query.
struct TensorAggTypeArg {
  std::string aggType;
  // Expected result for each of the groups `<s1>`, `<s2>`, `<s3>`.
  std::vector<std::vector<float>> expected;
  VocabularyType vocabType;
};
using TensorAggregateCall =
    TensorTestHelpers::TensorQueryTest<::testing::TestWithParam<TensorAggTypeArg>>;
TEST_P(TensorAggregateCall, TensorEnd2EndAggregateCall) {
  const auto& param = GetParam();
  auto qlv = qlvFresh(param.vocabType, R"(
  <s1> <p2> <o1> .
  <s3> <p1> "{\"data\":[1.0,2.0,-2.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s3> <p1> "{\"data\":[1.0,1.0,1.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s3> <p1> "{\"data\":[1.0,-1.0,2.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s2> <p2> <o2> .
  <s1> <p1> "{\"data\":[1.0,2.0,3.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s1> <p1> "{\"data\":[3.0,2.0,1.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s2> <p1> "{\"data\":[1.0,1.0,3.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s2> <p2> <o2> .
)");
  std::string query = absl::StrCat(R"(
PREFIX dt: <https://w3id.org/rdf-tensor/datatypes#>
PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
PREFIX dta: <https://w3id.org/rdf-tensor/aggregates#>
      SELECT ?s (dta:)",
                                   param.aggType, R"((?v) AS ?agg) WHERE {
      ?s <p1> ?v.
      }
      GROUP BY ?s ORDER BY ?s
      )");

  auto resultString = qlv->query(query, ad_utility::MediaType::sparqlJson);
  auto result = nlohmann::json::parse(resultString);

  ASSERT_TRUE(result.contains("results"));
  const auto& bindings = result["results"]["bindings"];
  ASSERT_EQ(bindings.size(), param.expected.size()) << resultString;

  for (size_t i = 0; i < bindings.size(); ++i) {
    auto tensor = TensorData::parseFromString(
        bindings[i]["agg"]["value"].get<std::string>());
    EXPECT_THAT(std::optional{tensor}, tensorNear(param.expected[i]))
        << "group " << i;
  }
}

std::vector<TensorAggTypeArg> aggregateTestCases() {
  const float sq2 = std::sqrt(2.f);
  std::vector<TensorAggTypeArg> cases{
      {"sum", {{4, 4, 4}, {1, 1, 3}, {3, 2, 1}}, {}},
      {"avg", {{2, 2, 2}, {1, 1, 3}, {1, 2.f / 3, 1.f / 3}}, {}},
      // Sample variance (divided by `n - 1`, zero for a single value).
      {"var", {{2, 0, 2}, {0, 0, 0}, {0, 7.f / 3, 13.f / 3}}, {}},
      {"std",
       {{sq2, 0, sq2},
        {0, 0, 0},
        {0, std::sqrt(7.f / 3), std::sqrt(13.f / 3)}},
       {}}};
  std::vector<TensorAggTypeArg> result;
  for (auto vocabType : {VocabularyType::Enum::OnDiskCompressed,
                         VocabularyType::Enum::OnDiskCompressedTensorSplit}) {
    for (auto c : cases) {
      c.vocabType = VocabularyType{vocabType};
      result.push_back(std::move(c));
    }
  }
  return result;
}

INSTANTIATE_TEST_SUITE_P(
    TensorAggregateCallVariation, TensorAggregateCall,
    ::testing::ValuesIn(aggregateTestCases()), [](const auto& info) {
      bool split = info.param.vocabType.value() ==
                   VocabularyType::Enum::OnDiskCompressedTensorSplit;
      return absl::StrCat(info.param.aggType, split ? "TensorSplit" : "");
    });
}  // namespace
