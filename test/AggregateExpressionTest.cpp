// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <optional>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "backports/type_traits.h"
#include "engine/ValuesForTesting.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/CountStarExpression.h"
#include "engine/sparqlExpressions/SampleExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "engine/sparqlExpressions/StdevExpression.h"
#include "global/ValueId.h"
#include "gtest/gtest.h"

using namespace sparqlExpression;
using namespace ad_utility::testing;
using ad_utility::source_location;

namespace {
auto I = IntId;
auto V = VocabId;
auto U = Id::makeUndefined();
auto D = DoubleId;
auto lit = [](auto s) {
  return IdOrLiteralOrIri(
      ad_utility::triple_component::LiteralOrIri(tripleComponentLiteral(s)));
};
static const Id NaN = D(std::numeric_limits<double>::quiet_NaN());
}  // namespace

// Test that an expression of type `AggregateExpressionT` when run on the
// `input` yields the `expectedResult`. This function can only be used for
// aggregate expressions where the input type and the output type are the same,
// e.g. MIN and MAX, but not for e.g. COUNT.
template <typename AggregateExpressionT, typename T, typename U = T>
auto testAggregate = [](std::vector<T> inputAsVector, U expectedResult,
                        bool distinct = false,
                        source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l);
  VectorWithMemoryLimit<T> input(inputAsVector.begin(), inputAsVector.end(),
                                 makeAllocator());
  auto d = std::make_unique<SingleUseExpression>(input.clone());
  auto t = TestContext{};
  t.context._endIndex = input.size();
  AggregateExpressionT m{distinct, std::move(d)};
  auto resAsVariant = m.evaluate(&t.context);
  auto res = std::get<U>(resAsVariant);
  EXPECT_EQ(res, expectedResult);
};

// Same as `testAggregate` above, but the input is specified as a variable.
template <typename AggregateExpressionT, typename T, typename U = T>
auto testAggregateWithVariable =
    [](Variable input, U expectedResult, bool distinct = false,
       source_location l = source_location::current()) {
      auto trace = generateLocationTrace(l);
      auto d = std::make_unique<VariableExpression>(std::move(input));
      auto t = TestContext{};
      AggregateExpressionT m{distinct, std::move(d)};
      auto resAsVariant = m.evaluate(&t.context);
      auto res = std::get<U>(resAsVariant);
      EXPECT_EQ(res, expectedResult);
    };

// Test `CountExpression`.
TEST(AggregateExpression, count) {
  auto testCountId = testAggregate<CountExpression, Id>;
  // Test cases. Make sure that UNDEF and NaN values are ignored and that the
  // result for an empty input is 0. The last (Boolean) argument indicates
  // whether the count should be distinct.
  testCountId({I(3), D(23.3), I(0), I(4), (I(-1))}, I(5));
  testCountId({D(2), D(2), I(2), V(17)}, I(3), true);
  testCountId({U, I(3), U}, I(1));
  testCountId({I(3), NaN, NaN}, I(2), true);
  testCountId({}, I(0));

  auto testCountString = testAggregate<CountExpression, IdOrLiteralOrIri, Id>;
  testCountString({lit("alpha"), lit("äpfel"), lit(""), lit("unfug")}, I(4));
}

// Test the behavior of COUNT for variables.
TEST(AggregateExpression, countForVariables) {
  auto testCount = testAggregateWithVariable<CountExpression, Id>;
  // Unbound variables always have a count of 0.
  testCount(Variable{"?thisVariableIsNotContained"}, I(0));
  // The static test context has three rows.
  testCount(Variable{"?ints"}, I(3));
}

// Test `SumExpression`.
TEST(AggregateExpression, sum) {
  auto testSumId = testAggregate<SumExpression, Id>;
  testSumId({I(3), D(23.3), I(0), I(4), (I(-1))}, D(29.3));
  testSumId({D(2), D(2), I(2)}, D(4), true);
  testSumId({I(3), U}, U);
  testSumId({I(3), NaN}, NaN);
  testSumId({}, I(0));

  auto testMaxString = testAggregate<MaxExpression, IdOrLiteralOrIri>;
  testMaxString({lit("alpha"), lit("äpfel"), lit("Beta"), lit("unfug")},
                lit("unfug"));
  auto testSumString = testAggregate<SumExpression, IdOrLiteralOrIri, Id>;
  testSumString({lit("alpha"), lit("äpfel"), lit("Beta"), lit("unfug")}, U);
}

// Test `AvgExpression`.
TEST(AggregateExpression, avg) {
  auto testAvgId = testAggregate<AvgExpression, Id>;
  testAvgId({I(3), D(0), I(0), I(4), (I(-2))}, D(1));
  testAvgId({D(2), D(2), I(2)}, D(2), true);
  testAvgId({I(3), U}, U);
  testAvgId({I(3), NaN}, NaN);
  testAvgId({}, I(0));

  auto testAvgString = testAggregate<AvgExpression, IdOrLiteralOrIri, Id>;
  testAvgString({lit("alpha"), lit("äpfel"), lit("Beta"), lit("unfug")}, U);
}

// Test `StdevExpression`.
TEST(StdevExpression, avg) {
  auto testStdevId = testAggregate<StdevExpression, Id>;

  auto inputAsVector = std::vector{I(3), D(0), I(0), I(4), I(-2)};
  VectorWithMemoryLimit<ValueId> input(inputAsVector.begin(),
                                       inputAsVector.end(), makeAllocator());
  auto d = std::make_unique<SingleUseExpression>(input.clone());
  auto t = TestContext{};
  t.context._endIndex = 5;
  StdevExpression m{false, std::move(d)};
  auto resAsVariant = m.evaluate(&t.context);
  ASSERT_NEAR(std::get<ValueId>(resAsVariant).getDouble(), 2.44949, 0.0001);

  testStdevId({D(2), D(2), D(2), D(2)}, D(0), true);

  testStdevId({I(3), U}, U);
  testStdevId({I(3), NaN}, NaN);

  testStdevId({}, D(0));
  testStdevId({D(500)}, D(0));
  testStdevId({D(500), D(500), D(500)}, D(0));

  auto testStdevString = testAggregate<StdevExpression, IdOrLiteralOrIri, Id>;
  testStdevString({lit("alpha"), lit("äpfel"), lit("Beta"), lit("unfug")}, U);
}

// Test `MinExpression`.
TEST(AggregateExpression, min) {
  auto testMinId = testAggregate<MinExpression, Id>;
  TestContext t;
  // IDs of one word from the vocabulary ("alpha") and two words
  // from the local vocabulary ("alx" and "aalx").
  Id alpha = t.alpha;
  LocalVocabEntry l1 =
      ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes("alx");
  Id alx = Id::makeFromLocalVocabIndex(&l1);
  LocalVocabEntry l2 =
      ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes("aalx");
  Id aalx = Id::makeFromLocalVocabIndex(&l2);

  // Test cases. Make sure that vocab entries and local vocab entries are
  // compared correctly, that UNDEF is smaller than any other value, and that
  // the result for an empty input is UNDEF.
  testMinId({I(3), I(0), I(4), (I(-1))}, I(-1));
  testMinId({V(7), V(2), V(4)}, V(2));
  testMinId({V(7), U, V(2), V(4)}, U);
  testMinId({I(3), alpha, alx, (I(-1))}, I(-1));
  testMinId({I(3), alpha, alx, (I(-1)), U}, U);
  testMinId({alpha, alx, aalx}, aalx);
  testMinId({}, U);
  auto testMinString = testAggregate<MinExpression, IdOrLiteralOrIri>;
  testMinString({lit("alpha"), lit("äpfel"), lit("Beta"), lit("unfug")},
                lit("alpha"));
}

// Test `MaxExpression`.
TEST(AggregateExpression, max) {
  auto testMaxId = testAggregate<MaxExpression, Id>;
  auto t = TestContext{};
  // IDs of two words from the vocabulary ("alpha" and "Beta") and one word
  // from the local vocabulary ("alx").
  Id alpha = t.alpha;
  Id beta = t.Beta;
  LocalVocabEntry l =
      ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes("alx");
  Id alx = Id::makeFromLocalVocabIndex(&l);

  // Test cases. Make sure that vocab entries and local vocab entries are
  // compared correctly, that UNDEF is smaller than any other value, and that
  // the result for an empty input is UNDEF.
  testMaxId({I(3), U, I(0), I(4), U, (I(-1))}, I(4));
  testMaxId({V(7), U, V(2), V(4)}, V(7));
  testMaxId({I(3), U, alpha, alx, U, (I(-1))}, alx);
  testMaxId({I(3), U, alpha, alx, beta, (I(-1))}, beta);
  testMaxId({U, U, U}, U);
  testMaxId({}, U);
}

// _____________________________________________________________________________
TEST(AggregateExpression, CountStar) {
  auto t = TestContext{};
  // A matcher that first clears the cache and then checks that the result of
  // evaluating a `SparqlExpression::Ptr` yields the single integer ID that
  // stores `i`.
  auto matcher = [&](int64_t i) {
    auto evaluate = [&](const SparqlExpression::Ptr& expr) {
      t.context._beginIndex = 0;
      t.context._endIndex = t.table.size();
      return expr->evaluate(&t.context);
    };
    using namespace ::testing;
    t.qec->getQueryTreeCache().clearAll();
    return ResultOf(evaluate, VariantWith<Id>(Eq(Id::makeFromInt(i))));
  };

  auto totalSize = t.table.size();
  using namespace sparqlExpression;
  auto m = makeCountStarExpression(false);
  EXPECT_THAT(m, matcher(totalSize));

  // Add some duplicates.
  t.table.push_back(t.table.at(0));
  t.table.push_back(t.table.at(1));
  t.table.push_back(t.table.at(0));
  t.table.at(0, 0) = I(193847521);

  t.context._endIndex += 3;

  // A COUNT * has now a size which is larger by 3, but a COUNT DISTINCT * still
  // has the same size.
  EXPECT_THAT(m, matcher(totalSize + 3));
  // We have added two duplicates, and one new distinct row
  m = makeCountStarExpression(true);
  EXPECT_THAT(m, matcher(totalSize + 1));

  // If we modify the `varToColMap` such that it doesn't contain our unique
  // value in column 0, then the number of distinct entries goes back to where
  // it originally was (columns that are hidden e.g. by a subquery have to be
  // ignored by COUNT DISTINCT *).
  t.varToColMap.clear();

  t.varToColMap[Variable{"?x"}] = {
      1, ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined};
  EXPECT_THAT(m, matcher(totalSize));

  // This variable is internal, so it doesn't count towards the `COUNT(DISTINCT
  // *)` and doesn't change the result.
  t.varToColMap[Variable{
      absl::StrCat(QLEVER_INTERNAL_VARIABLE_PREFIX, "someInternalVar")}] = {
      0, ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined};
  t.qec->getQueryTreeCache().clearAll();
  EXPECT_THAT(m, matcher(totalSize));

  // Add two rows that only consist of UNDEF values.
  // This increases the COUNT * by 2, but the COUNT DISTINCT * only by 1.
  t.table.push_back(t.table[0]);
  t.table.push_back(t.table[0]);

  auto setToUndef = [](IdTable::row_reference row) {
    for (Id& id : row) {
      id = Id::makeUndefined();
    }
  };
  setToUndef(t.table[t.table.numRows() - 1]);
  setToUndef(t.table[t.table.numRows() - 2]);

  t.qec->getQueryTreeCache().clearAll();
  // Here, m is a COUNT DISTINCT *.
  EXPECT_THAT(m, matcher(totalSize + 1));

  m = makeCountStarExpression(false);
  EXPECT_THAT(m, matcher(t.table.numRows()));

  // Test the correct behavior for an empty input.
  t.table.clear();
  EXPECT_THAT(m, matcher(0));
  m = makeCountStarExpression(true);
  EXPECT_THAT(m, matcher(0));
}

// _____________________________________________________________________________
TEST(AggregateExpression, CountStarSimpleMembers) {
  using namespace sparqlExpression;
  using enum SparqlExpression::AggregateStatus;
  auto m = makeCountStarExpression(false);
  const auto& exp = *m;
  EXPECT_THAT(exp.getCacheKey({}), ::testing::HasSubstr("COUNT *"));
  EXPECT_THAT(exp.children(), ::testing::IsEmpty());
  EXPECT_THAT(m->getUnaggregatedVariables(), ::testing::IsEmpty());
  EXPECT_EQ(exp.isAggregate(), NonDistinctAggregate);

  auto m2 = makeCountStarExpression(true);
  EXPECT_EQ(m2->isAggregate(), DistinctAggregate);
  EXPECT_NE(exp.getCacheKey({}), m2->getCacheKey({}));
}

// _____________________________________________________________________________
TEST(AggregateExpression, SampleExpression) {
  using namespace sparqlExpression;
  auto makeSample = [](ExpressionResult result) {
    return std::make_unique<SampleExpression>(
        false, std::make_unique<SingleUseExpression>(std::move(result)));
  };

  auto testSample = [&](ExpressionResult input, ExpressionResult expected) {
    TestContext testContext;
    std::visit(
        [&testContext](const auto& t) {
          using T = std::decay_t<decltype(t)>;
          if constexpr (ad_utility::isInstantiation<T, VectorWithMemoryLimit>) {
            testContext.context._endIndex = t.size();
          }
        },
        input);
    auto result = makeSample(std::move(input))->evaluate(&testContext.context);
    EXPECT_EQ(result, expected);
  };

  testSample(I(3), I(3));
  auto v = VectorWithMemoryLimit<Id>(makeAllocator());
  v.push_back(I(34));
  v.push_back(I(42));
  testSample(v.clone(), I(34));
  testSample(ad_utility::SetOfIntervals{}, BoolId(false));
  testSample(ad_utility::SetOfIntervals{{{3, 17}}}, BoolId(true));
  v.clear();
  testSample(v.clone(), U);
  // The first value of the ?ints variable inside the `TestContext` is `1`.
  testSample(Variable{"?ints"}, I(1));
}

// _____________________________________________________________________________
TEST(AggregateExpression, SampleExpressionSimpleMembers) {
  using namespace sparqlExpression;
  auto makeSample = [](Id result, bool distinct = false) {
    return std::make_unique<SampleExpression>(
        distinct, std::make_unique<IdExpression>(std::move(result)));
  };

  auto sample = makeSample(I(3478));
  using enum SparqlExpression::AggregateStatus;
  EXPECT_EQ(sample->isAggregate(), NonDistinctAggregate);
  EXPECT_TRUE(sample->getUnaggregatedVariables().empty());
  EXPECT_EQ(sample->children().size(), 1u);
  using namespace ::testing;
  EXPECT_THAT(sample->getCacheKey({}),
              AllOf(HasSubstr("SAMPLE"), HasSubstr("#valueId")));

  // DISTINCT makes no difference for sample, so the two SAMPLE s that only
  // differ in their distinctness even may have the same cache key.
  auto sample2 = makeSample(I(3478), true);
  EXPECT_EQ(sample->getCacheKey({}), sample2->getCacheKey({}));
  EXPECT_EQ(sample2->isAggregate(), NonDistinctAggregate);
}
