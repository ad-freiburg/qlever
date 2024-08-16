// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/ValuesForTesting.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/CountStarExpression.h"
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

// ______________________________________________________________________________
TEST(AggregateExpression, max) {
  auto testMaxId = testAggregate<MaxExpression, Id>;
  auto t = TestContext{};
  Id alpha = t.alpha;  // Vocab Id of the Literal "alpha"
  Id beta = t.Beta;    // Vocab Id of the Literal "Beta"

  LocalVocabEntry l =
      ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes("alx");
  Id alx = Id::makeFromLocalVocabIndex(&l);

  testMaxId({I(3), U, I(0), I(4), U, (I(-1))}, I(4));
  testMaxId({V(7), U, V(2), V(4)}, V(7));
  // Correct comparison between vocab and local vocab entries.
  testMaxId({I(3), U, alpha, alx, U, (I(-1))}, alx);
  testMaxId({I(3), U, alpha, alx, beta, (I(-1))}, beta);

  auto testMaxString = testAggregate<MaxExpression, IdOrLiteralOrIri>;
  testMaxString({lit("alpha"), lit("äpfel"), lit("Beta"), lit("unfug")},
                lit("unfug"));
}

// ______________________________________________________________________________
TEST(AggregateExpression, min) {
  auto testMinId = testAggregate<MinExpression, Id>;
  TestContext t;
  Id alpha = t.alpha;  // Vocab Id of the Literal "alpha"

  LocalVocabEntry l =
      ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes("alx");
  Id alx = Id::makeFromLocalVocabIndex(&l);
  LocalVocabEntry l2 =
      ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes("aalx");
  Id aalx = Id::makeFromLocalVocabIndex(&l2);

  testMinId({I(3), I(0), I(4), (I(-1))}, I(-1));
  testMinId({V(7), V(2), V(4)}, V(2));
  testMinId({V(7), U, V(2), V(4)}, U);
  testMinId({I(3), alpha, alx, (I(-1))}, I(-1));
  testMinId({I(3), alpha, alx, (I(-1)), U}, U);
  testMinId({alpha, alx, aalx}, aalx);

  auto testMinString = testAggregate<MinExpression, IdOrLiteralOrIri>;
  testMinString({lit("alpha"), lit("äpfel"), lit("Beta"), lit("unfug")},
                lit("alpha"));
}

// ______________________________________________________________________________
TEST(AggregateExpression, sum) {
  auto testSumId = testAggregate<SumExpression, Id>;
  testSumId({I(3), D(23.3), I(0), I(4), (I(-1))}, D(29.3));
  testSumId({D(2), D(2), I(2)}, D(4), true);

  testSumId({I(3), U}, U);
  testSumId({I(3), NaN}, NaN);

  auto testSumString = testAggregate<SumExpression, IdOrLiteralOrIri, Id>;
  testSumString({lit("alpha"), lit("äpfel"), lit("Beta"), lit("unfug")}, U);
}

// ______________________________________________________________________________
TEST(AggregateExpression, count) {
  auto testCountId = testAggregate<CountExpression, Id>;
  testCountId({I(3), D(23.3), I(0), I(4), (I(-1))}, I(5));
  testCountId({D(2), D(2), I(2), V(17)}, I(3), true);

  testCountId({U, I(3), U}, I(1));
  testCountId({I(3), NaN, NaN}, I(2), true);

  auto testCountString = testAggregate<CountExpression, IdOrLiteralOrIri, Id>;
  testCountString({lit("alpha"), lit("äpfel"), lit(""), lit("unfug")}, I(4));
}

// ______________________________________________________________________________
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
      absl::StrCat(INTERNAL_VARIABLE_PREFIX, "someInternalVar")}] = {
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
}

// ______________________________________________________________________________
TEST(AggregateExpression, CountStarSimpleMembers) {
  using namespace sparqlExpression;
  auto m = makeCountStarExpression(false);
  const auto& exp = *m;
  EXPECT_THAT(exp.getCacheKey({}), ::testing::HasSubstr("COUNT *"));
  auto m2 = makeCountStarExpression(true);
  EXPECT_NE(exp.getCacheKey({}), m2->getCacheKey({}));

  EXPECT_THAT(m->children(), ::testing::IsEmpty());
  EXPECT_THAT(m->getUnaggregatedVariables(), ::testing::IsEmpty());
  EXPECT_TRUE(m->isAggregate());
}
