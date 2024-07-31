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
