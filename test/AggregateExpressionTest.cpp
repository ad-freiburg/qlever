// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
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
auto L = LocalVocabId;
}  // namespace

// Test that an expression of type `AggregateExpressionT` when run on the
// `input` yields the `expectedResult`. This function can only be used for
// aggregate expressions where the input type and the output type are the same,
// e.g. MIN and MAX, but not for e.g. COUNT.
template <typename AggregateExpressionT, typename T>
auto testAggregate = [](std::vector<T> inputAsVector, T expectedResult,
                        bool distinct = false,
                        source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l);
  VectorWithMemoryLimit<T> input(inputAsVector.begin(), inputAsVector.end(),
                                 makeAllocator());
  auto d = std::make_unique<DummyExpression>(input.clone());
  auto t = TestContext{};
  t.context._endIndex = input.size();
  AggregateExpressionT m{distinct, std::move(d)};
  auto resAsVariant = m.evaluate(&t.context);
  auto res = std::get<T>(resAsVariant);
  EXPECT_EQ(res, expectedResult);
};

TEST(AggregateExpression, max) {
  auto testMaxId = testAggregate<MaxExpression, Id>;
  testMaxId({I(3), U, I(0), I(4), U, (I(-1))}, I(4));
  testMaxId({V(7), U, V(2), V(4)}, V(7));
  testMaxId({I(3), U, V(0), L(3), U, (I(-1))}, L(3));

  auto testMaxString = testAggregate<MaxExpression, std::string>;
  // TODO<joka921> Implement correct comparison on strings
  testMaxString({"alpha", "äpfel", "Beta", "unfug"}, "äpfel");
}

TEST(AggregateExpression, min) {
  auto testMinId = testAggregate<MinExpression, Id>;
  testMinId({I(3), U, I(0), I(4), U, (I(-1))}, I(-1));
  testMinId({V(7), U, V(2), V(4)}, V(2));
  testMinId({I(3), U, V(0), L(3), U, (I(-1))}, I(-1));

  auto testMinString = testAggregate<MinExpression, std::string>;
  // TODO<joka921> Implement correct comparison on strings
  testMinString({"alpha", "äpfel", "Beta", "unfug"}, "Beta");
}
