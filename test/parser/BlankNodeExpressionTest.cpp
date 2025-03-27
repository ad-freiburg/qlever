//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../SparqlExpressionTestHelpers.h"
#include "engine/sparqlExpressions/BlankNodeExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"

using namespace sparqlExpression;
using namespace ad_utility::triple_component;

// _____________________________________________________________________________
TEST(SpecificBlankNodeExpression, expectBlankNodeResultEquality) {
  TestContext context;
  auto expression0 = makeBlankNodeExpression(
      std::make_unique<IdExpression>(Id::makeUndefined()));
  EXPECT_THAT(expression0->evaluate(&context.context),
              ::testing::VariantWith<IdOrLiteralOrIri>(
                  ::testing::VariantWith<Id>(Id::makeUndefined())));
  auto expression1 =
      makeBlankNodeExpression(std::make_unique<StringLiteralExpression>(
          Literal::literalWithoutQuotes("Test")));
  auto expression2 =
      makeBlankNodeExpression(std::make_unique<StringLiteralExpression>(
          Literal::literalWithoutQuotes("Test")));
  EXPECT_EQ(expression1->evaluate(&context.context),
            expression2->evaluate(&context.context));
  auto expression3 =
      makeBlankNodeExpression(std::make_unique<StringLiteralExpression>(
          Literal::literalWithoutQuotes("Other")));
  EXPECT_NE(expression1->evaluate(&context.context),
            expression3->evaluate(&context.context));
  EXPECT_NE(expression2->evaluate(&context.context),
            expression3->evaluate(&context.context));

  VectorWithMemoryLimit<IdOrLiteralOrIri> vector{context.context._allocator};
  vector.emplace_back(LiteralOrIri{Literal::literalWithoutQuotes("Other")});
  vector.emplace_back(LiteralOrIri{Literal::literalWithoutQuotes("Test")});
  vector.emplace_back(LiteralOrIri{Iri::fromIriref("<http://example.com>")});

  auto expression4 =
      makeBlankNodeExpression(std::make_unique<SingleUseExpression>(
          ExpressionResult{std::move(vector)}));
  auto result = expression4->evaluate(&context.context);
  EXPECT_THAT(result,
              ::testing::VariantWith<VectorWithMemoryLimit<IdOrLiteralOrIri>>(
                  ::testing::ElementsAre(
                      ::testing::Eq(std::get<IdOrLiteralOrIri>(
                          expression3->evaluate(&context.context))),
                      ::testing::Eq(std::get<IdOrLiteralOrIri>(
                          expression1->evaluate(&context.context))),
                      ::testing::VariantWith<LocalVocabEntry>(::testing::_))));
}

// _____________________________________________________________________________
TEST(UniqueBlankNodeExpression, uniqueCacheKey) {
  auto expression0 = makeUniqueBlankNodeExpression(0);
  auto expression1 = makeUniqueBlankNodeExpression(0);
  EXPECT_EQ(expression0->getCacheKey({}), expression1->getCacheKey({}));
  EXPECT_NE(expression1->getCacheKey({}), expression1->getCacheKey({}));
}

// _____________________________________________________________________________
TEST(UniqueBlankNodeExpression, noChildren) {
  auto expression0 = makeUniqueBlankNodeExpression(0);
  EXPECT_TRUE(expression0->children().empty());
}

// _____________________________________________________________________________
TEST(UniqueBlankNodeExpression, uniqueValuesAcrossInstances) {
  TestContext context;
  auto expression0 = makeUniqueBlankNodeExpression(0);
  auto expression1 = makeUniqueBlankNodeExpression(1);
  auto result0 = expression0->evaluate(&context.context);
  auto result1 = expression1->evaluate(&context.context);
  ASSERT_TRUE(
      std::holds_alternative<VectorWithMemoryLimit<IdOrLiteralOrIri>>(result0));
  ASSERT_TRUE(
      std::holds_alternative<VectorWithMemoryLimit<IdOrLiteralOrIri>>(result1));
  // Check that both results are distinct.
  EXPECT_THAT(
      std::get<VectorWithMemoryLimit<IdOrLiteralOrIri>>(result0),
      ::testing::Each(::testing::ResultOf(
          [&result1](const auto& elem) {
            return ad_utility::contains(
                std::get<VectorWithMemoryLimit<IdOrLiteralOrIri>>(result1),
                elem);
          },
          ::testing::IsFalse())));
  EXPECT_THAT(
      std::get<VectorWithMemoryLimit<IdOrLiteralOrIri>>(result1),
      ::testing::Each(::testing::ResultOf(
          [&result0](const auto& elem) {
            return ad_utility::contains(
                std::get<VectorWithMemoryLimit<IdOrLiteralOrIri>>(result0),
                elem);
          },
          ::testing::IsFalse())));
}
