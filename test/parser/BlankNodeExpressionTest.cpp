//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../SparqlExpressionTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "engine/sparqlExpressions/BlankNodeExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"

using namespace sparqlExpression;
using namespace ad_utility::triple_component;

// _____________________________________________________________________________
TEST(BlankNodeExpression, expectBlankNodeResultEquality) {
  TestContext context;
  auto expression0 = makeBlankNodeExpression(
      std::make_unique<IdExpression>(Id::makeUndefined()));
  EXPECT_THAT(expression0->evaluate(&context.context),
              ::testing::VariantWith<Id>(Id::makeUndefined()));
  auto expression1 =
      makeBlankNodeExpression(std::make_unique<StringLiteralExpression>(
          Literal::literalWithoutQuotes("Test")));
  auto expression2 =
      makeBlankNodeExpression(std::make_unique<StringLiteralExpression>(
          Literal::literalWithoutQuotes("Test")));
  auto result1 = expression1->evaluate(&context.context);
  auto result2 = expression2->evaluate(&context.context);
  EXPECT_EQ(result1, result2);
  auto expression3 =
      makeBlankNodeExpression(std::make_unique<StringLiteralExpression>(
          Literal::literalWithoutQuotes("Other")));
  auto result3 = expression3->evaluate(&context.context);
  EXPECT_NE(result1, result3);
  EXPECT_NE(result2, result3);

  VectorWithMemoryLimit<IdOrLiteralOrIri> vector{context.context._allocator};
  vector.emplace_back(LiteralOrIri{Literal::literalWithoutQuotes("Other")});
  vector.emplace_back(LiteralOrIri{Literal::literalWithoutQuotes("Test")});
  vector.emplace_back(LiteralOrIri{Iri::fromIriref("<http://example.com>")});

  auto expression4 =
      makeBlankNodeExpression(std::make_unique<SingleUseExpression>(
          ExpressionResult{std::move(vector)}));
  auto result4 = expression4->evaluate(&context.context);
  const auto& secondTest =
      std::get<VectorWithMemoryLimit<IdOrLiteralOrIri>>(result1).at(1);
  const auto& firstOther =
      std::get<VectorWithMemoryLimit<IdOrLiteralOrIri>>(result3).at(0);
  EXPECT_THAT(result4,
              ::testing::VariantWith<VectorWithMemoryLimit<IdOrLiteralOrIri>>(
                  ::testing::ElementsAre(
                      ::testing::Eq(firstOther), ::testing::Eq(secondTest),
                      ::testing::VariantWith<LocalVocabEntry>(::testing::_))));
}

// _____________________________________________________________________________
TEST(BlankNodeExpression, labelsAreCorrectlyEscaped) {
  TestContext context;
  auto expectIrisAre = [&context](std::string_view input,
                                  const std::vector<std::string_view>& expected,
                                  ad_utility::source_location loc =
                                      ad_utility::source_location::current()) {
    auto t = generateLocationTrace(loc);
    auto expression =
        makeBlankNodeExpression(std::make_unique<StringLiteralExpression>(
            Literal::literalWithoutQuotes(input)));
    auto result = expression->evaluate(&context.context);
    ASSERT_TRUE(std::holds_alternative<VectorWithMemoryLimit<IdOrLiteralOrIri>>(
        result));
    const auto& litOrIris =
        std::get<VectorWithMemoryLimit<IdOrLiteralOrIri>>(result);
    ASSERT_EQ(litOrIris.size(), expected.size());
    size_t i = 0;
    for (const auto& litOrIri : litOrIris) {
      ASSERT_TRUE(std::holds_alternative<LocalVocabEntry>(litOrIri));
      EXPECT_EQ(std::get<LocalVocabEntry>(litOrIri).toStringRepresentation(),
                expected[i]);
      i++;
    }
  };
  auto makeIri = [](std::string_view s) {
    return absl::StrCat(QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX, "_:un", s, ">");
  };
  expectIrisAre("0Rr_3.", {makeIri("0Rr_3.46_0"), makeIri("0Rr_3.46_1"),
                           makeIri("0Rr_3.46_2")});
  expectIrisAre("\n?^~",
                {makeIri(".10.63.94.126_0"), makeIri(".10.63.94.126_1"),
                 makeIri(".10.63.94.126_2")});
  expectIrisAre(
      "..", {makeIri(".46.46_0"), makeIri(".46.46_1"), makeIri(".46.46_2")});
  expectIrisAre("", {makeIri("_0"), makeIri("_1"), makeIri("_2")});
  expectIrisAre(".46",
                {makeIri(".4646_0"), makeIri(".4646_1"), makeIri(".4646_2")});
}

// _____________________________________________________________________________
TEST(BlankNodeExpression, uniqueCacheKey) {
  auto expression0 = makeUniqueBlankNodeExpression();
  auto expression1 = makeUniqueBlankNodeExpression();
  EXPECT_EQ(expression0->getCacheKey({}), expression1->getCacheKey({}));
  EXPECT_NE(expression1->getCacheKey({}), expression1->getCacheKey({}));

  auto expression2 =
      makeBlankNodeExpression(std::make_unique<StringLiteralExpression>(
          Literal::literalWithoutQuotes("Test")));
  EXPECT_THAT(
      expression2->getCacheKey({}),
      ::testing::HasSubstr(expression2->children()[0]->getCacheKey({})));
  EXPECT_NE(expression1->getCacheKey({}), expression2->getCacheKey({}));
  EXPECT_NE(expression2->getCacheKey({}), expression2->getCacheKey({}));
}

// _____________________________________________________________________________
TEST(BlankNodeExpression, correctChildren) {
  auto expression0 = makeUniqueBlankNodeExpression();
  EXPECT_TRUE(expression0->children().empty());
  auto expression1 =
      makeBlankNodeExpression(std::make_unique<StringLiteralExpression>(
          Literal::literalWithoutQuotes("Test")));
  EXPECT_EQ(expression1->children().size(), 1);
}

// _____________________________________________________________________________
TEST(BlankNodeExpression, uniqueValuesAcrossInstances) {
  TestContext context;
  auto expression0 = makeUniqueBlankNodeExpression();
  auto expression1 = makeUniqueBlankNodeExpression();
  auto result0 = expression0->evaluate(&context.context);
  auto result1 = expression1->evaluate(&context.context);
  ASSERT_TRUE(std::holds_alternative<VectorWithMemoryLimit<Id>>(result0));
  ASSERT_TRUE(std::holds_alternative<VectorWithMemoryLimit<Id>>(result1));
  // Check that both results are distinct.
  EXPECT_THAT(std::get<VectorWithMemoryLimit<Id>>(result0),
              ::testing::Each(::testing::ResultOf(
                  [&result1](const auto& elem) {
                    return ad_utility::contains(
                        std::get<VectorWithMemoryLimit<Id>>(result1), elem);
                  },
                  ::testing::IsFalse())));
  EXPECT_THAT(std::get<VectorWithMemoryLimit<Id>>(result1),
              ::testing::Each(::testing::ResultOf(
                  [&result0](const auto& elem) {
                    return ad_utility::contains(
                        std::get<VectorWithMemoryLimit<Id>>(result0), elem);
                  },
                  ::testing::IsFalse())));
}

// _____________________________________________________________________________
TEST(BlankNodeExpression, consistentCounterWithUndefined) {
  TestContext context;
  VectorWithMemoryLimit<IdOrLiteralOrIri> vector{context.context._allocator};
  vector.emplace_back(LiteralOrIri{Literal::literalWithoutQuotes("T1")});
  vector.emplace_back(Id::makeUndefined());
  vector.emplace_back(LiteralOrIri{Literal::literalWithoutQuotes("T2")});

  auto expression0 =
      makeBlankNodeExpression(std::make_unique<SingleUseExpression>(
          ExpressionResult{std::move(vector)}));

  using namespace ::testing;
  // Make sure counter is incremented too for undefined values.
  EXPECT_THAT(expression0->evaluate(&context.context),
              VariantWith<VectorWithMemoryLimit<IdOrLiteralOrIri>>(ElementsAre(
                  VariantWith<LocalVocabEntry>(AD_PROPERTY(
                      LiteralOrIri, toStringRepresentation,
                      StrEq("<http://qlever.cs.uni-freiburg.de/"
                            "builtin-functions/blank-node/_:unT1_0>"))),
                  VariantWith<Id>(Eq(Id::makeUndefined())),
                  VariantWith<LocalVocabEntry>(AD_PROPERTY(
                      LiteralOrIri, toStringRepresentation,
                      StrEq("<http://qlever.cs.uni-freiburg.de/"
                            "builtin-functions/blank-node/_:unT2_2>"))))));
}
