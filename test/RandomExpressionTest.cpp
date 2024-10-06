//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <unordered_set>

#include "./SparqlExpressionTestHelpers.h"
#include "engine/sparqlExpressions/RandomExpression.h"
#include "engine/sparqlExpressions/UuidExpressions.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using namespace sparqlExpression;
TEST(RandomExpression, evaluate) {
  TestContext testContext{};
  auto& evaluationContext = testContext.context;
  evaluationContext._beginIndex = 43;
  evaluationContext._endIndex = 1044;
  auto resultAsVariant = RandomExpression{}.evaluate(&evaluationContext);

  using V = VectorWithMemoryLimit<Id>;
  ASSERT_TRUE(std::holds_alternative<V>(resultAsVariant));
  const auto& resultVector = std::get<V>(resultAsVariant);
  ASSERT_EQ(resultVector.size(), 1001);

  std::vector<int64_t> histogram(10);
  for (auto rand : resultVector) {
    ASSERT_EQ(rand.getDatatype(), Datatype::Double);
    ASSERT_GE(rand.getDouble(), 0.0);
    ASSERT_LT(rand.getDouble(), 1.0);
    histogram[std::abs(rand.getInt()) % 10]++;
  }

  // A simple check whether the numbers are sufficiently random. It has a
  // negligible probability of failure.
  for (const auto& count : histogram) {
    ASSERT_GE(count, 10);
    ASSERT_LE(count, 200);
  }

  size_t numSwaps = 0;
  for (size_t i = 1; i < resultVector.size(); ++i) {
    numSwaps += resultVector[i] < resultVector[i - 1];
  }
  ASSERT_GE(numSwaps, 100);
  ASSERT_LE(numSwaps, 900);

  // When we are part of a GROUP BY, we don't expect a vector but a single ID.
  {
    evaluationContext._isPartOfGroupBy = true;
    auto resultAsVariant2 = RandomExpression{}.evaluate(&evaluationContext);
    ASSERT_TRUE(std::holds_alternative<Id>(resultAsVariant2));
  }
}

TEST(RandomExpression, simpleMemberFunctions) {
  RandomExpression expr;
  ASSERT_TRUE(expr.getUnaggregatedVariables().empty());
  auto cacheKey = expr.getCacheKey({});
  ASSERT_THAT(cacheKey, ::testing::StartsWith("RAND "));
  ASSERT_EQ(cacheKey, expr.getCacheKey({}));
  // Note: Since the cache key is sampled randomly, the following test has a
  // probability of `1 / 2^64` of a spurious failure.
  ASSERT_NE(cacheKey, RandomExpression{}.getCacheKey({}));
}

using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
// The tests for UUID expressions follow almost exactly the pattern
// of the above defined test for RandomExpression.
TEST(UuidExpression, simpleMemberFunctionStrUuid) {
  StrUuidExpression strUuid;
  ASSERT_TRUE(strUuid.getUnaggregatedVariables().empty());
  auto cacheKeyStrUuid = strUuid.getCacheKey({});
  ASSERT_THAT(cacheKeyStrUuid, ::testing::StartsWith("STRUUID "));
  ASSERT_EQ(cacheKeyStrUuid, strUuid.getCacheKey({}));
  StrUuidExpression strUuid2;
  ASSERT_NE(cacheKeyStrUuid, strUuid2.getCacheKey({}));
}

TEST(UuidExpression, simpleMemberFunctionLitUuid) {
  UuidExpression iriUuid;
  ASSERT_TRUE(iriUuid.getUnaggregatedVariables().empty());
  auto cacheKeyIriUuid = iriUuid.getCacheKey({});
  ASSERT_THAT(cacheKeyIriUuid, ::testing::StartsWith("UUID "));
  ASSERT_EQ(cacheKeyIriUuid, iriUuid.getCacheKey({}));
  UuidExpression iriUuid2;
  ASSERT_NE(cacheKeyIriUuid, iriUuid2.getCacheKey({}));
}

TEST(UuidExpression, evaluateStrUuidExpression) {
  TestContext testContext{};
  auto& evaluationContext = testContext.context;
  evaluationContext._beginIndex = 43;
  evaluationContext._endIndex = 1044;
  auto resultAsVariant = StrUuidExpression{}.evaluate(&evaluationContext);

  using V = VectorWithMemoryLimit<IdOrLiteralOrIri>;
  ASSERT_TRUE(std::holds_alternative<V>(resultAsVariant));
  const auto& resultVector = std::get<V>(resultAsVariant);
  ASSERT_EQ(resultVector.size(), 1001);

  // check that none of the results equals all previous results
  std::unordered_set<std::string> strUuids;
  for (auto uuid : resultVector) {
    ASSERT_TRUE(std::holds_alternative<LocalVocabEntry>(uuid));
    LiteralOrIri litUuid = std::get<LocalVocabEntry>(uuid);
    ASSERT_TRUE(litUuid.isLiteral());
    std::string_view strUuid =
        asStringViewUnsafe(litUuid.getLiteral().getContent());
    ASSERT_EQ(strUuids.find(std::string(strUuid)), strUuids.end());
    strUuids.insert(std::string(strUuid));
  }

  evaluationContext._isPartOfGroupBy = true;
  auto resultAsVariant2 = StrUuidExpression{}.evaluate(&evaluationContext);
  ASSERT_TRUE(std::holds_alternative<IdOrLiteralOrIri>(resultAsVariant2));
  IdOrLiteralOrIri litOrIriUuid = std::get<IdOrLiteralOrIri>(resultAsVariant2);
  ASSERT_TRUE(std::holds_alternative<LocalVocabEntry>(litOrIriUuid));
  ASSERT_TRUE(std::get<LocalVocabEntry>(litOrIriUuid).isLiteral());
}

TEST(UuidExpression, evaluateUuidExpression) {
  TestContext testContext{};
  auto& evaluationContext = testContext.context;
  evaluationContext._beginIndex = 43;
  evaluationContext._endIndex = 1044;
  auto resultAsVariant = UuidExpression{}.evaluate(&evaluationContext);

  using V = VectorWithMemoryLimit<IdOrLiteralOrIri>;
  ASSERT_TRUE(std::holds_alternative<V>(resultAsVariant));
  const auto& resultVector = std::get<V>(resultAsVariant);
  ASSERT_EQ(resultVector.size(), 1001);

  // check that none of the results equals all of the other results
  std::unordered_set<std::string> strUuids;
  for (auto uuid : resultVector) {
    ASSERT_TRUE(std::holds_alternative<LocalVocabEntry>(uuid));
    LiteralOrIri litUuid = std::get<LocalVocabEntry>(uuid);
    ASSERT_TRUE(litUuid.isIri());
    std::string_view iriUuid =
        asStringViewUnsafe(litUuid.getIri().getContent());
    ASSERT_EQ(strUuids.find(std::string(iriUuid)), strUuids.end());
    strUuids.insert(std::string(iriUuid));
  }

  evaluationContext._isPartOfGroupBy = true;
  auto resultAsVariant2 = UuidExpression{}.evaluate(&evaluationContext);
  ASSERT_TRUE(std::holds_alternative<IdOrLiteralOrIri>(resultAsVariant2));
  IdOrLiteralOrIri litOrIriUuid = std::get<IdOrLiteralOrIri>(resultAsVariant2);
  ASSERT_TRUE(std::holds_alternative<LocalVocabEntry>(litOrIriUuid));
  ASSERT_TRUE(std::get<LocalVocabEntry>(litOrIriUuid).isIri());
}
