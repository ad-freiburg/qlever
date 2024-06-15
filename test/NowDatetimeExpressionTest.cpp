//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "./SparqlExpressionTestHelpers.h"
#include "engine/sparqlExpressions/NowDatetimeExpression.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using namespace sparqlExpression;

TEST(NowDatetimeExpression, nowExpressionEvaluate) {
  std::string strDate = "2011-01-10T14:45:13.815-05:00";
  TestContext testContext{};
  auto& evaluationContext = testContext.context;
  evaluationContext._beginIndex = 43;
  evaluationContext._endIndex = 1044;

  // The result should hold IDs (from Date) given that NOW() should return by
  // definition a xsd:dateTime: "2011-01-10T14:45:13.815-05:00"^^xsd:dateTime
  auto resultAsVariant =
      NowDatetimeExpression{strDate}.evaluate(&evaluationContext);

  using V = VectorWithMemoryLimit<Id>;
  ASSERT_TRUE(std::holds_alternative<V>(resultAsVariant));
  const auto& resultDateVector = std::get<V>(resultAsVariant);
  ASSERT_EQ(resultDateVector.size(), 1001);

  DateOrLargeYear dateNowTest =
      DateOrLargeYear(DateOrLargeYear::parseXsdDatetime(strDate));

  for (auto dateNow : resultDateVector) {
    ASSERT_EQ(dateNow.getDatatype(), Datatype::Date);
    // check we that NowDatetimeExpression returns in evaluation
    // always the same complete datetime.
    ASSERT_EQ(dateNow.getDate(), dateNowTest);
  }

  evaluationContext._isPartOfGroupBy = true;
  auto resultAsVariant2 =
      NowDatetimeExpression{strDate}.evaluate(&evaluationContext);
  ASSERT_TRUE(std::holds_alternative<Id>(resultAsVariant2));
  DateOrLargeYear singleDateNow = std::get<Id>(resultAsVariant2).getDate();
  ASSERT_EQ(singleDateNow, dateNowTest);
}

TEST(NowDatetimeExpression, getCacheKeyNowExpression) {
  std::string strDate = "2011-01-10T14:45:13.815-05:00";
  NowDatetimeExpression dateNow(strDate);
  ASSERT_TRUE(dateNow.getUnaggregatedVariables().empty());
  auto cacheKey = dateNow.getCacheKey({});
  ASSERT_THAT(cacheKey, ::testing::StartsWith("NOW "));
  ASSERT_EQ(cacheKey, dateNow.getCacheKey({}));
  ASSERT_NE(cacheKey, NowDatetimeExpression{strDate}.getCacheKey({}));
}
