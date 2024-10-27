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

  // technically the evaluation context isn't necessary.
  evaluationContext._beginIndex = 43;
  evaluationContext._endIndex = 1044;

  // The result should hold an ID (from Date) given that NOW() should return by
  // definition a xsd:dateTime: "2011-01-10T14:45:13.815-05:00"^^xsd:dateTime
  auto resultAsVariant =
      NowDatetimeExpression{strDate}.evaluate(&evaluationContext);
  ASSERT_TRUE(std::holds_alternative<Id>(resultAsVariant));
  const auto& resultDate = std::get<Id>(resultAsVariant);

  DateYearOrDuration dateNowTest =
      DateYearOrDuration(DateYearOrDuration::parseXsdDatetime(strDate));

  ASSERT_EQ(resultDate.getDatatype(), Datatype::Date);
  ASSERT_EQ(resultDate.getDate(), dateNowTest);

  evaluationContext._isPartOfGroupBy = true;
  auto resultAsVariant2 =
      NowDatetimeExpression{strDate}.evaluate(&evaluationContext);
  ASSERT_TRUE(std::holds_alternative<Id>(resultAsVariant2));
  DateYearOrDuration singleDateNow = std::get<Id>(resultAsVariant2).getDate();
  ASSERT_EQ(singleDateNow, dateNowTest);
}

TEST(NowDatetimeExpression, getCacheKeyNowExpression) {
  std::string strDate1 = "2011-01-10T14:45:13.815-05:00";
  std::string strDate2 = "2024-06-18T12:16:33.815-06:00";
  NowDatetimeExpression dateNow1(strDate1);
  NowDatetimeExpression dateNow2(strDate2);
  ASSERT_TRUE(dateNow1.getUnaggregatedVariables().empty());
  auto cacheKey1 = dateNow1.getCacheKey({});
  ASSERT_THAT(cacheKey1, ::testing::StartsWith("NOW "));
  ASSERT_EQ(cacheKey1, dateNow1.getCacheKey({}));
  // Given that these use the same date-time string the key should be equal.
  ASSERT_EQ(cacheKey1, NowDatetimeExpression{strDate1}.getCacheKey({}));
  // Given that dateNow1 and dateNow2 are constructed from different date-time
  // strings, it should be rather unlikely that their cache-keys are equal.
  auto cacheKey2 = dateNow2.getCacheKey({});
  ASSERT_NE(cacheKey1, cacheKey2);
}
