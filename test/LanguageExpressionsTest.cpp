//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "./util/GTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/ValueIdComparators.h"
#include "gtest/gtest.h"
#include "index/ConstantsIndexBuilding.h"
#include "util/IndexTestHelpers.h"

using namespace sparqlExpression;
using ad_utility::source_location;
using ad_utility::testing::DateId;
using ad_utility::testing::DoubleId;
using ad_utility::testing::IntId;
using ad_utility::triple_component::LiteralOrIri;
using strOpt = std::optional<std::string>;
using LanguageTagGetter = sparqlExpression::detail::LanguageTagValueGetter;

auto lit = ad_utility::testing::tripleComponentLiteral;
auto T = ad_utility::testing::BoolId(true);
auto F = ad_utility::testing::BoolId(false);
auto U = ad_utility::testing::UndefId();
auto maxId = Id::max();

// Define a test context like in SparqlExpressionTestHelpers.h (take a look for
// better description/explanation of the context). This is necessary because the
// context there is not arbitrarily adaptable (used by other tests which depend
// on its order and structure), thus we declare here a context which is more
// suitable when testing on language tags.
struct TestContext {
  static const inline std::string turtleInput =
      "<s0> <label> <https:://some_example/iri> . "
      "<s1> <city_name> \"leipzig\" . "
      "<s2> <value> <http://www.w3.org/2001/XMLSchema#int> . "
      "<s3> <at> <http://www.w3.org/2001/XMLSchema#date> . "
      "<s4> <city_name> \"friburgo\"@es . "
      "<s5> <city_name> \"freiburg\"@de-LATN-CH . "
      "<s6> <city_name> \"munich\"@de-DE . "
      "<s7> <city_name> \"hamburg\"@de . "
      "<s8> <city_name> \"düsseldorf\"@de-AT";

  QueryExecutionContext* qec = ad_utility::testing::getQec(turtleInput);
  VariableToColumnMap varToColMap;
  LocalVocab localVocab;
  IdTable table{qec->getAllocator()};
  sparqlExpression::EvaluationContext context{
      *qec,
      varToColMap,
      table,
      qec->getAllocator(),
      localVocab,
      std::make_shared<ad_utility::CancellationHandle<>>(),
      EvaluationContext::TimePoint::max()};
  std::function<Id(const std::string&)> getId =
      ad_utility::testing::makeGetId(qec->getIndex());

  // Vocab Ids which represent literals (with and without language tags)
  Id litId1 = getId("\"leipzig\""), litId2 = getId("\"friburgo\"@es"),
     litId3 = getId("\"freiburg\"@de-LATN-CH"),
     litId4 = getId("\"munich\"@de-DE"), litId5 = getId("\"hamburg\"@de"),
     litId6 = getId("\"düsseldorf\"@de-AT");
  // Vocab Id which are non-literal
  Id iriId1 = getId("<https:://some_example/iri>"),
     iriId2 = getId("<http://www.w3.org/2001/XMLSchema#int>"),
     iriId3 = getId("<http://www.w3.org/2001/XMLSchema#date>");

  // Local-Vocab Ids which represent are either iri or literal
  Id locVocIri1, locVocIri2, locVocLit1, locVocLit2, locVocLit3, locVocLit4;

  // valid variable strings (used for table columns)
  std::vector<std::string> strVar = {"?literals", "?mixed"};
  // assert valid variable w.r.t. the table in context
  auto isValidVariableStr(const std::string& var) -> bool {
    return std::find(strVar.begin(), strVar.end(), var) != strVar.end();
  };

  TestContext() {
    constexpr auto lit = [](const std::string& s) {
      return ad_utility::triple_component::LiteralOrIri::
          fromStringRepresentation(s);
    };
    constexpr auto iri = [](const std::string& s) {
      return ad_utility::triple_component::LiteralOrIri::iriref(s);
    };

    locVocIri1 =
        Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
            iri("<https:://some_example/iri>")));
    locVocIri2 =
        Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
            iri("<http://www.w3.org/2001/XMLSchema#integer>")));
    locVocLit1 = Id::makeFromLocalVocabIndex(
        localVocab.getIndexAndAddIfNotContained(lit("\"leipzig\"")));
    locVocLit2 = Id::makeFromLocalVocabIndex(
        localVocab.getIndexAndAddIfNotContained(lit("\"munich\"@de-DE")));
    locVocLit3 = Id::makeFromLocalVocabIndex(
        localVocab.getIndexAndAddIfNotContained(lit("\"hamburg\"@de")));
    locVocLit4 = Id::makeFromLocalVocabIndex(
        localVocab.getIndexAndAddIfNotContained(lit("\"düsseldorf\"@de-AT")));

    table.setNumColumns(2);
    // Order of the columns:
    // ?literals, ?mixed
    table.push_back({litId1, DoubleId(0.1)});
    table.push_back({litId2, IntId(1)});
    table.push_back({litId3, locVocLit3});
    table.push_back({litId4, iriId1});
    table.push_back({locVocLit2, iriId2});
    table.push_back({locVocLit4, locVocIri1});
    table.push_back({litId5, locVocIri1});
    table.push_back({litId6, locVocLit1});

    context._beginIndex = 0;
    context._endIndex = table.size();

    // Define the mapping from variable names to column indices.
    using V = Variable;
    varToColMap[V{strVar[0]}] = makeAlwaysDefinedColumn(0);
    varToColMap[V{strVar[1]}] = makeAlwaysDefinedColumn(1);
  }
};

// ____________________________________________________________________________
auto litOrIri = [](const std::string& literal) {
  return LiteralOrIri::fromStringRepresentation(
      absl::StrCat("\""sv, literal, "\""sv));
};

// ____________________________________________________________________________
auto assertLangTagValueGetter =
    [](const std::vector<Id>& input, const std::vector<strOpt>& expected,
       LanguageTagGetter& langTagValueGetter, TestContext& testContext,
       ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
      auto trace = generateLocationTrace(loc);
      EXPECT_EQ(input.size(), expected.size());
      auto ctx = &testContext.context;
      for (size_t i = 0; i < input.size(); i++) {
        ASSERT_EQ(expected[i], langTagValueGetter(input[i], ctx));
      }
    };

// ____________________________________________________________________________
auto getLangExpression =
    [](const std::string& variable) -> SparqlExpression::Ptr {
  // initialize with a child expression which has to be a VariableExpression!
  return makeLangExpression(
      std::make_unique<VariableExpression>(Variable{variable}));
};

// ____________________________________________________________________________
auto getLangMatchesExpression =
    [](const std::string& variable,
       const std::string& langRange) -> SparqlExpression::Ptr {
  return makeLangMatchesExpression(
      getLangExpression(variable),
      std::make_unique<StringLiteralExpression>(lit(langRange)));
};

// ____________________________________________________________________________
template <auto GetExpr, typename T>
auto testLanguageExpressions = [](const std::vector<T>& expected,
                                  const std::string& variable,
                                  const auto&... args) {
  TestContext context;
  ASSERT_TRUE(context.isValidVariableStr(variable));
  SparqlExpression::Ptr expr = GetExpr(variable, args...);
  auto resultAsVariant = expr->evaluate(&context.context);
  const auto& result = std::get<VectorWithMemoryLimit<T>>(resultAsVariant);
  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
};

// ____________________________________________________________________________
TEST(LanguageTagGetter, testLanguageTagValueGetterWithoutVocabId) {
  TestContext testContext{};
  LanguageTagGetter langTagGetter{};
  Id dateId1 = DateId(DateYearOrDuration::parseXsdDatetime,
                      "1900-12-13T03:12:00.33Z"),
     dateId2 = DateId(DateYearOrDuration::parseXsdDate, "2025-01-01");

  // define the input containing IDs (non literal) and corresponding expected
  // return values w.r.t. LanguageTagValueGetter
  std::vector<Id> in = {dateId1,    dateId2,          F, T,
                        IntId(323), DoubleId(234.23), U};
  std::vector<strOpt> expected{"", "", "", "", "", "", std::nullopt};

  assertLangTagValueGetter(in, expected, langTagGetter, testContext);

  // test corner case (should trigger AD_FAIL())
  AD_EXPECT_THROW_WITH_MESSAGE(LanguageTagGetter{}(maxId, &testContext.context),
                               ::testing::HasSubstr("should be unreachable"));
}

// ____________________________________________________________________________
TEST(LanguageTagGetter, testLanguageTagValueGetterWithVocab) {
  TestContext testContext{};
  LanguageTagGetter langTagGetter{};

  // input and expectation values w.r.t. Vocab-IDs
  std::vector<Id> in = {testContext.litId1, testContext.litId2,
                        testContext.litId3, testContext.litId4,
                        testContext.litId5, testContext.litId6,
                        testContext.iriId1, testContext.iriId2};
  std::vector<strOpt> expected = {"",   "es",    "de-LATN-CH", "de-DE",
                                  "de", "de-AT", std::nullopt, std::nullopt};
  assertLangTagValueGetter(in, expected, langTagGetter, testContext);
}

// ____________________________________________________________________________
TEST(LanguageTagGetter, testLanguageTagValueGetterWithLocalVocab) {
  TestContext testContext{};
  LanguageTagGetter langTagGetter{};

  // input and expectation values w.r.t. LocalVocab-IDs
  std::vector<Id> in = {testContext.locVocIri1, testContext.locVocIri2,
                        testContext.locVocLit1, testContext.locVocLit2,
                        testContext.locVocLit3, testContext.locVocLit4};
  std::vector<strOpt> expected = {std::nullopt, std::nullopt, "",
                                  "de-DE",      "de",         "de-AT"};
  assertLangTagValueGetter(in, expected, langTagGetter, testContext);
}

// ____________________________________________________________________________
TEST(LangExpression, testLangExpressionOnLiteralColumn) {
  testLanguageExpressions<getLangExpression, IdOrLiteralOrIri>(
      {litOrIri(""), litOrIri("es"), litOrIri("de-LATN-CH"), litOrIri("de-DE"),
       litOrIri("de-DE"), litOrIri("de-AT"), litOrIri("de"), litOrIri("de-AT")},
      "?literals");
}

// ____________________________________________________________________________
TEST(LangExpression, testLangExpressionOnMixedColumn) {
  testLanguageExpressions<getLangExpression, IdOrLiteralOrIri>(
      {litOrIri(""), litOrIri(""), litOrIri("de"), U, U, U, U, litOrIri("")},
      "?mixed");
}

// ____________________________________________________________________________
TEST(LangExpression, testSimpleMethods) {
  auto langExpr =
      makeLangExpression(std::make_unique<VariableExpression>(Variable{"?x"}));
  ASSERT_TRUE(langExpr->containsLangExpression());
  auto optVar = getVariableFromLangExpression(langExpr.get());
  ASSERT_TRUE(optVar.has_value());
  ASSERT_EQ(optVar.value().name(), "?x");
  langExpr = makeLangExpression(std::make_unique<IdExpression>(IntId(1)));
  ASSERT_TRUE(langExpr->containsLangExpression());
  optVar = getVariableFromLangExpression(langExpr.get());
  ASSERT_TRUE(!optVar.has_value());
}

// ____________________________________________________________________________
TEST(SparqlExpression, testLangMatchesOnLiteralColumn) {
  testLanguageExpressions<getLangMatchesExpression, Id>(
      {F, F, T, T, T, T, T, T}, "?literals", "de");
  testLanguageExpressions<getLangMatchesExpression, Id>(
      {F, T, T, T, T, T, T, T}, "?literals", "*");
  testLanguageExpressions<getLangMatchesExpression, Id>(
      {F, F, T, F, F, F, F, F}, "?literals", "de-LATN-CH");
  testLanguageExpressions<getLangMatchesExpression, Id>(
      {F, F, T, F, F, F, F, F}, "?literals", "DE-LATN-CH");
  testLanguageExpressions<getLangMatchesExpression, Id>(
      {F, F, F, F, F, F, F, F}, "?literals", "en-US");
  testLanguageExpressions<getLangMatchesExpression, Id>(
      {F, F, F, F, F, F, F, F}, "?literals", "");
  testLanguageExpressions<getLangMatchesExpression, Id>(
      {F, F, T, T, T, T, F, T}, "?literals", "de-*");
  testLanguageExpressions<getLangMatchesExpression, Id>(
      {F, F, T, T, T, T, F, T}, "?literals", "De-*");
}

// ____________________________________________________________________________
TEST(SparqlExpression, testLangMatchesOnMixedColumn) {
  testLanguageExpressions<getLangMatchesExpression, Id>(
      {F, F, T, U, U, U, U, F}, "?mixed", "de");
  testLanguageExpressions<getLangMatchesExpression, Id>(
      {F, F, T, U, U, U, U, F}, "?mixed", "dE");
  testLanguageExpressions<getLangMatchesExpression, Id>(
      {F, F, T, U, U, U, U, F}, "?mixed", "*");
  testLanguageExpressions<getLangMatchesExpression, Id>(
      {F, F, F, U, U, U, U, F}, "?mixed", "en-US");
  testLanguageExpressions<getLangMatchesExpression, Id>(
      {F, F, F, U, U, U, U, F}, "?mixed", "");
}
