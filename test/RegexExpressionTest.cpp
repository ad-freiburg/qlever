// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include <optional>
#include <string>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/RegexExpression.h"

using namespace sparqlExpression;
using ad_utility::source_location;

namespace {
auto lit = ad_utility::testing::tripleComponentLiteral;

constexpr auto T = Id::makeFromBool(true);
constexpr auto F = Id::makeFromBool(false);
constexpr Id U = Id::makeUndefined();

// _____________________________________________________________________________
auto literal(const std::string& literal,
             std::string_view langtagOrDatatype = "") {
  return std::make_unique<StringLiteralExpression>(
      lit(literal, langtagOrDatatype));
}

// _____________________________________________________________________________
auto variable(std::string literal) {
  return std::make_unique<VariableExpression>(Variable{std::move(literal)});
}

// _____________________________________________________________________________
bool isPrefixExpression(const SparqlExpression::Ptr& expression) {
  return dynamic_cast<PrefixRegexExpression*>(expression.get());
}

// Make `RegexExpression` from given the `child` (the expression on which to
// apply the regex), `regex`, and optional `flags`. The argument `childAsStr` is
// true iff the expression is enclosed in a `STR()` function.
SparqlExpression::Ptr makeTestRegexExpression(
    SparqlExpression::Ptr child, SparqlExpression::Ptr regex,
    SparqlExpression::Ptr flags = nullptr, bool childAsStr = false) {
  if (childAsStr) {
    child = makeStrExpression(std::move(child));
  }

  return makeRegexExpression(std::move(child), std::move(regex),
                             std::move(flags));
}

// Special case of the `makeRegexExpression` above, where the `child`
// expression is a variable.
SparqlExpression::Ptr makeRegexExpression(
    std::string variable, std::string regex,
    std::optional<std::string> flags = std::nullopt, bool childAsStr = false) {
  SparqlExpression::Ptr variableExpression =
      std::make_unique<VariableExpression>(Variable{std::move(variable)});
  // The regex and the flags both have to be enquoted. This is normally ensured
  // by the SPARQL parser. For easier readability of the tests we add those
  // quotes here.
  regex = absl::StrCat("\"", regex, "\"");
  SparqlExpression::Ptr regexExpression =
      std::make_unique<StringLiteralExpression>(lit(std::move(regex)));
  SparqlExpression::Ptr flagsExpression = nullptr;
  if (flags.has_value()) {
    flagsExpression =
        SparqlExpression::Ptr{std::make_unique<StringLiteralExpression>(
            lit(absl::StrCat("\"", flags.value(), "\"")))};
  }
  return makeTestRegexExpression(std::move(variableExpression),
                                 std::move(regexExpression),
                                 std::move(flagsExpression), childAsStr);
}
}  // namespace

// Test that the expression `leftValue Comparator rightValue`, when evaluated on
// the `TestContext` (see above), yields the `expected` result.
void testWithExplicitResult(
    const SparqlExpression& expression, const std::vector<Id>& expected,
    const std::optional<size_t>& numInputs = std::nullopt,
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  TestContext ctx;
  auto trace = generateLocationTrace(std::move(l), "testWithExplicitResult");
  if (numInputs.has_value()) {
    ctx.context._endIndex = numInputs.value();
  }
  auto resultAsVariant = expression.evaluate(&ctx.context);
  ASSERT_TRUE(
      std::holds_alternative<VectorWithMemoryLimit<Id>>(resultAsVariant));
  const auto& result = std::get<VectorWithMemoryLimit<Id>>(resultAsVariant);

  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}

// Test that the expression works with variables which cannot be evaluated
// during creation of the expression.
void testValuesInVariables(
    const std::vector<std::array<std::string, 3>>& inputValues,
    const std::vector<Id>& expected, bool flagsUsed,
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  TestContext ctx;
  auto trace = generateLocationTrace(std::move(l), "testWithExplicitResult");
  ctx.varToColMap.clear();
  ctx.varToColMap[Variable{"?string"}] = makeAlwaysDefinedColumn(0);
  ctx.varToColMap[Variable{"?regex"}] = makeAlwaysDefinedColumn(1);
  ctx.varToColMap[Variable{"?flags"}] = makeAlwaysDefinedColumn(2);
  ctx.table.clear();
  ctx.table.setNumColumns(3);
  auto toLiteralId = [&ctx](const std::string& value) {
    return Id::makeFromLocalVocabIndex(
        ctx.localVocab.getIndexAndAddIfNotContained(
            ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes(
                value)));
  };
  for (const auto& value : inputValues) {
    ctx.table.push_back({toLiteralId(value.at(0)), toLiteralId(value.at(1)),
                         toLiteralId(value.at(2))});
  }
  ctx.context._endIndex = inputValues.size();
  auto resultAsVariant =
      makeRegexExpression(variable("?string"), variable("?regex"),
                          flagsUsed ? variable("?flags") : nullptr)
          ->evaluate(&ctx.context);
  const auto& result = std::get<VectorWithMemoryLimit<Id>>(resultAsVariant);

  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}

// _____________________________________________________________________________
auto testNonPrefixRegex = [](std::string variable, std::string regex,
                             const std::vector<Id>& expectedResult,
                             bool childAsStr = false,
                             source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(std::move(l), "testNonPrefixRegex");
  auto expr = makeRegexExpression(std::move(variable), std::move(regex),
                                  std::nullopt, childAsStr);
  EXPECT_FALSE(isPrefixExpression(expr));
  testWithExplicitResult(*expr, expectedResult);
};

// Tests where the expression is a variable and the regex is not a simple prefix
// regex (that translates to a simple range search).
TEST(RegexExpression, nonPrefixRegex) {
  // ?vocab column is `"Beta", "alpha", "älpha"
  // ?mixed column is `1, -0.1, <x>`
  auto test = testNonPrefixRegex;
  test("?vocab", "ph", {F, T, T});
  test("?vocab", "l.h", {F, T, T});
  test("?vocab", "l[^a]{2}a", {F, T, T});
  test("?vocab", "[el][^a]*a", {T, T, T});
  test("?vocab", "B", {T, F, F});

  // The match is case-sensitive by default.
  test("?vocab", "b", {F, F, F});

  // A prefix regex, but not a fixed string.
  test("?vocab", "^a.*", {F, T, F});

  test("?mixed", "x", {U, U, U});
  test("?mixed", "x", {F, F, T}, true);
  test("?mixed", "1$", {U, U, U});
  test("?mixed", "1$", {T, T, F}, true);

  // ?localVocab column is "notInVocabA", "notInVocabB", <"notInVocabD">
  test("?localVocab", "InV", {T, T, U});

  // The IRI is only considered when testing with a STR expression
  test("?localVocab", "Vocab[AD]", {T, F, T}, true);

  {
    auto expr = makeTestRegexExpression(variable("?vocab"), variable("?vocab"));
    EXPECT_FALSE(isPrefixExpression(expr));
    testWithExplicitResult(*expr, {T, T, T});
  }

  // Values that are not simple literals should result in undefined
  {
    auto expr =
        makeTestRegexExpression(variable("?vocab"), variable("?doubles"));
    EXPECT_FALSE(isPrefixExpression(expr));
    testWithExplicitResult(*expr, {U, U, U});
  }

  testValuesInVariables({{"Abc", "[A-Z]", ""},
                         {"abc", "[A-Z]", ""},
                         {"aBc", "[A-Z]", ""},
                         {"abC", "[A-Z]", ""},
                         {"", "", ""},
                         {"", "(invalid", ""}},
                        {T, F, T, T, T, U}, false);
}

// Test where the expression is not simply a variable.
TEST(RegexExpression, inputNotVariable) {
  // Our expression is a fixed string literal: "hallo".
  VectorWithMemoryLimit<IdOrLiteralOrIri> input{
      ad_utility::testing::getQec()->getAllocator()};
  input.push_back(ad_utility::triple_component::LiteralOrIri(lit("\"hallo\"")));

  {
    auto child =
        std::make_unique<sparqlExpression::detail::SingleUseExpression>(
            input.clone());

    // "hallo" matches the regex "ha".
    auto expr = makeRegexExpression(std::move(child), literal("\"ha\""),
                                    literal("\"\""));
    std::vector<Id> expected;
    expected.push_back(Id::makeFromBool(true));
    testWithExplicitResult(*expr, expected, input.size());
  }

  // Without variable it cannot be a prefix regex expression.
  {
    auto child =
        std::make_unique<sparqlExpression::detail::SingleUseExpression>(
            input.clone());

    auto expr =
        makeTestRegexExpression(std::move(child), literal("\"^ha\""), nullptr);
    EXPECT_FALSE(isPrefixExpression(expr));
  }
}

// _____________________________________________________________________________
auto testNonPrefixRegexWithFlags =
    [](std::string variable, std::string regex, std::string flags,
       const std::vector<Id>& expectedResult,
       source_location l = AD_CURRENT_SOURCE_LOC()) {
      auto trace = generateLocationTrace(l, "testNonPrefixRegexWithFlags");
      auto expr = makeRegexExpression(std::move(variable), std::move(regex),
                                      std::move(flags));
      EXPECT_FALSE(isPrefixExpression(expr));
      testWithExplicitResult(*expr, expectedResult);
    };

// Fun with flags.
TEST(RegexExpression, nonPrefixRegexWithFlags) {
  // ?vocab column is `"Beta", "alpha", "älpha"
  // ?mixed column is `1, -0.1, A`
  auto test = testNonPrefixRegexWithFlags;
  // case-insensitive.
  test("?vocab", "L.H", "", {F, F, F});
  test("?vocab", "L.H", "i", {F, T, T});
  test("?vocab", "l[^a]{2}A", "", {F, F, F});
  test("?vocab", "l[^a]{2}A", "i", {F, T, T});
  test("?vocab", "[El][^a]*A", "", {F, F, F});
  test("?vocab", "[El][^a]*A", "i", {T, T, T});
  test("?vocab", "b", "", {F, F, F});
  test("?vocab", "b", "i", {T, F, F});

  // Not a special prefix filter because of the explicit flags.
  // TODO<joka921>, Discuss with Hannah: The behavior here is inconsistent
  // because of the primary level prefix filter. Should we introduce a special
  // syntax for the prefix filter, as it is non-standard?
  // Note that for our special prefix filter the third comparison would be true
  // (for almost all locales). To remove this inconsistency we could introduce a
  // special syntax for the prefix filter, but then users would only benefit if
  // they use the special syntax, which most users will not do.
  // TODO<joka921> check whether the SPARQL STARTSWITH function is consistent
  // with the behavior of our prefix filter.

  test("?vocab", "^alp", "i", {F, T, F});

  // TODO<joka921>  Add tests for other flags (maybe the non-greedy one?)

  // Undefined variable should result in undefined results
  {
    auto expr = makeRegexExpression(variable("?vocab"), literal("\"b\""),
                                    variable("?b"));
    EXPECT_FALSE(isPrefixExpression(expr));
    testWithExplicitResult(*expr, {U, U, U});
  }
  // Values that are not simple literals should result in undefined
  {
    auto expr = makeRegexExpression(variable("?vocab"), literal("\"b\""),
                                    variable("?ints"));
    EXPECT_FALSE(isPrefixExpression(expr));
    testWithExplicitResult(*expr, {U, U, U});
  }
  {
    auto expr = makeRegexExpression(variable("?vocab"), variable("?ints"),
                                    literal("i"));
    EXPECT_FALSE(isPrefixExpression(expr));
    testWithExplicitResult(*expr, {U, U, U});
  }

  testValuesInVariables({{"Abc", "[A-Z]", ""},
                         {"abc", "[A-Z]", ""},
                         {"abc", "[A-Z]", "i"},
                         {"abc", "[A-Z]", "imsU"},
                         {"", "", ""},
                         {"", "(invalid", ""},
                         {"", "", "invalid"}},
                        {T, F, T, T, T, U, U}, true);
}

namespace sparqlExpression {
// Test the `getPrefixRegex` function (which returns `std::nullopt` if the regex
// is not a simple prefix regex).
TEST(RegexExpression, getPrefixRegex) {
  ASSERT_EQ(std::nullopt, PrefixRegexExpression::getPrefixRegex("alpha"));
  ASSERT_EQ(std::nullopt, PrefixRegexExpression::getPrefixRegex("^al.ha"));
  ASSERT_EQ(std::nullopt, PrefixRegexExpression::getPrefixRegex("^alh*"));
  ASSERT_EQ(std::nullopt, PrefixRegexExpression::getPrefixRegex("^a(lh)"));

  ASSERT_EQ("alpha", PrefixRegexExpression::getPrefixRegex("^alpha"));
  ASSERT_EQ(R"(\al*ph.a()",
            PrefixRegexExpression::getPrefixRegex(R"(^\\al\*ph\.a\()"));
  // Invalid escaping of `"` (no need to escape it).
  ASSERT_THROW(PrefixRegexExpression::getPrefixRegex(R"(^\")"),
               std::runtime_error);
}
}  // namespace sparqlExpression

// _____________________________________________________________________________
auto testPrefixRegexUnorderedColumn =
    [](std::string variable, std::string regex,
       const std::vector<Id>& expectedResult, bool childAsStr = false,
       source_location l = AD_CURRENT_SOURCE_LOC()) {
      auto trace = generateLocationTrace(l, "testUnorderedPrefix");
      auto expr = makeRegexExpression(std::move(variable), std::move(regex),
                                      std::nullopt, childAsStr);
      EXPECT_TRUE(isPrefixExpression(expr));
      testWithExplicitResult(*expr, expectedResult);
    };

// _____________________________________________________________________________
TEST(RegexExpression, unorderedPrefixRegexUnorderedColumn) {
  auto test = testPrefixRegexUnorderedColumn;
  // ?vocab column is `"Beta", "alpha", "älpha"
  // ?mixed column is `1, -0.1, <x>`

  test("?vocab", "^Be", {T, F, F});
  // Prefix filters are currently always case-insensitive.
  test("?vocab", "^be", {T, F, F});
  // Prefix filters currently always work on the primary level, where `a` and
  // `ä` are considered equal.
  test("?vocab", "^al", {F, T, T});
  test("?vocab", "^äl", {F, T, T});

  test("?vocab", "^c", {F, F, F});

  // We explicitly need to pass the STR() function for non-literal entries.
  test("?mixed", "^x", {F, F, T}, true);
  test("?mixed", "^x", {F, F, F}, false);

  // TODO<joka921> Prefix filters on numbers do not yet work.
}

// _____________________________________________________________________________
auto testPrefixRegexOrderedColumn =
    [](std::string variableAsString, std::string regex,
       ad_utility::SetOfIntervals expected, bool childAsStr = false,
       source_location l = AD_CURRENT_SOURCE_LOC()) {
      auto trace = generateLocationTrace(l, "testPrefixRegexOrderedColumn");
      auto variable = Variable{variableAsString};
      TestContext ctx = TestContext::sortedBy(variable);
      auto expression = makeRegexExpression(variableAsString, regex,
                                            std::nullopt, childAsStr);
      EXPECT_TRUE(isPrefixExpression(expression));
      auto resultAsVariant = expression->evaluate(&ctx.context);
      const auto& result =
          std::get<ad_utility::SetOfIntervals>(resultAsVariant);
      ASSERT_EQ(result, expected);
    };

// _____________________________________________________________________________
TEST(RegexExpression, prefixRegexOrderedColumn) {
  auto test = testPrefixRegexOrderedColumn;
  // Sorted order (by bits of the valueIds):
  // ?vocab column is  "alpha", "älpha", "Beta"
  // ?mixed column is `1, -0.1, <x>`
  test("?vocab", "^Be", {{{2, 3}}});
  // Prefix filters are currently always case-insensitive.
  test("?vocab", "^be", {{{2, 3}}});
  // Prefix filters currently always work on the primary level, where `a` and
  // `ä` are considered equal.
  test("?vocab", "^al", {{{0, 2}}});
  test("?vocab", "^äl", {{{0, 2}}});
  test("?vocab", "^c", {});
  test("?mixed", "^x", {{{2, 3}}}, true);
}

// _____________________________________________________________________________
TEST(RegexExpression, getCacheKey) {
  using namespace ::testing;
  auto exp0 = makeRegexExpression("?first", "^alp");
  auto exp1 = makeRegexExpression("?first", "alp");
  auto exp2 = makeRegexExpression("?first", "alp");

  VariableToColumnMap map;
  map[Variable{"?first"}] = makeAlwaysDefinedColumn(0);
  map[Variable{"?second"}] = makeAlwaysDefinedColumn(1);
  EXPECT_TRUE(isPrefixExpression(exp0));
  EXPECT_THAT(
      exp0->getCacheKey(map),
      AllOf(StartsWith("Prefix REGEX"), HasSubstr("str:0"), HasSubstr("alp"),
            HasSubstr(exp0->children()[0]->getCacheKey(map))));
  EXPECT_NE(exp0->getCacheKey(map), exp1->getCacheKey(map));
  ASSERT_EQ(exp1->getCacheKey(map), exp2->getCacheKey(map));

  // Different regex, different cache key.
  auto exp3 = makeRegexExpression("?first", "alk");
  ASSERT_NE(exp1->getCacheKey(map), exp3->getCacheKey(map));

  // Different variable, different cache key.
  auto exp4 = makeRegexExpression("?second", "alp");
  ASSERT_NE(exp1->getCacheKey(map), exp4->getCacheKey(map));

  // Different flags, different cache key.
  auto exp5 = makeRegexExpression("?first", "alp", "im");
  ASSERT_NE(exp1->getCacheKey(map), exp5->getCacheKey(map));

  // Different variable name, but the variable is stored in the same column ->
  // same cache key.
  auto map2 = map;
  map2[Variable{"?otherFirst"}] = makeAlwaysDefinedColumn(0);
  auto exp6 = makeRegexExpression("?otherFirst", "alp");
  ASSERT_EQ(exp1->getCacheKey(map), exp6->getCacheKey(map2));

  auto exp7 = makeRegexExpression(variable("?first"), literal("alp"),
                                  variable("?second"));
  EXPECT_NE(exp6->getCacheKey(map), exp7->getCacheKey(map));

  auto exp8 = makeRegexExpression(variable("?first"), variable("?second"),
                                  literal("i"));
  EXPECT_NE(exp7->getCacheKey(map), exp8->getCacheKey(map));

  map[Variable{"?third"}] = makeAlwaysDefinedColumn(1);
  auto exp9 = makeRegexExpression(variable("?first"), variable("?second"),
                                  variable("?third"));
  EXPECT_NE(exp8->getCacheKey(map), exp9->getCacheKey(map));

  auto exp10 = makeRegexExpression("?first", "^alp", std::nullopt, true);
  EXPECT_THAT(exp10->getCacheKey(map),
              AllOf(HasSubstr("str:1"), HasSubstr("alp"),
                    HasSubstr(exp10->children()[0]->getCacheKey(map))));
  EXPECT_NE(exp0->getCacheKey(map), exp10->getCacheKey(map));
}

// _____________________________________________________________________________
TEST(RegexExpression, getChildren) {
  using namespace ::testing;
  EXPECT_THAT(makeRegexExpression("?a", "someRegex")->containedVariables(),
              ElementsAre(Pointee(Variable{"?a"})));
  EXPECT_THAT(makeRegexExpression("?a", "^someRegex")->containedVariables(),
              ElementsAre(Pointee(Variable{"?a"})));
  EXPECT_THAT(
      makeRegexExpression("?a", "someRegex", "imsU")->containedVariables(),
      ElementsAre(Pointee(Variable{"?a"})));
  EXPECT_THAT(makeTestRegexExpression(variable("?a"), literal("someRegex"),
                                      variable("?c"))
                  ->containedVariables(),
              ElementsAre(Pointee(Variable{"?a"}), Pointee(Variable{"?c"})));
  EXPECT_THAT(makeTestRegexExpression(variable("?a"), variable("?b"))
                  ->containedVariables(),
              ElementsAre(Pointee(Variable{"?a"}), Pointee(Variable{"?b"})));
  EXPECT_THAT(
      makeTestRegexExpression(variable("?a"), variable("?b"), variable("?c"))
          ->containedVariables(),
      ElementsAre(Pointee(Variable{"?a"}), Pointee(Variable{"?b"}),
                  Pointee(Variable{"?c"})));
}

// _____________________________________________________________________________
TEST(RegexExpression, invalidConstruction) {
  // The second argument must not have a datatype or langtag
  EXPECT_THROW(makeTestRegexExpression(variable("?a"), literal("\"b\"", "@en")),
               std::runtime_error);
  EXPECT_THROW(
      makeTestRegexExpression(variable("?a"), literal("\"b\"", "^^<someType>")),
      std::runtime_error);
  // The third argument must not have a language tag or datatype
  EXPECT_THROW(makeTestRegexExpression(variable("?a"), literal("\"b\""),
                                       literal("\"i\"", "@en")),
               std::runtime_error);
  EXPECT_THROW(makeTestRegexExpression(variable("?a"), literal("\"b\""),
                                       literal("\"i\"", "^^<someType>")),
               std::runtime_error);

  // Invalid regex (parentheses that are never closed).
  EXPECT_THROW(makeTestRegexExpression(variable("?a"), literal("\"(open\"")),
               std::runtime_error);

  // Invalid option flag.
  EXPECT_THROW(makeTestRegexExpression(variable("?a"), literal("\"a\""),
                                       literal("\"x\"")),
               std::runtime_error);
}

// _____________________________________________________________________________
TEST(RegexExpression, getEstimatesForFilterExpression) {
  using Estimates = SparqlExpressionPimpl::Estimates;
  auto hasEstimate = [](size_t sizeEstimate, size_t costEstimate) {
    using namespace ::testing;
    return AllOf(AD_FIELD(Estimates, sizeEstimate, Eq(sizeEstimate)),
                 AD_FIELD(Estimates, costEstimate, Eq(costEstimate)));
  };
  auto expression = makeRegexExpression("?a", "^abc");
  EXPECT_THAT(expression->getEstimatesForFilterExpression(10000, std::nullopt),
              hasEstimate(10, 10010));
  EXPECT_THAT(expression->getEstimatesForFilterExpression(100000, std::nullopt),
              hasEstimate(100, 100100));
  EXPECT_THAT(
      expression->getEstimatesForFilterExpression(10000, Variable{"?b"}),
      hasEstimate(10, 10010));
  EXPECT_THAT(
      expression->getEstimatesForFilterExpression(100000, Variable{"?b"}),
      hasEstimate(100, 100100));
  EXPECT_THAT(
      expression->getEstimatesForFilterExpression(10000, Variable{"?a"}),
      hasEstimate(10, 10));
  EXPECT_THAT(
      expression->getEstimatesForFilterExpression(100000, Variable{"?a"}),
      hasEstimate(100, 100));

  auto longRegexExpression = makeRegexExpression("?a", "^thisisverylong");
  EXPECT_THAT(longRegexExpression->getEstimatesForFilterExpression(
                  1000000000, std::nullopt),
              hasEstimate(10, 1000000010));
  EXPECT_THAT(longRegexExpression->getEstimatesForFilterExpression(
                  1000000000, Variable{"?a"}),
              hasEstimate(10, 10));

  auto zeroLengthExpression = makeRegexExpression("?a", "^");
  ASSERT_TRUE(isPrefixExpression(zeroLengthExpression));
  EXPECT_THAT(
      zeroLengthExpression->getEstimatesForFilterExpression(100, std::nullopt),
      hasEstimate(100, 200));
  EXPECT_THAT(zeroLengthExpression->getEstimatesForFilterExpression(
                  1000000000, Variable{"?a"}),
              hasEstimate(1000000000, 1000000000));
}
