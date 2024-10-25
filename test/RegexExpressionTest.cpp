// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <optional>
#include <string>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/RegexExpression.h"
#include "gtest/gtest.h"

using namespace sparqlExpression;
using ad_utility::source_location;

namespace {
auto lit = ad_utility::testing::tripleComponentLiteral;

constexpr auto T = Id::makeFromBool(true);
constexpr auto F = Id::makeFromBool(false);
constexpr Id U = Id::makeUndefined();

// Make `RegexExpression` from given the `child` (the expression on which to
// apply the regex), `regex`, and optional `flags`. The argument `childAsStr` is
// true iff the expression is enclosed in a `STR()` function.
RegexExpression makeRegexExpression(
    SparqlExpression::Ptr child, std::string regex,
    std::optional<std::string> flags = std::nullopt, bool childAsStr = false) {
  // The regex and the flags both have to be enquoted. This is normally ensured
  // by the SPARQL parser. For easier readability of the tests we add those
  // quotes here.
  regex = absl::StrCat("\"", regex, "\"");
  if (flags.has_value()) {
    flags.value() = absl::StrCat("\"", flags.value(), "\"");
  }
  if (childAsStr) {
    child = makeStrExpression(std::move(child));
  }
  auto regexExpression = std::make_unique<StringLiteralExpression>(lit(regex));
  std::optional<SparqlExpression::Ptr> flagsExpression = std::nullopt;
  if (flags.has_value()) {
    flagsExpression = SparqlExpression::Ptr{
        std::make_unique<StringLiteralExpression>(lit(flags.value()))};
  }

  return {std::move(child), std::move(regexExpression),
          std::move(flagsExpression)};
}

// Special case of the `makeRegexExpression` above, where the `child`
// expression is a variable.
RegexExpression makeRegexExpression(
    std::string variable, std::string regex,
    std::optional<std::string> flags = std::nullopt, bool childAsStr = false) {
  SparqlExpression::Ptr variableExpression =
      std::make_unique<VariableExpression>(Variable{std::move(variable)});
  return makeRegexExpression(std::move(variableExpression), std::move(regex),
                             std::move(flags), childAsStr);
}
}  // namespace

// Test that the expression `leftValue Comparator rightValue`, when evaluated on
// the `TestContext` (see above), yields the `expected` result.
void testWithExplicitResult(const SparqlExpression& expression,
                            std::vector<Id> expected,
                            std::optional<size_t> numInputs = std::nullopt,
                            source_location l = source_location::current()) {
  TestContext ctx;
  auto trace = generateLocationTrace(l, "testWithExplicitResult");
  if (numInputs.has_value()) {
    ctx.context._endIndex = numInputs.value();
  }
  auto resultAsVariant = expression.evaluate(&ctx.context);
  const auto& result = std::get<VectorWithMemoryLimit<Id>>(resultAsVariant);

  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}

auto testNonPrefixRegex = [](std::string variable, std::string regex,
                             const std::vector<Id>& expectedResult,
                             bool childAsStr = false,
                             source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "testNonPrefixRegex");
  auto expr = makeRegexExpression(std::move(variable), std::move(regex),
                                  std::nullopt, childAsStr);
  ASSERT_FALSE(expr.isPrefixExpression());
  testWithExplicitResult(expr, expectedResult);
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
}

// Test where the expression is not simply a variable.
TEST(RegexExpression, inputNotVariable) {
  // Our expression is a fixed string literal: "hallo".
  VectorWithMemoryLimit<IdOrLiteralOrIri> input{
      ad_utility::testing::getQec()->getAllocator()};
  input.push_back(ad_utility::triple_component::LiteralOrIri(lit("\"hallo\"")));
  auto child = std::make_unique<sparqlExpression::detail::SingleUseExpression>(
      input.clone());

  // "hallo" matches the regex "ha".
  auto expr = makeRegexExpression(std::move(child), "ha", "");
  std::vector<Id> expected;
  expected.push_back(Id::makeFromBool(true));
  testWithExplicitResult(expr, expected, input.size());
}

auto testNonPrefixRegexWithFlags =
    [](std::string variable, std::string regex, std::string flags,
       const std::vector<Id>& expectedResult,
       source_location l = source_location::current()) {
      auto trace = generateLocationTrace(l, "testNonPrefixRegexWithFlags");
      auto expr = makeRegexExpression(std::move(variable), std::move(regex),
                                      std::move(flags));
      ASSERT_FALSE(expr.isPrefixExpression());
      testWithExplicitResult(expr, expectedResult);
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
}

// Test the `getPrefixRegex` function (which returns `std::nullopt` if the regex
// is not a simple prefix regex).
TEST(RegexExpression, getPrefixRegex) {
  using namespace sparqlExpression::detail;
  ASSERT_EQ(std::nullopt, getPrefixRegex("alpha"));
  ASSERT_EQ(std::nullopt, getPrefixRegex("^al.ha"));
  ASSERT_EQ(std::nullopt, getPrefixRegex("^alh*"));
  ASSERT_EQ(std::nullopt, getPrefixRegex("^a(lh)"));

  ASSERT_EQ("alpha", getPrefixRegex("^alpha"));
  ASSERT_EQ(R"(\al*ph.a()", getPrefixRegex(R"(^\\al\*ph\.a\()"));
  // Invalid escaping of `"` (no need to escape it).
  ASSERT_THROW(getPrefixRegex(R"(^\")"), std::runtime_error);
}

auto testPrefixRegexUnorderedColumn =
    [](std::string variable, std::string regex,
       const std::vector<Id>& expectedResult, bool childAsStr = false,
       source_location l = source_location::current()) {
      auto trace = generateLocationTrace(l, "testUnorderedPrefix");
      auto expr = makeRegexExpression(std::move(variable), std::move(regex),
                                      std::nullopt, childAsStr);
      ASSERT_TRUE(expr.isPrefixExpression());
      testWithExplicitResult(expr, expectedResult);
    };

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

auto testPrefixRegexOrderedColumn =
    [](std::string variableAsString, std::string regex,
       ad_utility::SetOfIntervals expected, bool childAsStr = false,
       source_location l = source_location::current()) {
      auto trace = generateLocationTrace(l, "testPrefixRegexOrderedColumn");
      auto variable = Variable{variableAsString};
      TestContext ctx = TestContext::sortedBy(variable);
      auto expression = makeRegexExpression(variableAsString, regex,
                                            std::nullopt, childAsStr);
      ASSERT_TRUE(expression.isPrefixExpression());
      auto resultAsVariant = expression.evaluate(&ctx.context);
      const auto& result =
          std::get<ad_utility::SetOfIntervals>(resultAsVariant);
      ASSERT_EQ(result, expected);
    };

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

TEST(RegexExpression, getCacheKey) {
  const auto exp1 = makeRegexExpression("?first", "alp");
  auto exp2 = makeRegexExpression("?first", "alp");

  VariableToColumnMap map;
  map[Variable{"?first"}] = makeAlwaysDefinedColumn(0);
  map[Variable{"?second"}] = makeAlwaysDefinedColumn(1);
  ASSERT_EQ(exp1.getCacheKey(map), exp2.getCacheKey(map));

  // Different regex, different cache key.
  auto exp3 = makeRegexExpression("?first", "alk");
  ASSERT_NE(exp1.getCacheKey(map), exp3.getCacheKey(map));

  // Different variable, different cache key.
  auto exp4 = makeRegexExpression("?second", "alp");
  ASSERT_NE(exp1.getCacheKey(map), exp4.getCacheKey(map));

  // Different flags, different cache key.
  auto exp5 = makeRegexExpression("?first", "alp", "im");
  ASSERT_NE(exp1.getCacheKey(map), exp5.getCacheKey(map));

  // Different variable name, but the variable is stored in the same column ->
  // same cache key.
  auto map2 = map;
  map2[Variable{"?otherFirst"}] = makeAlwaysDefinedColumn(0);
  auto exp6 = makeRegexExpression("?otherFirst", "alp");
  ASSERT_EQ(exp1.getCacheKey(map), exp6.getCacheKey(map2));
}

TEST(RegexExpression, getChildren) {
  auto expression = makeRegexExpression("?a", "someRegex");
  auto vars = expression.containedVariables();
  ASSERT_EQ(vars.size(), 1);
  ASSERT_EQ(*vars[0], Variable{"?a"});
}

TEST(RegexExpression, invalidConstruction) {
  auto literal = [](const std::string& literal,
                    std::string_view langtagOrDatatype = "") {
    return std::make_unique<StringLiteralExpression>(
        lit(literal, langtagOrDatatype));
  };
  auto variable = [](std::string literal) {
    return std::make_unique<VariableExpression>(Variable{std::move(literal)});
  };

  // The second argument must be a string literal.
  EXPECT_THROW(RegexExpression(variable("?a"), variable("?b"), std::nullopt),
               std::runtime_error);

  // The second argument must not have a datatype or langtag
  EXPECT_THROW(
      RegexExpression(variable("?a"), literal("\"b\"", "@en"), std::nullopt),
      std::runtime_error);
  EXPECT_THROW(RegexExpression(variable("?a"), literal("\"b\"", "^^<someType>"),
                               std::nullopt),
               std::runtime_error);

  // The third argument must be a string literal.
  EXPECT_THROW(
      RegexExpression(variable("?a"), literal("\"b\""), variable("?b")),
      std::runtime_error);
  // The third argument must not have a language tag or datatype
  EXPECT_THROW(RegexExpression(variable("?a"), literal("\"b\""),
                               literal("\"i\"", "@en")),
               std::runtime_error);
  EXPECT_THROW(RegexExpression(variable("?a"), literal("\"b\""),
                               literal("\"i\"", "^^<someType>")),
               std::runtime_error);

  // Invalid regex (parentheses that are never closed).
  EXPECT_THROW(
      RegexExpression(variable("?a"), literal("\"(open\""), std::nullopt),
      std::runtime_error);

  // Invalid option flag.
  EXPECT_THROW(
      RegexExpression(variable("?a"), literal("\"a\""), literal("\"x\"")),
      std::runtime_error);
}
