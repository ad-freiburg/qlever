//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <optional>
#include <string>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/RegexExpression.h"
#include "gtest/gtest.h"

using namespace sparqlExpression;
using ad_utility::source_location;

namespace {
auto lit = ad_utility::testing::tripleComponentLiteral;

RegexExpression makeRegexExpression(
    std::string variable, std::string regex,
    std::optional<std::string> flags = std::nullopt) {
  // The regex and the flags both have to be enquoted. This is normally ensured
  // by the SPARQL parser. For easier readability of the tests we add those
  // quotes here.
  regex = absl::StrCat("\"", regex, "\"");
  if (flags.has_value()) {
    flags.value() = absl::StrCat("\"", flags.value(), "\"");
  }
  auto variableExpression =
      std::make_unique<VariableExpression>(Variable{std::move(variable)});
  auto regexExpression = std::make_unique<StringLiteralExpression>(lit(regex));
  std::optional<SparqlExpression::Ptr> flagsExpression = std::nullopt;
  if (flags.has_value()) {
    flagsExpression = SparqlExpression::Ptr{
        std::make_unique<StringLiteralExpression>(lit(flags.value()))};
  }

  return {std::move(variableExpression), std::move(regexExpression),
          std::move(flagsExpression)};
}
}  // namespace

// Test that the expression `leftValue Comp rightValue`, when evaluated on the
// `TestContext` (see above), yields the `expected` result.
void testWithExplicitResult(const SparqlExpression& expression,
                            std::vector<Bool> expected,
                            source_location l = source_location::current()) {
  static TestContext ctx;
  auto trace = generateLocationTrace(l, "testWithExplicitResult");
  auto resultAsVariant = expression.evaluate(&ctx.context);
  const auto& result = std::get<VectorWithMemoryLimit<Bool>>(resultAsVariant);
  ASSERT_EQ(expected.size(), result.size());
  for (size_t i = 0; i < result.size(); ++i) {
    EXPECT_EQ(expected[i], result[i]) << "i = " << i;
  }
}

auto testNonPrefixRegex = [](std::string variable, std::string regex,
                             const std::vector<Bool>& expectedResult,
                             source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "testNonPrefixRegex");
  auto expr = makeRegexExpression(std::move(variable), std::move(regex));
  ASSERT_FALSE(expr.isPrefixExpression());
  testWithExplicitResult(expr, expectedResult);
};

TEST(RegexExpression, nonPrefixRegex) {
  // ?vocab column is `"Beta", "alpha", "älpha"
  // ?mixed column is `1, -0.1, A`
  auto test = testNonPrefixRegex;
  test("?vocab", "ph", {false, true, true});
  test("?vocab", "l.h", {false, true, true});
  test("?vocab", "l[^a]{2}a", {false, true, true});
  test("?vocab", "[el][^a]*a", {true, true, true});
  test("?vocab", "B", {true, false, false});
  // case-sensitive by default.
  test("?vocab", "b", {false, false, false});

  // Not a prefix expression because of the "special" regex characters
  test("?vocab", "^\"a.*", {false, true, false});
}

auto testNonPrefixRegexWithFlags =
    [](std::string variable, std::string regex, std::string flags,
       const std::vector<Bool>& expectedResult,
       source_location l = source_location::current()) {
      auto trace = generateLocationTrace(l, "testNonPrefixRegexWithFlags");
      auto expr = makeRegexExpression(std::move(variable), std::move(regex),
                                      std::move(flags));
      ASSERT_FALSE(expr.isPrefixExpression());
      testWithExplicitResult(expr, expectedResult);
    };

TEST(RegexExpression, nonPrefixRegexWithFlags) {
  // ?vocab column is `"Beta", "alpha", "älpha"
  // ?mixed column is `1, -0.1, A`
  auto test = testNonPrefixRegexWithFlags;
  // case-insensitive.
  test("?vocab", "L.H", "", {false, false, false});
  test("?vocab", "L.H", "i", {false, true, true});
  test("?vocab", "l[^a]{2}A", "", {false, false, false});
  test("?vocab", "l[^a]{2}A", "i", {false, true, true});
  test("?vocab", "[El][^a]*A", "", {false, false, false});
  test("?vocab", "[El][^a]*A", "i", {true, true, true});
  test("?vocab", "b", "", {false, false, false});
  test("?vocab", "b", "i", {true, false, false});

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

  test("?vocab", "^\"alp", "i", {false, true, false});

  // TODO<joka921>  Add tests for other flags (maybe the non-greedy one?)
}

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
       const std::vector<Bool>& expectedResult,
       source_location l = source_location::current()) {
      auto trace = generateLocationTrace(l, "testUnorderedPrefix");
      auto expr = makeRegexExpression(std::move(variable), std::move(regex));
      ASSERT_TRUE(expr.isPrefixExpression());
      testWithExplicitResult(expr, expectedResult);
    };

TEST(RegexExpression, unorderedPrefixRegexUnorderedColumn) {
  auto test = testPrefixRegexUnorderedColumn;
  // ?vocab column is `"Beta", "alpha", "älpha"
  // ?mixed column is `1, -0.1, A`
  test("?vocab", "^\"Be", {true, false, false});
  // Prefix filters are currently always case-insensitive.
  test("?vocab", "^\"be", {true, false, false});
  // Prefix filters currently always work on the primary level, where `a` and
  // `ä` are considered equal.
  test("?vocab", "^\"al", {false, true, true});
  test("?vocab", "^\"äl", {false, true, true});

  test("?vocab", "^\"c", {false, false, false});
}

auto testPrefixRegexOrderedColumn = [](std::string variableAsString,
                                       std::string regex,
                                       ad_utility::SetOfIntervals expected,
                                       source_location l =
                                           source_location::current()) {
  auto trace = generateLocationTrace(l, "testPrefixRegexOrderedColumn");
  auto variable = Variable{variableAsString};
  TestContext ctx = TestContext::sortedBy(variable);
  auto expression = makeRegexExpression(variableAsString, regex);
  ASSERT_TRUE(expression.isPrefixExpression());
  auto resultAsVariant = expression.evaluate(&ctx.context);
  const auto& result = std::get<ad_utility::SetOfIntervals>(resultAsVariant);
  ASSERT_EQ(result, expected);
};

TEST(RegexExpression, prefixRegexOrderedColumn) {
  auto test = testPrefixRegexOrderedColumn;
  // Sorted order (by bits of the valueIds):
  // ?vocab column is  "alpha", "älpha", "Beta"
  // ?mixed column is `1, -0.1, A`
  test("?vocab", "^\"Be", {{{2, 3}}});
  // Prefix filters are currently always case-insensitive.
  test("?vocab", "^\"be", {{{2, 3}}});
  // Prefix filters currently always work on the primary level, where `a` and
  // `ä` are considered equal.
  test("?vocab", "^\"al", {{{0, 2}}});
  test("?vocab", "^\"äl", {{{0, 2}}});
  test("?vocab", "^\"c", {});
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

  // The first argument must be a variable.
  EXPECT_THROW(
      RegexExpression(literal("\"a\""), literal("\"b\""), std::nullopt),
      std::runtime_error);

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
