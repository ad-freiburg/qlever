//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <optional>
#include <string>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/RegexExpression.h"
#include "gtest/gtest.h"

using namespace sparqlExpression;
using ad_utility::source_location;

SparqlExpression::Ptr makeRegexExpression(
    std::string variable, std::string regex,
    std::optional<std::string> flags = std::nullopt) {
  auto variableExpression =
      std::make_unique<VariableExpression>(Variable{std::move(variable)});
  auto regexExpression =
      std::make_unique<StringOrIriExpression>(std::move(regex));
  std::optional<SparqlExpression::Ptr> flagsExpression = std::nullopt;
  if (flags.has_value()) {
    flagsExpression = SparqlExpression::Ptr{
        std::make_unique<StringOrIriExpression>(std::move(flags.value()))};
  }

  return std::make_unique<RegexExpression>(std::move(variableExpression),
                                           std::move(regexExpression),
                                           std::move(flagsExpression));
}

// Assert that the expression `leftValue Comp rightValue`, when evaluated on the
// `TestContext` (see above), yields the `expected` result.
void testWithExplicitResult(const SparqlExpression& expression,
                            std::vector<Bool> expected,
                            source_location l = source_location::current()) {
  static TestContext ctx;
  auto trace = generateLocationTrace(l, "testWithExplicitResult");
  auto resultAsVariant = expression.evaluate(&ctx.context);
  const auto& result = std::get<VectorWithMemoryLimit<Bool>>(resultAsVariant);
  ASSERT_EQ(result.size(), expected.size());
  for (size_t i = 0; i < result.size(); ++i) {
    EXPECT_EQ(expected[i], result[i]) << "i = " << i;
  }
}

auto testUnorderedRegexExpressionWithoutFlags =
    [](std::string variable, std::string regex,
       const std::vector<Bool>& expectedResult,
       source_location l = source_location::current()) {
      auto trace = generateLocationTrace(l, "testUnorderedWithoutFlags");
      // Add the quotation marks around the regex. Those are normally added by
      // the SPARQL parser.
      regex = absl::StrCat("\"", regex, "\"");
      auto expr = makeRegexExpression(std::move(variable), std::move(regex));
      testWithExplicitResult(*expr, expectedResult);
    };

TEST(RegexExpression, regexExpressionUnorderedWithoutFlags) {
  // ?vocab column is `"Beta", "alpha", "älpha"
  // ?mixed column is `1, -0.1, A`
  auto test = testUnorderedRegexExpressionWithoutFlags;
  test("?vocab", "ph", {false, true, true});
  test("?vocab", "l.h", {false, true, true});
  test("?vocab", "l[^a]{2}a", {false, true, true});
  test("?vocab", "[el][^a]*a", {true, true, true});
  test("?vocab", "B", {true, false, false});
  // case-sensitive by default.
  test("?vocab", "b", {false, false, false});
}

auto testUnorderedRegexExpressionWithFlags =
    [](std::string variable, std::string regex, std::string flags,
       const std::vector<Bool>& expectedResult,
       source_location l = source_location::current()) {
      auto trace = generateLocationTrace(l, "testUnorderedWithFlags");
      // Add the quotation marks around the regex and flags. Those are normally
      // added by the SPARQL parser.
      regex = absl::StrCat("\"", regex, "\"");
      flags = absl::StrCat("\"", flags, "\"");
      auto expr = makeRegexExpression(std::move(variable), std::move(regex),
                                      std::move(flags));
      testWithExplicitResult(*expr, expectedResult);
    };

TEST(RegexExpression, regexExpressionUnorderedWithFlags) {
  // ?vocab column is `"Beta", "alpha", "älpha"
  // ?mixed column is `1, -0.1, A`
  auto test = testUnorderedRegexExpressionWithFlags;
  // case-insensitive.
  test("?vocab", "L.H", "", {false, false, false});
  test("?vocab", "L.H", "i", {false, true, true});
  test("?vocab", "l[^a]{2}A", "", {false, false, false});
  test("?vocab", "l[^a]{2}A", "i", {false, true, true});
  test("?vocab", "[El][^a]*A", "", {false, false, false});
  test("?vocab", "[El][^a]*A", "i", {true, true, true});
  test("?vocab", "b", "", {false, false, false});
  test("?vocab", "b", "i", {true, false, false});

  // TODO<joka921>  Add tests for other flags (maybe the non-greedy one?)
}
