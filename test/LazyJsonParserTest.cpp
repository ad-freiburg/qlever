// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <string>

#include "util/LazyJsonParser.h"

using ad_utility::LazyJsonParser;

TEST(parseTest, parse) {
  const std::vector<std::string> arrayPath = {"results", "bindings"};

  // CHECK 1: Expected results for empty or complete results.
  EXPECT_EQ(LazyJsonParser::parse("", arrayPath), "");
  EXPECT_EQ(LazyJsonParser::parse(
                R"({"results": {"bindings": [{"x": {"value": "\"esc\""}}]}})",
                arrayPath),
            R"({"results": {"bindings": [{"x": {"value": "\"esc\""}}]}})");

  // Check if the parser yields the expected results when parsing each char
  // individually.
  auto expectYields = [](const std::string& s,
                         const std::vector<std::string>& exp,
                         const std::vector<std::string>& arrayPath) {
    auto yieldChars =
        [](const std::string& s) -> cppcoro::generator<std::string> {
      for (auto& c : s) {
        co_yield std::string(1, c);
      }
    };
    auto parsedResult = LazyJsonParser::parse(yieldChars(s), arrayPath);

    size_t expIdx = 0;
    for (const auto& y : parsedResult) {
      if (!y.empty()) {
        ASSERT_TRUE(expIdx < exp.size());
        ASSERT_EQ(y, exp[expIdx]);
        ++expIdx;
      }
    }
    EXPECT_EQ(expIdx, exp.size());
  };

  // CHECK 2: Empty ArrayPath
  expectYields("[1,2,3]", {"[1]", "[2]", "[3]"}, {});

  // CHECK 3: Normal result split at every char.
  const std::string result3a =
      R"({"head": {"vars": ["x", "y"], "nested arrays": [[1,2,3], [4,5,6]]},)"
      R"("results": {"bindings": [)"
      R"({"x": {"value": 1, "datatype": )"
      R"("http://www.w3.org/2001/XMLSchema#integer"}, )"
      R"("y": {"value": 2, "datatype": )"
      R"("http://www.w3.org/2001/XMLSchema#integer"}},)";

  const std::string result3b =
      R"({"x": {"value": 3, "datatype": )"
      R"("http://www.w3.org/2001/XMLSchema#integer"}, )"
      R"("y": {"value": 4, "datatype": )"
      R"("http://www.w3.org/2001/XMLSchema#integer"}}]}})";

  expectYields(absl::StrCat(result3a, result3b),
               {absl::StrCat(result3a.substr(0, result3a.size() - 1), "]}}"),
                absl::StrCat(R"({"results": {"bindings": [)", result3b)},
               arrayPath);

  // CHECK 4: Result with changed order of results-/head object.
  // Also added another key-value pair in the results path and nested arrays.
  const std::string result4a =
      R"({"results": {"bindings": [)"
      R"({"x": {"value": 5, "datatype": )"
      R"("http://www.w3.org/2001/XMLSchema#integer"}, )"
      R"("y": {"value": 6, "datatype": )"
      R"("http://www.w3.org/2001/XMLSchema#integer"}},)";
  const std::string result4b = R"([[1,2], [3,4]])"
                               R"(], "key": [[1,2], [3,4]]}, )"
                               R"("head": {"vars": ["x", "y"]}})";

  expectYields(result4a + result4b,
               {absl::StrCat(result4a.substr(0, result4a.size() - 1), "]}}"),
                absl::StrCat(R"({"results": {"bindings": [)", result4b)},
               arrayPath);

  // CHECK 4: Throw when exceeding input size limit.
  // parseLiteral
  EXPECT_ANY_THROW(LazyJsonParser::parse(
      absl::StrCat(R"({"key":")", std::string(1'000'000, '0'), R"(")"), {}));
  // BeforeArrayPath
  EXPECT_ANY_THROW(LazyJsonParser::parse(
      absl::StrCat(R"({"key":)", std::string(1'000'000, 0), R"(})"),
      arrayPath));
  // InArrayPath
  EXPECT_ANY_THROW(LazyJsonParser::parse(
      absl::StrCat(R"({"head":[)", std::string(1'000'000, 0)), {"head"}));
  // AfterArrayPath
  EXPECT_ANY_THROW(
      LazyJsonParser::parse(absl::StrCat(R"({"head":[],"key": )",
                                         std::string(1'000'000, '0'), R"(})"),
                            {"head"}));

  // CHECK 5: Cornercases
  EXPECT_NO_THROW(LazyJsonParser::parse("[1,2,3]", arrayPath));
  EXPECT_NO_THROW(LazyJsonParser::parse("{}", arrayPath));
}
