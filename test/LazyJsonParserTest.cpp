// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include <gtest/gtest.h>

#include <string>

#include "util/LazyJsonParser.h"

using ad_utility::LazyJsonParser;

TEST(parseTest, parse) {
  const std::vector<std::string> arrayPath = {"results", "bindings"};

  // CHECK 1: Expected results for empty or complete results.
  EXPECT_EQ(LazyJsonParser::parse("", arrayPath), "");
  EXPECT_EQ(LazyJsonParser::parse(
                "{\"results\": {\"bindings\": [{\"x\": {\"\"value\"\": 1}}]}}",
                arrayPath),
            "{\"results\": {\"bindings\": [{\"x\": {\"\"value\"\": 1}}]}}");

  // CHECK 2: Handle invalid JSON.
  EXPECT_NO_THROW(LazyJsonParser::parse("[{x}]", arrayPath));
  EXPECT_NO_THROW(LazyJsonParser::parse("]{\"s\"}}[]", {}));

  // Check if the parser yields the expected results when parsing each char
  // individually.
  auto expectYields = [&arrayPath](const std::string& s,
                                   const std::vector<std::string>& exp) {
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

  // CHECK 3: Normal result split at every char.
  const std::string result3a =
      R"({"head": {"vars": ["x", "y"]}, "results": {"bindings": ["
      "{"x": {"value": 1, "datatype": "
      ""http://www.w3.org/2001/XMLSchema#integer"}, "
      ""y": {"value": 2, "datatype": "
      ""http://www.w3.org/2001/XMLSchema#integer"}},)";

  const std::string result3b =
      R"({"x": {"value": 3, "datatype": ")
      ""http://www.w3.org/2001/XMLSchema#integer"}, "
      ""y": {"value": 4, "datatype": "
      ""http://www.w3.org/2001/XMLSchema#integer"}}]}})";

  expectYields(absl::StrCat(result3a, result3b),
               {absl::StrCat(result3a.substr(0, result3a.size() - 1), "]}}"),
                absl::StrCat(R"({"results": {"bindings": [)", result3b)});

  // CHECK 4: Result with changed order of results-/head object.
  // Also added another key-value pair in the results path and nested arrays.
  const std::string result4a =
      R"({"results": {"bindings": ["
      "{"x": {"value": 5, "datatype": "
      ""http://www.w3.org/2001/XMLSchema#integer"}, "
      ""y": {"value": 6, "datatype": "
      ""http://www.w3.org/2001/XMLSchema#integer"}},)";
  const std::string result4b =
      R"([[1,2], [3,4]]"
      "], "key": [[1,2], [3,4]]}, "
      ""head": {"vars": ["x", "y"]}})";

  expectYields(result4a + result4b,
               {absl::StrCat(result4a.substr(0, result4a.size() - 1), "]}}"),
                absl::StrCat(R"({"results": {"bindings": [)", result4b)});
}
