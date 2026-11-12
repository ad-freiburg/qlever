// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <string>

#include "util/GTestHelpers.h"
#include "util/LazyJsonParser.h"

using ad_utility::LazyJsonParser;

TEST(parseTest, parse) {
  const std::vector<std::string> arrayPath = {"results", "bindings"};

  auto yieldChars =
      [](const std::string_view s) -> cppcoro::generator<std::string_view> {
    for (size_t i = 0; i < s.size(); ++i) {
      co_yield std::string_view(s.data() + i, 1);
    }
  };
  // Check if the parser yields the expected results when parsing each char
  // individually.
  auto expectYields = [&yieldChars](const std::string& s,
                                    const std::vector<std::string>& exp,
                                    const std::vector<std::string> arrayPath) {
    auto parsedResult = LazyJsonParser::parse(yieldChars(s), arrayPath);

    size_t expIdx = 0;
    for (const auto& y : parsedResult) {
      if (!y.empty()) {
        ASSERT_TRUE(expIdx < exp.size());
        ASSERT_EQ(y, nlohmann::json::parse(exp[expIdx]));
        ++expIdx;
      }
    }
    EXPECT_EQ(expIdx, exp.size());
  };

  // CHECK 1: Expected results for empty or complete results.
  expectYields("", {}, arrayPath);

  expectYields(R"({"results": {"bindings": [{"x": {"value": "\"esc\""}}]}})",
               {R"({"results": {"bindings": [{"x": {"value": "\"esc\""}}]}})"},
               arrayPath);

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

  // CHECK 5: Cornercases
  EXPECT_NO_THROW(expectYields("[1,2,3]", {}, arrayPath));
  EXPECT_NO_THROW(expectYields("{}", {}, arrayPath));

  EXPECT_ANY_THROW(expectYields(std::string(1'000'000, '0'), {}, arrayPath));

  // Ignore any input after the main object.
  expectYields(
      R"({"results": {"bindings": [{"x": {"value": "\"esc\""}}]}}{"k": "v"})",
      {R"({"results": {"bindings": [{"x": {"value": "\"esc\""}}]}})"},
      arrayPath);
}

// If the input ends after the `arrayPath` has been reached, but before the
// JSON object is complete, the parser has to throw instead of silently
// ignoring the rest of the input. This happens when the sender of a result
// aborts the transmission, for example because of a timeout.
TEST(parseTest, truncatedInputThrows) {
  const std::vector<std::string> arrayPath = {"results", "bindings"};
  auto yieldInChunks =
      [](std::string_view s,
         size_t chunkSize) -> cppcoro::generator<std::string_view> {
    for (size_t i = 0; i < s.size(); i += chunkSize) {
      co_yield s.substr(i, chunkSize);
    }
  };
  // Consume all the parts that the parser yields for `s` (split into chunks of
  // size `chunkSize`) and return the number of parts.
  auto parseAll = [&yieldInChunks](std::string_view s, size_t chunkSize,
                                   const std::vector<std::string>& path) {
    size_t numParts = 0;
    for ([[maybe_unused]] const auto& part :
         LazyJsonParser::parse(yieldInChunks(s, chunkSize), path)) {
      ++numParts;
    }
    return numParts;
  };

  const std::string complete =
      R"({"head": {"vars": ["x"]}, "results": {"bindings": [)"
      R"({"x": {"type": "uri", "value": "a"}},)"
      R"({"x": {"type": "uri", "value": "b]}}\"\\"}},)"
      R"({"x": {"type": "uri", "value": "c"}}]}})";
  const size_t arrayStart = complete.find('[', complete.find("bindings")) + 1;
  for (size_t chunkSize : {size_t{1}, size_t{7}, complete.size()}) {
    SCOPED_TRACE(chunkSize);
    EXPECT_NO_THROW(parseAll(complete, chunkSize, arrayPath));
    // Every proper prefix that reaches the `arrayPath` is incomplete.
    for (size_t end = arrayStart; end < complete.size(); ++end) {
      SCOPED_TRACE(end);
      AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
          parseAll(complete.substr(0, end), chunkSize, arrayPath),
          ::testing::HasSubstr("ended before it was complete"),
          LazyJsonParser::Error);
    }
    // This is what the result of QLever looks like when its computation fails
    // while it is being sent.
    std::string truncatedByError = absl::StrCat(
        complete.substr(0, complete.find("\"c\"")),
        "\n !!!!>># An error has occurred while exporting the query result. "
        "The error message was:\nOperation timed out.");
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        parseAll(truncatedByError, chunkSize, arrayPath),
        ::testing::HasSubstr("ended before it was complete"),
        LazyJsonParser::Error);
  }

  // The same for an empty `arrayPath`.
  EXPECT_EQ(parseAll("[1,2,3]", 1, {}), 3);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      parseAll("[1,2", 1, {}),
      ::testing::HasSubstr("ended before it was complete"),
      LazyJsonParser::Error);

  // If the `arrayPath` is never reached, nothing is yielded, but there is also
  // no exception (the caller has to handle the missing results).
  EXPECT_EQ(
      parseAll(R"({"head": {"vars": ["x"]}, "results": {"bindi)", 1, arrayPath),
      0);
}
