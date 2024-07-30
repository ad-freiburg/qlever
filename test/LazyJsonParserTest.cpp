// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include <gtest/gtest.h>

#include <string>

#include "util/LazyJsonParser.h"

using ad_utility::LazyJsonParser;
using nlohmann::json;

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
}

TEST(parseTest, parseChunk) {
  const std::vector<std::string> arrayPath = {"results", "bindings"};

  // Let the parser parse each char of s individually, expect to return the
  // expected result when called to parse the last char.
  auto expectYieldOnLastChar = [](LazyJsonParser& p, const std::string& s,
                                  const std::string& exp) {
    for (size_t i = 0; i < s.size() - 1; ++i) {
      EXPECT_EQ(p.parseChunk(s.substr(i, 1)), "");
    }
    EXPECT_EQ(p.parseChunk(s.substr(s.size() - 1, 1)), exp);
  };

  // CHECK 1: Normal result split at every char.
  const std::string result1a =
      "{\"head\": {\"vars\": [\"x\", \"y\"]}, \"results\": {\"bindings\": ["
      "{\"x\": {\"value\": 1, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}, "
      "\"y\": {\"value\": 2, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}},";

  const std::string result1b =
      "{\"x\": {\"value\": 3, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}, "
      "\"y\": {\"value\": 4, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}}]}}";

  LazyJsonParser p2(arrayPath);

  expectYieldOnLastChar(
      p2, result1a,
      absl::StrCat(result1a.substr(0, result1a.size() - 1), "]}}"));
  expectYieldOnLastChar(
      p2, result1b, absl::StrCat("{\"results\": {\"bindings\": [", result1b));

  // CHECK 2: Result with changed order of results-/head object.
  // Also added another key-value pair in the results path and nested arrays.
  const std::string result2a =
      "{\"results\": {\"bindings\": ["
      "{\"x\": {\"value\": 5, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}, "
      "\"y\": {\"value\": 6, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}},";
  const std::string result2b = "[[1,2], [3,4]]";
  const std::string result2c =
      "], \"key\": [[1,2], [3,4]]}, "
      "\"head\": {\"vars\": [\"x\", \"y\"]}}";

  LazyJsonParser p3(arrayPath);
  expectYieldOnLastChar(
      p3, result2a,
      absl::StrCat(result2a.substr(0, result2a.size() - 1), "]}}"));

  expectYieldOnLastChar(p3, result2b, "");

  expectYieldOnLastChar(
      p3, result2c,
      absl::StrCat("{\"results\": {\"bindings\": [", result2b, result2c));
}
