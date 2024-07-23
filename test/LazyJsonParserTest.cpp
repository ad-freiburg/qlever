// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include <gtest/gtest.h>

#include "util/LazyJsonParser.h"

using ad_utility::LazyJsonParser;
using nlohmann::json;

TEST(parseTest, parse) {
  const std::vector<LazyJsonParser::JsonNode> arrayPath = {{"results", true},
                                                           {"bindings", false}};

  // CHECK 1: Expected results for empty or complete results.
  EXPECT_EQ(LazyJsonParser::parse("", arrayPath), "");
  EXPECT_EQ(LazyJsonParser::parse(
                "{\"results\": {\"bindings\": [{\"x\": {\"value\": 1}}]}}",
                arrayPath),
            "{\"results\": {\"bindings\": [{\"x\": {\"value\": 1}}]}}");
}

TEST(parseTest, parseChunk) {
  const std::vector<LazyJsonParser::JsonNode> arrayPath = {{"results", true},
                                                           {"bindings", false}};

  EXPECT_EQ(LazyJsonParser::parse("{\"head\": {\"vars\": [\"x\", \"y\"]}, "
                                  "\"results\": {\"bindings\": [{\"a\": 1},",
                                  arrayPath),
            absl::StrCat("{\"head\": {\"vars\": [\"x\", \"y\"]}, \"results\": "
                         "{\"bindings\": [",
                         "{\"a\": 1}]}}"));

  // Let the parser parse each char of s individually, expect to return the
  // expected result exp when called to parse the last char.
  auto expectYieldOnLastChar = [](LazyJsonParser& p, const std::string& s,
                                  const std::string& exp) {
    for (size_t i = 0; i < s.size() - 1; ++i) {
      EXPECT_EQ(p.parseChunk(s.substr(i, 1)), "");
    }
    EXPECT_EQ(p.parseChunk(s.substr(s.size() - 1, 1)), exp);
  };

  // CHECK 2: Normal result split at every char.
  const std::string result2a =
      "{\"head\": {\"vars\": [\"x\", \"y\"]}, \"results\": {\"bindings\": ["
      "{\"x\": {\"value\": 1, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}, "
      "\"y\": {\"value\": 2, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}}";

  const std::string result2b =
      ", {\"x\": {\"value\": 3, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}, "
      "\"y\": {\"value\": 4, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}}";

  LazyJsonParser p2(arrayPath);

  expectYieldOnLastChar(p2, result2a, absl::StrCat(result2a, "]}}"));
  expectYieldOnLastChar(p2, result2b,
                        absl::StrCat("{\"results\": {\"bindings\": [",
                                     result2b.substr(2), "]}}"));

  // CHECK 3: Result with changed order of results-/head object.
  // Also added another key-value pair in the results path.
  const std::string result3a =
      "{\"results\": {\"bindings\": ["
      "{\"x\": {\"value\": 1, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}, "
      "\"y\": {\"value\": 2, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}}";
  const std::string result3b =
      ", "
      "{\"x\": {\"value\": 3, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}, "
      "\"y\": {\"value\": 4, \"datatype\": "
      "\"http://www.w3.org/2001/XMLSchema#integer\"}}";
  const std::string result3c =
      "], \"key\": \"value\"}, "
      "\"head\": {\"vars\": [\"x\", \"y\"]}}";

  LazyJsonParser p3(arrayPath);
  expectYieldOnLastChar(p3, result3a, absl::StrCat(result3a, "]}}"));

  expectYieldOnLastChar(p3, result3b,
                        absl::StrCat("{\"results\": {\"bindings\": [",
                                     result3b.substr(2), "]}}"));

  expectYieldOnLastChar(p3, result3c,
                        absl::StrCat("{\"results\": {", result3c.substr(3)));
}
