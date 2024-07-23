// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.

#ifndef QLEVER_LAZY_JSON_PARSER_H_
#define QLEVER_LAZY_JSON_PARSER_H_

#include "util/json.h"

namespace ad_utility {
/*
 * A simple parser for split up JSON-data with known structure.
 *
 * Given the path to an array containing the majority of the given JSON-object,
 * the Parser will yield chunks of the object seperated after completed objects
 * in the arrayPath or after reading the entire object itself.
 */
class LazyJsonParser {
 public:
  struct JsonNode {
    std::string key;
    bool isObject{true};

    bool operator==(const JsonNode& other) const {
      return key == other.key && isObject == other.isObject;
    }
  };
  LazyJsonParser(std::vector<JsonNode> arrayPath) : arrayPath_(arrayPath) {}

  // TODO<unexenu> this should take a generator as argument instead
  static std::string parse(std::string s, std::vector<JsonNode> arrayPath) {
    LazyJsonParser p(arrayPath);
    return p.parseChunk(s);
  }

  // Parses a chunk of JSON data and returns it with reconstructed structure.
  std::string parseChunk(std::string inStr);

 private:
  constexpr bool isInArrayPath() const { return curPath_ == arrayPath_; }

  std::string remainingInput_;
  std::string tempKey_;
  bool isEscaped_{false};
  bool inString_{false};
  bool expectKey_{false};
  bool inArrayPath_{false};
  bool setKeysValueType_{false};
  int openBracesInArrayPath_{0};

  std::vector<JsonNode> prefixPath_;
  std::vector<JsonNode> curPath_;
  const std::vector<JsonNode> arrayPath_;
};
}  // namespace ad_utility

#endif  // QLEVER_LAZY_JSON_PARSER_H_
