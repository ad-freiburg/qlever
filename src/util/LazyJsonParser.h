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
 * the Parser will yield chunks of the object separated after completed elements
 * in the arrayPath or after reading the entire object itself.
 */
class LazyJsonParser {
 public:
  LazyJsonParser(std::vector<std::string> arrayPath);

  // TODO<unexenu> this should take a generator as argument instead
  static std::string parse(std::string s,
                           const std::vector<std::string>& arrayPath) {
    LazyJsonParser p(arrayPath);
    return p.parseChunk(s);
  }

  // Parses a chunk of JSON data and returns it with reconstructed structure.
  std::string parseChunk(std::string inStr);

 private:
  // Checks if the current path is the arrayPath.
  bool isInArrayPath() const { return curPath_ == arrayPath_; }

  // Parses strings in the input.
  void parseString(size_t& idx);

  // Parses the arrayPath.
  int parseArrayPath(size_t& idx);

  std::string input_;
  bool isEscaped_{false};
  bool inString_{false};
  bool inArrayPath_{false};
  int openBracesInArrayPath_{0};
  int openBrackets_{0};
  int yieldCount_{0};

  int strStart_{-1};
  int strEnd_{-1};

  std::vector<std::string> curPath_;
  const std::vector<std::string> arrayPath_;

  const std::string prefixInArray_;
  const std::string suffixInArray_;
};
}  // namespace ad_utility

#endif  // QLEVER_LAZY_JSON_PARSER_H_
