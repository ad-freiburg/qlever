// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.

#ifndef QLEVER_LAZY_JSON_PARSER_H_
#define QLEVER_LAZY_JSON_PARSER_H_

#include "util/Generator.h"

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
  // Parse partial json chunks yielding them reconstructed.
  [[nodiscard]] static cppcoro::generator<std::string> parse(
      cppcoro::generator<std::string> partJson,
      const std::vector<std::string>& arrayPath) {
    LazyJsonParser p(arrayPath);
    for (const auto& chunk : partJson) {
      co_yield p.parseChunk(chunk);
    }
  }

  // As above just on a single string.
  [[nodiscard]] static std::string parse(
      const std::string& s, const std::vector<std::string>& arrayPath) {
    LazyJsonParser p(arrayPath);
    return p.parseChunk(s);
  }

 private:
  explicit LazyJsonParser(std::vector<std::string> arrayPath);

  // Parses a chunk of JSON data and returns it with reconstructed structure.
  std::string parseChunk(const std::string& inStr);

  // Checks if the current path is the arrayPath.
  bool isInArrayPath() const { return curPath_ == arrayPath_; }

  // Parses strings in the input.
  void parseString(size_t& idx);

  // Parses the arrayPath.
  size_t parseArrayPath(size_t& idx);

  std::string input_;
  bool isEscaped_{false};
  bool inString_{false};
  bool inArrayPath_{false};
  int openBracesInArrayPath_{0};
  int openBrackets_{0};
  unsigned int yieldCount_{0};

  size_t strStart_{0};
  size_t strEnd_{0};

  std::vector<std::string> curPath_;
  const std::vector<std::string> arrayPath_;

  const std::string prefixInArray_;
  const std::string suffixInArray_;
};
}  // namespace ad_utility

#endif  // QLEVER_LAZY_JSON_PARSER_H_
