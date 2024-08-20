// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#pragma once

#include <variant>

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
  // Parse chunks of json-strings yielding them reconstructed.
  static cppcoro::generator<std::string> parse(
      cppcoro::generator<std::string> partJson,
      std::vector<std::string> arrayPath) {
    LazyJsonParser p(arrayPath);
    for (const auto& chunk : partJson) {
      co_yield p.parseChunk(chunk);
    }
  }

  // As above just on a single string.
  static std::string parse(const std::string& s,
                           const std::vector<std::string>& arrayPath) {
    LazyJsonParser p(arrayPath);
    return p.parseChunk(s);
  }

 private:
  explicit LazyJsonParser(std::vector<std::string> arrayPath);

  // Parses a chunk of JSON data and returns it with reconstructed structure.
  std::string parseChunk(std::string_view inStr);

  // Parses literals in the input.
  void parseLiteral(size_t& idx);

  // Parse the different sections before/in/after the arrayPath.
  void parseBeforeArrayPath(size_t& idx);
  void parseInArrayPath(size_t& idx, size_t& materializeEnd);
  void parseAfterArrayPath(size_t& idx, size_t& materializeEnd);

  // Constructs the result to be returned after parsing a chunk.
  std::string constructResultFromParsedChunk(size_t materializeEnd);

  // Context for the 3 parsing sections.
  struct BeforeArrayPath {
    // Indices of the latest parsed literal, used to add keys to the curPath_.
    size_t litStart_{0};
    size_t litLength_{0};
    std::vector<std::string> curPath_;
    // Open Brackets counter to track nested arrays.
    int openBrackets_{0};

    // Attempts to add a key to the current Path, based on strStart/strEnd.
    void tryAddKeyToPath(std::string_view input);
  };
  struct InArrayPath {
    // Track brackets/braces to find the end of the array.
    int openBracketsAndBraces_{0};
    // Start index of the ArrayPath to measure the size of the first element.
    size_t arrayStartIdx_{0};
  };
  struct AfterArrayPath {
    // Remaining braces until the end of the input-object.
    size_t remainingBraces_;
    // End index of the ArrayPath to measure the size of the "suffix".
    size_t arrayEndIdx_{0};
  };
  std::variant<BeforeArrayPath, InArrayPath, AfterArrayPath> state_{
      BeforeArrayPath()};

  // Current (not yet materialized) input-string.
  std::string input_;

  // If the next character is escaped or not.
  bool isEscaped_{false};

  // If the parser is currently positioned within a literal.
  bool inLiteral_{false};

  // Counter for the so far returned results.
  unsigned int yieldCount_{0};

  // Key-path to the array containing many elements.
  const std::vector<std::string> arrayPath_;

  // Precomputed prefix/suffix used to construct results.
  const std::string prefixInArray_;
  const std::string suffixInArray_;
};
}  // namespace ad_utility
