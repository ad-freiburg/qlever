// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_LAZYJSONPARSER_H
#define QLEVER_SRC_UTIL_LAZYJSONPARSER_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <optional>
#include <variant>

#include "backports/span.h"
#include "util/Generator.h"
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
  // Generator detail, the first/last 100 input characters for better error
  // context.
  struct Details {
    std::string first100_;
    std::string last100_;
  };
  using Generator = cppcoro::generator<nlohmann::json, Details>;

  class Error : public std::runtime_error {
   public:
    explicit Error(const std::string& msg) : std::runtime_error(msg) {}
  };

  // Parse chunks of json-strings yielding them reconstructed.
  static Generator parse(cppcoro::generator<std::string_view> partialJson,
                         std::vector<std::string> arrayPath);

  // Convenient alternative for the function above using bytes.
  static Generator parse(cppcoro::generator<ql::span<std::byte>> partialJson,
                         std::vector<std::string> arrayPath);

 private:
  explicit LazyJsonParser(std::vector<std::string> arrayPath);

  // Parses a chunk of JSON data and returns it with reconstructed structure.
  std::optional<nlohmann::json> parseChunk(std::string_view inStr);

  // The following 3 methods parse the different sections before/in/after the
  // arrayPath starting at the given index `idx` on the `input_` string.
  void parseBeforeArrayPath(size_t& idx);

  // Returns the index of the last `,` between array elements or 0.
  size_t parseInArrayPath(size_t& idx);

  // Returns the index after the input, when reading the input is complete.
  std::optional<size_t> parseAfterArrayPath(size_t& idx);

  // Parses literals in the input.
  void parseLiteral(size_t& idx);

  // Constructs the result to be returned after parsing a chunk.
  std::optional<nlohmann::json> constructResultFromParsedChunk(
      size_t materializeEnd);

  // Context for the 3 parsing sections.
  struct BeforeArrayPath {
    // Indices of the latest parsed literal, used to add keys to the curPath_.
    struct LiteralView {
      size_t start_{0};
      size_t length_{0};
    };
    std::optional<LiteralView> optLiteral_;
    std::vector<std::string> curPath_;
    // Open Brackets counter to track nested arrays.
    int openBrackets_{0};

    // Attempts to add a key to the current Path, based on strStart/strEnd.
    void tryAddKeyToPath(std::string_view input);
  };
  struct InArrayPath {
    // Track brackets/braces to find the end of the array.
    int openBracketsAndBraces_{0};
  };
  struct AfterArrayPath {
    // Remaining braces until the end of the input-object.
    size_t remainingBraces_;
  };
  std::variant<BeforeArrayPath, InArrayPath, AfterArrayPath> state_{
      BeforeArrayPath()};

  // Current (not yet materialized) input-string.
  std::string input_;

  // If the next character is escaped or not.
  bool isEscaped_{false};

  // If the parser is currently positioned within a literal.
  bool inLiteral_{false};

  // Indicates whether the end of the object has been reached.
  bool endReached_{false};

  // Counter for the so far returned results.
  unsigned int yieldCount_{0};

  // Key-path to the array containing many elements.
  const std::vector<std::string> arrayPath_;

  // Precomputed prefix/suffix used to construct results.
  const std::string prefixInArray_;
  const std::string suffixInArray_;
};
}  // namespace ad_utility

#endif
#endif  // QLEVER_SRC_UTIL_LAZYJSONPARSER_H
