//  Copyright 2022-2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "parser/SparqlParserHelpers.h"

#include <charconv>
#include <cstdint>
#include <ctre-unicode.hpp>

#include "sparqlParser/generated/SparqlAutomaticLexer.h"
#include "util/StringUtils.h"

namespace sparqlParserHelpers {
using std::string;

namespace detail {
// CTRE regex pattern for C++17 compatibility
constexpr ctll::fixed_string unicodeEscapeRegex =
    R"(\\U[0-9A-Fa-f]{8}|\\u[0-9A-Fa-f]{4})";

// Small ICU-free helpers for UTF-16 surrogate handling. These replace the
// `U16_IS_LEAD`, `U16_IS_TRAIL` and `U16_GET_SUPPLEMENTARY` macros from ICU so
// that `unescapeUnicodeSequences` does not depend on ICU.

// Return true iff `c` is a high (leading) surrogate code unit.
constexpr bool isHighSurrogate(uint32_t c) {
  return c >= 0xD800 && c <= 0xDBFF;
}

// Return true iff `c` is a low (trailing) surrogate code unit.
constexpr bool isLowSurrogate(uint32_t c) { return c >= 0xDC00 && c <= 0xDFFF; }

// Combine a high and a low surrogate into the corresponding supplementary
// Unicode codepoint.
constexpr uint32_t combineSurrogates(uint32_t high, uint32_t low) {
  return 0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00);
}
}  // namespace detail

// _____________________________________________________________________________
ParserAndVisitor::ParserAndVisitor(
    ad_utility::BlankNodeManager* blankNodeManager,
    const EncodedIriManager* encodedIriManager, std::string input,
    std::optional<ParsedQuery::DatasetClauses> datasetClauses,
    SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks)
    : Base{unescapeUnicodeSequences(std::move(input)),
           SparqlQleverVisitor{blankNodeManager,
                               encodedIriManager,
                               {},
                               std::move(datasetClauses),
                               disableSomeChecks}} {}

// _____________________________________________________________________________
ParserAndVisitor::ParserAndVisitor(
    ad_utility::BlankNodeManager* blankNodeManager,
    const EncodedIriManager* encodedIriManager, std::string input,
    SparqlQleverVisitor::PrefixMap prefixes,
    std::optional<ParsedQuery::DatasetClauses> datasetClauses,
    SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks)
    : ParserAndVisitor{blankNodeManager, encodedIriManager, std::move(input),
                       std::move(datasetClauses), disableSomeChecks} {
  visitor_.setPrefixMapManually(std::move(prefixes));
}

// _____________________________________________________________________________
std::string ParserAndVisitor::unescapeUnicodeSequences(std::string input) {
  std::string_view view{input};
  std::string output;
  bool noEscapeSequenceFound = true;
  size_t lastPos = 0;
  uint32_t highSurrogate = 0;

  auto throwError = [](bool condition, std::string_view message) {
    if (!condition) {
      throw InvalidSparqlQueryException{
          absl::StrCat("Error in unicode escape sequence. ", message)};
    }
  };

  for (const auto& match : ctre::search_all<detail::unicodeEscapeRegex>(view)) {
    if (noEscapeSequenceFound) {
      output.reserve(input.size());
      noEscapeSequenceFound = false;
    }
    auto inBetweenPart =
        view.substr(lastPos, match.data() - (view.data() + lastPos));

    throwError(
        inBetweenPart.empty() || highSurrogate == 0,
        "A high surrogate must be directly followed by a low surrogate.");

    output += inBetweenPart;
    lastPos = match.data() + match.size() - view.data();

    auto hexValue = match.to_view();
    hexValue.remove_prefix(std::string_view{"\\U"}.size());

    uint32_t codePoint = 0;
    auto result = std::from_chars(
        hexValue.data(), hexValue.data() + hexValue.size(), codePoint, 16);
    AD_CORRECTNESS_CHECK(result.ec == std::errc{});
    AD_CORRECTNESS_CHECK(
        hexValue.size() == 8 || hexValue.size() == 4,
        "Unicode escape sequences must be either 8 or 4 characters long.");

    bool isFullCodePoint = hexValue.size() == 8;

    // See https://symbl.cc/en/unicode/blocks/high-surrogates/ for more
    // information.
    if (detail::isHighSurrogate(codePoint)) {
      throwError(!isFullCodePoint,
                 "Surrogates should not be encoded as full code points.");
      throwError(
          highSurrogate == 0,
          "A high surrogate cannot be followed by another high surrogate.");
      highSurrogate = codePoint;
      continue;
    } else if (detail::isLowSurrogate(codePoint)) {
      throwError(!isFullCodePoint,
                 "Surrogates should not be encoded as full code points.");
      throwError(highSurrogate != 0,
                 "A low surrogate cannot be the first surrogate.");
      codePoint = detail::combineSurrogates(highSurrogate, codePoint);
      highSurrogate = 0;
    } else {
      throwError(
          highSurrogate == 0,
          "A high surrogate cannot be followed by a regular code point.");
    }

    ad_utility::utf8EncodeCodepoint(codePoint, output);
  }

  // Avoid redundant copy if no escape sequences were found.
  if (noEscapeSequenceFound) {
    return input;
  }

  throwError(highSurrogate == 0,
             "A high surrogate must be followed by a low surrogate.");

  output += view.substr(lastPos);
  return output;
}
}  // namespace sparqlParserHelpers
