//  Copyright 2022-2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "SparqlParserHelpers.h"

#include <unicode/unistr.h>

#include <charconv>
#include <ctre-unicode.hpp>

#include "sparqlParser/generated/SparqlAutomaticLexer.h"

namespace sparqlParserHelpers {
using std::string;

namespace detail {
// CTRE regex pattern for C++17 compatibility
constexpr ctll::fixed_string unicodeEscapeRegex =
    R"(\\U[0-9A-Fa-f]{8}|\\u[0-9A-Fa-f]{4})";
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
  UChar32 highSurrogate = 0;

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

    UChar32 codePoint;
    auto result = std::from_chars(
        hexValue.data(), hexValue.data() + hexValue.size(), codePoint, 16);
    AD_CORRECTNESS_CHECK(result.ec == std::errc{});
    AD_CORRECTNESS_CHECK(
        hexValue.size() == 8 || hexValue.size() == 4,
        "Unicode escape sequences must be either 8 or 4 characters long.");

    bool isFullCodePoint = hexValue.size() == 8;

    // See https://symbl.cc/en/unicode/blocks/high-surrogates/ for more
    // information.
    if (U16_IS_LEAD(codePoint)) {
      throwError(!isFullCodePoint,
                 "Surrogates should not be encoded as full code points.");
      throwError(
          highSurrogate == 0,
          "A high surrogate cannot be followed by another high surrogate.");
      highSurrogate = codePoint;
      continue;
    } else if (U16_IS_TRAIL(codePoint)) {
      throwError(!isFullCodePoint,
                 "Surrogates should not be encoded as full code points.");
      throwError(highSurrogate != 0,
                 "A low surrogate cannot be the first surrogate.");
      codePoint = U16_GET_SUPPLEMENTARY(highSurrogate, codePoint);
      highSurrogate = 0;
    } else {
      throwError(
          highSurrogate == 0,
          "A high surrogate cannot be followed by a regular code point.");
    }

    icu::UnicodeString helper{codePoint};
    helper.toUTF8String(output);
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
