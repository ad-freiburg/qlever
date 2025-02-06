//
// Created by johannes on 16.05.21.
//

#include "SparqlParserHelpers.h"

#include <unicode/unistr.h>

#include <charconv>
#include <ctre-unicode.hpp>

#include "sparqlParser/generated/SparqlAutomaticLexer.h"

namespace sparqlParserHelpers {
using std::string;

// _____________________________________________________________________________
ParserAndVisitor::ParserAndVisitor(
    std::string input,
    SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks)
    : input_{unescapeUnicodeSequences(std::move(input))},
      visitor_{{}, disableSomeChecks} {
  // The default in ANTLR is to log all errors to the console and to continue
  // the parsing. We need to turn parse errors into exceptions instead to
  // propagate them to the user.
  parser_.removeErrorListeners();
  parser_.addErrorListener(&errorListener_);
  lexer_.removeErrorListeners();
  lexer_.addErrorListener(&errorListener_);
}

// _____________________________________________________________________________
ParserAndVisitor::ParserAndVisitor(
    std::string input, SparqlQleverVisitor::PrefixMap prefixes,
    SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks)
    : ParserAndVisitor{std::move(input), disableSomeChecks} {
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

  for (const auto& match :
       ctre::search_all<R"(\\U[0-9A-Fa-f]{8}|\\u[0-9A-Fa-f]{4})">(view)) {
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
