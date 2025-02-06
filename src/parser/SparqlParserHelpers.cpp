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
    : ParserAndVisitor{unescapeUnicodeSequences(std::move(input)),
                       disableSomeChecks} {
  visitor_.setPrefixMapManually(std::move(prefixes));
}

// _____________________________________________________________________________
std::string ParserAndVisitor::unescapeUnicodeSequences(std::string input) {
  std::u8string_view utf8View{reinterpret_cast<char8_t*>(input.data()),
                              input.size()};
  std::string output;
  size_t lastPos = 0;
  UChar32 highSurrogate = '\0';

  for (auto match :
       ctre::search_all<R"(\\U[0-9A-Fa-f]{8}|\\u[0-9A-Fa-f]{4})">(utf8View)) {
    output += input.substr(lastPos, match.data() - (utf8View.data() + lastPos));
    lastPos = match.data() + match.size() - utf8View.data();

    auto hexValue = match.to_view();

    UChar32 codePoint;
    const char* startPointer = reinterpret_cast<const char*>(hexValue.data());
    auto result = std::from_chars(
        startPointer + 2, startPointer + hexValue.size(), codePoint, 16);
    AD_CORRECTNESS_CHECK(result.ec == std::errc{});

    bool isFullCodePoint = hexValue.size() == 10;

    if (U16_IS_LEAD(codePoint)) {
      AD_CORRECTNESS_CHECK(!isFullCodePoint,
                           "Surrogates should not be encoded "
                           "as full code points.");
      AD_CORRECTNESS_CHECK(highSurrogate == '\0',
                           "A high surrogate cannot be "
                           "followed by another high "
                           "surrogate.");
      highSurrogate = codePoint;
      continue;
    } else if (U16_IS_TRAIL(codePoint)) {
      AD_CORRECTNESS_CHECK(!isFullCodePoint,
                           "Surrogates should not be encoded "
                           "as full code points.");
      AD_CORRECTNESS_CHECK(highSurrogate != '\0',
                           "A low surrogate cannot "
                           "be the first surrogate.");
      codePoint = U16_GET_SUPPLEMENTARY(highSurrogate, codePoint);
      highSurrogate = '\0';
    } else {
      AD_CORRECTNESS_CHECK(highSurrogate == '\0',
                           "A high surrogate cannot be "
                           "followed by a code point.");
    }

    icu::UnicodeString helper{codePoint};
    helper.toUTF8String(output);
  }

  AD_CORRECTNESS_CHECK(highSurrogate == '\0',
                       "A high surrogate must be followed by a low surrogate.");

  output += input.substr(lastPos);
  return output;
}
}  // namespace sparqlParserHelpers
