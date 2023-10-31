// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "parser/RdfEscaping.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <ctre/ctre.h>
#include <unicode/ustream.h>

#include <sstream>
#include <string>

#include "util/Exception.h"
#include "util/HashSet.h"
#include "util/Log.h"
#include "util/StringUtils.h"

namespace RdfEscaping {
using namespace std::string_literals;
namespace detail {

/// Turn a sequence of characters that encode hexadecimal numbers(e.g. "00e4")
/// into the corresponding UTF-8 string (e.g. "ä").
std::string hexadecimalCharactersToUtf8(std::string_view hex) {
  UChar32 x;
  std::stringstream sstream;
  sstream << std::hex << hex;
  sstream >> x;
  std::string res;
  icu::UnicodeString(x).toUTF8String(res);
  return res;
}

/**
 * Internal helper function. Unescape all string escapes (e.g. "\\n"-> '\n') and
 * all numeric escapes (e.g. "\\u00E4" -> 'ä'). Using the template bools this
 * function can be configured to unescape only string escapes, or even only
 * newlines and backslashes. It throws an exception if an escape sequence that
 * is not allowed is found.
 */
template <bool acceptOnlyNumericEscapes, bool acceptOnlyBackslashAndNewline,
          typename InputIterator, typename OutputIterator>
void unescapeStringAndNumericEscapes(InputIterator beginIterator,
                                     InputIterator endIterator,
                                     OutputIterator outputIterator) {
  static_assert(!(acceptOnlyNumericEscapes && acceptOnlyBackslashAndNewline));

  // Append the `character` to the output, but only if newlines/backslashes are
  // allowed via the configuration
  auto pushNewlineOrBackslash = [&outputIterator](
                                    const char newlineOrBackslash) {
    if constexpr (!acceptOnlyNumericEscapes || acceptOnlyBackslashAndNewline) {
      *outputIterator = newlineOrBackslash;
      outputIterator++;
    } else {
      (void)outputIterator;
      (void)newlineOrBackslash;
      throw std::runtime_error(
          "String escapes like \\n or \\t are not allowed in this context");
    }
  };

  // Append the `character` to the output, but only if general string escapes
  // (e.g. "\\t") are allowed via the configuration
  auto pushOtherStringEscape = [&outputIterator](const char otherStringEscape) {
    if constexpr (!acceptOnlyNumericEscapes && !acceptOnlyBackslashAndNewline) {
      *outputIterator = otherStringEscape;
      outputIterator++;
    } else {
      (void)outputIterator;
      (void)otherStringEscape;
      throw std::runtime_error(
          "String escapes like \\n or \\t are not allowed in this context");
    }
  };

  // Convert `length` hexadecimal characters (e.g. "00e4" from the `iterator`)
  // to UTF-8 and append them to the output. Throw an exception if numeric
  // escapes are not allowed via the configuration.
  auto pushNumericEscape = [&outputIterator, &endIterator](const auto& iterator,
                                                           size_t length) {
    if constexpr (!acceptOnlyBackslashAndNewline) {
      AD_CONTRACT_CHECK(iterator + length <= endIterator);
      auto unesc =
          hexadecimalCharactersToUtf8(std::string_view(iterator, length));
      std::copy(unesc.begin(), unesc.end(), outputIterator);
    } else {
      (void)outputIterator;
      (void)endIterator;
      throw std::runtime_error(
          "Numeric escapes escapes like \"\\u00e4\" are not allowed in this "
          "context");
    }
  };

  while (true) {
    auto nextBackslashIterator = std::find(beginIterator, endIterator, '\\');
    std::copy(beginIterator, nextBackslashIterator, outputIterator);
    if (nextBackslashIterator == endIterator) {
      break;
    }
    AD_CONTRACT_CHECK(nextBackslashIterator + 1 < endIterator);

    size_t numCharactersFromInput = 2;  // backslash + 1 character
    switch (*(nextBackslashIterator + 1)) {
      case 't':
        pushOtherStringEscape('\t');
        break;
      case 'n':
        pushNewlineOrBackslash('\n');
        break;
      case 'r':
        pushOtherStringEscape('\r');
        break;
      case 'b':
        pushOtherStringEscape('\b');
        break;
      case 'f':
        pushOtherStringEscape('\f');
        break;
      case '"':
        pushOtherStringEscape('\"');
        break;
      case '\'':
        pushOtherStringEscape('\'');
        break;
      case '\\':
        pushNewlineOrBackslash('\\');
        break;
      case 'u': {
        pushNumericEscape(nextBackslashIterator + 2, 4);
        numCharactersFromInput = 6;  // \UXXXX
        break;
      }
      case 'U': {
        pushNumericEscape(nextBackslashIterator + 2, 8);
        numCharactersFromInput = 10;  // \UXXXXXXXX
        break;
      }

      default:
        // should never happen
        AD_FAIL();
    }
    beginIterator = nextBackslashIterator + numCharactersFromInput;
  }
}
}  // namespace detail

// _____________________________________________________________________________
std::string unescapeNewlinesAndBackslashes(std::string_view literal) {
  std::string result;
  RdfEscaping::detail::unescapeStringAndNumericEscapes<false, true>(
      literal.begin(), literal.end(), std::back_inserter(result));
  return result;
}

// ____________________________________________________________________________
std::string escapeNewlinesAndBackslashes(std::string_view literal) {
  return absl::StrReplaceAll(literal, {{"\n", "\\n"}, {R"(\)", R"(\\)"}});
}

// ________________________________________________________________________
NormalizedRDFString normalizeRDFLiteral(const std::string_view origLiteral) {
  auto literal = origLiteral;

  // always start with one double quote "
  std::string res = "\"";

  // Find out, which of the forms "literal", 'literal', """literal""" or
  // '''literal''' the input has, and strip all the quotes.
  if (literal.starts_with(R"(""")") || literal.starts_with("'''")) {
    AD_CONTRACT_CHECK(literal.ends_with(literal.substr(0, 3)));
    literal.remove_prefix(3);
    literal.remove_suffix(3);
  } else {
    AD_CONTRACT_CHECK(literal.starts_with('"') || literal.starts_with('\''));
    AD_CONTRACT_CHECK(literal.ends_with(literal[0]));
    literal.remove_prefix(1);
    literal.remove_suffix(1);
  }

  // All numeric and string escapes are allowed for RDF literals
  detail::unescapeStringAndNumericEscapes<false, false>(
      literal.begin(), literal.end(), std::back_inserter(res));
  res.push_back('\"');
  return NormalizedRDFString{std::move(res)};
}

// ____________________________________________________________________________
std::string validRDFLiteralFromNormalized(std::string_view normLiteral) {
  AD_CONTRACT_CHECK(normLiteral.starts_with('"'));
  size_t posSecondQuote = normLiteral.find('"', 1);
  AD_CONTRACT_CHECK(posSecondQuote != std::string::npos);
  size_t posLastQuote = normLiteral.rfind('"');
  // If there are onyl two quotes (the first and the last, which every
  // normalized literal has), there is nothing to do.
  if (posSecondQuote == posLastQuote &&
      normLiteral.find_first_of("\\\n\r") == std::string::npos) {
    return std::string{normLiteral};
  }
  // Otherwise escape first all backlashes then all quotes (the order is
  // important) in the part between the first and the last quote and leave the
  // rest unchanged.
  std::string_view normalizedContent = normLiteral.substr(1, posLastQuote - 1);
  std::string content = absl::StrReplaceAll(
      normalizedContent,
      {{R"(\)", R"(\\)"}, {"\n", "\\n"}, {"\r", "\\r"}, {R"(")", R"(\")"}});
  return absl::StrCat("\"", content, normLiteral.substr(posLastQuote));
}

/**
 * In an Iriref, the only allowed escapes are \uXXXX and '\UXXXXXXXX' ,where X
 * is hexadecimal ([0-9a-fA-F]). This function replaces these escapes by the
 * corresponding UTF-8 character.
 * @param iriref An Iriref of the form <Content...>
 * @return The same Iriref but with the escape sequences replaced by their
 * actual value
 */
std::string unescapeIriref(std::string_view iriref) {
  AD_CONTRACT_CHECK(iriref.starts_with('<'));
  AD_CONTRACT_CHECK(iriref.ends_with('>'));
  iriref.remove_prefix(1);
  iriref.remove_suffix(1);
  std::string result = "<";
  // Only numeric escapes are allowed for iriefs.
  RdfEscaping::detail::unescapeStringAndNumericEscapes<true, false>(
      iriref.begin(), iriref.end(), std::back_inserter(result));
  result.push_back('>');
  return result;
}

// __________________________________________________________________________
std::string unescapePrefixedIri(std::string_view literal) {
  std::string_view origLiteral = literal;
  std::string res;
  ad_utility::HashSet<char> m{'_', '~',  '.', '-', '-', '!', '$',
                              '&', '\'', '(', ')', '*', '+', ',',
                              ';', '=',  '/', '?', '#', '@', '%'};
  auto pos = literal.find('\\');
  while (pos != literal.npos) {
    res.append(literal.begin(), literal.begin() + pos);
    if (pos + 1 >= literal.size() || !m.contains(literal[pos + 1])) {
      LOG(ERROR) << "Error in function unescapePrefixedIri, could not unescape "
                    "prefixed iri "
                 << origLiteral << '\n';
    }
    AD_CONTRACT_CHECK(pos + 1 < literal.size());
    AD_CONTRACT_CHECK(m.contains(literal[pos + 1]));
    res += literal[pos + 1];

    literal.remove_prefix(pos + 2);
    pos = literal.find('\\');
  }
  // the remainder
  res.append(literal);
  return res;
}

// __________________________________________________________________________
std::string escapeForCsv(std::string input) {
  if (!ctre::search<"[\r\n\",]">(input)) [[likely]] {
    return input;
  }
  return absl::StrCat("\"", absl::StrReplaceAll(input, {{"\"", "\"\""}}), "\"");
}

// __________________________________________________________________________
std::string escapeForTsv(std::string input) {
  if (ctre::search<"[\n\t]">(input)) [[unlikely]] {
    absl::StrReplaceAll({{"\t", " "}, {"\n", "\\n"}}, &input);
  }
  return input;
}

// __________________________________________________________________________
std::string escapeForXml(std::string input) {
  if (ctre::search<"[&\"<>']">(input)) [[unlikely]] {
    absl::StrReplaceAll({{"&", "&amp;"},
                         {"<", "&lt;"},
                         {">", "&gt;"},
                         {"\"", "&quot;"},
                         {"'", "&apos;"}},
                        &input);
  }
  return input;
}

// __________________________________________________________________________
std::string normalizedContentFromLiteralOrIri(std::string&& input) {
  if (input.starts_with('<')) {
    AD_CORRECTNESS_CHECK(input.ends_with('>'));
    std::shift_left(input.begin(), input.end(), 1);
    input.resize(input.size() - 2);
  } else if (input.starts_with('"')) {
    auto posLastQuote = static_cast<int64_t>(input.rfind('"'));
    AD_CORRECTNESS_CHECK(posLastQuote > 0);
    std::shift_left(input.begin(), input.begin() + posLastQuote, 1);
    input.resize(posLastQuote - 1);
  }
  return std::move(input);
}

}  // namespace RdfEscaping
