// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include <unicode/ustream.h>

#include <sstream>
#include <string>

#include "../util/Exception.h"
#include "../util/HashSet.h"
#include "../util/Log.h"
#include "../util/StringUtils.h"
#include "ctre/ctre.h"

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
      AD_CHECK(iterator + length <= endIterator);
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
    AD_CHECK(nextBackslashIterator + 1 < endIterator);

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
        AD_CHECK(false);
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
  std::string result;
  while (true) {
    auto pos = std::min(literal.find_first_of("\n\\"), literal.size());
    result.append(literal.begin(), literal.begin() + pos);
    if (pos == literal.size()) {
      break;
    }
    switch (literal[pos]) {
      case '\n':
        result.append("\\n");
        break;
      case '\\':
        result.append("\\\\");
        break;
      default:
        AD_CHECK(false);
    }
    literal.remove_prefix(pos + 1);
  }
  return result;
}

// ________________________________________________________________________
std::string normalizeRDFLiteral(const std::string_view origLiteral) {
  auto literal = origLiteral;

  // always start with one double quote "
  std::string res = "\"";

  // Find out, which of the forms "literal", 'literal', """literal""" or
  // '''literal''' the input has, and strip all the quotes.
  if (literal.starts_with(R"(""")") || literal.starts_with("'''")) {
    AD_CHECK(literal.ends_with(literal.substr(0, 3)));
    literal.remove_prefix(3);
    literal.remove_suffix(3);
  } else {
    AD_CHECK(literal.starts_with('"') || literal.starts_with('\''));
    AD_CHECK(literal.ends_with(literal[0]));
    literal.remove_prefix(1);
    literal.remove_suffix(1);
  }

  // All numeric and string escapes are allowed for RDF literals
  detail::unescapeStringAndNumericEscapes<false, false>(
      literal.begin(), literal.end(), std::back_inserter(res));
  res.push_back('\"');
  return res;
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
  AD_CHECK(iriref.starts_with('<'));
  AD_CHECK(iriref.ends_with('>'));
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
                 << origLiteral;
    }
    AD_CHECK(pos + 1 < literal.size());
    AD_CHECK(m.contains(literal[pos + 1]));
    res += literal[pos + 1];

    literal.remove_prefix(pos + 2);
    pos = literal.find('\\');
  }
  // the remainder
  res.append(literal);
  return res;
}

std::string replaceAll(std::string str, const std::string_view from,
                       const std::string_view to) {
  size_t start_pos = 0;
  while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos += to.length();
  }
  return str;
}

std::string escapeForCsv(std::string input) {
  if (!ctre::search<"[\r\n\",]">(input)) [[likely]] {
    return input;
  }
  return '"' + replaceAll(std::move(input), "\"", "\"\"") + '"';
}

std::string escapeForTsv(std::string input) {
  if (!ctre::search<"[\n\t]">(input)) [[likely]] {
    return input;
  }
  auto stage1 = replaceAll(std::move(input), "\t", " ");
  return replaceAll(std::move(stage1), "\n", "\\n");
}
}  // namespace RdfEscaping
