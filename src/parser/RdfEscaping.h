// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_RDFESCAPING_H
#define QLEVER_RDFESCAPING_H

#include <unicode/ustream.h>

#include <sstream>
#include <string>

#include "../util/Exception.h"
#include "../util/HashSet.h"
#include "../util/StringUtils.h"

namespace RdfEscaping {
using namespace std::string_literals;
namespace detail {

/// turn a number of hex-chars like '00e4' into utf-8
inline std::string unescapeUchar(std::string_view hex) {
  UChar32 x;
  std::stringstream sstream;
  sstream << std::hex << hex;
  sstream >> x;
  std::string res;
  icu::UnicodeString(x).toUTF8String(res);
  return res;
}

template <bool acceptStringEscapes, bool acceptOnlyBackslashAndNewline,
          typename InputIterator, typename OutputIterator>
void unescapeStringAndNumericEscapes(InputIterator beginIterator,
                                     InputIterator endIterator,
                                     OutputIterator outputIterator) {
  static_assert(!(acceptStringEscapes && acceptOnlyBackslashAndNewline));

  auto pushNewlineOrBackslash = [&outputIterator](const auto& character) {
    if constexpr (acceptStringEscapes || acceptOnlyBackslashAndNewline) {
      *outputIterator = character;
      outputIterator++;
    } else {
      throw std::runtime_error(
          "String escapes like \\n or \\t are not allowed in this context");
    }
  };

  auto pushOtherStringEscape = [&outputIterator](const auto& character) {
    if constexpr (acceptStringEscapes) {
      *outputIterator = character;
      outputIterator++;
    } else {
      throw std::runtime_error(
          "String escapes like \\n or \\t are not allowed in this context");
    }
  };

  auto pushNumericEscape = [&outputIterator, &endIterator](const auto& iterator,
                                                           size_t length) {
    AD_CHECK(iterator + length < endIterator);
    auto unesc = unescapeUchar(std::string_view(iterator, length));
    std::copy(unesc.begin(), unesc.end(), outputIterator);
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
        throw std::runtime_error(
            "Illegal escape sequence in RDF Literal. This should never "
            "happen, please report this");
    }
    beginIterator = nextBackslashIterator + numCharactersFromInput;
  }
}

}  // namespace detail
inline std::string unescapeNewlineAndBackslash(std::string_view literal) {
  std::string result;
  RdfEscaping::detail::unescapeStringAndNumericEscapes<false, true>(
      literal.begin(), literal.end(), std::back_inserter(result));
  return result;
}

inline std::string escapeNewlineAndBackslash(std::string_view literal) {
  const string charactersToEscape = "\n\\";
  std::string result;
  while (true) {
    auto pos =
        std::min(literal.find_first_of(charactersToEscape), literal.size());
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

/**
 * @brief convert a RDF Literal to a unified form that is used inside QLever.
 * Inputs that are no literals (don't start with a single or double quote)
 * will be returned unchanged.
 *
 * RDFLiterals in Turtle or Sparql can have several forms: Either starting
 * with one (" or ') quotation mark and containing escape sequences like
 * "\\\t" or with three (""" or ''') quotation marks and allowing most control
 * sequences to be contained in the string directly.
 *
 * This function converts any of this forms to a literal that starts and ends
 * with a single quotation mark '"' and contains the originally escaped
 * characters directly, e.g. "al\"pha" becomes "al"pha".
 *
 * This is NOT a valid RDF form of literals, but this format is only used
 * inside QLever. By stripping the leading and trailing quotation mark and
 * possible langtags or datatype URIS one can directly obtain the actual
 * content of the literal.
 *
 *
 * @param literal
 * @return
 */
inline std::string normalizeRDFLiteral(const std::string_view origLiteral) {
  auto literal = origLiteral;

  std::string res = "\"";

  if (ad_utility::startsWith(literal, "\"\"\"") ||
      ad_utility::startsWith(literal, "'''")) {
    AD_CHECK(ad_utility::endsWith(literal, literal.substr(0, 3)));
    literal.remove_prefix(3);
    literal.remove_suffix(3);
  } else {
    AD_CHECK(ad_utility::startsWith(literal, "\"") ||
             ad_utility::startsWith(literal, "'"));
    AD_CHECK(ad_utility::endsWith(literal, literal.substr(0, 1)));
    literal.remove_prefix(1);
    literal.remove_suffix(1);
  }
  detail::unescapeStringAndNumericEscapes<true, false>(
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
inline std::string unescapeIriref(std::string_view iriref) {
  AD_CHECK(ad_utility::startsWith(iriref, "<"));
  AD_CHECK(ad_utility::endsWith(iriref, ">"));
  iriref.remove_prefix(1);
  iriref.remove_suffix(1);
  std::string result = "<";
  RdfEscaping::detail::unescapeStringAndNumericEscapes<false, false>(
      iriref.begin(), iriref.end(), std::back_inserter(result));
  result.push_back('>');
  return result;
}

/**
 * This function unescapes a prefixedIri (the "local" part in the form
 * prefix:local). These may only contain socalled "reserved character escape
 * sequences": reserved character escape sequences consist of a '\' followed
 * by one of ~.-!$&'()*+,;=/?#@%_ and represent the character to the right of
 * the '\'.
 */
inline std::string unescapePrefixedIri(std::string_view literal) {
  std::string res;
  static const ad_utility::HashSet<char> m = []() {
    ad_utility::HashSet<char> r;
    for (auto c : {'_', '~', '.', '-', '-', '!', '$', '&', '\'', '(', ')',
                   '*', '+', ',', ';', '=', '/', '?', '#', '@',  '%'}) {
      r.insert(c);
    }
    return r;
  }();
  auto pos = literal.find('\\');
  while (pos != literal.npos) {
    res.append(literal.begin(), literal.begin() + pos);
    if (pos + 1 >= literal.size()) {
      throw std::runtime_error(
          "Trying to unescape a literal or iri that ended with a single "
          "backslash. This should not happen, please report this");
    }
    if (m.count(literal[pos + 1])) {
      res += literal[pos + 1];
    } else {
      throw std::runtime_error(
          std::string{"Illegal escape sequence \\"} + literal[pos + 1] +
          " encountered while trying to unescape an iri. Please report this");
    }

    literal.remove_prefix(pos + 2);
    pos = literal.find('\\');
  }
  // the remainder
  res.append(literal);
  return res;
}
}  // namespace RdfEscaping

#endif  // QLEVER_RDFESCAPING_H
