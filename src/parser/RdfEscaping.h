// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_RDFESCAPING_H
#define QLEVER_RDFESCAPING_H

#include <unicode/ustream.h>

#include <sstream>
#include <string>

#include "global/TypedIndex.h"
#include "util/Exception.h"
#include "util/HashSet.h"
#include "util/StringUtils.h"

namespace RdfEscaping {
/// Replaces each newline '\n' by an escaped newline '\\n', and each backslash
/// '\\' by an escaped backslash "\\\\". This is the minimal amount of escaping
/// that has to be done in order to store strings in a line-based text file.
std::string escapeNewlinesAndBackslashes(std::string_view literal);

/// Replaces each escaped newline "\\n" by a newline '\n', and each escaped
/// backslash "\\\\" by a single backslash '\\'. This is the inverted function
/// of escapeNewlinesAndBackslashes.
std::string unescapeNewlinesAndBackslashes(std::string_view literal);

/**
 * @brief convert a RDF Literal to a unified form that is used inside QLever.
 * Inputs that are no literals (are not surrounded by ', ", ''' or """) will
 * trigger an exception
 *
 * RDFLiterals in Turtle or Sparql can have several forms: They may either
 * be surrounded with one (" or ') quotation mark and contain all special
 * characters in escaped form, like "\\\t". Alternatively literals may be
 * surrounded by three (""" or ''') quotation marks. Then escapes are still
 * allowed, but several special characters (e.g. '\n' or '\t' may be contained
 * directly in the string (For details, see the Turtle or Sparql standard).
 *
 * This function converts any of these forms to a literal that starts and ends
 * with a single quotation mark ("content") and contains the originally escaped
 * characters directly, e.g. "al\"pha" becomes "al"pha".
 *
 * This is NOT a valid RDF form of literals, but this format is only used
 * inside QLever.
 */

// The following two strong typedefs statically keep track of the fact whether a
// string literal is stored in the normalized form that was described above.

// A strong typedef for a `std::string_view` that stores a normalized RDF
// literal.
using NormalizedRDFStringView =
    ad_utility::TypedIndex<std::string_view, "NormalizedRDFString">;

// A strong typedef for a `std::string` that stores a normalized RDF literal.
class NormalizedRDFString {
  std::string
      data_;  // The actual string content. Always is enclosed in quotes.

 public:
  // Const access to the underlying data in the normalized form.
  const std::string& get() const { return data_; }

  // Explicit conversion from a `NormalizedRDFStringView`.
  // Note: Conversion to a `NormalizedRDFStringView` is currently not needed in
  // QLever, but could be easily implemented using a (possibly implicit)
  // `operator NormalizedRDFStringView()`
  explicit NormalizedRDFString(NormalizedRDFStringView sv) : data_{sv.get()} {}

 private:
  // Construct from the raw `data`. This function can only be called by the
  // free `normalizeRDFLiteral` function, so that function is the only way to
  // create a new `NormalizedRDFString`.
  explicit NormalizedRDFString(std::string data) : data_{std::move(data)} {
    AD_CORRECTNESS_CHECK(data_.size() >= 2 && data_.starts_with('"') &&
                         data_.ends_with('"'));
  }
  friend NormalizedRDFString normalizeRDFLiteral(std::string_view origLiteral);
};

// The actual function to obtain a `NormalizedRDFString` from any valid form of
// RDF literals. For details on the normalization format, see above.
NormalizedRDFString normalizeRDFLiteral(std::string_view origLiteral);

/**
 * Convert a literal in the form produced by `normalizeRDFLiteral` into a form
 * that is a valid literal in Turtle. For example, "al"pah" becomes "al\"pha"
 * and "be"ta"@en becomes "be\"ta"@en.
 *
 * If the `normLiteral` is not a literal, an AD_CONTRACT_CHECK will fail.
 *
 * TODO: This function currently only handles the escaping of " inside the
 * literal, no other characters. Is that enough?
 */
std::string validRDFLiteralFromNormalized(std::string_view normLiteral);

// If `input` is an IRI in `<angleBrackets>` remove those. If it is a
// "literal"^^<withDatatype> (or with a language tag or nothing),
// only return the part between the quotation marks (`literal`) in the
// example. All other strings are returned unchanged.
std::string normalizedContentFromLiteralOrIri(std::string&& input);

/**
 * In an Iriref, the only allowed escapes are \uXXXX and '\UXXXXXXXX' ,where X
 * is hexadecimal ([0-9a-fA-F]). This function replaces these escapes by the
 * corresponding UTF-8 character.
 * @param iriref An Iriref of the form <Content...>
 * @return The same Iriref but with the escape sequences replaced by their
 * actual value
 */
std::string unescapeIriref(std::string_view iriref);

/**
 * This function unescapes a prefixedIri (the "local" part in the form
 * prefix:local). These may only contain so-called "reserved character escape
 * sequences": reserved character escape sequences consist of a '\' followed
 * by one of ~.-!$&'()*+,;=/?#@%_ and represent the character to the right of
 * the '\'.
 */
std::string unescapePrefixedIri(std::string_view literal);

/**
 * Escape a string according to RFC4180 for a csv field by adding quotes
 * around the input and escaping any existing quotes if necessary.
 *
 * See https://www.ietf.org/rfc/rfc4180.txt for more information.
 */
std::string escapeForCsv(std::string input);

/**
 * Escape a string to be compatible with the IANA-TSV specification by
 * replacing tabs with spaces and newlines with '\n'.
 *
 * See https://www.iana.org/assignments/media-types/text/tab-separated-values
 * for more information.
 */
std::string escapeForTsv(std::string input);

// Escape a string to be compatible with XML.
std::string escapeForXml(std::string input);

}  // namespace RdfEscaping

#endif  // QLEVER_RDFESCAPING_H
