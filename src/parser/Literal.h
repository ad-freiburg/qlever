// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <optional>
#include <variant>

#include "parser/Iri.h"
#include "parser/NormalizedString.h"

namespace ad_utility::triple_component {
// A class to hold literal values.
class Literal {
 private:
  // Store the normalized version of the literal, including possible datatypes
  // and descriptors.
  //  For example `"Hello World"@en`  or `"With"Quote"^^<someDatatype>` (note
  //  that the quote in the middle is unescaped because this is the normalized
  //  form that QLever stores.
  std::string content_;
  // The position after the closing `"`, so either the size of the string, or
  // the position of the `@` or `^^` for literals with language tags or
  // datatypes.
  std::size_t beginOfSuffix_;

  // Create a new literal without any descriptor
  explicit Literal(std::string content, size_t beginOfSuffix_);

  // Internal helper function. Return either the empty string (for a plain
  // literal), `@langtag` or `^^<datatypeIri>`.
  std::string_view getSuffix() const {
    std::string_view result = content_;
    result.remove_prefix(beginOfSuffix_);
    return result;
  }

  NormalizedStringView content() const {
    return asNormalizedStringViewUnsafe(content_);
  }

 public:
  template <typename H>
  friend H AbslHashValue(H h, const std::same_as<Literal> auto& literal) {
    return H::combine(std::move(h), literal.content_);
  }
  bool operator==(const Literal&) const = default;

  const std::string& toStringRepresentation() const;
  std::string& toStringRepresentation();

  static Literal fromStringRepresentation(std::string internal);

  // Return true if the literal has an assigned language tag
  bool hasLanguageTag() const;

  // Return true if the literal has an assigned datatype
  bool hasDatatype() const;

  // Return the value of the literal without quotation marks and  without any
  // datatype or language tag
  NormalizedStringView getContent() const;

  // Return the language tag of the literal, if available, without leading @
  // character. Throws an exception if the literal has no language tag.
  NormalizedStringView getLanguageTag() const;

  // Return the datatype of the literal, if available, without leading ^^
  // prefix. Throws an exception if the literal has no datatype.
  NormalizedStringView getDatatype() const;

  // For documentation, see documentation of function
  // LiteralORIri::fromEscapedRdfLiteral
  static Literal fromEscapedRdfLiteral(
      std::string_view rdfContentWithQuotes,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);

  // Similar to `fromEscapedRdfLiteral`, except the rdfContent is expected to
  // already be normalized
  static Literal literalWithNormalizedContent(
      NormalizedStringView normalizedRdfContent,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);

  void addLanguageTag(std::string_view languageTag);
  void addDatatype(const Iri& datatype);

  // For documentation, see documentation of function
  // LiteralORIri::literalWithoutQuotes
  static Literal literalWithoutQuotes(
      std::string_view rdfContentWithoutQuotes,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);
};
}  // namespace ad_utility::triple_component
