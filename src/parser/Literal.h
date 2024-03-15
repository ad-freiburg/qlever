// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include "parser/Iri.h"
#include "parser/NormalizedString.h"

namespace ad_utility::triple_component {
// A class to hold literal values.
class Literal {
 private:
  // Store the string value of the literal without the surrounding quotation
  // marks or trailing descriptor.
  //  "Hello World"@en -> Hello World
  NormalizedString content_;
  std::size_t beginOfSuffix_;

  // Create a new literal without any descriptor
  explicit Literal(NormalizedString content, size_t beginOfSuffix_);

  // Similar to `literalWithQuotes`, except the rdfContent is expected to
  // already be normalized
  static Literal literalWithNormalizedContent(
      NormalizedString normalizedRdfContent,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);

  NormalizedStringView getSuffix() const {
    NormalizedStringView result = content_;
    result.remove_prefix(beginOfSuffix_);
    return result;
  }

 public:
  template <typename H>
  friend H AbslHashValue(H h, const std::same_as<Literal> auto& literal) {
    return H::combine(std::move(h), asStringViewUnsafe(literal.content_));
  }
  bool operator==(const Literal&) const = default;

  std::string_view toInternalRepresentation() const;

  static Literal fromInternalRepresentation(std::string_view internal);

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
  // LiteralORIri::literalWithQuotes
  static Literal literalWithQuotes(
      std::string_view rdfContentWithQuotes,
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
