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

  using LiteralDescriptorVariant =
      std::variant<std::monostate, NormalizedString, Iri>;

  // Store the optional language tag or the optional datatype if applicable
  // without their prefixes.
  //  "Hello World"@en -> en
  //  "Hello World"^^test:type -> test:type
  LiteralDescriptorVariant descriptor_;

  // Create a new literal without any descriptor
  explicit Literal(NormalizedString content);

  // Create a new literal with a datatype
  Literal(NormalizedString content, Iri datatype);

  // Create a new literal with a language tag
  Literal(NormalizedString content, NormalizedString languageTag);

  // Similar to `literalWithQuotes`, except the rdfContent is expected to
  // already be normalized
  static Literal literalWithNormalizedContent(
      NormalizedString normalizedRdfContent,
      std::optional<std::variant<Iri, string>> descriptor = std::nullopt);

 public:
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
  Iri getDatatype() const;

  // For documentation, see documentation of function
  // LiteralORIri::literalWithQuotes
  static Literal literalWithQuotes(
      std::string_view rdfContentWithQuotes,
      std::optional<std::variant<Iri, string>> descriptor = std::nullopt);

  // For documentation, see documentation of function
  // LiteralORIri::literalWithoutQuotes
  static Literal literalWithoutQuotes(
      std::string_view rdfContentWithoutQuotes,
      std::optional<std::variant<Iri, string>> descriptor = std::nullopt);
};
}  // namespace ad_utility::triple_component
