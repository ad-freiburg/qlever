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
      const NormalizedString& normalizedRdfContent,
      const std::optional<std::variant<Iri, string>>& iri = std::nullopt);

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

  // Create a new Literal with optional datatype or language tag.
  //   The rdfContent is expected to be a valid string according to SPARQL 1.1
  //   Query Language, 19.8 Grammar, Rule [145], and to be surrounded by
  //   quotation marks (", """, ', or '''). If the second argument is set and of
  //   type IRI, it is stored as the datatype of the given literal. If the
  //   second argument is set and of type string, it is interpreted as the
  //   language tag of the given literal. The language tag string can optionally
  //   start with an @ character, which is removed during the automatic
  //   normalization. If no second argument is set, the literal is stored
  //   without any descriptor.
  static Literal literalWithQuotes(
      const std::string& rdfContentWithQuotes,
      const std::optional<std::variant<Iri, string>>& descriptor =
          std::nullopt);

  // Similar to `literalWithQuotes`, except the rdfContent is expected to NOT BE
  // surrounded by quotation marks.
  static Literal literalWithoutQuotes(
      const std::string& rdfContentWithoutQuotes,
      const std::optional<std::variant<Iri, string>>& descriptor =
          std::nullopt);
};
}  // namespace ad_utility::triple_component
