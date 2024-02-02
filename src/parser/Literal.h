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

  // Create a new Literal with optional datatype or language tag.
  //   The literal content is expected to be already normalized (see
  //   RDFEscaping.h). If the second argument is set and of type IRI, it is
  //   stored as the datatype of the given literal. If the second argument is
  //   set and of type string, it is interpreted as the language tag of the
  //   given literal. Otherwise, the literal is stored without any descriptor.
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

  // Factory functions to create `Literal`s. For their exact documentation, see
  // the functions with the exact same name in the `LiteralOrIri` class.
  static Literal literalWithQuotes(
      std::string_view rdfContentWithQuotes,
      std::optional<std::variant<Iri, string>> descriptor = std::nullopt);

  static Literal literalWithoutQuotes(
      std::string_view rdfContentWithoutQuotes,
      std::optional<std::variant<Iri, string>> descriptor = std::nullopt);
};
}  // namespace ad_utility::triple_component
