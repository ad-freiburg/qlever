// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include "parser/Iri.h"
#include "parser/NormalizedString.h"

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

  // Create a new Literal without datatype or language tag
  //   Content is expected to be surrounded by quotation marks.
  static Literal literalWithQuotes(const std::string& contentWithQuotes);

  // Create a new Literal without datatype or language tag
  //   Content is expected to not be surrounded by quotation marks.
  static Literal literalWithoutQuotes(const std::string& contentWithoutQuotes);

  // Create a new Literal with datatype
  //   Content is expected to be surrounded by quotation marks.
  static Literal literalWithQuotesWithDatatype(
      const std::string& contentWithQuotes, Iri datatype);

  // Create a new Literal with datatype
  //   Content is expected to not be surrounded by quotation marks.
  static Literal literalWithoutQuotesWithDatatype(
      const std::string& contentWithoutQuotes, Iri datatype);

  // Create a new Literal with language tag
  //   Content is expected to be surrounded by quotation marks.
  //   Language Tag can optionally start with an @ character.
  static Literal literalWithQuotesWithLanguageTag(
      const std::string& contentWithQuotes, const std::string& languageTag);

  // Create a new Literal with language tag
  //   Content is expected to not be surrounded by quotation marks.
  //   Language Tag can optionally start with an @ character.
  static Literal literalWithoutQuotesWithLanguageTag(
      const std::string& contentWithoutQuotes, const std::string& languageTag);
};
