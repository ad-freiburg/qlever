// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <variant>

#include "parser/Iri.h"
#include "parser/Literal.h"

// A wrapper class that can contain either an Iri or a Literal object.
class LiteralOrIri {
 private:
  using LiteralOrIriVariant = std::variant<Literal, Iri>;
  LiteralOrIriVariant data_;

  // Return contained Iri object if available, throw exception otherwise
  const Iri& getIri() const;

  // Return contained Literal object if available, throw exception
  // otherwise
  const Literal& getLiteral() const;

 public:
  // Create a new LiteralOrIri based on a Literal object
  explicit LiteralOrIri(Literal literal);

  // Create a new LiteralOrIri based on an Iri object
  explicit LiteralOrIri(Iri iri);

  // Return true if object contains an Iri object
  bool isIri() const;

  // Return iri content of contained Iri object without any leading or trailing
  // angled brackets. Throws Exception if object does not contain an Iri object.
  NormalizedStringView getIriContent() const;

  // Return true if object contains a Literal object
  bool isLiteral() const;

  // Return true if contained Literal object has a language tag, throw
  // exception if no Literal object is contained
  bool hasLanguageTag() const;

  // Return true if contained Literal object has a datatype, throw
  // exception if no Literal object is contained
  bool hasDatatype() const;

  // Return content of contained Literal as string without leading or training
  // quotation marks. Throw exception if no Literal object is contained
  NormalizedStringView getLiteralContent() const;

  // Return the language tag of the contained Literal without leading @
  // character. Throw exception if no Literal object is contained or object has
  // no language tag.
  NormalizedStringView getLanguageTag() const;

  // Return the datatype of the contained Literal without "^^" prefix.
  // Throw exception if no Literal object is contained or object has no
  // datatype.
  Iri getDatatype() const;

  // Return the content of the contained Iri, or the contained Literal
  NormalizedStringView getContent() const;

  // Create a new Literal without datatype or language tag
  //   Content is expected to be surrounded by quotation marks.
  static LiteralOrIri literalWithQuotes(const std::string& contentWithQuotes);

  // Create a new Literal without datatype or language tag
  //   Content is expected to not be surrounded by quotation marks.
  static LiteralOrIri literalWithoutQuotes(
      const std::string& contentWithoutQuotes);

  // Create a new Literal with datatype
  //   Content is expected to be surrounded by quotation marks.
  static LiteralOrIri literalWithQuotesWithDatatype(
      const std::string& contentWithQuotes, Iri datatype);

  // Create a new Literal with datatype
  //   Content is expected to not be surrounded by quotation marks.
  static LiteralOrIri literalWithoutQuotesWithDatatype(
      const std::string& contentWithoutQuotes, Iri datatype);

  // Create a new Literal with language tag
  //   Content is expected to be surrounded by quotation marks.
  //   Language Tag can optionally start with an @ character.
  static LiteralOrIri literalWithQuotesWithLanguageTag(
      const std::string& contentWithQuotes, const std::string& languageTag);

  // Create a new Literal with language tag
  //   Content is expected to not be surrounded by quotation marks.
  //   Language Tag can optionally start with an @ character.
  static LiteralOrIri literalWithoutQuotesWithLanguageTag(
      const std::string& contentWithoutQuotes, const std::string& languageTag);

  // Create a new iri given an iri with surrounding brackets
  static LiteralOrIri iriref(const std::string& stringWithBrackets);

  // Create a new iri given a prefix iri and its suffix
  static LiteralOrIri prefixedIri(const Iri& prefix, const std::string& suffix);
};
