// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <variant>

#include "Iri.h"
#include "Literal.h"

class LiteralOrIri {
  // A wrapper class that can contain either an Iri or a Literal object.

 private:
  using LiteralOrIriVariant = std::variant<Literal, Iri>;
  LiteralOrIriVariant data_;

  // Return contained Iri object if available, throw exception otherwise
  Iri& getIri();

  // Return contained Literal object if available, throw exception
  // otherwise
  Literal& getLiteral();

 public:
  // Create a new LiteralOrIri based on a Literal object
  explicit LiteralOrIri(Literal literal);
  // Create a new LiteralOrIri based on an Iri object
  explicit LiteralOrIri(Iri iri);

  // Return true if object contains an Iri object
  bool isIri() const;

  // Return iri string of contained Iri object if available, throw
  // exception otherwise
  NormalizedStringView getIriContent();

  // Return true if object contains a Literal object
  bool isLiteral() const;

  // Return true if contained Literal object has a language tag, throw
  // exception if no Literal object is contained
  bool hasLanguageTag();

  // Return true if contained Literal object has a datatype, throw
  // exception if no Literal object is contained
  bool hasDatatype();

  // Return content of contained Literal as string without leading or training
  // quotation marks. Throw exception if no Literal object is contained
  NormalizedStringView getLiteralContent();

  // Return the language tag of the contained Literal without leading @
  // character. Throw exception if no Literal object is contained or object has
  // no language tag.
  NormalizedStringView getLanguageTag();

  // Return the datatype of the contained Literal without "^^" prefix.
  // Throw exception if no Literal object is contained or object has no
  // datatype.
  NormalizedStringView getDatatype();

  // Return the stored value as valid rdf, including angled brackets if value
  // is an Iri, or including quotes (and if applicable descriptor prefix "@" or
  // "^^") if value is a Literal.
  std::string toRdf();

  // Parse the given input and if it is a valid rdf term and return a
  // LiteralOrIri object containing the parsed iri or literal.
  static LiteralOrIri parseRdf(std::string_view input);
};
