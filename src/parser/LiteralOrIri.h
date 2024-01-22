// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include "Iri.h"
#include "Literal.h"

class LiteralOrIri {
  // A wrapper class that can contain either an Iri or a Literal object.

 private:
  using LiteralOrIriVariant = std::variant<Literal, Iri>;
  LiteralOrIriVariant data;

  // Return contained Iri object if available, throw exception otherwise
  Iri& getIri();

  // Return contained Literal object if available, throw exception
  // otherwise
  Literal& getLiteral();

 public:
  // Create a new LiteralOrIri based on a Literal object
  explicit LiteralOrIri(Literal data);
  // Create a new LiteralOrIri based on an Iri object
  explicit LiteralOrIri(Iri data);

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

  // Return content of contained Literal as string, throw exception if no
  // Literal object is contained
  NormalizedStringView getLiteralContent();

  // Return the language tag of the contained Literal, throw exception if
  // no Literal object is contained or object has no language tag
  NormalizedStringView getLanguageTag();
  
  // Return the datatype of the contained Literal, throw exception if no
  // Literal object is contained or object has no datatype
  NormalizedStringView getDatatype();

  // Parse the given input and if it is a valid rdf term, return a
  // LiteralOrIri object containing the parsed iri or literal.
  static LiteralOrIri fromRdfToLiteralOrIri(std::string_view input);
};
