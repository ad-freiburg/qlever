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

  // Return iri content of contained Iri object without any leading or trailing
  // angled brackets. Throws Exception if object does not contain an Iri object.
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
  Iri getDatatype();
};
