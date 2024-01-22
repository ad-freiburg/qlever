// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include "Iri.h"
#include "Literal.h"

class LiteralOrIri {
 private:
  using LiteralOrIriVariant = std::variant<Literal, Iri>;
  LiteralOrIriVariant data;

  // Returns contained Iri object if available, throws exception otherwise
  Iri& getIriTypeObject();
  // Returns contained Literal object if available, throws exception
  // otherwise
  Literal& getLiteralTypeObject();

 public:
  // Creates a new LiteralOrIri based on a Literal object
  explicit LiteralOrIri(Literal data);
  // Creates a new LiteralOrIri based on a Iri object
  explicit LiteralOrIri(Iri data);

  // Returns true, if object contains an Iri object
  [[nodiscard]] bool isIri() const;
  // Returns iri string of contained Iri object if available, throws
  // exception otherwise
  NormalizedStringView getIriString();

  // Returns true, if object contains an Literal object
  [[nodiscard]] bool isLiteral() const;
  // Returns true if contained Literal object has a language tag, throws
  // exception if no Literal object is contained
  bool hasLanguageTag();
  // Returns true if contained Literal object has a datatype, throws
  // exception if no Literal object is contained
  bool hasDatatype();
  // Returns content of contained Literal as string, throws exception if no
  // Literal object is contained
  NormalizedStringView getLiteralContent();
  // Returns the language tag of the contained Literal, throws exception if
  // no Literal object is contained or object has no language tag
  NormalizedStringView getLanguageTag();
  // Returns the datatype of the contained Literal, throws exception if no
  // Literal object is contained or object has no datatype
  NormalizedStringView getDatatype();

  static LiteralOrIri fromStringToLiteralOrIri(std::string_view input);
};
