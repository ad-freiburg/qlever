// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include "IriType.h"
#include "LiteralType.h"

class LiteralOrIriType {
 private:
  using LiteralOrIriVariant = std::variant<LiteralType, IriType>;
  LiteralOrIriVariant data;

  // Returns contained IriType object if available, throws exception otherwise
  IriType& getIriTypeObject();
  // Returns contained LiteralType object if available, throws exception
  // otherwise
  LiteralType& getLiteralTypeObject();

 public:
  // Creates a new LiteralOrIriType based on a LiteralType object
  explicit LiteralOrIriType(LiteralType data);
  // Creates a new LiteralOrIriType based on a IriType object
  explicit LiteralOrIriType(IriType data);

  // Returns true, if object contains an IriType object
  [[nodiscard]] bool isIri() const;
  // Returns iri string of contained IriType object if available, throws
  // exception otherwise
  NormalizedStringView getIriString();

  // Returns true, if object contains an LiteralType object
  [[nodiscard]] bool isLiteral() const;
  // Returns true if contained LiteralType object has a language tag, throws
  // exception if no LiteralType object is contained
  bool hasLanguageTag();
  // Returns true if contained LiteralType object has a datatype, throws
  // exception if no LiteralType object is contained
  bool hasDatatype();
  // Returns content of contained LiteralType as string, throws exception if no
  // LiteralType object is contained
  NormalizedStringView getLiteralContent();
  // Returns the language tag of the contained LiteralType, throws exception if
  // no LiteralType object is contained or object has no language tag
  NormalizedStringView getLanguageTag();
  // Returns the datatype of the contained LiteralType, throws exception if no
  // LiteralType object is contained or object has no datatype
  NormalizedStringView getDatatype();

  static LiteralOrIriType fromStringToLiteralOrIri(std::string_view input);
};
