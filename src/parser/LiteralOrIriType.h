// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <string>

#include "IriType.h"
#include "LiteralType.h"

class LiteralOrIriType {
 private:
  using LiteralOrIriVariant = std::variant<LiteralType, IriType>;
  LiteralOrIriVariant data;

 public:
  // Creates a new LiteralOrIriType based on a LiteralType object
  LiteralOrIriType(LiteralType data);
  // Creates a new LiteralOrIriType based on a IriType object
  LiteralOrIriType(IriType data);

  // Returns true, if object contains an IriType object
  bool isIri() const;
  // Returns contained IriType object if available, throws exception otherwise
  IriType& getIriTypeObject();
  // Returns iri string of contained IriType object if available, throws
  // exception otherwise
  std::string_view getIriString();

  // Returns true, if object contains an LiteralType object
  bool isLiteral() const;
  // Returns contained LiteralType object if available, throws exception
  // otherwise
  LiteralType& getLiteralTypeObject();
  // Returns true if contained LiteralType object has a language tag, throws
  // exception if no LiteralType object is contained
  bool hasLanguageTag();
  // Returns true if contained LiteralType object has a datatype, throws
  // exception if no LiteralType object is contained
  bool hasDatatype();
  // Returns content of contained LiteralType as string, throws exception if no
  // LiteralType object is contained
  std::string_view getLiteralContent();
  // Returns the language tag of the contained LiteralType, throws exception if
  // no LiteralType object is contained or object has no language tag
  std::string_view getLanguageTag();
  // Returns the datatype of the contained LiteralType, throws exception if no
  // LiteralType object is contained or object has no datatype
  std::string_view getDatatype();
};
