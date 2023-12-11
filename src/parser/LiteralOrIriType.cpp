// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "LiteralOrIriType.h"

LiteralOrIriType::LiteralOrIriType(IriType data) : data(data) {}
LiteralOrIriType::LiteralOrIriType(LiteralType data) : data(data) {}

bool LiteralOrIriType::isIri() const {
  return std::holds_alternative<IriType>(data);
}

IriType& LiteralOrIriType::getIriTypeObject() {
  if (!isIri()) {
    AD_THROW(
        "LiteralOrIriType object does not contain an IriType object and thus "
        "cannot return it");
  }
  return std::get<IriType>(data);
}

std::string_view LiteralOrIriType::getIriString() {
  IriType& iriType = getIriTypeObject();
  return iriType.getIri();
}

bool LiteralOrIriType::isLiteral() const {
  return std::holds_alternative<LiteralType>(data);
}

LiteralType& LiteralOrIriType::getLiteralTypeObject() {
  if (!isLiteral()) {
    AD_THROW(
        "LiteralOrIriType object does not contain an LiteralType object and "
        "thus cannot return it");
  }
  return std::get<LiteralType>(data);
}

bool LiteralOrIriType::hasLanguageTag() {
  LiteralType& literal = getLiteralTypeObject();
  return literal.hasLanguageTag();
}

bool LiteralOrIriType::hasDatatype() {
  LiteralType& literal = getLiteralTypeObject();
  return literal.hasDatatype();
}

std::string_view LiteralOrIriType::getLiteralContent() {
  LiteralType& literal = getLiteralTypeObject();
  return literal.getContent();
}

std::string_view LiteralOrIriType::getLanguageTag() {
  LiteralType& literal = getLiteralTypeObject();
  return literal.getLanguageTag();
}

std::string_view LiteralOrIriType::getDatatype() {
  LiteralType& literal = getLiteralTypeObject();
  return literal.getDatatype();
}
