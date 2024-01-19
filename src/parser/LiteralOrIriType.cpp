// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "LiteralOrIriType.h"

#include <algorithm>

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

NormalizedStringView LiteralOrIriType::getIriString() {
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

NormalizedStringView LiteralOrIriType::getLiteralContent() {
  LiteralType& literal = getLiteralTypeObject();
  return literal.getContent();
}

NormalizedStringView LiteralOrIriType::getLanguageTag() {
  LiteralType& literal = getLiteralTypeObject();
  return literal.getLanguageTag();
}

NormalizedStringView LiteralOrIriType::getDatatype() {
  LiteralType& literal = getLiteralTypeObject();
  return literal.getDatatype();
}

LiteralOrIriType fromStringToLiteral(std::string_view input,
                                     std::string_view c) {
  auto pos = input.find(c, c.length());
  if (pos == 1) {
    AD_THROW("Cannot create LiteralOrIriType from the input " + input +
             " because of missing or invalid closing quote character");
  }

  std::string_view literal_content = input.substr(c.length(), pos - c.length());

  // No language tag or datatype
  if (pos == input.length() - c.length()) {
    return LiteralOrIriType(LiteralType(fromStringUnsafe(literal_content)));
  }

  std::string_view suffix =
      input.substr(pos + c.length(), input.length() - pos - c.length());

  if (suffix.starts_with("@")) {
    std::string_view language_tag = suffix.substr(1, suffix.length() - 1);
    LiteralType literal = LiteralType(fromStringUnsafe(literal_content),
                                      fromStringUnsafe(language_tag),
                                      LiteralDescriptor::LANGUAGE_TAG);
    return LiteralOrIriType(literal);
  }
  if (suffix.starts_with("^^")) {
    std::string_view datatype = suffix.substr(2, suffix.length() - 2);
    LiteralType literal =
        LiteralType(fromStringUnsafe(literal_content),
                    fromStringUnsafe(datatype), LiteralDescriptor::DATATYPE);
    return LiteralOrIriType(literal);
  }
  AD_THROW("Cannot create LiteralOrIriType from the input " + input +
           "because of invalid suffix.");
}

LiteralOrIriType LiteralOrIriType::fromStringToLiteralOrIri(
    std::string_view input) {
  if (input.starts_with("<") && input.ends_with(">")) {
    std::string_view iri_content = input.substr(1, input.size() - 2);
    if (auto pos =
            iri_content.find_first_of("<>\" {}|\\^`", 0) != std::string::npos) {
      AD_THROW("Iri " + input + " contains invalid character " +
               input.substr(pos, 1) + ".");
    }
    return LiteralOrIriType(IriType(fromStringUnsafe(iri_content)));
  }

  else if (input.starts_with(R"(""")"))
    return fromStringToLiteral(input, R"(""")");
  else if (input.starts_with("'''"))
    return fromStringToLiteral(input, "'''");
  else if (input.starts_with('"'))
    return fromStringToLiteral(input, "\"");
  else if (input.starts_with('\''))
    return fromStringToLiteral(input, "`");

  AD_THROW("Cannot create LiteralOrIriType from the input " + input);
}
