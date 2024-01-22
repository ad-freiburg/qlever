// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "LiteralOrIri.h"

#include <algorithm>

LiteralOrIri::LiteralOrIri(Iri data) : data(data) {}
LiteralOrIri::LiteralOrIri(Literal data) : data(data) {}

bool LiteralOrIri::isIri() const {
  return std::holds_alternative<Iri>(data);
}

Iri& LiteralOrIri::getIriTypeObject() {
  if (!isIri()) {
    AD_THROW(
        "LiteralOrIri object does not contain an Iri object and thus "
        "cannot return it");
  }
  return std::get<Iri>(data);
}

NormalizedStringView LiteralOrIri::getIriString() {
  Iri& iriType = getIriTypeObject();
  return iriType.getIri();
}

bool LiteralOrIri::isLiteral() const {
  return std::holds_alternative<Literal>(data);
}

Literal& LiteralOrIri::getLiteralTypeObject() {
  if (!isLiteral()) {
    AD_THROW(
        "LiteralOrIri object does not contain an Literal object and "
        "thus cannot return it");
  }
  return std::get<Literal>(data);
}

bool LiteralOrIri::hasLanguageTag() {
  Literal& literal = getLiteralTypeObject();
  return literal.hasLanguageTag();
}

bool LiteralOrIri::hasDatatype() {
  Literal& literal = getLiteralTypeObject();
  return literal.hasDatatype();
}

NormalizedStringView LiteralOrIri::getLiteralContent() {
  Literal& literal = getLiteralTypeObject();
  return literal.getContent();
}

NormalizedStringView LiteralOrIri::getLanguageTag() {
  Literal& literal = getLiteralTypeObject();
  return literal.getLanguageTag();
}

NormalizedStringView LiteralOrIri::getDatatype() {
  Literal& literal = getLiteralTypeObject();
  return literal.getDatatype();
}

LiteralOrIri fromStringToLiteral(std::string_view input,
                                     std::string_view c) {
  auto pos = input.find(c, c.length());
  if (pos == 1) {
    AD_THROW("Cannot create LiteralOrIri from the input " + input +
             " because of missing or invalid closing quote character");
  }

  std::string_view literal_content = input.substr(c.length(), pos - c.length());

  // No language tag or datatype
  if (pos == input.length() - c.length()) {
    return LiteralOrIri(Literal(fromStringUnsafe(literal_content)));
  }

  std::string_view suffix =
      input.substr(pos + c.length(), input.length() - pos - c.length());

  if (suffix.starts_with("@")) {
    std::string_view language_tag = suffix.substr(1, suffix.length() - 1);
    Literal literal = Literal(fromStringUnsafe(literal_content),
                                      fromStringUnsafe(language_tag),
                                      LiteralDescriptor::LANGUAGE_TAG);
    return LiteralOrIri(literal);
  }
  if (suffix.starts_with("^^")) {
    std::string_view datatype = suffix.substr(2, suffix.length() - 2);
    Literal literal =
        Literal(fromStringUnsafe(literal_content),
                    fromStringUnsafe(datatype), LiteralDescriptor::DATATYPE);
    return LiteralOrIri(literal);
  }
  AD_THROW("Cannot create LiteralOrIri from the input " + input +
           "because of invalid suffix.");
}

LiteralOrIri LiteralOrIri::fromStringToLiteralOrIri(
    std::string_view input) {
  if (input.starts_with("<") && input.ends_with(">")) {
    std::string_view iri_content = input.substr(1, input.size() - 2);
    if (auto pos =
            iri_content.find_first_of("<>\" {}|\\^`", 0) != std::string::npos) {
      AD_THROW("Iri " + input + " contains invalid character " +
               input.substr(pos, 1) + ".");
    }
    return LiteralOrIri(Iri(fromStringUnsafe(iri_content)));
  }

  else if (input.starts_with(R"(""")"))
    return fromStringToLiteral(input, R"(""")");
  else if (input.starts_with("'''"))
    return fromStringToLiteral(input, "'''");
  else if (input.starts_with('"'))
    return fromStringToLiteral(input, "\"");
  else if (input.starts_with('\''))
    return fromStringToLiteral(input, "`");

  AD_THROW("Cannot create LiteralOrIri from the input " + input);
}
