// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "Literal.h"

#include <utility>

// __________________________________________
Literal::Literal(NormalizedString content)
    : content_{std::move(content)}, descriptorType_{LiteralDescriptor::NONE} {}

// __________________________________________
Literal::Literal(NormalizedString content,
                 NormalizedString datatypeOrLanguageTag, LiteralDescriptor type)
    : content_{std::move(content)},
      descriptorValue_{std::move(datatypeOrLanguageTag)},
      descriptorType_{type} {}

// __________________________________________
bool Literal::hasLanguageTag() const {
  return descriptorType_ == LiteralDescriptor::LANGUAGE_TAG;
}

// __________________________________________
bool Literal::hasDatatype() const {
  return descriptorType_ == LiteralDescriptor::DATATYPE;
}

// __________________________________________
NormalizedStringView Literal::getContent() const { return content_; }

// __________________________________________
NormalizedStringView Literal::getDatatype() const {
  if (!hasDatatype()) {
    AD_THROW("The literal does not have an explicit datatype.");
  }
  return this->descriptorValue_;
}

// __________________________________________
NormalizedStringView Literal::getLanguageTag() const {
  if (!hasLanguageTag()) {
    AD_THROW("The literal does not have an explicit language tag.");
  }
  return this->descriptorValue_;
}

// __________________________________________
std::string Literal::toRdf() const {
  std::string rdf = "\"" + asStringView(content_) + "\"";

  switch (descriptorType_) {
    case LiteralDescriptor::NONE:
      return rdf;
    case LiteralDescriptor::LANGUAGE_TAG:
      return rdf + "@" + asStringView(descriptorValue_);
    case LiteralDescriptor::DATATYPE:
      return rdf + "^^" + asStringView(descriptorValue_);
  }

  AD_THROW("Invalid value of descriptorType_");
}
