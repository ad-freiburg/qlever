// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "Literal.h"

#include <utility>

Literal::Literal(NormalizedString content) : content{std::move(content)}, descriptorType{LiteralDescriptor::NONE} {}

Literal::Literal(NormalizedString content,
                         NormalizedString datatypeOrLanguageTag,
                         LiteralDescriptor type) : content{std::move(content)}, descriptorValue{std::move(datatypeOrLanguageTag)}, descriptorType{type} {}

//
bool Literal::hasLanguageTag() const {
  return descriptorType == LiteralDescriptor::LANGUAGE_TAG;
}

//
bool Literal::hasDatatype() const {
  return descriptorType == LiteralDescriptor::DATATYPE;
}

//
NormalizedStringView Literal::getContent() const { return content; }

//
NormalizedStringView Literal::getDatatype() const {
  if (!hasDatatype()) {
    AD_THROW("The literal does not have an explicit datatype.");
  }
  return this->descriptorValue;
}

NormalizedStringView Literal::getLanguageTag() const {
  if (!hasLanguageTag()) {
    AD_THROW("The literal does not have an explicit language tag.");
  }
  return this->descriptorValue;
}
