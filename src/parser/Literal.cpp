// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "Literal.h"

#include <utility>

Literal::Literal(NormalizedString content) {
  this->content = std::move(content);
  this->descriptorType = LiteralDescriptor::NONE;
}

Literal::Literal(NormalizedString content,
                         NormalizedString datatypeOrLanguageTag,
                         LiteralDescriptor type) {
  this->content = std::move(content);
  this->descriptorType = type;
  this->descriptorValue = std::move(datatypeOrLanguageTag);
}

bool Literal::hasLanguageTag() const {
  return this->descriptorType == LiteralDescriptor::LANGUAGE_TAG;
}

bool Literal::hasDatatype() const {
  return this->descriptorType == LiteralDescriptor::DATATYPE;
}

NormalizedStringView Literal::getContent() const { return this->content; }

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
