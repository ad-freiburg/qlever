// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "LiteralType.h"

#include <utility>

LiteralType::LiteralType(NormalizedString content) {
  this->content = std::move(content);
  this->descriptorType = LiteralDescriptor::NONE;
}

LiteralType::LiteralType(NormalizedString content,
                         NormalizedString datatypeOrLanguageTag,
                         LiteralDescriptor type) {
  this->content = std::move(content);
  this->descriptorType = type;
  this->descriptorValue = std::move(datatypeOrLanguageTag);
}

bool LiteralType::hasLanguageTag() const {
  return this->descriptorType == LiteralDescriptor::LANGUAGE_TAG;
}

bool LiteralType::hasDatatype() const {
  return this->descriptorType == LiteralDescriptor::DATATYPE;
}

NormalizedStringView LiteralType::getContent() const { return this->content; }

NormalizedStringView LiteralType::getDatatype() const {
  if (!hasDatatype()) {
    AD_THROW("The literal does not have an explicit datatype.");
  }
  return this->descriptorValue;
}

NormalizedStringView LiteralType::getLanguageTag() const {
  if (!hasLanguageTag()) {
    AD_THROW("The literal does not have an explicit language tag.");
  }
  return this->descriptorValue;
}
