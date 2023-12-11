// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "LiteralType.h"

LiteralType::LiteralType(std::string content) {
  this->content = content;
  this->descriptorType = LiteralDescriptor::NONE;
}

LiteralType::LiteralType(std::string content, std::string datatypeOrLanguageTag,
                         LiteralDescriptor type) {
  this->content = content;
  this->descriptorType = type;
  this->descriptorValue = datatypeOrLanguageTag;
}

bool LiteralType::hasLanguageTag() const {
  return this->descriptorType == LiteralDescriptor::LANGUAGE_TAG;
}

bool LiteralType::hasDatatype() const {
  return this->descriptorType == LiteralDescriptor::DATATYPE;
}

std::string_view LiteralType::getContent() const { return this->content; }

std::string_view LiteralType::getDatatype() const {
  if (!hasDatatype()) {
    AD_THROW("The literal does not have an explicit datatype.");
  }
  return this->descriptorValue;
}

std::string_view LiteralType::getLanguageTag() const {
  if (!hasLanguageTag()) {
    AD_THROW("The literal does not have an explicit language tag.");
  }
  return this->descriptorValue;
}
