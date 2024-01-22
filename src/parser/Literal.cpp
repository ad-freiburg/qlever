// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "Literal.h"

#include <utility>

Literal::Literal(NormalizedString content) : content_{std::move(content)}, descriptorType_{LiteralDescriptor::NONE} {}

Literal::Literal(NormalizedString content,
                         NormalizedString datatypeOrLanguageTag,
                         LiteralDescriptor type) : content_{std::move(content)}, descriptorValue_{std::move(datatypeOrLanguageTag)}, descriptorType_{type} {}

//
bool Literal::hasLanguageTag() const {
  return descriptorType_ == LiteralDescriptor::LANGUAGE_TAG;
}

//
bool Literal::hasDatatype() const {
  return descriptorType_ == LiteralDescriptor::DATATYPE;
}

//
NormalizedStringView Literal::getContent() const { return content_; }

//
NormalizedStringView Literal::getDatatype() const {
  if (!hasDatatype()) {
    AD_THROW("The literal does not have an explicit datatype.");
  }
  return this->descriptorValue_;
}

NormalizedStringView Literal::getLanguageTag() const {
  if (!hasLanguageTag()) {
    AD_THROW("The literal does not have an explicit language tag.");
  }
  return this->descriptorValue_;
}
