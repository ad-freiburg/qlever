// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include "NormalizedString.h"

enum class LiteralDescriptor { NONE, LANGUAGE_TAG, DATATYPE };

class Literal {
 private:
  // Store the string value of the literal
  NormalizedString content_;

  // Store the optional language tag or the optional datatype if applicable
  NormalizedString descriptorValue_;

  // Store information if the literal has a language tag, a datatype, or non of
  // these two assigned to it
  LiteralDescriptor descriptorType_;

 public:
  // Create a new literal without any descriptor
  explicit Literal(NormalizedString content);

  // Create a new literal with the given descriptor
  Literal(NormalizedString content, NormalizedString datatypeOrLanguageTag,
          LiteralDescriptor type);

  // Return true if the literal has an assigned language tag
  bool hasLanguageTag() const;

  // Return true if the literal has an assigned datatype
  bool hasDatatype() const;

  // Return the value of the literal, without any datatype or language tag
  NormalizedStringView getContent() const;

  // Return the language tag of the literal if available.
  // Throws an exception if the literal has no language tag.
  NormalizedStringView getLanguageTag() const;

  // Return the datatype of the literal if available.
  // Throws an exception if the literal has no datatype.
  NormalizedStringView getDatatype() const;
};
