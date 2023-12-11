// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <string>

enum LiteralDescriptor { NONE, LANGUAGE_TAG, DATATYPE };

class LiteralType {
 private:
  // Stores the string value of the literal
  std::string content;
  // Stores the optional language tag or the optional datatype if applicable
  std::string descriptorValue;
  // Stores information if the literal has a language tag, a datatype, or non of
  // these two assigned to it
  LiteralDescriptor descriptorType;

 public:
  // Creates a new literal without any descriptor
  LiteralType(std::string content);

  // Created a new literal with the given descriptor
  LiteralType(std::string content, std::string datatypeOrLanguageTag,
              LiteralDescriptor type);

  // Returns true if the literal has an assigned language tag
  bool hasLanguageTag() const;

  // Returns true if the literal has an assigned datatype
  bool hasDatatype() const;

  // Returns the value of the literal, without any datatype or language tag
  std::string_view getContent() const;

  // Returns the language tag of the literal if available.
  // Throws an exception if the literal has no language tag.
  std::string_view getLanguageTag() const;

  // Returns the datatype of the literal if available.
  // Throws an exception if the literal has no datatype.
  std::string_view getDatatype() const;
};
