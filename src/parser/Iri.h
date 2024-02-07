// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <string_view>

#include "parser/NormalizedString.h"

namespace ad_utility::triple_component {

// A class to hold IRIs. It does not store the leading or trailing
// angled bracket.
//
// E.g. For the input "<http://example.org/books/book1>",
// only "http://example.org/books/book1" is to be stored in the iri_ variable.
class Iri {
 private:
  // Store the string value of the IRI without any leading or trailing angled
  // brackets.
  NormalizedString iri_;

  // Create a new iri object
  explicit Iri(NormalizedString iri);

  // Create a new iri using a prefix
  Iri(const Iri& prefix, NormalizedStringView suffix);

 public:
  // Create a new iri given an iri with brackets
  static Iri iriref(std::string_view stringWithBrackets);

  // Create a new iri given a prefix iri and its suffix
  static Iri prefixed(const Iri& prefix, std::string_view suffix);

  // Return the string value of the iri object without any leading or trailing
  // angled brackets.
  NormalizedStringView getContent() const;
};

}  // namespace ad_utility::triple_component
