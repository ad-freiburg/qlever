// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <string>

#include "NormalizedString.h"

class Iri {
  // A class to hold IRIs. It does not store the leading or trailing
  // angled bracket.
  //
  // E.g. For the input "<http://example.org/books/book1>",
  // only "http://example.org/books/book1" is to be stored in the iri variable.

 private:
  // Store the string value of the IRI
  NormalizedString iri_;

 public:
  // Create a new iri object
  explicit Iri(NormalizedString iri);

  // Return the string value of the iri object
  [[nodiscard]] NormalizedStringView getContent() const;
};
