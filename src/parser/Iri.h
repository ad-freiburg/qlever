// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <string>

#include "NormalizedString.h"

class Iri {

  // A class to hold IRIs. It does not contain the leading or trailing
  // angled bracket.
  //
  // E.g. For the input "<http://example.org/books/book1>",
  // only "http://example.org/books/book1" is to be stored in the iri variable.

 private:
  // Stores the string value of the IRI
  NormalizedString iri;

 public:
  // Created a new iri object
  explicit Iri(NormalizedString iri);

  // Returns the string value of the iri object
  [[nodiscard]] NormalizedStringView getContent() const;
};
