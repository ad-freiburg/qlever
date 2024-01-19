// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <string>

#include "NormalizedString.h"

class IriType {
 private:
  // TODO Should we store the base and the prefix separately as string_view to
  // save memory?

  // Stores the string value of the IRI
  NormalizedString iri;

 public:
  // Created a new iri object
  explicit IriType(NormalizedString iri);

  // Returns the string value of the iri object
  [[nodiscard]] NormalizedStringView getIri() const;
};
