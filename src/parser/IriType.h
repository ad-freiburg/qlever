// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors:
//   2023 -    Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <string>

class IriType {
 private:
  // TODO Should we store the base and the prefix separately as string_view to
  // save memory?

  // Stores the string value of the IRI
  std::string iri;

 public:
  // Created a new iri object
  IriType(std::string iri);

  // Returns the string value of the iri object
  std::string_view getIri() const;
};
