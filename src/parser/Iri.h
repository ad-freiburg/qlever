// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <string_view>

#include "parser/NormalizedString.h"

namespace ad_utility::triple_component {

// A class to hold IRIs.
class Iri {
 private:
  // Store the string value of the IRI including the angle brackets.
  // brackets.
  std::string iri_;

  // Create a new iri object
  explicit Iri(std::string iri);

  // Create a new iri using a prefix
  Iri(const Iri& prefix, NormalizedStringView suffix);

 public:
  // A default constructed IRI is empty.
  Iri() = default;
  template <typename H>
  friend H AbslHashValue(H h, const std::same_as<Iri> auto& iri) {
    return H::combine(std::move(h), iri.iri_);
  }
  bool operator==(const Iri&) const = default;
  static Iri fromStringRepresentation(std::string s);

  const std::string& toStringRepresentation() const;
  std::string& toStringRepresentation();

  // Create a new iri given an iri with brackets
  static Iri fromIriref(std::string_view stringWithBrackets);

  // Create a new iri given a prefix iri and its suffix
  static Iri fromPrefixAndSuffix(const Iri& prefix, std::string_view suffix);

  // Return the string value of the iri object without any leading or trailing
  // angled brackets.
  NormalizedStringView getContent() const;
};

}  // namespace ad_utility::triple_component
