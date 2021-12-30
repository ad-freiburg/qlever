// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <utility>

#include "./Context.h"

class Iri {
  static const std::regex iri_check;
  std::string _string;

 public:
  explicit Iri(std::string str) : _string{std::move(str)} {
    AD_CHECK(std::regex_match(_string, iri_check));
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> toString(
      [[maybe_unused]] const Context& context,
      [[maybe_unused]] ContextRole role) const {
    return _string;
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toString() const { return _string; }
};

inline const std::regex Iri::iri_check{
    "(?:@[a-zA-Z]+(?:-(?:[a-zA-Z]|\\d)+)*@)?(?:<.+>|[^:]+:[^:]+)"};
