// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>

#include "parser/data/Context.h"

class Iri {
  std::string _string;

 public:
  explicit Iri(std::string str);

  // ___________________________________________________________________________
  // Used for testing
  [[nodiscard]] const std::string& iri() const { return _string; }

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(
      [[maybe_unused]] const Context& context,
      [[maybe_unused]] ContextRole role) const {
    return _string;
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const { return _string; }

  bool operator==(const Iri& other) const = default;
};
