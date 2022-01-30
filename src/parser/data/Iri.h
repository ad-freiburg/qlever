// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>

#include "./Context.h"
#include "ctre/ctre.h"

class Iri {
  std::string _string;

 public:
  explicit Iri(std::string str) : _string{std::move(str)} {
    AD_CHECK(ctre::match<"(?:@[a-zA-Z]+(?:-(?:[a-zA-Z]|\\d)+)*@)?"
                         "<[^<>\"{}|^\\\\`\\0- ]*>">(_string));
  }

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
};
