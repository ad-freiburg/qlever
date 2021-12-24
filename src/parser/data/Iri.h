// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <utility>

#include "./Context.h"

class Iri {
  std::string _string;

 public:
  // TODO<Robin> check format
  explicit Iri(std::string str) : _string(std::move(str)) {}

  [[nodiscard]] std::optional<std::string> toString(
      [[maybe_unused]] const Context& context,
      [[maybe_unused]] ContextRole role) const {
    return _string;
  }
};