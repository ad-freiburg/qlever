// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <sstream>
#include <string>

class Literal {
  std::string _stringRepresentation;

  template <typename T>
  static std::string toString(const T& t) {
    std::ostringstream stream;
    stream << t;
    return stream.str();
  }

 public:
  explicit Literal(auto&& t)
      : _stringRepresentation(toString(std::forward<decltype(t)>(t))) {}

  [[nodiscard]] std::optional<std::string> toString(
      [[maybe_unused]] const Context& context, ContextRole role) const {
    if (role == OBJECT) {
      return _stringRepresentation;
    }
    return std::nullopt;
  }

  [[nodiscard]] std::string toString() const { return _stringRepresentation; }
};
