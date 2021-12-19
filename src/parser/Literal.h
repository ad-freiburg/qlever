// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <sstream>


class Literal {
  const std::string _stringRepresentation;

  template <typename T>
  static std::string toString(const T& t) {
    std::ostringstream stream;
    stream << t;
    return stream.str();
  }

 public:
  template <typename T>
  explicit Literal(const T& t) : _stringRepresentation(toString(t))  {}

  [[nodiscard]] std::string toString() const {
    return _stringRepresentation;
  }
};
