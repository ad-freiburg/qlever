// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <sstream>
#include <string>

namespace {
/**
 * A concept to ensure objects can be formatted by std::ostream.
 * @tparam T The Type to be formatted
 */
template <typename T>
concept Streamable = requires(T x, std::ostream& os) {
  os << x;
};
};  // namespace

class Literal {
  std::string _stringRepresentation;

  template <Streamable T>
  static std::string toString(const T& t) {
    std::ostringstream stream;
    stream << t;
    return stream.str();
  }

  static std::string toString(bool boolean) {
    return boolean ? "true" : "false";
  }

 public:
  template <Streamable T>
  explicit Literal(T&& t)
      : _stringRepresentation(toString(std::forward<T>(t))) {}

  static_assert(!Streamable<Literal>,
                "If Literal satisfies the Streamable concept, copy and move "
                "constructors are hidden, leading to unexpected behaviour");

  // ___________________________________________________________________________
  // Used for testing
  [[nodiscard]] const std::string& literal() const {
    return _stringRepresentation;
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(
      [[maybe_unused]] const Context& context, ContextRole role) const {
    if (role == OBJECT) {
      return _stringRepresentation;
    }
    return std::nullopt;
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const { return _stringRepresentation; }
};
