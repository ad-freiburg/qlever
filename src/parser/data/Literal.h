// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <sstream>
#include <string>

#include "util/Concepts.h"

class Literal {
  std::string _stringRepresentation;

  template <ad_utility::Streamable T>
  static std::string toString(const T& t) {
    std::ostringstream stream;
    stream << t;
    return std::move(stream).str();
  }

  static std::string toString(bool boolean) {
    return boolean ? "true" : "false";
  }

 public:
  template <typename T>
  requires(!std::same_as<std::remove_cvref_t<T>, Literal> &&
           ad_utility::Streamable<T>) explicit Literal(T&& t)
      : _stringRepresentation(toString(std::forward<T>(t))) {}

  explicit Literal(std::variant<int64_t, double> t) {
    std::visit([this](auto& x) { _stringRepresentation = toString(x); }, t);
  }

  static_assert(!ad_utility::Streamable<Literal>,
                "If Literal satisfies the Streamable concept, copy and move "
                "constructors are hidden, leading to unexpected behaviour");

  // ___________________________________________________________________________
  // Used for testing
  [[nodiscard]] const std::string& literal() const {
    return _stringRepresentation;
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(
      [[maybe_unused]] const ConstructQueryExportContext& context,
      PositionInTriple role) const {
    if (role == PositionInTriple::OBJECT) {
      return _stringRepresentation;
    }
    return std::nullopt;
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const { return _stringRepresentation; }

  bool operator==(const Literal& other) const = default;
};
