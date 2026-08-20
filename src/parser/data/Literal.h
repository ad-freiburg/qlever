// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_LITERAL_H
#define QLEVER_SRC_PARSER_DATA_LITERAL_H

#include <optional>
#include <sstream>
#include <string>

#include "backports/three_way_comparison.h"
#include "backports/type_traits.h"
#include "util/Concepts.h"

class Literal {
  std::string _stringRepresentation;

  CPP_template_2(typename T)(
      requires ad_utility::Streamable<T>) static std::string
      toString(const T& t) {
    std::ostringstream stream;
    stream << t;
    return std::move(stream).str();
  }

  static std::string toString(bool boolean) {
    return boolean ? "true" : "false";
  }

 public:
  CPP_template_2(typename T)(
      requires CPP_NOT(ql::concepts::same_as<ql::remove_cvref_t<T>, Literal>)
          CPP_and_2 ad_utility::Streamable<T>) explicit Literal(T&& t)
      : _stringRepresentation(toString(std::forward<T>(t))) {}

  explicit Literal(std::variant<int64_t, double> t) {
    std::visit([this](auto& x) { _stringRepresentation = toString(x); }, t);
  }

  static_assert(!ad_utility::Streamable<Literal>,
                "If Literal satisfies the Streamable concept, copy and move "
                "constructors are hidden, leading to unexpected behaviour");

  // ___________________________________________________________________________
  // Used for testing
  const std::string& literal() const { return _stringRepresentation; }

  // ___________________________________________________________________________
  std::string toSparql() const { return _stringRepresentation; }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Literal, _stringRepresentation)
};

#endif  // QLEVER_SRC_PARSER_DATA_LITERAL_H
