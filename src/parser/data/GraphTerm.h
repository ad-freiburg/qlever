// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <variant>

#include "./BlankNode.h"
#include "./Context.h"
#include "./Literal.h"

class GraphTerm : public std::variant<Literal, BlankNode> {
 public:
  using std::variant<Literal, BlankNode>::variant;

  [[nodiscard]] std::string toString(const Context& context) const {
    return std::visit(
        [&](const auto& object) { return object.toString(context); },
        static_cast<const std::variant<Literal, BlankNode>&>(*this));
  }
};
