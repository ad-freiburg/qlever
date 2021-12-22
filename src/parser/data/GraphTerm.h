// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <variant>

#include "./BlankNode.h"
#include "./Context.h"
#include "./Iri.h"
#include "./Literal.h"

using GraphTermBase = std::variant<Literal, BlankNode, Iri>;

class GraphTerm : public GraphTermBase {
 public:
  using GraphTermBase::GraphTermBase;

  [[nodiscard]] std::string toString(const Context& context) const {
    return std::visit(
        [&](const auto& object) { return object.toString(context); },
        static_cast<const GraphTermBase&>(*this));
  }
};
