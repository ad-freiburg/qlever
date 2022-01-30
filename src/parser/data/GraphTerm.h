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

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(const Context& context,
                                                    ContextRole role) const {
    // TODO<C++23>: remove static_cast as soon as we can visit types that
    // inherit from std::variant
    return std::visit(
        [&](const auto& object) { return object.evaluate(context, role); },
        static_cast<const GraphTermBase&>(*this));
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const {
    // TODO<C++23>: remove static_cast as soon as we can visit types that
    // inherit from std::variant
    return std::visit([&](const auto& object) { return object.toSparql(); },
                      static_cast<const GraphTermBase&>(*this));
  }
};
