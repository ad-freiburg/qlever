// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <variant>

#include "../../util/VisitMixin.h"
#include "./BlankNode.h"
#include "./Context.h"
#include "./Iri.h"
#include "./Literal.h"

using GraphTermBase = std::variant<Literal, BlankNode, Iri>;

class GraphTerm : public GraphTermBase,
                  public VisitMixin<GraphTerm, GraphTermBase> {
 public:
  using GraphTermBase::GraphTermBase;

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(const Context& context,
                                                    ContextRole role) const {
    // TODO<C++23>: Use std::visit when it is possible
    return visit([&context, &role](const auto& object) {
      return object.evaluate(context, role);
    });
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const {
    // TODO<C++23>: Use std::visit when it is possible
    return visit([](const auto& object) { return object.toSparql(); });
  }
};
