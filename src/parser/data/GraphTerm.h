// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_GRAPHTERM_H
#define QLEVER_SRC_PARSER_DATA_GRAPHTERM_H

#include <string>
#include <variant>

#include "../../util/VisitMixin.h"
#include "./BlankNode.h"
#include "./ConstructQueryExportContext.h"
#include "./Iri.h"
#include "./Literal.h"

using GraphTermBase = std::variant<Literal, BlankNode, Iri, Variable>;

class GraphTerm : public GraphTermBase,
                  public VisitMixin<GraphTerm, GraphTermBase> {
 public:
  using GraphTermBase::GraphTermBase;

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(
      const ConstructQueryExportContext& context, PositionInTriple role) const {
    // TODO<C++23>: Use std::visit when it is possible
    return visit([&context, &role]<typename T>(
                     const T& object) -> std::optional<std::string> {
      return object.evaluate(context, role);
    });
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const {
    return visit(
        [](const auto& object) -> std::string { return object.toSparql(); });
  }
};

#endif  // QLEVER_SRC_PARSER_DATA_GRAPHTERM_H
