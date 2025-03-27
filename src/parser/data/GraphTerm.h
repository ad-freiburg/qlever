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
#include "parser/RdfParser.h"

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

  // ___________________________________________________________________________
  // Constructs a TripleComponent from the GraphTerm.
  [[nodiscard]] TripleComponent toTripleComponent() const {
    return visit([]<typename T>(const T& element) -> TripleComponent {
      if constexpr (std::is_same_v<T, Variable>) {
        return element;
      } else if constexpr (std::is_same_v<T, Literal> ||
                           std::is_same_v<T, Iri>) {
        return RdfStringParser<TurtleParser<TokenizerCtre>>::parseTripleObject(
            element.toSparql());
      } else {
        return element.toSparql();
      }
    });
  }
};

#endif  // QLEVER_SRC_PARSER_DATA_GRAPHTERM_H
