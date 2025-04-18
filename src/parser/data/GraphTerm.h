// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_PARSER_DATA_GRAPHTERM_H
#define QLEVER_SRC_PARSER_DATA_GRAPHTERM_H

#include <string>
#include <variant>

#include "parser/RdfParser.h"
#include "parser/TokenizerCtre.h"
#include "parser/data/BlankNode.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "parser/data/Iri.h"
#include "parser/data/Literal.h"
#include "util/VisitMixin.h"

using GraphTermBase = std::variant<Literal, BlankNode, Iri, Variable>;

class GraphTerm : public GraphTermBase,
                  public VisitMixin<GraphTerm, GraphTermBase> {
 public:
  using GraphTermBase::GraphTermBase;

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(
      const ConstructQueryExportContext& context, PositionInTriple role) const {
    // TODO<C++23>: Use std::visit when it is possible
    return visit(
        [&context, &role](const auto& object) -> std::optional<std::string> {
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
    return visit([](const auto& element) -> TripleComponent {
      using T = std::decay_t<decltype(element)>;
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
