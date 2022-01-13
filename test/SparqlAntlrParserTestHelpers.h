// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <gmock/gmock.h>

#include "../src/parser/data/VarOrTerm.h"

template <class>
constexpr bool always_false_v = false;

std::ostream& operator<<(std::ostream& out, const GraphTerm& graphTerm) {
  std::visit(
      [&](const auto& object) {
        using T = std::decay_t<decltype(object)>;
        if constexpr (std::is_same_v<T, Literal>) {
          out << "Literal " << object.getLiteral();
        } else if constexpr (std::is_same_v<T, BlankNode>) {
          out << "BlankNode " << object.getBlankNode();
        } else if constexpr (std::is_same_v<T, Iri>) {
          out << "Iri " << object.getIri();
        } else {
          static_assert(always_false_v<T>, "unknown type");
        }
      },
      static_cast<const GraphTermBase&>(graphTerm));
  return out;
}

std::ostream& operator<<(std::ostream& out, const VarOrTerm& varOrTerm) {
  std::visit(
      [&](const auto& object) {
        using T = std::decay_t<decltype(object)>;
        if constexpr (std::is_same_v<T, GraphTerm>) {
          out << object;
        } else if constexpr (std::is_same_v<T, Variable>) {
          out << "Variable " << object.getName();
        } else {
          static_assert(always_false_v<T>, "unknown type");
        }
      },
      static_cast<const VarOrTermBase&>(varOrTerm));
  return out;
}

MATCHER_P(IsIri, value, "") {
  if (const auto graphTerm = std::get_if<GraphTerm>(&arg)) {
    if (const auto iri = std::get_if<Iri>(graphTerm)) {
      return iri->getIri() == value;
    }
  }
  return false;
}

MATCHER_P(IsBlankNode, value, "") {
  if (const auto graphTerm = std::get_if<GraphTerm>(&arg)) {
    if (const auto blankNode = std::get_if<BlankNode>(graphTerm)) {
      return blankNode->getBlankNode() == value;
    }
  }
  return false;
}

MATCHER_P(IsVariable, value, "") {
  if (const auto variable = std::get_if<Variable>(&arg)) {
    return variable->getName() == value;
  }
  return false;
}
