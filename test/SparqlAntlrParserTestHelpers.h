// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <gmock/gmock.h>

#include "../src/parser/data/VarOrTerm.h"

template <class>
constexpr bool always_false_v = false;

// Not relevant for the actual test logic, but provides
// human-readable output if a test fails.
std::ostream& operator<<(std::ostream& out, const GraphTerm& graphTerm) {
  std::visit(
      [&](const auto& object) {
        using T = std::decay_t<decltype(object)>;
        if constexpr (std::is_same_v<T, Literal>) {
          out << "Literal " << object.literal();
        } else if constexpr (std::is_same_v<T, BlankNode>) {
          out << "BlankNode generated: " << object.generated()
              << ", label: " << object.label();
        } else if constexpr (std::is_same_v<T, Iri>) {
          out << "Iri " << object.iri();
        } else {
          static_assert(always_false_v<T>, "unknown type");
        }
      },
      static_cast<const GraphTermBase&>(graphTerm));
  return out;
}

// _____________________________________________________________________________

std::ostream& operator<<(std::ostream& out, const VarOrTerm& varOrTerm) {
  std::visit(
      [&](const auto& object) {
        using T = std::decay_t<decltype(object)>;
        if constexpr (std::is_same_v<T, GraphTerm>) {
          out << object;
        } else if constexpr (std::is_same_v<T, Variable>) {
          out << "Variable " << object.name();
        } else {
          static_assert(always_false_v<T>, "unknown type");
        }
      },
      static_cast<const VarOrTermBase&>(varOrTerm));
  return out;
}

// _____________________________________________________________________________

MATCHER_P(IsIri, value, "") {
  if (const auto graphTerm = std::get_if<GraphTerm>(&arg)) {
    if (const auto iri = std::get_if<Iri>(graphTerm)) {
      return iri->iri() == value;
    }
  }
  return false;
}

// _____________________________________________________________________________

MATCHER_P2(IsBlankNode, generated, label, "") {
  if (const auto graphTerm = std::get_if<GraphTerm>(&arg)) {
    if (const auto blankNode = std::get_if<BlankNode>(graphTerm)) {
      return blankNode->generated() == generated && blankNode->label() == label;
    }
  }
  return false;
}

// _____________________________________________________________________________

MATCHER_P(IsVariable, value, "") {
  if (const auto variable = std::get_if<Variable>(&arg)) {
    return variable->name() == value;
  }
  return false;
}
