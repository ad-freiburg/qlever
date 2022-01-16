// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <gmock/gmock.h>

#include "../src/parser/data/VarOrTerm.h"
#include "../src/util/TypeTraits.h"

// Not relevant for the actual test logic, but provides
// human-readable output if a test fails.
std::ostream& operator<<(std::ostream& out, const GraphTerm& graphTerm) {
  std::visit(
      [&]<typename T>(const T& object) {
        if constexpr (ad_utility::isSimilar<T, Literal>) {
          out << "Literal " << object.literal();
        } else if constexpr (ad_utility::isSimilar<T, BlankNode>) {
          out << "BlankNode generated: " << object.generated()
              << ", label: " << object.label();
        } else if constexpr (ad_utility::isSimilar<T, Iri>) {
          out << "Iri " << object.iri();
        } else {
          static_assert(ad_utility::alwaysFalse<T>, "unknown type");
        }
      },
      static_cast<const GraphTermBase&>(graphTerm));
  return out;
}

// _____________________________________________________________________________

std::ostream& operator<<(std::ostream& out, const VarOrTerm& varOrTerm) {
  std::visit(
      [&]<typename T>(const T& object) {
        if constexpr (ad_utility::isSimilar<T, GraphTerm>) {
          out << object;
        } else if constexpr (ad_utility::isSimilar<T, Variable>) {
          out << "Variable " << object.name();
        } else {
          static_assert(ad_utility::alwaysFalse<T>, "unknown type");
        }
      },
      static_cast<const VarOrTermBase&>(varOrTerm));
  return out;
}

// _____________________________________________________________________________

// Potentially unwrap VarOrTerm to a GraphTerm object, or return a pointer
// to the argument directly if it is already unwrapped.

constexpr auto unwrapGraphTerm(const auto& arg) {
  if constexpr (ad_utility::isSimilar<decltype(arg), VarOrTerm>) {
    return std::get_if<GraphTerm>(&arg);
  } else if constexpr (ad_utility::isSimilar<decltype(arg), GraphTerm>) {
    return &arg;
  } else {
    static_assert(ad_utility::alwaysFalse<decltype(arg)>, "unknown type");
  }
}

// _____________________________________________________________________________

MATCHER_P(IsIri, value, "") {
  if (const auto graphTerm = unwrapGraphTerm(arg)) {
    if (const auto iri = std::get_if<Iri>(graphTerm)) {
      return iri->iri() == value;
    }
  }
  return false;
}

// _____________________________________________________________________________

MATCHER_P2(IsBlankNode, generated, label, "") {
  if (const auto graphTerm = unwrapGraphTerm(arg)) {
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

// _____________________________________________________________________________

MATCHER_P(IsLiteral, value, "") {
  if (const auto graphTerm = unwrapGraphTerm(arg)) {
    if (const auto literal = std::get_if<Literal>(graphTerm)) {
      return literal->literal() == value;
    }
  }
  return false;
}
