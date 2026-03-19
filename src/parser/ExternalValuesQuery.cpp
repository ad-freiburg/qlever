// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Generated with Claude Code

#include "parser/ExternalValuesQuery.h"

#include <string_view>

#include "parser/MagicServiceIriConstants.h"
#include "parser/SparqlTriple.h"

namespace parsedQuery {

// ____________________________________________________________________________
void ExternalValuesQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  auto predString = extractParameterName(predicate, EXTERNAL_VALUES_IRI);

  if (predString == "variables") {
    if (!object.isVariable()) {
      throw ExternalValuesException(
          "The parameter <variables> expects a variable");
    }
    variables_.push_back(object.getVariable());
  } else if (predString == "identifier") {
    if (!identifier_.empty()) {
      throw ExternalValuesException(
          "The parameter <identifier> must not be set more than once");
    }
    if (!object.isLiteral()) {
      throw ExternalValuesException(
          "The parameter <identifier> expects a string literal");
    }
    identifier_ =
        std::string{asStringViewUnsafe(object.getLiteral().getContent())};
    if (identifier_.empty()) {
      throw ExternalValuesException(
          "The parameter <identifier> must be a non-empty string literal");
    }
  } else {
    throw ExternalValuesException(absl::StrCat(
        "Unknown parameter for external values query: <", predString, ">"));
  }
}

// ____________________________________________________________________________
void ExternalValuesQuery::validate() const {
  if (identifier_.empty()) {
    throw ExternalValuesException(
        "An external values query requires an <identifier> parameter");
  }
  if (variables_.empty()) {
    throw ExternalValuesException(
        "An external values query requires at least one <variables> parameter");
  }
}

}  // namespace parsedQuery
