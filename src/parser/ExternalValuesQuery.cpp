// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

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

// ____________________________________________________________________________
std::string ExternalValuesQuery::extractIdentifier(
    const std::string& serviceIri) {
  if (serviceIri == EXTERNAL_VALUES_IRI) {
    return "";
  }
  // Extract identifier from IRI like
  // <https://qlever.cs.uni-freiburg.de/external-values-myid>
  constexpr std::string_view prefix =
      "<https://qlever.cs.uni-freiburg.de/external-values-";
  constexpr std::string_view suffix = ">";

  AD_CONTRACT_CHECK(ql::starts_with(serviceIri, prefix),
                    "unexpected SERVICE IRI for `ExternalValuesQuery`");
  AD_CORRECTNESS_CHECK(ql::ends_with(serviceIri, suffix));

  // Extract the identifier between prefix and suffix.
  auto identifier = serviceIri.substr(
      prefix.size(), serviceIri.size() - prefix.size() - suffix.size());

  if (identifier.empty()) {
    throw ExternalValuesException(
        "The identifier of an `ExternalValuesQuery` must not be empty");
  }
  return identifier;
}

}  // namespace parsedQuery
