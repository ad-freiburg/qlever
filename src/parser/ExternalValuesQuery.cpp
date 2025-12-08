// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Generated with Claude Code

#include "parser/ExternalValuesQuery.h"

#include <string_view>

#include "parser/MagicServiceIriConstants.h"
#include "parser/SparqlTriple.h"
#include "util/StringUtils.h"

namespace parsedQuery {

// ____________________________________________________________________________
void ExternalValuesQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  // For external values, we expect a predicate like <variables>
  // and the object should be a variable
  // We use a generic IRI as the magic service base since the actual IRI
  // includes the identifier
  std::string_view magicIri = "https://qlever.cs.uni-freiburg.de";
  auto predString = extractParameterName(predicate, magicIri);

  if (predString == "variables" || predString == "<variables>") {
    // The object should be a variable
    if (!object.isVariable()) {
      throw ExternalValuesException(
          "The parameter <variables> expects a variable");
    }
    variables_.push_back(object.getVariable());
  } else {
    throw ExternalValuesException(absl::StrCat(
        "Unknown parameter for external values query: <", predString, ">"));
  }
}

// ____________________________________________________________________________
std::string ExternalValuesQuery::extractIdentifier(
    const std::string& serviceIri) {
  // Extract identifier from IRI like
  // <https://qlever.cs.uni-freiburg.de/external-values-myid>
  constexpr std::string_view prefix =
      "<https://qlever.cs.uni-freiburg.de/external-values-";
  constexpr std::string_view suffix = ">";

  if (!ql::starts_with(serviceIri, prefix)) {
    throw ExternalValuesException(absl::StrCat(
        "External values service IRI must start with '", prefix, "' but got: ",
        serviceIri));
  }

  if (!ql::ends_with(serviceIri, suffix)) {
    throw ExternalValuesException(
        absl::StrCat("External values service IRI must end with '", suffix,
                     "' but got: ", serviceIri));
  }

  // Extract the identifier between prefix and suffix
  std::string identifier = serviceIri.substr(
      prefix.size(), serviceIri.size() - prefix.size() - suffix.size());

  if (identifier.empty()) {
    throw ExternalValuesException(
        "External values service IRI must contain a non-empty identifier");
  }

  return identifier;
}

}  // namespace parsedQuery
