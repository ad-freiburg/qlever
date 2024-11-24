// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Herrmann <johannes.r.herrmann(at)gmail.com>
//          Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "parser/MagicServiceQuery.h"

#include <string_view>

#include "parser/GraphPatternOperation.h"
#include "util/Exception.h"

namespace parsedQuery {

// ____________________________________________________________________________
void MagicServiceQuery::addBasicPattern(const BasicGraphPattern& pattern) {
  for (SparqlTriple triple : pattern._triples) {
    addParameter(triple);
  }
}

// ____________________________________________________________________________
void MagicServiceQuery::addGraph(const GraphPatternOperation& op) {
  if (childGraphPattern_.has_value()) {
    throw MagicServiceException(
        "A magic SERVICE query must not contain more than one graph pattern.");
  }
  auto pattern = std::get<parsedQuery::GroupGraphPattern>(op);
  childGraphPattern_ = std::move(pattern._child);
}

// ____________________________________________________________________________
Variable MagicServiceQuery::getVariable(std::string_view parameter,
                                        const TripleComponent& object) const {
  if (!object.isVariable()) {
    throw MagicServiceException(absl::StrCat("The value ", object.toString(),
                                             " for parameter <", parameter,
                                             "> has to be a variable"));
  }

  return object.getVariable();
};

// ____________________________________________________________________________
void MagicServiceQuery::setVariable(std::string_view parameter,
                                    const TripleComponent& object,
                                    std::optional<Variable>& existingValue) {
  auto variable = getVariable(parameter, object);

  if (existingValue.has_value()) {
    throw MagicServiceException(absl::StrCat(
        "The parameter <", parameter, "> has already been set to variable: '",
        existingValue.value().toSparql(), "'. New variable: '",
        object.toString(), "'."));
  }

  existingValue = object.getVariable();
};

// ____________________________________________________________________________
std::string MagicServiceQuery::extractParameterName(
    const TripleComponent& tripleComponent,
    const std::string_view& magicIRI) const {
  if (!tripleComponent.isIri()) {
    throw MagicServiceException("Parameters must be IRIs");
  }

  // Get IRI without brackets
  std::string_view iri = magicIRI.substr(0, magicIRI.size() - 1);

  // Remove prefix and brackets
  std::string paramString = tripleComponent.getIri().toStringRepresentation();
  if (paramString.starts_with(iri)) {
    paramString = paramString.substr(iri.size(), paramString.size() - 1);
  } else {
    paramString = paramString.substr(1);
  }
  return paramString.substr(0, paramString.size() - 1);
};

}  // namespace parsedQuery
