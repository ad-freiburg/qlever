// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Herrmann <johannes.r.herrmann(at)gmail.com>
//          Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "parser/MagicServiceQuery.h"

#include "parser/GraphPatternOperation.h"

namespace parsedQuery {

// ____________________________________________________________________________
void MagicServiceQuery::addBasicPattern(const BasicGraphPattern& pattern) {
  for (SparqlTriple triple : pattern._triples) {
    addParameter(triple);
  }
}

// ____________________________________________________________________________
void MagicServiceQuery::addGraph(const GraphPatternOperation& op) {
  if (childGraphPattern_._graphPatterns.empty()) {
    auto pattern = std::get<parsedQuery::GroupGraphPattern>(op);
    childGraphPattern_ = std::move(pattern._child);
  }
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

}  // namespace parsedQuery
