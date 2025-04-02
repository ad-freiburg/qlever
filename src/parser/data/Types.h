// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_TYPES_H
#define QLEVER_SRC_PARSER_DATA_TYPES_H

#include <array>
#include <tuple>
#include <vector>

#include "parser/Alias.h"
#include "parser/PropertyPath.h"
#include "parser/data/GraphTerm.h"

namespace ad_utility::sparql_types {
// In the following, `Path` stands for `property path` (which can also be a
// single predicate). More precisely, in a type that has `Path` in its name, all
// the stored predicates can be property paths. This is in accordance with the
// terminology of the SPARQL 1.1 grammar. The types without `Path` are used in
// the templates of CONSTRUCT queries where no property paths are allowed.
using Objects = std::vector<GraphTerm>;
using PredicateObjectPairs = std::vector<std::array<GraphTerm, 2>>;
using Triples = std::vector<std::array<GraphTerm, 3>>;
using PredicateObjectPairsAndTriples = std::pair<PredicateObjectPairs, Triples>;
using ObjectsAndTriples = std::pair<Objects, Triples>;
using SubjectOrObjectAndTriples = std::pair<GraphTerm, Triples>;

using VarOrPath = std::variant<Variable, PropertyPath>;
using VarOrIri = std::variant<Variable, ad_utility::triple_component::Iri>;
using PathObjectPair = std::pair<VarOrPath, GraphTerm>;
using PathObjectPairs = std::vector<PathObjectPair>;
struct TripleWithPropertyPath {
  GraphTerm subject_;
  VarOrPath predicate_;
  GraphTerm object_;

  bool operator==(const TripleWithPropertyPath&) const = default;
};
using PathObjectPairsAndTriples =
    std::pair<PathObjectPairs, std::vector<TripleWithPropertyPath>>;
using SubjectOrObjectAndPathTriples =
    std::pair<GraphTerm, std::vector<TripleWithPropertyPath>>;
using ObjectsAndPathTriples =
    std::pair<Objects, std::vector<TripleWithPropertyPath>>;

using VarOrAlias = std::variant<Variable, Alias>;
}  // namespace ad_utility::sparql_types

#endif  // QLEVER_SRC_PARSER_DATA_TYPES_H
