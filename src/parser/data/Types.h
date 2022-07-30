// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <array>
#include <tuple>
#include <vector>

#include "../PropertyPath.h"
#include "./VarOrTerm.h"

namespace ad_utility::sparql_types {
using Objects = std::vector<VarOrTerm>;
using Tuples = std::vector<std::array<VarOrTerm, 2>>;
using VarOrPath = std::variant<Variable, PropertyPath>;
using PredicateAndObject = std::pair<VarOrPath, VarOrTerm>;
using PathTuples = std::vector<PredicateAndObject>;
using Triples = std::vector<std::array<VarOrTerm, 3>>;
struct TripleWithPropertyPath {
  VarOrTerm subject_;
  VarOrPath predicate_;
  VarOrTerm object_;

  bool operator==(const TripleWithPropertyPath&) const = default;
};
using Node = std::pair<VarOrTerm, Triples>;
using ObjectList = std::pair<Objects, Triples>;
using PropertyList = std::pair<Tuples, Triples>;
}  // namespace ad_utility::sparql_types
