// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <array>
#include <vector>

#include "./VarOrTerm.h"

namespace ad_utility::sparql_types {
using Objects = std::vector<VarOrTerm>;
using Tuples = std::vector<std::array<VarOrTerm, 2>>;
using Triples = std::vector<std::array<VarOrTerm, 3>>;
using Node = std::pair<VarOrTerm, Triples>;
using ObjectList = std::pair<Objects, Triples>;
using PropertyList = std::pair<Tuples, Triples>;
}  // namespace ad_utility::sparql_types
