// Copyright 2024, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#pragma once

#include <variant>

#include "parser/Iri.h"

using GraphRef = TripleComponent::Iri;
// Denotes the target graph for an operation. Here the target is the default
// graph.
struct DEFAULT {
  // For testing
  bool operator==(const DEFAULT&) const = default;
};
// Denotes the target graphs for an operation. Here the target are all named
// graphs.
struct NAMED {
  // For testing
  bool operator==(const NAMED&) const = default;
};
// Denotes the target graphs for an operation. Here the target are all graphs.
struct ALL {
  // For testing
  bool operator==(const ALL&) const = default;
};

using GraphRefAll = std::variant<GraphRef, DEFAULT, NAMED, ALL>;
using GraphOrDefault = std::variant<GraphRef, DEFAULT>;
