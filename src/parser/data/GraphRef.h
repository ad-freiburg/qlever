// Copyright 2024, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#pragma once

#include <variant>

#include "parser/Iri.h"

using GraphRef = TripleComponent::Iri;
struct DEFAULT {
  bool operator==(const DEFAULT&) const = default;
};
struct NAMED {
  bool operator==(const NAMED&) const = default;
};
struct ALL {
  bool operator==(const ALL&) const = default;
};

using GraphRefAll = std::variant<GraphRef, DEFAULT, NAMED, ALL>;
using GraphOrDefault = std::variant<GraphRef, DEFAULT>;
