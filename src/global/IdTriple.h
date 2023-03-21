// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

// Should we have an own class for this? We need this at several places.
using IdTriple = std::array<Id, 3>;

// Hash value for such triple.
template <typename H>
H AbslHashValue(H h, const IdTriple& triple) {
  return H::combine(std::move(h), triple[0], triple[1], triple[2]);
}
