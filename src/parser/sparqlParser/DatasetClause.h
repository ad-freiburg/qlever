//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include "parser/TripleComponent.h"

// A named or default graph
struct DatasetClause {
  TripleComponent::Iri dataset_;
  bool isNamed_;

  // For testing
  bool operator==(const DatasetClause& other) const = default;
};
