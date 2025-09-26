// Copyright 2024, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_GRAPHREF_H
#define QLEVER_SRC_PARSER_DATA_GRAPHREF_H

#include <variant>

#include "backports/three_way_comparison.h"
#include "rdfTypes/Iri.h"

using GraphRef = ad_utility::triple_component::Iri;
// Denotes the target graph for an operation. Here the target is the default
// graph.
struct DEFAULT {
  // For testing
  QL_DEFINE_CLASS_MEMBERS_AS_TIE()
  QL_DEFINE_EQUALITY_OPERATOR(DEFAULT)
};
// Denotes the target graphs for an operation. Here the target are all named
// graphs.
struct NAMED {
  // For testing
  QL_DEFINE_CLASS_MEMBERS_AS_TIE()
  QL_DEFINE_EQUALITY_OPERATOR(NAMED)
};
// Denotes the target graphs for an operation. Here the target are all graphs.
struct ALL {
  // For testing
  QL_DEFINE_CLASS_MEMBERS_AS_TIE()
  QL_DEFINE_EQUALITY_OPERATOR(ALL)
};

using GraphRefAll = std::variant<GraphRef, DEFAULT, NAMED, ALL>;
using GraphOrDefault = std::variant<GraphRef, DEFAULT>;

#endif  // QLEVER_SRC_PARSER_DATA_GRAPHREF_H
