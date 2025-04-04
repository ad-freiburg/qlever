//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_SPARQLPARSER_DATASETCLAUSE_H
#define QLEVER_SRC_PARSER_SPARQLPARSER_DATASETCLAUSE_H

#include "parser/TripleComponent.h"

// A named or default graph
struct DatasetClause {
  TripleComponent::Iri dataset_;
  bool isNamed_;

  // For testing
  bool operator==(const DatasetClause& other) const = default;
};

#endif  // QLEVER_SRC_PARSER_SPARQLPARSER_DATASETCLAUSE_H
