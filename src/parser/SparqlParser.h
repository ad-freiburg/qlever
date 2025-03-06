// Copyright 2022, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)
#pragma once

#include <string>

#include "parser/ParsedQuery.h"

// The SPARQL parser used by QLever. The actual parsing is delegated to a parser
// that is based on ANTLR4, which recognises the complete SPARQL 1.1 QL grammar.
// For valid SPARQL queries that are not supported by QLever a reasonable error
// message is given.
class SparqlParser {
 public:
  // `datasets` are the fixed datasets as per the SPARQL protocol. Passing no
  // datasets means that they are not fixed.
  static ParsedQuery parseQuery(std::string operation,
                                std::vector<DatasetClause> datasets = {});
};
