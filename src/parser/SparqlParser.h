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
  static ParsedQuery parseQuery(std::string query);
  // A convenience function for parsing the query and setting the datasets.
  static ParsedQuery parseQuery(std::string operation,
                                const std::vector<DatasetClause>& datasets);
};
