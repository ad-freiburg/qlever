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
  static std::vector<ParsedQuery> parseUpdate(std::string update);
  // Convenience functions for parsing the operation and setting the datasets.
  static ParsedQuery parseQuery(std::string query,
                                const std::vector<DatasetClause>& datasets);
  static std::vector<ParsedQuery> parseUpdate(
      std::string update, const std::vector<DatasetClause>& datasets);
};
