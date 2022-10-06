// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>

#include "parser/ParsedQuery.h"

using std::string;

// The SPARQL parser used by QLever. The actual parsing is delegated to a parser
// that is based on ANTLR4, which recognises the complete SPARQL 1.1 QL grammar.
// For valid SPARQL queries that are not supported by QLever a reasonable error
// message is given. The only thing that is still parsed manually by this class
// are FILTER clauses, because QLever doesn't support arbitrary expressions in
// filters yet.
class SparqlParser {
 public:
  static ParsedQuery parseQuery(std::string query);
};
