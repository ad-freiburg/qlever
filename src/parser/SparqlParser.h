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
  // `datasets` are fixed datasets as per the SPARQL protocol. These cannot be
  // overwritten from inside the query (using `FROM`) or update (using `USING`).
  // Passing no datasets means that the datasets are set normally from the
  // query or update.
  static ParsedQuery parseQuery(
      std::string queryOrUpdate,
      const std::vector<DatasetClause>& datasets = {});
};
