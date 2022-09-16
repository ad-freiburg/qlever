// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>

#include "../engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "../util/HashMap.h"
#include "./TripleComponent.h"
#include "ParsedQuery.h"
#include "SparqlLexer.h"
#include "SparqlParserHelpers.h"
#include "sparqlParser/SparqlQleverVisitor.h"

using std::string;

// The SPARQL parser used by QLever. The actual parsing is delegated to a parser
// that is based on ANTLR4, which recognises the complete SPARQL 1.1 QL grammar.
// For valid SPARQL queries that are not supported by QLever a reasonable error
// message is given. The only thing that is still parsed manually by this class
// are FILTER clauses, because QLever doesn't support arbitrary expressions in
// filters yet.
class SparqlParser {
 public:
  explicit SparqlParser(const string& query);
  static ParsedQuery parseQuery(std::string query);

  /// Parse the expression of a filter statement (without the `FILTER` keyword).
  /// This helper method is needed as long as the set of expressions supported
  /// by FILTER and BIND/GROUP BY is not the same.
  static SparqlFilter parseFilterExpression(
      const string& filterContent,
      const SparqlQleverVisitor::PrefixMap& prefixMap);

 private:
  // Returns true if it found a filter
  std::optional<SparqlFilter> parseFilter(bool failOnNoFilter = true);

  SparqlLexer lexer_;
  string query_;
  SparqlFilter parseRegexFilter(bool expectKeyword);
};
