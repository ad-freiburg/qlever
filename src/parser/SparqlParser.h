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

// A simple parser of SPARQL.
// No supposed to feature the complete query language.
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
