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

enum QueryType { CONSTRUCT_QUERY, SELECT_QUERY };

// A simple parser of SPARQL.
// No supposed to feature the complete query language.
class SparqlParser {
 public:
  explicit SparqlParser(const string& query);
  ParsedQuery parse();

  /// Parse the expression of a filter statement (without the `FILTER` keyword).
  /// This helper method is needed as long as the set of expressions supported
  /// by FILTER and BIND/GROUP BY is not the same.
  static SparqlFilter parseFilterExpression(const string& filterContent);

 private:
  void parseQuery(ParsedQuery* query, QueryType queryType);
  void parseSelect(ParsedQuery* query);
  void parseWhere(ParsedQuery* query);
  void parseSolutionModifiers(ParsedQuery* query);
  // Returns true if it found a filter
  std::optional<SparqlFilter> parseFilter(bool failOnNoFilter = true);

  SparqlLexer lexer_;
  string query_;
  SparqlFilter parseRegexFilter(bool expectKeyword);

  // Helper function that converts the prefix map from `parsedQuery` (a vector
  // of pairs of prefix and IRI) to the prefix map we need for the
  // `SparqlQleverVisitor` (a hash map from prefixes to IRIs).
  static SparqlQleverVisitor::PrefixMap getPrefixMap(
      const ParsedQuery& parsedQuery);
  // Parse the clause with the prefixes of the given ParsedQuery.
  template <typename ContextType>
  auto parseWithAntlr(ContextType* (SparqlAutomaticParser::*F)(void),
                      const ParsedQuery& parsedQuery)
      -> decltype((std::declval<sparqlParserHelpers::ParserAndVisitor>())
                      .parseTypesafe(F)
                      .resultOfParse_);

  // Parse the clause with the given explicitly specified prefixes.
  template <typename ContextType>
  auto parseWithAntlr(ContextType* (SparqlAutomaticParser::*F)(void),
                      SparqlQleverVisitor::PrefixMap prefixMap)
      -> decltype((std::declval<sparqlParserHelpers::ParserAndVisitor>())
                      .parseTypesafe(F)
                      .resultOfParse_);
};
