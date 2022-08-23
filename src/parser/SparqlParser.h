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

  /**
   * @brief This method looks for the first string literal it can find and
   * parses it. During the parsing any escaped characters are resolved (e.g. \")
   * If isEntireString is true an exception is thrown if the entire string
   * is not a literal (apart from any leading and trailing whitespace).
   **/
  static TripleComponent parseLiteral(const ParsedQuery& pq,
                                      const string& literal,
                                      bool isEntireString, size_t off = 0);

 private:
  void parseQuery(ParsedQuery* query, QueryType queryType);
  void parseSelect(ParsedQuery* query);
  void parseWhere(ParsedQuery* query,
                  ParsedQuery::GraphPattern* currentPattern = nullptr);
  void parseSolutionModifiers(ParsedQuery* query);
  // Returns true if it found a filter
  std::optional<SparqlFilter> parseFilter(bool failOnNoFilter = true);

  // Reads the next element of a triple (an iri, a variable, a property path,
  // etc.) out of s beginning at the current value of pos. Sets pos to the
  // position after the read value, and returns a string view of the triple part
  // in s.
  std::string_view readTriplePart(const std::string& s, size_t* pos);

  static string stripAndLowercaseKeywordLiteral(std::string_view lit);

  /**
   * If *ptr 's last child is a BasicGraphPattern, return a reference to it.
   * If not, first append a BasicGraphPattern and then return a reference
   * to the added child
   */
  GraphPatternOperation::BasicGraphPattern& lastBasicPattern(
      ParsedQuery::GraphPattern* ptr) const;

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
