// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include "ParsedQuery.h"
#include "SparqlLexer.h"

using std::string;

// A simple parser of SPARQL.
// No supposed to feature the complete query language.
class SparqlParser {
 public:
  SparqlParser(const string& query);
  ParsedQuery parse();

  /**
   * @brief This method looks for the first string literal it can find and
   * parses it. During the parsing any escaped characters are resolved (e.g. \")
   * If isEntireString is true an exception is thrown if the entire string
   * is not a literal (apart from any leading and trailing whitespace).
   **/
  static string parseLiteral(const string& literal, bool isEntireString,
                             size_t off = 0);

 private:
  void parseQuery(ParsedQuery* query);
  void parsePrologue(ParsedQuery* query);
  void parseSelect(ParsedQuery* query);
  void parseWhere(
          ParsedQuery* query,
          ParsedQuery::GraphPattern *currentPattern = nullptr);
  void parseSolutionModifiers(ParsedQuery* query);
  void addPrefix(const string& key, const string& value, ParsedQuery* query);
  void addWhereTriple(const string& str,
                      std::shared_ptr<ParsedQuery::GraphPattern> pattern);
  // Returns true if it found a filter
  bool parseFilter(
          vector<SparqlFilter>* _filters, bool failOnNoFilter = true,
          ParsedQuery::GraphPattern *pattern = nullptr);
  // Parses an expressiong of the form (?a) = "en"
  void addLangFilter(const std::string& lhs, const std::string& rhs,
                     ParsedQuery::GraphPattern *pattern);

  // takes either DESC or ASC as the parameter
  OrderKey parseOrderKey(const std::string& order, ParsedQuery* query);
  ParsedQuery::Alias parseAlias();

  // Reads the next element of a triple (an iri, a variable, a property path,
  // etc.) out of s beginning at the current value of pos. Sets pos to the
  // position after the read value, and returns a string view of the triple part
  // in s.
  std::string_view readTriplePart(const std::string& s, size_t* pos);

  static string stripAndLowercaseKeywordLiteral(const string& lit);

  SparqlLexer _lexer;
  string _query;
};
