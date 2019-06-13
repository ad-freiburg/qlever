// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include "ParsedQuery.h"

using std::string;

// A simple parser of SPARQL.
// No supposed to feature the complete query language.
class SparqlParser {
 public:
  static ParsedQuery parse(const string& query);

  /**
   * @brief This method looks for the first string literal it can find and
   * parses it. During the parsing any escaped characters are resolved (e.g. \")
   * If isEntireString is true an exception is thrown if the entire string
   * is not a literal (apart from any leading and trailing whitespace).
   **/
  static string parseLiteral(const string& literal, bool isEntireString,
                             size_t off = 0);

 private:
  static void parsePrologue(string str, ParsedQuery& query);
  static void parseSelect(string const& str, ParsedQuery& query);
  static void parseWhere(
      string const& str, ParsedQuery& query,
      std::shared_ptr<ParsedQuery::GraphPattern> currentPattern = nullptr);
  static void parseSolutionModifiers(const string& str, ParsedQuery& query);
  static void addPrefix(const string& str, ParsedQuery& query);
  static void addWhereTriple(
      const string& str, std::shared_ptr<ParsedQuery::GraphPattern> pattern);
  static void addFilter(
      const string& str, vector<SparqlFilter>* _filters,
      std::shared_ptr<ParsedQuery::GraphPattern> pattern = nullptr);

  // Reads the next element of a triple (an iri, a variable, a property path,
  // etc.) out of s beginning at the current value of pos. Sets pos to the
  // position after the read value, and returns a string view of the triple part
  // in s.
  static std::string_view readTriplePart(const std::string& s, size_t* pos);

  static string stripAndLowercaseKeywordLiteral(const string& lit);
};
