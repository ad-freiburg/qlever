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

 private:
  static void parsePrologue(string str, ParsedQuery& query);
  static void parseSelect(string const& str, ParsedQuery& query);
  static void parseWhere(string const& str, ParsedQuery& query,
                         ParsedQuery::GraphPattern* currentPattern = nullptr);
  static void parseSolutionModifiers(const string& str, ParsedQuery& query);
  static void addPrefix(const string& str, ParsedQuery& query);
  static void addWhereTriple(const string& str,
                             ParsedQuery::GraphPattern* pattern);
  static void addFilter(const string& str, vector<SparqlFilter>* _filters);

  static string stripAndLowercaseKeywordLiteral(const string& lit);
};
