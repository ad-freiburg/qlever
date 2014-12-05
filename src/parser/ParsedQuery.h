// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <vector>

using std::string;
using std::vector;


// Data container for prefixes
class SparqlPrefix {
public:
  SparqlPrefix(const string& prefix, const string& uri) : _prefix(prefix), _uri
      (uri) {
  }

  string _prefix;
  string _uri;

  string asString() const;
};

// Data container for parsed triples from the where clause
class SparqlTriple {
public:
  SparqlTriple(const string& s, const string& p, const string& o) : _s(s), _p(p)
      , _o(o) {
  }

  string _s, _p, _o;

  string asString() const;
};

// A parsed SPARQL query. To be extended.
class ParsedQuery {
public:
  vector<SparqlPrefix> _prefixes;
  vector<string> _selectedVariables;
  vector<SparqlTriple> _whereClauseTriples;

  string asString() const;
};
