// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include "../util/StringUtils.h"

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
  SparqlTriple(const string& s, const string& p, const string& o)
      : _s(s), _p(p), _o(o) {
  }

  string _s, _p, _o;

  string asString() const;
};


class OrderKey {
public:
  OrderKey(const string& key, bool desc): _key(key), _desc(desc) {}
  explicit OrderKey(const string& textual) {
    _desc = ad_utility::startsWith(textual, "DESC");
    size_t i = textual.find('?');
    if (i != string::npos) {
      _key = ad_utility::rstrip(textual.substr(i), ')');
    }
  }
  string _key;
  bool _desc;
};

// A parsed SPARQL query. To be extended.
class ParsedQuery {
public:

  ParsedQuery() : _reduced(false), _distinct(false) { }

  vector<SparqlPrefix> _prefixes;
  vector<string> _selectedVariables;
  vector<SparqlTriple> _whereClauseTriples;
  vector<OrderKey> _orderBy;
  string _limit;
  string _offset;
  bool _reduced;
  bool _distinct;

  void expandPrefixes();

  string asString() const;

private:
  static void expandPrefix(string& item,
      const std::unordered_map<string, string>& prefixMap);
};
