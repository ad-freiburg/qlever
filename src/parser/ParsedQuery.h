// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <vector>

#include "../util/StringUtils.h"
#include "../util/HashMap.h"

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

  bool operator==(const SparqlTriple& other) const {
    return _s == other._s && _p == other._p &&_o == other._o;
  }
  string _s, _p, _o;

  string asString() const;
};


class OrderKey {
public:
  OrderKey(const string& key, bool desc): _key(key), _desc(desc) {}
  explicit OrderKey(const string& textual) {
    _desc = ad_utility::startsWith(textual, "DESC");
    bool scoreModifier = textual.find("SCORE") != string::npos;
    size_t i = textual.find('?');
    if (i != string::npos) {
      _key = ad_utility::rstrip(textual.substr(i), ')');
      if (scoreModifier) {
        _key = string("SCORE(") + _key + ")";
      }
    }
  }
  string _key;
  bool _desc;
};

class SparqlFilter {
public:
  enum FilterType {
    EQ = 0,
    NE = 1,
    LT = 2,
    LE = 3,
    GT = 5,
    GE = 6
  };

  string asString() const;

  FilterType _type;
  string _lhs;
  string _rhs;
};

// A parsed SPARQL query. To be extended.
class ParsedQuery {
public:
  // Groups triplets and filters. Represents a node in a tree (as graph patterns
  // are recursive).
  class GraphPattern {
  public:
    // deletes the patterns children.
    GraphPattern() { }
    // Move and copyconstructors to avoid double deletes on the trees children
    GraphPattern(GraphPattern&& other);
    GraphPattern(const GraphPattern& other);
    GraphPattern& operator=(const GraphPattern& other);
    virtual ~GraphPattern();
    void toString(std::ostringstream& os) const;

    vector<SparqlTriple> _whereClauseTriples;
    vector<SparqlFilter> _filters;
    bool _optional;
    size_t _id;

    vector<GraphPattern*> _children;
  };


  ParsedQuery() : _numGraphPatterns(1), _reduced(false), _distinct(false) { }

  vector<SparqlPrefix> _prefixes;
  vector<string> _selectedVariables;
  GraphPattern _rootGraphPattern;
  size_t _numGraphPatterns;
  vector<OrderKey> _orderBy;
  string _limit;
  string _textLimit;
  string _offset;
  bool _reduced;
  bool _distinct;
  string _originalString;

  void expandPrefixes();

  string asString() const;

private:
  static void expandPrefix(string& item,
      const ad_utility::HashMap<string, string>& prefixMap);
};
