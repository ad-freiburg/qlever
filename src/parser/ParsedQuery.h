// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "../util/HashMap.h"
#include "../util/StringUtils.h"
#include "ParseException.h"

using std::string;
using std::vector;

// Data container for prefixes
class SparqlPrefix {
 public:
  SparqlPrefix(const string& prefix, const string& uri)
      : _prefix(prefix), _uri(uri) {}

  string _prefix;
  string _uri;

  string asString() const;
};

// Data container for parsed triples from the where clause
class SparqlTriple {
 public:
  SparqlTriple(const string& s, const string& p, const string& o)
      : _s(s), _p(p), _o(o) {}

  bool operator==(const SparqlTriple& other) const {
    return _s == other._s && _p == other._p && _o == other._o;
  }
  string _s, _p, _o;

  string asString() const;
};

class OrderKey {
 public:
  OrderKey(const string& key, bool desc) : _key(key), _desc(desc) {}
  explicit OrderKey(const string& textual) {
    std::string lower = ad_utility::getLowercaseUtf8(textual);
    size_t pos = 0;
    _desc = ad_utility::startsWith(lower, "desc(");
    // skip the 'desc('
    if (_desc) {
      pos += 5;
      // skip any whitespace after the opening bracket
      while (pos < textual.size() &&
             ::isspace(static_cast<unsigned char>(textual[pos]))) {
        pos++;
      }
    }
    // skip 'asc('
    if (ad_utility::startsWith(lower, "asc(")) {
      pos += 4;
      // skip any whitespace after the opening bracket
      while (pos < textual.size() &&
             ::isspace(static_cast<unsigned char>(textual[pos]))) {
        pos++;
      }
    }
    if (lower[pos] == '(') {
      // key is an alias
      size_t bracketDepth = 1;
      size_t end = pos + 1;
      while (bracketDepth > 0 && end < textual.size()) {
        if (lower[end] == '(') {
          bracketDepth++;
        } else if (lower[end] == ')') {
          bracketDepth--;
        }
        end++;
      }
      if (bracketDepth != 0) {
        throw ParseException("Unbalanced brackets in alias order by key: " +
                             textual);
      }
      _key = textual.substr(pos, end - pos);
    } else if (lower[pos] == 's') {
      // key is a text score
      if (!ad_utility::startsWith(lower.substr(pos), "score(")) {
        throw ParseException("Expected keyword score in order by key: " +
                             textual);
      }
      pos += 6;
      size_t end = pos + 1;
      // look for the first space or closing bracket character
      while (end < textual.size() && textual[end] != ')' &&
             !::isspace(static_cast<unsigned char>(textual[end]))) {
        end++;
      }
      _key = "SCORE(" + textual.substr(pos, end - pos) + ")";
    } else if (lower[pos] == '?') {
      // key is simple variable
      size_t end = pos + 1;
      // look for the first space or closing bracket character
      while (end < textual.size() && textual[end] != ')' &&
             !::isspace(static_cast<unsigned char>(textual[end]))) {
        end++;
      }
      _key = textual.substr(pos, end - pos);
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
    GE = 6,
    LANG_MATCHES = 7,
    REGEX = 8
  };

  string asString() const;

  FilterType _type;
  string _lhs;
  string _rhs;
  bool _regexIgnoreCase;
};

// A parsed SPARQL query. To be extended.
class ParsedQuery {
 public:
  // Groups triplets and filters. Represents a node in a tree (as graph patterns
  // are recursive).
  class GraphPattern {
   public:
    // deletes the patterns children.
    GraphPattern() {}
    // Move and copyconstructors to avoid double deletes on the trees children
    GraphPattern(GraphPattern&& other);
    GraphPattern(const GraphPattern& other);
    GraphPattern& operator=(const GraphPattern& other);
    virtual ~GraphPattern();
    void toString(std::ostringstream& os, int indentation = 0) const;

    vector<SparqlTriple> _whereClauseTriples;
    vector<SparqlFilter> _filters;
    bool _optional;
    size_t _id;

    vector<GraphPattern*> _children;
  };

  struct Alias {
    string _inVarName;
    string _outVarName;
    bool _isAggregate;
    // The mapping from the original var to the new one
    string _function;
  };

  ParsedQuery() : _numGraphPatterns(1), _reduced(false), _distinct(false) {}

  vector<SparqlPrefix> _prefixes;
  vector<string> _selectedVariables;
  GraphPattern _rootGraphPattern;
  size_t _numGraphPatterns;
  vector<OrderKey> _orderBy;
  vector<string> _groupByVariables;
  string _limit;
  string _textLimit;
  string _offset;
  bool _reduced;
  bool _distinct;
  string _originalString;
  std::vector<Alias> _aliases;

  void expandPrefixes();
  void parseAliases();

  string asString() const;

 private:
  static void expandPrefix(
      string& item, const ad_utility::HashMap<string, string>& prefixMap);

  /**
   * @brief Parses the given alias and adds it to the list of _aliases.
   * @param alias The inner part of the alias (without the surrounding brackets)
   * @return The new name of the variable after the aliasing was completed
   */
  std::string parseAlias(const std::string& alias);
};
