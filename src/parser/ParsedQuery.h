// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <initializer_list>
#include <string>
#include <utility>
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

class PropertyPath {
 public:
  enum class Operation {
    SEQUENCE,
    ALTERNATIVE,
    TRANSITIVE,
    TRANSITIVE_MIN,  // e.g. +
    TRANSITIVE_MAX,  // e.g. *n or ?
    INVERSE,
    IRI
  };

  PropertyPath()
      : _operation(Operation::IRI),
        _limit(0),
        _iri(),
        _children(),
        _can_be_null(false) {}
  explicit PropertyPath(Operation op)
      : _operation(op), _limit(0), _iri(), _children(), _can_be_null(false) {}
  PropertyPath(Operation op, uint16_t limit, const std::string& iri,
               std::initializer_list<PropertyPath> children);

  bool operator==(const PropertyPath& other) const {
    return _operation == other._operation && _limit == other._limit &&
           _iri == other._iri && _children == other._children &&
           _can_be_null == other._can_be_null;
  }

  void writeToStream(std::ostream& out) const;
  std::string asString() const;

  void computeCanBeNull();

  Operation _operation;
  // For the limited transitive operations
  uint_fast16_t _limit;

  // In case of an iri
  std::string _iri;

  std::vector<PropertyPath> _children;

  /**
   * True iff this property path is either a transitive path with minimum length
   * of 0, or if all of this transitive path's children can be null.
   */
  bool _can_be_null;
};

inline bool isVariable(const string& elem) {
  return ad_utility::startsWith(elem, "?");
}

inline bool isVariable(const PropertyPath& elem) {
  return elem._operation == PropertyPath::Operation::IRI &&
         isVariable(elem._iri);
}

std::ostream& operator<<(std::ostream& out, const PropertyPath& p);

// Data container for parsed triples from the where clause
class SparqlTriple {
 public:
  SparqlTriple(const string& s, const PropertyPath& p, const string& o)
      : _s(s), _p(p), _o(o) {}

  SparqlTriple(const string& s, const std::string& p_iri, const string& o)
      : _s(s), _p(PropertyPath::Operation::IRI, 0, p_iri, {}), _o(o) {}

  bool operator==(const SparqlTriple& other) const {
    return _s == other._s && _p == other._p && _o == other._o;
  }
  string _s;
  PropertyPath _p;
  string _o;

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
    REGEX = 8,
    PREFIX = 9
  };

  string asString() const;

  FilterType _type;
  string _lhs;
  string _rhs;
  bool _regexIgnoreCase = false;
  // True if the str function was applied to the left side.
  bool _lhsAsString = false;
};

// Represents a VALUES statement in the query.
class SparqlValues {
 public:
  // The variables to which the values will be bound
  vector<string> _variables;
  // A table storing the values in their string form
  vector<vector<string>> _values;
};

struct GraphPatternOperation;

// A parsed SPARQL query. To be extended.
class ParsedQuery {
 public:
  class GraphPattern;



  // Groups triplets and filters. Represents a node in a tree (as graph patterns
  // are recursive).
  class GraphPattern {
   public:
    // deletes the patterns children.
    GraphPattern() : _optional(false) {}
    // Move and copyconstructors to avoid double deletes on the trees children
    GraphPattern(GraphPattern&& other) = default;
    GraphPattern(const GraphPattern& other) = default;
    GraphPattern& operator=(const GraphPattern& other) = default;
    GraphPattern& operator=( GraphPattern&& other) noexcept = default;
    ~GraphPattern() = default;
    void toString(std::ostringstream& os, int indentation = 0) const;
    // Traverses the graph pattern tree and assigns a unique id to every graph
    // pattern
    void recomputeIds(size_t* id_count = nullptr);

    vector<SparqlTriple> _whereClauseTriples;
    vector<SparqlFilter> _filters;
    vector<SparqlValues> _inlineValues;
    bool _optional;
    /**
     * @brief A id that is unique for the ParsedQuery. Ids are guaranteed to
     * start with zero and to be dense.
     */
    size_t _id;

    vector<GraphPatternOperation> _children;
  };

  /**
   * @brief All supported types of aggregate aliases
   */
  enum class AggregateType {
    COUNT,
    GROUP_CONCAT,
    FIRST,
    LAST,
    SAMPLE,
    MIN,
    MAX,
    SUM,
    AVG
  };

  struct Alias {
    AggregateType _type;
    string _inVarName;
    string _outVarName;
    bool _isAggregate = true;
    bool _isDistinct = false;
    std::string _function;
    // The deilimiter used by group concat
    std::string _delimiter = " ";
  };

  ParsedQuery()
      : _rootGraphPattern(std::make_shared<GraphPattern>()),
        _numGraphPatterns(1),
        _reduced(false),
        _distinct(false) {}

  vector<SparqlPrefix> _prefixes;
  vector<string> _selectedVariables;
  std::shared_ptr<GraphPattern> _rootGraphPattern;
  vector<SparqlFilter> _havingClauses;
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

  /**
   * @brief Adds all elements from p's rootGraphPattern to this parsed query's
   * root graph pattern. This changes the graph patterns ids.
   */
  void merge(const ParsedQuery& p);

  string asString() const;

 private:
  static void expandPrefix(
      PropertyPath& item, const ad_utility::HashMap<string, string>& prefixMap);
  static void expandPrefix(
      string& item, const ad_utility::HashMap<string, string>& prefixMap);

  /**
   * @brief Parses the given alias and adds it to the list of _aliases.
   * @param alias The inner part of the alias (without the surrounding brackets)
   * @return The new name of the variable after the aliasing was completed
   */
  std::string parseAlias(const std::string& alias);
};


struct GraphPatternOperation {
  struct Optional {
    ParsedQuery::GraphPattern _child;
  };
  struct Union {
    ParsedQuery::GraphPattern _child1;
    ParsedQuery::GraphPattern _child2;
  };
  struct Subquery {
    ParsedQuery _subquery;
  };

  struct TransPath {
    // The name of the left and right end of the transitive operation
    std::string _left;
    std::string _right;
    // The name of the left and right end of the subpath
    std::string _innerLeft;
    std::string _innerRight;
    size_t _min = 0;
    size_t _max = 0;
    ParsedQuery::GraphPattern _childGraphPattern;
  };

  std::variant<Optional, Union, Subquery, TransPath> variant_;
  template<typename A, typename... Args, typename=std::enable_if_t<!std::is_base_of_v<GraphPatternOperation, std::decay_t<A>>>>
  GraphPatternOperation(A&& a, Args&&... args) : variant_(std::forward<A>(a), std::forward<Args>(args)...) {}
  GraphPatternOperation() = delete;
  GraphPatternOperation(const GraphPatternOperation&) = default;
  GraphPatternOperation(GraphPatternOperation&&) noexcept = default;
  GraphPatternOperation& operator=(const GraphPatternOperation&) = default;
  GraphPatternOperation& operator=(GraphPatternOperation&&) noexcept = default;
  template<typename F>
  const auto visit(F f) const {
    return std::visit(f, variant_);
  }

  template<typename F>
  auto visit(F f) {
    return std::visit(f, variant_);
  }
  template<class T>
  constexpr T& get() {return std::get<T>(variant_);}
  template<class T>
  constexpr const T& get() const {return std::get<T>(variant_);}

  void toString(std::ostringstream& os, int indentation = 0) const;
};

