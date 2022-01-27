// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <initializer_list>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "../engine/ResultType.h"
#include "../engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "../util/Exception.h"
#include "../util/HashMap.h"
#include "../util/StringUtils.h"
#include "ParseException.h"
#include "data/Types.h"
#include "data/VarOrTerm.h"

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

inline bool isVariable(const string& elem) { return elem.starts_with("?"); }

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
    _desc = lower.starts_with("desc(");
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
    if (lower.starts_with("asc(")) {
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
      if (!lower.substr(pos).starts_with("score(")) {
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
  vector<string> _additionalLhs;
  vector<string> _additionalPrefixes;
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
    GraphPattern& operator=(GraphPattern&& other) noexcept = default;
    ~GraphPattern() = default;
    void toString(std::ostringstream& os, int indentation = 0) const;
    // Traverses the graph pattern tree and assigns a unique id to every graph
    // pattern
    void recomputeIds(size_t* id_count = nullptr);

    bool _optional;
    /**
     * @brief A id that is unique for the ParsedQuery. Ids are guaranteed to
     * start with zero and to be dense.
     */
    size_t _id = size_t(-1);

    // Filters always apply to the complete GraphPattern, no matter where
    // they appear. For VALUES and Triples, the order matters, so they
    // become children.
    std::vector<SparqlFilter> _filters;
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

  static std::string AggregateTypeAsString(AggregateType t) {
    switch (t) {
      case AggregateType::COUNT:
        return "COUNT";
      case AggregateType::GROUP_CONCAT:
        return "GROUP_CONCAT";
      case AggregateType::FIRST:
        return "FIRST";
      case AggregateType::LAST:
        return "LAST";
      case AggregateType::SAMPLE:
        return "SAMPLE";
      case AggregateType::MIN:
        return "MIN";
      case AggregateType::MAX:
        return "MAX";
      case AggregateType::SUM:
        return "SUM";
      case AggregateType::AVG:
        return "AVG";
      default:
        AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
                 "Illegal/unimplemented enum value in AggregateTypeAsString. "
                 "Should never happen, please report this");
    }
  }

  struct Alias {
    sparqlExpression::SparqlExpressionPimpl _expression;
    string _outVarName;
    std::string getDescriptor() const {
      return "(" + _expression.getDescriptor() + " as " + _outVarName + ")";
    }
  };

  struct SelectClause {
    vector<string> _selectedVariables;
    std::vector<Alias> _aliases;
    bool _reduced = false;
    bool _distinct = false;
  };

  using ConstructClause = ad_utility::sparql_types::Triples;

  ParsedQuery() = default;

  vector<SparqlPrefix> _prefixes;
  GraphPattern _rootGraphPattern;
  vector<SparqlFilter> _havingClauses;
  size_t _numGraphPatterns = 1;
  vector<OrderKey> _orderBy;
  vector<string> _groupByVariables;
  std::optional<size_t> _limit = std::nullopt;
  string _textLimit;
  std::optional<size_t> _offset = std::nullopt;
  string _originalString;
  // explicit default initialisation because the constructor
  // of SelectClause is private
  std::variant<SelectClause, ConstructClause> _clause{SelectClause{}};

  [[nodiscard]] bool hasSelectClause() const {
    return std::holds_alternative<SelectClause>(_clause);
  }

  [[nodiscard]] bool hasConstructClause() const {
    return std::holds_alternative<ConstructClause>(_clause);
  }

  [[nodiscard]] decltype(auto) selectClause() const {
    return std::get<SelectClause>(_clause);
  }

  [[nodiscard]] decltype(auto) constructClause() const {
    return std::get<ConstructClause>(_clause);
  }

  [[nodiscard]] decltype(auto) selectClause() {
    return std::get<SelectClause>(_clause);
  }

  [[nodiscard]] decltype(auto) constructClause() {
    return std::get<ConstructClause>(_clause);
  }

  void expandPrefixes();
  void parseAliases();

  auto& children() { return _rootGraphPattern._children; }
  const auto& children() const { return _rootGraphPattern._children; }

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
};

struct GraphPatternOperation {
  struct BasicGraphPattern {
    vector<SparqlTriple> _whereClauseTriples;
  };
  struct Values {
    SparqlValues _inlineValues;
    size_t _id;
  };
  struct GroupGraphPattern {
    ParsedQuery::GraphPattern _child;
  };
  struct Optional {
    ParsedQuery::GraphPattern _child;
  };
  struct Minus {
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

  // A SPARQL Bind construct.  Currently supports either an unsigned integer or
  // a knowledge base constant. For simplicity, we store both values instead of
  // using a union or std::variant here.
  struct Bind {
    sparqlExpression::SparqlExpressionPimpl _expression;
    std::string _target;  // the variable to which the expression will be bound

    // Return all the strings contained in the BIND expression (variables,
    // constants, etc. Is required e.g. by ParsedQuery::expandPrefix.
    vector<string*> strings() {
      auto r = _expression.strings();
      r.push_back(&_target);
      return r;
    }

    // Const overload, needed by the query planner. The actual behavior is
    // always const, so this is fine.
    [[nodiscard]] vector<const string*> strings() const {
      auto r = const_cast<Bind*>(this)->strings();
      return {r.begin(), r.end()};
    }

    [[nodiscard]] string getDescriptor() const {
      auto inner = _expression.getDescriptor();
      return "BIND (" + inner + " AS " + _target + ")";
    }
  };

  std::variant<Optional, Union, Subquery, TransPath, Bind, BasicGraphPattern,
               Values, Minus>
      variant_;
  // Construct from one of the variant types (or anything that is convertible to
  // them.
  template <typename A, typename = std::enable_if_t<!std::is_base_of_v<
                            GraphPatternOperation, std::decay_t<A>>>>
  explicit GraphPatternOperation(A&& a) : variant_(std::forward<A>(a)) {}
  GraphPatternOperation() = delete;
  GraphPatternOperation(const GraphPatternOperation&) = default;
  GraphPatternOperation(GraphPatternOperation&&) noexcept = default;
  GraphPatternOperation& operator=(const GraphPatternOperation&) = default;
  GraphPatternOperation& operator=(GraphPatternOperation&&) noexcept = default;

  template <typename T>
  constexpr bool is() noexcept {
    return std::holds_alternative<T>(variant_);
  }

  template <typename F>
  decltype(auto) visit(F f) {
    return std::visit(f, variant_);
  }

  template <typename F>
  decltype(auto) visit(F f) const {
    return std::visit(f, variant_);
  }
  template <class T>
  constexpr T& get() {
    return std::get<T>(variant_);
  }
  template <class T>
  [[nodiscard]] constexpr const T& get() const {
    return std::get<T>(variant_);
  }

  auto& getBasic() { return get<BasicGraphPattern>(); }
  [[nodiscard]] const auto& getBasic() const {
    return get<BasicGraphPattern>();
  }

  void toString(std::ostringstream& os, int indentation = 0) const;
};
