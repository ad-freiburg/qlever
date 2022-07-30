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
#include "../util/Algorithm.h"
#include "../util/Exception.h"
#include "../util/HashMap.h"
#include "../util/OverloadCallOperator.h"
#include "../util/StringUtils.h"
#include "./TripleComponent.h"
#include "ParseException.h"
#include "data/Types.h"
#include "data/VarOrTerm.h"

using std::string;
using std::vector;

// Data container for prefixes
class SparqlPrefix {
 public:
  SparqlPrefix(string prefix, string uri)
      : _prefix(std::move(prefix)), _uri(std::move(uri)) {}

  string _prefix;
  string _uri;

  [[nodiscard]] string asString() const;
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
  PropertyPath(Operation op, uint16_t limit, std::string iri,
               std::initializer_list<PropertyPath> children);

  static PropertyPath fromIri(std::string iri) {
    PropertyPath p(PropertyPath::Operation::IRI);
    p._iri = std::move(iri);
    return p;
  }

  static PropertyPath fromVariable(Variable var) {
    PropertyPath p(PropertyPath::Operation::IRI);
    p._iri = std::move(var.name());
    return p;
  }

  static PropertyPath makeWithChildren(std::vector<PropertyPath> children,
                                       PropertyPath::Operation op) {
    PropertyPath p(std::move(op));
    p._children = std::move(children);
    return p;
  }

  static PropertyPath makeAlternative(std::vector<PropertyPath> children) {
    return makeWithChildren(std::move(children), Operation::ALTERNATIVE);
  }

  static PropertyPath makeSequence(std::vector<PropertyPath> children) {
    return makeWithChildren(std::move(children), Operation::SEQUENCE);
  }

  static PropertyPath makeInverse(PropertyPath child) {
    return makeWithChildren({std::move(child)}, Operation::INVERSE);
  }

  static PropertyPath makeWithChildLimit(PropertyPath child,
                                         uint_fast16_t limit,
                                         PropertyPath::Operation op) {
    PropertyPath p = makeWithChildren({std::move(child)}, op);
    p._limit = limit;
    return p;
  }

  static PropertyPath makeTransitiveMin(PropertyPath child,
                                        uint_fast16_t limit) {
    return makeWithChildLimit(std::move(child), limit,
                              Operation::TRANSITIVE_MIN);
  }

  static PropertyPath makeTransitiveMax(PropertyPath child,
                                        uint_fast16_t limit) {
    return makeWithChildLimit(std::move(child), limit,
                              Operation::TRANSITIVE_MAX);
  }

  static PropertyPath makeTransitive(PropertyPath child) {
    return makeWithChildren({std::move(child)}, Operation::TRANSITIVE);
  }

  bool operator==(const PropertyPath& other) const {
    return _operation == other._operation && _limit == other._limit &&
           _iri == other._iri && _children == other._children &&
           _can_be_null == other._can_be_null;
  }

  void writeToStream(std::ostream& out) const;
  [[nodiscard]] std::string asString() const;

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
inline bool isVariable(const TripleComponent& elem) {
  return elem.isString() && isVariable(elem.getString());
}

inline bool isVariable(const PropertyPath& elem) {
  return elem._operation == PropertyPath::Operation::IRI &&
         isVariable(elem._iri);
}

std::ostream& operator<<(std::ostream& out, const PropertyPath& p);

// Data container for parsed triples from the where clause
class SparqlTriple {
 public:
  SparqlTriple(TripleComponent s, PropertyPath p, TripleComponent o)
      : _s(std::move(s)), _p(std::move(p)), _o(std::move(o)) {}

  SparqlTriple(TripleComponent s, const std::string& p_iri, TripleComponent o)
      : _s(std::move(s)),
        _p(PropertyPath::Operation::IRI, 0, p_iri, {}),
        _o(std::move(o)) {}

  bool operator==(const SparqlTriple& other) const {
    return _s == other._s && _p == other._p && _o == other._o;
  }
  TripleComponent _s;
  PropertyPath _p;
  TripleComponent _o;

  [[nodiscard]] string asString() const;
};

/// Store an expression that appeared in an ORDER BY clause.
class ExpressionOrderKey {
 public:
  bool isDescending_;
  sparqlExpression::SparqlExpressionPimpl expression_;
  // ___________________________________________________________________________
  explicit ExpressionOrderKey(
      sparqlExpression::SparqlExpressionPimpl expression,
      bool isDescending = false)
      : isDescending_{isDescending}, expression_{std::move(expression)} {}
};

/// Store a variable that appeared in an ORDER BY clause.
class VariableOrderKey {
 public:
  string variable_;
  bool isDescending_;
  // ___________________________________________________________________________
  explicit VariableOrderKey(string variable, bool isDescending = false)
      : variable_{std::move(variable)}, isDescending_{isDescending} {}
};

// Represents an ordering by a variable or an expression.
using OrderKey = std::variant<VariableOrderKey, ExpressionOrderKey>;

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

  [[nodiscard]] string asString() const;

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

// Represents the data returned by a limitOffsetClause.
struct LimitOffsetClause {
  uint64_t _limit = std::numeric_limits<uint64_t>::max();
  uint64_t _textLimit = 1;
  uint64_t _offset = 0;
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
    // The constructor has to be implemented in the .cpp file because of the
    // circular dependencies of `GraphPattern` and `GraphPatternOperation`
    GraphPattern();
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
    [[nodiscard]] std::string getDescriptor() const {
      return "(" + _expression.getDescriptor() + " as " + _outVarName + ")";
    }
    bool operator==(const Alias& other) const {
      return _expression.getDescriptor() == other._expression.getDescriptor() &&
             _outVarName == other._outVarName;
    }

    [[nodiscard]] const std::string& targetVariable() const {
      return _outVarName;
    }
  };

  using VarOrAlias = std::variant<Variable, Alias>;

  // Represents either "all Variables" (Select *) or a list of explicitly
  // selected Variables (Select ?a ?b).
  // Represents the SELECT clause with all the possible outcomes.
  struct SelectClause {
    bool _reduced = false;
    bool _distinct = false;

   private:
    struct VarsAndAliases {
      std::vector<Variable> _vars;
      std::vector<Alias> _aliases;
    };
    std::variant<VarsAndAliases, char> _varsAndAliasesOrAsterisk;
    std::vector<Variable> _variablesFromQueryBody;

   public:
    [[nodiscard]] bool isAsterisk() const {
      return std::holds_alternative<char>(_varsAndAliasesOrAsterisk);
    }

    // Set the selector to '*'.
    void setAsterisk() { _varsAndAliasesOrAsterisk = '*'; }

    void setSelected(std::vector<Variable> variables) {
      std::vector<VarOrAlias> v(std::make_move_iterator(variables.begin()),
                                std::make_move_iterator(variables.end()));
      setSelected(v);
    }

    void setSelected(std::vector<VarOrAlias> varsOrAliases) {
      VarsAndAliases v;
      auto processVariable = [&v](Variable var) {
        v._vars.push_back(std::move(var));
      };
      auto processAlias = [&v](Alias alias) {
        v._vars.emplace_back(alias._outVarName);
        v._aliases.push_back(std::move(alias));
      };

      for (auto& el : varsOrAliases) {
        std::visit(
            ad_utility::OverloadCallOperator{processVariable, processAlias},
            std::move(el));
      }
      _varsAndAliasesOrAsterisk = std::move(v);
    }

    // Add a variable that was found in the query body. The added variables
    // will only be used if `isAsterisk` is true.
    void addVariableForAsterisk(const Variable& variable) {
      if (!ad_utility::contains(_variablesFromQueryBody, variable)) {
        _variablesFromQueryBody.emplace_back(variable);
      }
    }

    // Get the variables accordingly to established Selector:
    // Select All (Select '*')
    // or
    // explicit variables selection (Select 'var_1' ... 'var_n')
    [[nodiscard]] const std::vector<Variable>& getSelectedVariables() const {
      return isAsterisk()
                 ? _variablesFromQueryBody
                 : std::get<VarsAndAliases>(_varsAndAliasesOrAsterisk)._vars;
    }
    [[nodiscard]] std::vector<std::string> getSelectedVariablesAsStrings()
        const {
      std::vector<std::string> result;
      const auto& vars = getSelectedVariables();
      result.reserve(vars.size());
      for (const auto& var : vars) {
        result.push_back(var.name());
      }
      return result;
    }

    [[nodiscard]] const std::vector<Alias>& getAliases() const {
      static const std::vector<Alias> emptyDummy;
      // Aliases are always manually specified
      if (isAsterisk()) {
        return emptyDummy;
      } else {
        return std::get<VarsAndAliases>(_varsAndAliasesOrAsterisk)._aliases;
      }
    }
  };

  SelectClause _selectedVarsOrAsterisk{SelectClause{}};

  using ConstructClause = ad_utility::sparql_types::Triples;

  ParsedQuery() = default;

  // The ql prefix for QLever specific additions is always defined.
  vector<SparqlPrefix> _prefixes = {SparqlPrefix(
      INTERNAL_PREDICATE_PREFIX_NAME, INTERNAL_PREDICATE_PREFIX_IRI)};
  GraphPattern _rootGraphPattern;
  vector<SparqlFilter> _havingClauses;
  size_t _numGraphPatterns = 1;
  vector<VariableOrderKey> _orderBy;
  vector<Variable> _groupByVariables;
  LimitOffsetClause _limitOffset{};
  string _originalString;

  // explicit default initialisation because the constructor
  // of SelectClause is private
  std::variant<SelectClause, ConstructClause> _clause{_selectedVarsOrAsterisk};

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

  // Add a variable, that was found in the SubQuery body, when query has a
  // Select Clause
  [[maybe_unused]] bool registerVariableVisibleInQueryBody(
      const Variable& variable) {
    if (!hasSelectClause()) return false;
    selectClause().addVariableForAsterisk(variable);
    return true;
  }

  void expandPrefixes();

  auto& children() { return _rootGraphPattern._children; }
  [[nodiscard]] const auto& children() const {
    return _rootGraphPattern._children;
  }

  /**
   * @brief Adds all elements from p's rootGraphPattern to this parsed query's
   * root graph pattern. This changes the graph patterns ids.
   */
  void merge(const ParsedQuery& p);

  [[nodiscard]] string asString() const;

  static void expandPrefix(
      PropertyPath& item, const ad_utility::HashMap<string, string>& prefixMap);
  static void expandPrefix(
      string& item, const ad_utility::HashMap<string, string>& prefixMap);
};

using GroupKey = std::variant<sparqlExpression::SparqlExpressionPimpl,
                              ParsedQuery::Alias, Variable>;

struct GraphPatternOperation {
  struct BasicGraphPattern {
    vector<SparqlTriple> _whereClauseTriples;
  };
  struct Values {
    SparqlValues _inlineValues;
    // This value will be overwritten later.
    size_t _id = std::numeric_limits<size_t>::max();
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
    TripleComponent _left;
    TripleComponent _right;
    // The name of the left and right end of the subpath
    std::string _innerLeft;
    std::string _innerRight;
    size_t _min = 0;
    size_t _max = 0;
    ParsedQuery::GraphPattern _childGraphPattern;
  };

  // A SPARQL Bind construct.
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
