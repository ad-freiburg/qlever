//  Copyright 2022, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <limits>
#include <memory>

#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/GraphPattern.h"
#include "parser/TripleComponent.h"
#include "util/Algorithm.h"
#include "util/VisitMixin.h"

class SparqlTriple;

class ParsedQuery;

namespace parsedQuery {

class GraphPattern;

// Represents a VALUES statement in the query.
class SparqlValues {
 public:
  // The variables to which the values will be bound
  std::vector<std::string> _variables;
  // A table storing the values in their string form
  std::vector<std::vector<std::string>> _values;
};

struct BasicGraphPattern {
  std::vector<SparqlTriple> _triples;

  void appendTriples(BasicGraphPattern pattern);
};
struct Values {
  SparqlValues _inlineValues;
  // This value will be overwritten later.
  size_t _id = std::numeric_limits<size_t>::max();
};
struct GroupGraphPattern {
  GraphPattern _child;
};
struct Optional {
  Optional(GraphPattern child) : _child{std::move(child)} {
    _child._optional = true;
  };
  GraphPattern _child;
};
struct Minus {
  GraphPattern _child;
};
struct Union {
  GraphPattern _child1;
  GraphPattern _child2;
};
class Subquery {
 private:
  std::unique_ptr<ParsedQuery> _subquery;

 public:
  explicit Subquery(const ParsedQuery&);
  explicit Subquery(ParsedQuery&&);
  Subquery();
  Subquery(const Subquery&);
  Subquery(Subquery&&);
  Subquery& operator=(const Subquery&);
  Subquery& operator=(Subquery&&);
  ParsedQuery& get();
  const ParsedQuery& get() const;
  // The subquery's children to not influence the outer query.
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
  GraphPattern _childGraphPattern;
};

// A SPARQL Bind construct.
struct Bind {
  sparqlExpression::SparqlExpressionPimpl _expression;
  std::string _target;  // the variable to which the expression will be bound

  // Return all the strings contained in the BIND expression (variables,
  // constants, etc. Is required e.g. by ParsedQuery::expandPrefix.
  std::vector<std::string*> strings() {
    auto r = _expression.strings();
    r.push_back(&_target);
    return r;
  }

  // Const overload, needed by the query planner. The actual behavior is
  // always const, so this is fine.
  [[nodiscard]] std::vector<const string*> strings() const {
    auto r = const_cast<Bind*>(this)->strings();
    return {r.begin(), r.end()};
  }

  [[nodiscard]] string getDescriptor() const {
    auto inner = _expression.getDescriptor();
    return "BIND (" + inner + " AS " + _target + ")";
  }
};

using GraphPatternOperationVariant =
    std::variant<Optional, Union, Subquery, TransPath, Bind, BasicGraphPattern,
                 Values, Minus, GroupGraphPattern>;
struct GraphPatternOperation
    : public GraphPatternOperationVariant,
      public VisitMixin<GraphPatternOperation, GraphPatternOperationVariant> {
  using GraphPatternOperationVariant::GraphPatternOperationVariant;

  template <typename T>
  constexpr bool is() noexcept {
    return std::holds_alternative<T>(*this);
  }

  template <class T>
  constexpr T& get() {
    return std::get<T>(*this);
  }
  template <class T>
  [[nodiscard]] constexpr const T& get() const {
    return std::get<T>(*this);
  }

  auto& getBasic() { return get<BasicGraphPattern>(); }
  [[nodiscard]] const auto& getBasic() const {
    return get<BasicGraphPattern>();
  }

  void toString(std::ostringstream& os, int indentation = 0) const;
};
}  // namespace parsedQuery
