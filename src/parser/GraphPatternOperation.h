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

// First some forward declarations.
// TODO<joka921> More stuff should consistently be in the `parsedQuery`
// namespace.
class SparqlTriple;
class ParsedQuery;

namespace parsedQuery {

class GraphPattern;

/// The actual data of a `VALUES` clause.
/// TODO<joka921> the two classes `SparqlValues` and `Values` (below) can be
/// merged, but we first have to figure out and refactor the `id`-business in
/// the query planner.
class SparqlValues {
 public:
  // The variables to which the values will be bound
  std::vector<std::string> _variables;
  // A table storing the values in their string form
  std::vector<std::vector<std::string>> _values;
};

/// A `BasicGraphPattern` represents a consecutive block of triples.
struct BasicGraphPattern {
  std::vector<SparqlTriple> _triples;
  /// Append the triples from `other` to this `BasicGraphPattern`
  void appendTriples(BasicGraphPattern other);
};

/// A `Values` clause
struct Values {
  SparqlValues _inlineValues;
  // This value will be overwritten later.
  size_t _id = std::numeric_limits<size_t>::max();
};

/// A `GroupGraphPattern` is anything enclosed in `{}`.
/// TODO<joka921> The naming is inconsistent between `GroupGraphPattern` and
/// `GraphPattern`.
struct GroupGraphPattern {
  GraphPattern _child;
};

/// An `OPTIONAL` clause.
/// TODO<joka921> the `_optional` member of the child should not be necessary.
struct Optional {
  Optional(GraphPattern child) : _child{std::move(child)} {
    _child._optional = true;
  };
  GraphPattern _child;
};

/// A SPARQL `MINUS` construct.
struct Minus {
  GraphPattern _child;
};

/// A SPARQL `UNION` construct.
struct Union {
  GraphPattern _child1;
  GraphPattern _child2;
};

// A subquery.
class Subquery {
 private:
  // Note: This is  a `unique_ptr` because of the  circular dependency between
  // `ParsedQuery`, `GraphPattern` and `GraphPatternOperation` (`Subquery` is
  // one of the alternatives of the variant `GraphPatternOperation`). The
  // special member functions make sure that this class can be used like a plain
  // value of type `ParsedQuery`.
  std::unique_ptr<ParsedQuery> _subquery;

 public:
  // TODO<joka921> Make this an abstraction `TypeErasingPimpl`.

  // Deliberately not explicit, because semantically those are copy/move
  // constructors.
  Subquery(const ParsedQuery&);
  Subquery(ParsedQuery&&);
  Subquery();
  ~Subquery();
  Subquery(const Subquery&);
  Subquery(Subquery&&);
  Subquery& operator=(const Subquery&);
  Subquery& operator=(Subquery&&);
  ParsedQuery& get();
  const ParsedQuery& get() const;
};

// It seems as if this class is never actually set.
// TODO<joka921> Further explore this and throw it out.
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

// TODO<joka921> Further refactor this, s.t. the whole `GraphPatternOperation`
// class actually becomes `using GraphPatternOperation = std::variant<...>`
using GraphPatternOperationVariant =
    std::variant<Optional, Union, Subquery, TransPath, Bind, BasicGraphPattern,
                 Values, Minus, GroupGraphPattern>;
struct GraphPatternOperation
    : public GraphPatternOperationVariant,
      public VisitMixin<GraphPatternOperation, GraphPatternOperationVariant> {
  using GraphPatternOperationVariant::GraphPatternOperationVariant;

  // TODO<joka921> First refactor `SparqlParserTest.cpp`,
  // then get rid of this function.
  auto& getBasic() { return std::get<BasicGraphPattern>(*this); }
  [[nodiscard]] const auto& getBasic() const {
    return std::get<BasicGraphPattern>(*this);
  }

  void toString(std::ostringstream& os, int indentation = 0) const;
};
}  // namespace parsedQuery
