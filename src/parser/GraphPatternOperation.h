// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_PARSER_GRAPHPATTERNOPERATION_H
#define QLEVER_SRC_PARSER_GRAPHPATTERNOPERATION_H

#include <util/TransparentFunctors.h>

#include <limits>
#include <memory>
#include <vector>

#include "engine/PathSearch.h"
#include "engine/SpatialJoin.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/DatasetClauses.h"
#include "parser/GraphPattern.h"
#include "parser/PathQuery.h"
#include "parser/SpatialQuery.h"
#include "parser/TextSearchQuery.h"
#include "parser/TripleComponent.h"
#include "parser/data/Variable.h"
#include "util/Algorithm.h"
#include "util/VisitMixin.h"
#include "util/http/HttpUtils.h"

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
struct SparqlValues {
 public:
  // The variables to which the values will be bound
  std::vector<Variable> _variables;
  // A table storing the values in their string form
  std::vector<std::vector<TripleComponent>> _values;
  // The `_variables` as a string, in the format like so: "?x ?y ?z".
  std::string variablesToString() const;
  // The `_values` as a string, in the format like so: "(<v12> <v12> <v13>)
  // (<v21> <v22> <v23>)".
  std::string valuesToString() const;
};

/// A `SERVICE` clause.
struct Service {
 public:
  // The visible variables of the service clause.
  std::vector<Variable> visibleVariables_;
  // The URL of the service clause.
  TripleComponent::Iri serviceIri_;
  // The prologue (prefix definitions).
  std::string prologue_;
  // The body of the SPARQL query for the remote endpoint.
  std::string graphPatternAsString_;
  // The existence of the `SILENT`-keyword.
  bool silent_;
};

/// An internal pattern used in the `LOAD` update operation.
struct Load {
 public:
  ad_utility::httpUtils::Url url_;
  bool silent_;
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
  // If not `monostate`, then this group is a `GRAPH` clause, either with a
  // fixed graph IRI, or with a variable.
  using GraphSpec =
      std::variant<std::monostate, TripleComponent::Iri, Variable>;
  GraphSpec graphSpec_ = std::monostate{};
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

  // NOTE: The first two constructors are deliberately not `explicit` because
  // we want to used them like copy/move constructors (semantically, a
  // `Subquery` is like a `ParsedQuery`, just with an own type).
  Subquery(const ParsedQuery&);
  Subquery(ParsedQuery&&) noexcept;
  Subquery();
  ~Subquery();
  Subquery(const Subquery&);
  Subquery(Subquery&&) noexcept;
  Subquery& operator=(const Subquery&);
  Subquery& operator=(Subquery&&) noexcept;
  ParsedQuery& get();
  const ParsedQuery& get() const;
};

// A SPARQL `DESCRIBE` query.
struct Describe {
  using VarOrIri = std::variant<TripleComponent::Iri, Variable>;
  // The resources (variables or IRIs) that are to be described, for example
  // `?x` and `<y>` in `DESCRIBE ?x <y>`.
  std::vector<VarOrIri> resources_;
  // The FROM clauses of the DESCRIBE query
  DatasetClauses datasetClauses_;
  // The WHERE clause of the DESCRIBE query. It is used to compute the values
  // for the variables in the DESCRIBE clause.
  Subquery whereClause_;
};

struct TransPath {
  // The name of the left and right end of the transitive operation
  TripleComponent _left;
  TripleComponent _right;
  // The name of the left and right end of the subpath
  // TODO<joka921> Should this be a `Variable`? or `optional<Variable>`?
  TripleComponent _innerLeft;
  TripleComponent _innerRight;
  size_t _min = 0;
  size_t _max = 0;
  GraphPattern _childGraphPattern;
};

// A SPARQL Bind construct.
struct Bind {
  sparqlExpression::SparqlExpressionPimpl _expression;
  Variable _target;  // the variable to which the expression will be bound

  // Return all the variables that are used in the BIND expression (the target
  // variable as well as all variables from the expression). The lifetime of the
  // resulting `view` is bound to the lifetime of this `Bind` instance.
  auto containedVariables() const {
    auto result = _expression.containedVariables();
    result.push_back(&_target);
    return ad_utility::OwningView{std::move(result)} |
           ql::views::transform(ad_utility::dereference);
  }

  [[nodiscard]] string getDescriptor() const;
};

// TODO<joka921> Further refactor this, s.t. the whole `GraphPatternOperation`
// class actually becomes `using GraphPatternOperation = std::variant<...>`
using GraphPatternOperationVariant =
    std::variant<Optional, Union, Subquery, TransPath, Bind, BasicGraphPattern,
                 Values, Service, PathQuery, SpatialQuery, TextSearchQuery,
                 Minus, GroupGraphPattern, Describe>;
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
};

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_GRAPHPATTERNOPERATION_H
