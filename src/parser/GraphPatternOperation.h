// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <limits>
#include <memory>

#include "engine/PathSearch.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/GraphPattern.h"
#include "parser/TripleComponent.h"
#include "parser/data/Variable.h"
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

class PathSearchException : public std::exception {
  std::string message_;

 public:
  explicit PathSearchException(const std::string& message)
      : message_(message) {}
  const char* what() const noexcept override { return message_.data(); }
};

// The PathQuery object holds intermediate information for the PathSearch.
// The PathSearchConfiguration requires concrete Ids. The vocabulary from the
// QueryPlanner is needed to translate the TripleComponents to ValueIds.
// Also, the members of the PathQuery have defaults and can be set after
// the object creation, simplifying the parsing process. If a required
// value has not been set during parsing, the method 'toPathSearchConfiguration'
// will throw an exception.
// All the error handling for the PathSearch happens in the PathQuery object.
// Thus, if a PathSearchConfiguration can be constructed, it is valid.
struct PathQuery {
  std::vector<TripleComponent> sources_;
  std::vector<TripleComponent> targets_;
  std::optional<Variable> start_;
  std::optional<Variable> end_;
  std::optional<Variable> pathColumn_;
  std::optional<Variable> edgeColumn_;
  std::vector<Variable> edgeProperties_;
  PathSearchAlgorithm algorithm_;

  GraphPattern childGraphPattern_;
  bool cartesian_ = true;
  std::optional<uint64_t> numPathsPerTarget_ = std::nullopt;

  /**
   * @brief Add a parameter to the PathQuery from the given triple.
   * The predicate of the triple determines the parameter name and the object
   * of the triple determines the parameter value. The subject is ignored.
   * Throws a PathSearchException if an unsupported algorithm is given or if the
   * predicate contains an unknown parameter name.
   *
   * @param triple A SparqlTriple that contains the parameter info
   */
  void addParameter(const SparqlTriple& triple);

  /**
   * @brief Add the parameters from a BasicGraphPattern to the PathQuery
   *
   * @param pattern
   */
  void addBasicPattern(const BasicGraphPattern& pattern);

  /**
   * @brief Add a GraphPatternOperation to the PathQuery. The pattern specifies
   * the edges of the graph that is used by the path search
   *
   * @param childGraphPattern
   */
  void addGraph(const GraphPatternOperation& childGraphPattern);

  /**
   * @brief Convert the vector of triple components into a SearchSide
   * The SeachSide can either be a variable or a list of Ids.
   * A PathSearchException is thrown if more than one variable is given.
   *
   * @param side A vector of TripleComponents, containing either exactly one
   *             Variable or zero or more ValueIds
   * @param vocab A Vocabulary containing the Ids of the TripleComponents.
   *              The Vocab is only used if the given vector contains IRIs.
   */
  std::variant<Variable, std::vector<Id>> toSearchSide(
      std::vector<TripleComponent> side, const Index::Vocab& vocab) const;

  /**
   * @brief Convert this PathQuery into a PathSearchConfiguration object.
   * This method checks if all required parameters are set and converts
   * the PathSearch sources and targets into SearchSides.
   * A PathSearchException is thrown if required parameters are missing.
   * The required parameters are start, end, pathColumn and edgeColumn.
   *
   * @param vocab A vocab containing the Ids of the IRIs in
   *              sources_ and targets_
   * @return A valid PathSearchConfiguration
   */
  PathSearchConfiguration toPathSearchConfiguration(
      const Index::Vocab& vocab) const;
};

// A SPARQL Bind construct.
struct Bind {
  sparqlExpression::SparqlExpressionPimpl _expression;
  Variable _target;  // the variable to which the expression will be bound

  // Return all the variables that are used in the BIND expression (the target
  // variable as well as all variables from the expression).
  cppcoro::generator<const Variable> containedVariables() const;

  [[nodiscard]] string getDescriptor() const;
};

// TODO<joka921> Further refactor this, s.t. the whole `GraphPatternOperation`
// class actually becomes `using GraphPatternOperation = std::variant<...>`
using GraphPatternOperationVariant =
    std::variant<Optional, Union, Subquery, TransPath, Bind, BasicGraphPattern,
                 Values, Service, PathQuery, Minus, GroupGraphPattern>;
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
