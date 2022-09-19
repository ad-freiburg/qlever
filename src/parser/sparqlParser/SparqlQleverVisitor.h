// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors:
//   2021 - Johannes Kalmbach <kalmbacj@informatik.uni-freiburg.de>
//   2022   Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include <antlr4-runtime.h>

#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/SampleExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/Alias.h"
#include "parser/ConstructClause.h"
#include "parser/ParsedQuery.h"
#include "parser/RdfEscaping.h"
#include "parser/data/BlankNode.h"
#include "parser/data/Iri.h"
#include "parser/data/SolutionModifiers.h"
#include "parser/data/Types.h"
#include "parser/data/VarOrTerm.h"
#include "parser/sparqlParser/generated/SparqlAutomaticVisitor.h"
#include "util/HashMap.h"
#include "util/OverloadCallOperator.h"
#include "util/StringUtils.h"
#include "util/antlr/ANTLRErrorHandling.h"

template <typename T>
class Reversed {
  T& _iterable;

 public:
  explicit Reversed(T& iterable) : _iterable(iterable) {}

  auto begin() { return _iterable.rbegin(); };

  auto end() { return _iterable.rend(); }
};

/**
 * This class provides an empty implementation of SparqlVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the
 * available methods.
 */
class SparqlQleverVisitor {
  using GraphPatternOperation = parsedQuery::GraphPatternOperation;
  using Objects = ad_utility::sparql_types::Objects;
  using Tuples = ad_utility::sparql_types::Tuples;
  using PathTuples = ad_utility::sparql_types::PathTuples;
  using TripleWithPropertyPath =
      ad_utility::sparql_types::TripleWithPropertyPath;
  using Triples = ad_utility::sparql_types::Triples;
  using Node = ad_utility::sparql_types::Node;
  using ObjectList = ad_utility::sparql_types::ObjectList;
  using PropertyList = ad_utility::sparql_types::PropertyList;
  using OperationsAndFilters =
      std::pair<vector<GraphPatternOperation>, vector<SparqlFilter>>;
  using OperationOrFilterAndMaybeTriples =
      std::pair<std::variant<GraphPatternOperation, SparqlFilter>,
                std::optional<parsedQuery::BasicGraphPattern>>;
  using OperationOrFilter = std::variant<GraphPatternOperation, SparqlFilter>;
  using SubQueryAndMaybeValues =
      std::pair<parsedQuery::Subquery, std::optional<parsedQuery::Values>>;
  using PatternAndVisibleVariables =
      std::pair<ParsedQuery::GraphPattern, vector<Variable>>;
  size_t _blankNodeCounter = 0;
  int64_t numInternalVariables_ = 0;
  int64_t numGraphPatterns_ = 0;
  // A Stack of vector<Variable> that store the variables that are visible in a
  // query body. Each element corresponds to nested Queries that the parse is
  // currently parsing.
  std::vector<std::vector<Variable>> visibleVariables_{{}};

 public:
  using PrefixMap = ad_utility::HashMap<string, string>;
  using Parser = SparqlAutomaticParser;
  const PrefixMap& prefixMap() const { return _prefixMap; }
  SparqlQleverVisitor() = default;
  SparqlQleverVisitor(PrefixMap prefixMap) : _prefixMap{std::move(prefixMap)} {}
  using ExpressionPtr = sparqlExpression::SparqlExpression::Ptr;

  void setPrefixMapManually(PrefixMap map) { _prefixMap = std::move(map); }

 private:
  PrefixMap _prefixMap{};

  BlankNode newBlankNode() {
    std::string label = std::to_string(_blankNodeCounter);
    _blankNodeCounter++;
    // true means automatically generated
    return {true, std::move(label)};
  }

  // Process an IRI function call. This is used in both `visitFunctionCall` and
  // `visitIriOrFunction`.
  ExpressionPtr processIriFunctionCall(const std::string& iri,
                                       std::vector<ExpressionPtr> argList);

  // TODO: Remove addVisibleVariable(const string&) when all Types use the
  //  strong type `Variable`.
  void addVisibleVariable(string var);
  void addVisibleVariable(Variable var);

 public:
  // ___________________________________________________________________________
  ParsedQuery visit(Parser::QueryContext* ctx);

  // ___________________________________________________________________________
  PrefixMap visit(SparqlAutomaticParser::PrologueContext* ctx);

  // ___________________________________________________________________________
  SparqlPrefix visit(Parser::BaseDeclContext* ctx);

  // ___________________________________________________________________________
  SparqlPrefix visit(Parser::PrefixDeclContext* ctx);

  ParsedQuery visit(Parser::SelectQueryContext* ctx);

  SubQueryAndMaybeValues visit(Parser::SubSelectContext* ctx);

  parsedQuery::SelectClause visit(Parser::SelectClauseContext* ctx);

  std::variant<Variable, Alias> visit(Parser::VarOrAliasContext* ctx);

  Alias visit(Parser::AliasContext* ctx);

  Alias visit(Parser::AliasWithoutBracketsContext* ctx);

  ParsedQuery visit(Parser::ConstructQueryContext* ctx);

  [[noreturn]] ParsedQuery visit(Parser::DescribeQueryContext* ctx);

  [[noreturn]] ParsedQuery visit(Parser::AskQueryContext* ctx);

  [[noreturn]] void visit(Parser::DatasetClauseContext* ctx);

  [[noreturn]] void visit(Parser::DefaultGraphClauseContext* ctx);

  [[noreturn]] void visit(Parser::NamedGraphClauseContext* ctx);

  [[noreturn]] void visit(Parser::SourceSelectorContext* ctx);

  PatternAndVisibleVariables visit(Parser::WhereClauseContext* ctx);

  SolutionModifiers visit(Parser::SolutionModifierContext* ctx);

  vector<GroupKey> visit(Parser::GroupClauseContext* ctx);

  GroupKey visit(Parser::GroupConditionContext* ctx);

  vector<SparqlFilter> visit(Parser::HavingClauseContext* ctx);

  SparqlFilter visit(Parser::HavingConditionContext* ctx);

  vector<OrderKey> visit(Parser::OrderClauseContext* ctx);

  OrderKey visit(Parser::OrderConditionContext* ctx);

  LimitOffsetClause visit(Parser::LimitOffsetClausesContext* ctx);

  unsigned long long int visit(Parser::LimitClauseContext* ctx);

  unsigned long long int visit(Parser::OffsetClauseContext* ctx);

  unsigned long long int visit(Parser::TextLimitClauseContext* ctx);

  std::optional<parsedQuery::Values> visit(Parser::ValuesClauseContext* ctx);

  Triples visit(Parser::TriplesTemplateContext* ctx);

  ParsedQuery::GraphPattern visit(Parser::GroupGraphPatternContext* ctx);

  OperationsAndFilters visit(Parser::GroupGraphPatternSubContext* ctx);

  OperationOrFilterAndMaybeTriples visit(
      Parser::GraphPatternNotTriplesAndMaybeTriplesContext* ctx);

  parsedQuery::BasicGraphPattern visit(Parser::TriplesBlockContext* ctx);

  // Filter clauses are no independent graph patterns themselves, but their
  // scope is always the complete graph pattern enclosing them.
  OperationOrFilter visit(Parser::GraphPatternNotTriplesContext* ctx);

  parsedQuery::GraphPatternOperation visit(
      Parser::OptionalGraphPatternContext* ctx);

  [[noreturn]] parsedQuery::GraphPatternOperation visit(
      Parser::GraphGraphPatternContext* ctx);

  [[noreturn]] parsedQuery::GraphPatternOperation visit(
      Parser::ServiceGraphPatternContext* ctx);

  parsedQuery::GraphPatternOperation visit(Parser::BindContext* ctx);

  parsedQuery::GraphPatternOperation visit(Parser::InlineDataContext* ctx);

  parsedQuery::Values visit(Parser::DataBlockContext* ctx);

  parsedQuery::SparqlValues visit(Parser::InlineDataOneVarContext* ctx);

  parsedQuery::SparqlValues visit(Parser::InlineDataFullContext* ctx);

  vector<std::string> visit(Parser::DataBlockSingleContext* ctx);

  std::string visit(Parser::DataBlockValueContext* ctx);

  GraphPatternOperation visit(Parser::MinusGraphPatternContext* ctx);

  GraphPatternOperation visit(Parser::GroupOrUnionGraphPatternContext* ctx);

  SparqlFilter visit(Parser::FilterRContext* ctx);

  ExpressionPtr visit(Parser::ConstraintContext* ctx);

  ExpressionPtr visit(SparqlAutomaticParser::FunctionCallContext* ctx);

  vector<ExpressionPtr> visit(Parser::ArgListContext* ctx);

  [[noreturn]] void visit(Parser::ExpressionListContext* ctx);

  std::optional<parsedQuery::ConstructClause> visit(
      Parser::ConstructTemplateContext* ctx);

  Triples visit(Parser::ConstructTriplesContext* ctx);

  Triples visit(Parser::TriplesSameSubjectContext* ctx);

  PropertyList visit(Parser::PropertyListContext* ctx);

  PropertyList visit(Parser::PropertyListNotEmptyContext* ctx);

  VarOrTerm visit(Parser::VerbContext* ctx);

  ObjectList visit(Parser::ObjectListContext* ctx);

  Node visit(Parser::ObjectRContext* ctx);

  vector<TripleWithPropertyPath> visit(
      Parser::TriplesSameSubjectPathContext* ctx);

  PathTuples visit(Parser::TupleWithoutPathContext* ctx);

  PathTuples visit(Parser::TupleWithPathContext* ctx);

  [[noreturn]] void throwCollectionsAndBlankNodePathsNotSupported(auto* ctx) {
    reportError(
        ctx, "( ... ) and [ ... ] in triples are not yet supported by QLever.");
  }

  std::optional<PathTuples> visit(
      SparqlAutomaticParser::PropertyListPathContext* ctx);

  PathTuples visit(Parser::PropertyListPathNotEmptyContext* ctx);

  PropertyPath visit(Parser::VerbPathContext* ctx);

  Variable visit(Parser::VerbSimpleContext* ctx);

  ad_utility::sparql_types::VarOrPath visit(
      Parser::VerbPathOrSimpleContext* ctx);

  ObjectList visit(Parser::ObjectListPathContext* ctx);

  VarOrTerm visit(Parser::ObjectPathContext* ctx);

  PropertyPath visit(Parser::PathContext* ctx);

  PropertyPath visit(Parser::PathAlternativeContext* ctx);

  PropertyPath visit(Parser::PathSequenceContext* ctx);

  PropertyPath visit(Parser::PathEltContext* ctx);

  PropertyPath visit(Parser::PathEltOrInverseContext* ctx);

  [[noreturn]] void visit(Parser::PathModContext* ctx);

  PropertyPath visit(Parser::PathPrimaryContext* ctx);

  [[noreturn]] PropertyPath visit(Parser::PathNegatedPropertySetContext*);

  [[noreturn]] PropertyPath visit(Parser::PathOneInPropertySetContext* ctx);

  /// Note that in the SPARQL grammar the INTEGER rule refers to positive
  /// integers without an explicit sign.
  unsigned long long int visit(Parser::IntegerContext* ctx);

  Node visit(Parser::TriplesNodeContext* ctx);

  Node visit(Parser::BlankNodePropertyListContext* ctx);

  [[noreturn]] void visit(Parser::TriplesNodePathContext* ctx);

  [[noreturn]] void visit(Parser::BlankNodePropertyListPathContext* ctx);

  Node visit(Parser::CollectionContext* ctx);

  [[noreturn]] void visit(Parser::CollectionPathContext* ctx);

  Node visit(Parser::GraphNodeContext* ctx);

  VarOrTerm visit(Parser::GraphNodePathContext* ctx);

  VarOrTerm visit(Parser::VarOrTermContext* ctx);

  VarOrTerm visit(Parser::VarOrIriContext* ctx);

  Variable visit(Parser::VarContext* ctx);

  GraphTerm visit(Parser::GraphTermContext* ctx);

  ExpressionPtr visit(Parser::ExpressionContext* ctx);

  std::vector<std::string> visitOperationTags(
      const std::vector<antlr4::tree::ParseTree*>& childContexts,
      const ad_utility::HashSet<string>& allowedTags) {
    std::vector<std::string> operations;

    for (const auto& c : childContexts) {
      if (allowedTags.contains(c->getText())) {
        operations.emplace_back(c->getText());
      }
    }
    return operations;
  }

  ExpressionPtr visit(Parser::ConditionalOrExpressionContext* ctx);

  ExpressionPtr visit(Parser::ConditionalAndExpressionContext* ctx);

  ExpressionPtr visit(Parser::ValueLogicalContext* ctx);

  ExpressionPtr visit(Parser::RelationalExpressionContext* ctx);

  ExpressionPtr visit(Parser::NumericExpressionContext* ctx);

  template <typename Expr>
  ExpressionPtr createExpression(auto... children) {
    return std::make_unique<Expr>(
        std::array<ExpressionPtr, sizeof...(children)>{std::move(children)...});
  }

  ExpressionPtr visit(Parser::AdditiveExpressionContext* ctx);

  [[noreturn]] void visit(
      Parser::StrangeMultiplicativeSubexprOfAdditiveContext* ctx);

  ExpressionPtr visit(Parser::MultiplicativeExpressionContext* ctx);

  ExpressionPtr visit(Parser::UnaryExpressionContext* ctx);

  ExpressionPtr visit(Parser::PrimaryExpressionContext* ctx);

  ExpressionPtr visit(Parser::BrackettedExpressionContext* ctx);

  ExpressionPtr visit(Parser::BuiltInCallContext* ctx);

  [[noreturn]] void visit(Parser::RegexExpressionContext* ctx);

  [[noreturn]] void visit(Parser::SubstringExpressionContext* ctx);

  [[noreturn]] void visit(Parser::StrReplaceExpressionContext* ctx);

  [[noreturn]] void visit(Parser::ExistsFuncContext* ctx);

  [[noreturn]] void visit(Parser::NotExistsFuncContext* ctx);

  ExpressionPtr visit(Parser::AggregateContext* ctx);

  ExpressionPtr visit(Parser::IriOrFunctionContext* ctx);

  std::string visit(Parser::RdfLiteralContext* ctx);

  std::variant<int64_t, double> visit(Parser::NumericLiteralContext* ctx);

  std::variant<int64_t, double> visit(
      Parser::NumericLiteralUnsignedContext* ctx);

  std::variant<int64_t, double> visit(
      Parser::NumericLiteralPositiveContext* ctx);

  std::variant<int64_t, double> visit(
      Parser::NumericLiteralNegativeContext* ctx);

  bool visit(Parser::BooleanLiteralContext* ctx);

  string visit(Parser::StringContext* ctx);

  string visit(Parser::IriContext* ctx);

  string visit(Parser::IrirefContext* ctx);

  string visit(Parser::PrefixedNameContext* ctx);

  BlankNode visit(Parser::BlankNodeContext* ctx);

  string visit(Parser::PnameLnContext* ctx);

  string visit(Parser::PnameNsContext* ctx);

  // Visit a vector of Contexts and return the corresponding results as a
  // vector. Only Contexts that have a matching `visit` are supported.
  template <typename Ctx>
  auto visitVector(const std::vector<Ctx*>& childContexts)
      -> std::vector<decltype(visit(childContexts[0]))>;

  // Check that exactly one of the `ctxs` is not `null`, visit that context,
  // cast the result to `Out` and return it. Requires that for all of the
  // `ctxs`, `visit(ctxs)` is convertible to `Out`.
  template <typename Out, typename... Contexts>
  Out visitAlternative(Contexts*... ctxs);

  // Returns `std::nullopt` if the pointer is the `nullptr`. Otherwise return
  // `visit(ctx)`.
  template <typename Ctx>
  auto visitOptional(Ctx* ctx) -> std::optional<decltype(visit(ctx))>;

  /// If `ctx` is not `nullptr`, visit it, convert the result to `Intermediate`
  /// and assign it to `*target`. The case where `Intermediate!=Target` is
  /// useful, when the result of `visit(ctx)` cannot be converted to `Target`,
  /// but the conversion chain `VisitResult -> Intermediate -> Target` is valid.
  /// For example when `visit(ctx)` yields `A`, `A` is explicitly convertible to
  /// `B` and `Target` is `optional<B>`, then `B` has to be specified as
  /// `Intermediate` (see for example the implementation of `visitAlternative`).
  template <typename Target, typename Intermediate = Target, typename Ctx>
  void visitIf(Target* target, Ctx* ctx);

  [[noreturn]] void reportError(antlr4::ParserRuleContext* ctx,
                                const std::string& msg);

  // Parse both `ConstructTriplesContext` and `TriplesTemplateContext` because
  // they have the same structure.
  template <typename Context>
  Triples parseTriplesConstruction(Context* ctx);
};
