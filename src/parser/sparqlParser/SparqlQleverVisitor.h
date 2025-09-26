// Copyright 2021 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@tf.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_SPARQLPARSER_SPARQLQLEVERVISITOR_H
#define QLEVER_SRC_PARSER_SPARQLPARSER_SPARQLQLEVERVISITOR_H

#include <antlr4-runtime.h>
#include <gtest/gtest_prod.h>

#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/StdevExpression.h"
#include "parser/data/GraphRef.h"
#include "parser/sparqlParser/DatasetClause.h"
#undef EOF
#include "parser/Quads.h"
#include "parser/sparqlParser/generated/SparqlAutomaticVisitor.h"
#define EOF std::char_traits<char>::eof()
#include "util/BlankNodeManager.h"

/**
 * This is a visitor that takes the parse tree from ANTLR and transforms it into
 * a `ParsedQuery`.
 */
class SparqlQleverVisitor {
 public:
  using GraphPatternOperation = parsedQuery::GraphPatternOperation;
  using Objects = ad_utility::sparql_types::Objects;
  using PredicateObjectPairs = ad_utility::sparql_types::PredicateObjectPairs;
  using VarOrIri = ad_utility::sparql_types::VarOrIri;
  using PathObjectPairs = ad_utility::sparql_types::PathObjectPairs;
  using PathObjectPairsAndTriples =
      ad_utility::sparql_types::PathObjectPairsAndTriples;
  using TripleWithPropertyPath =
      ad_utility::sparql_types::TripleWithPropertyPath;
  using Triples = ad_utility::sparql_types::Triples;
  using SubjectOrObjectAndTriples =
      ad_utility::sparql_types::SubjectOrObjectAndTriples;
  using SubjectOrObjectAndPathTriples =
      ad_utility::sparql_types::SubjectOrObjectAndPathTriples;
  using ObjectsAndTriples = ad_utility::sparql_types::ObjectsAndTriples;
  using ObjectsAndPathTriples = ad_utility::sparql_types::ObjectsAndPathTriples;
  using PredicateObjectPairsAndTriples =
      ad_utility::sparql_types::PredicateObjectPairsAndTriples;
  using OperationsAndFilters =
      std::pair<std::vector<GraphPatternOperation>, std::vector<SparqlFilter>>;
  using OperationOrFilterAndMaybeTriples =
      std::pair<std::variant<GraphPatternOperation, SparqlFilter>,
                std::optional<parsedQuery::BasicGraphPattern>>;
  using OperationOrFilter = std::variant<GraphPatternOperation, SparqlFilter>;
  using SubQueryAndMaybeValues =
      std::pair<parsedQuery::Subquery, std::optional<parsedQuery::Values>>;
  using PatternAndVisibleVariables =
      std::pair<ParsedQuery::GraphPattern, std::vector<Variable>>;
  using SparqlExpressionPimpl = sparqlExpression::SparqlExpressionPimpl;
  using PrefixMap = ad_utility::HashMap<std::string, std::string>;
  using Parser = SparqlAutomaticParser;
  using ExpressionPtr = sparqlExpression::SparqlExpression::Ptr;
  using IntOrDouble = std::variant<int64_t, double>;

  enum struct DisableSomeChecksOnlyForTesting { False, True };

 private:
  // NOTE: adjust `resetStateForMultipleUpdates()` when adding or updating
  // members.

  // The blank node manager is needed to handle blank nodes in the templates of
  // UPDATE requests.
  ad_utility::BlankNodeManager* blankNodeManager_;

  // Needed to efficiently encode common IRIs directly into the ID.
  const EncodedIriManager* encodedIriManager_;

  // Convert a GraphTerm to TripleComponent with IRI encoding support
  TripleComponent graphTermToTripleComponentWithEncoding(
      const GraphTerm& graphTerm) const;

  size_t _blankNodeCounter = 0;
  // Counter that increments for every variable generated using
  // `getNewInternalVariable` to ensure distinctness.
  int64_t numInternalVariables_ = 0;
  int64_t numGraphPatterns_ = 0;
  // The visible variables in the order in which they are encountered in the
  // query. This may contain duplicates. A variable is added via
  // `addVisibleVariable`.
  std::vector<Variable> visibleVariables_{};

  // The `FROM` and `FROM NAMED` clauses of the query that is currently
  // being parsed. Those are inherited by certain constructs, which are
  // otherwise independent (in particular, `EXISTS` and `DESCRIBE`). If
  // `datasetsAreFixed_` is set then datasets are set outside the operation
  // itself and cannot be overwritten in the operation through `FROM` and `FROM
  // NAMED`.
  ParsedQuery::DatasetClauses activeDatasetClauses_;
  bool datasetsAreFixed_ = false;

  // The map from prefixes to their full IRIs.
  PrefixMap prefixMap_{};

  // The `BASE` IRI of the query if any.
  ad_utility::triple_component::Iri baseIri_{};

  // We need to remember the prologue (prefix declarations) when we encounter it
  // because we need it when we encounter a SERVICE query. When there is no
  // prologue, this string simply remains empty.
  std::string prologueString_;

  DisableSomeChecksOnlyForTesting disableSomeChecksOnlyForTesting_;

  // This is the parsed query so far. Currently, this only contains information
  // about the number of internal variables that have already been assigned.
  ParsedQuery parsedQuery_;

  // In most contexts, blank node labels in a SPARQL query are actually
  // variables. But sometimes they are in fact blank node labels (e.g. in
  // CONSTRUCT or UPDATE templates) and sometimes they are simply forbidden by
  // the standard (e.g. in DELETE clauses). The following enum keeps track of
  // which of these modes is currently active.
  enum struct TreatBlankNodesAs { InternalVariables, BlankNodes, Illegal };
  TreatBlankNodesAs treatBlankNodesAs_ = TreatBlankNodesAs::InternalVariables;

  // Set the blank node treatment (see above) the `newValue` and return a
  // cleanup object that in its destructor restores the original blank node
  // treatment. If blank nodes were already illegal before calling this
  // function, then they remain illegal (because "blank nodes forbidden" from an
  // outer scope is more important than a local rule). This behavior is used
  // when parsing DELETE requests, which don't support updates, but recursively
  // use the same parser rules as INSERT requests which do support updates, but
  // under some circumstances require them to be treated as blank nodes instead
  // of internal variables.
  [[nodiscard]] auto setBlankNodeTreatmentForScope(TreatBlankNodesAs newValue) {
    bool wasIllegal = treatBlankNodesAs_ == TreatBlankNodesAs::Illegal;
    auto previous = wasIllegal ? treatBlankNodesAs_
                               : std::exchange(treatBlankNodesAs_, newValue);
    return absl::Cleanup{[previous, this]() { treatBlankNodesAs_ = previous; }};
  }

  // NOTE: adjust `resetStateForMultipleUpdates()` when adding or updating
  // members.

  // Resets the Visitors state between updates. This resets everything except
  // prefix and base, because those are shared between consecutive updates.
  void resetStateForMultipleUpdates();

  // Turns a vector of `SubjectOrObjectAndTriples` into a single
  // `SubjectsOrObjectsAndTriples` object that represents an RDF collection.
  template <typename TripleType, typename Func>
  TripleType toRdfCollection(std::vector<TripleType> elements,
                             Func iriStringToPredicate);

 public:
  // If `datasetOverride` contains datasets, then the datasets in
  // the operation itself are ignored. This is used for the datasets from the
  // url parameters which override those in the operation.
  explicit SparqlQleverVisitor(
      ad_utility::BlankNodeManager* bnodeManager,
      const EncodedIriManager* encodedIriManager, PrefixMap prefixMap,
      std::optional<ParsedQuery::DatasetClauses> datasetOverride,
      DisableSomeChecksOnlyForTesting disableSomeChecksOnlyForTesting =
          DisableSomeChecksOnlyForTesting::False)
      : blankNodeManager_{bnodeManager},
        encodedIriManager_{encodedIriManager},
        prefixMap_{std::move(prefixMap)},
        disableSomeChecksOnlyForTesting_{disableSomeChecksOnlyForTesting} {
    if (datasetOverride.has_value()) {
      activeDatasetClauses_ = std::move(*datasetOverride);
      datasetsAreFixed_ = true;
    }
    AD_CORRECTNESS_CHECK(blankNodeManager_ != nullptr);
  }

  const PrefixMap& prefixMap() const { return prefixMap_; }
  void setPrefixMapManually(PrefixMap map) { prefixMap_ = std::move(map); }

  void setParseModeToInsideConstructTemplateForTesting() {
    treatBlankNodesAs_ = TreatBlankNodesAs::BlankNodes;
  }

  // ___________________________________________________________________________
  ParsedQuery visit(Parser::QueryContext* ctx);

  // ___________________________________________________________________________
  void visit(Parser::PrologueContext* ctx);

  // ___________________________________________________________________________
  void visit(Parser::BaseDeclContext* ctx);

  // ___________________________________________________________________________
  void visit(Parser::PrefixDeclContext* ctx);

  ParsedQuery visit(Parser::SelectQueryContext* ctx);

  SubQueryAndMaybeValues visit(Parser::SubSelectContext* ctx);

  parsedQuery::SelectClause visit(Parser::SelectClauseContext* ctx);

  std::variant<Variable, Alias> visit(Parser::VarOrAliasContext* ctx);

  Alias visit(Parser::AliasContext* ctx);

  Alias visit(Parser::AliasWithoutBracketsContext* ctx);

  ParsedQuery visit(Parser::ConstructQueryContext* ctx);

  ParsedQuery visit(Parser::DescribeQueryContext* ctx);

  ParsedQuery visit(Parser::AskQueryContext* ctx);

  DatasetClause visit(Parser::DatasetClauseContext* ctx);

  TripleComponent::Iri visit(Parser::DefaultGraphClauseContext* ctx);

  TripleComponent::Iri visit(Parser::SourceSelectorContext* ctx);

  TripleComponent::Iri visit(Parser::NamedGraphClauseContext* ctx);

  PatternAndVisibleVariables visit(Parser::WhereClauseContext* ctx);

  // Parse the WHERE clause represented by the `whereClauseContext` and store
  // its result (the GroupGraphPattern representing the where clause + the set
  // of visible variables) in the `query`.
  void visitWhereClause(Parser::WhereClauseContext* whereClauseContext,
                        ParsedQuery& query);

  SolutionModifiers visit(Parser::SolutionModifierContext* ctx);

  std::vector<GroupKey> visit(Parser::GroupClauseContext* ctx);

  GroupKey visit(Parser::GroupConditionContext* ctx);

  std::vector<SparqlFilter> visit(Parser::HavingClauseContext* ctx);

  SparqlFilter visit(Parser::HavingConditionContext* ctx);

  OrderClause visit(Parser::OrderClauseContext* ctx);

  OrderKey visit(Parser::OrderConditionContext* ctx);

  LimitOffsetClause visit(Parser::LimitOffsetClausesContext* ctx);

  static uint64_t visit(Parser::LimitClauseContext* ctx);

  static uint64_t visit(Parser::OffsetClauseContext* ctx);

  static uint64_t visit(Parser::TextLimitClauseContext* ctx);

  std::optional<parsedQuery::Values> visit(Parser::ValuesClauseContext* ctx);

  std::vector<ParsedQuery> visit(Parser::UpdateContext* ctx);

  std::vector<ParsedQuery> visit(Parser::Update1Context* ctx);

  ParsedQuery visit(Parser::LoadContext* ctx);

  ParsedQuery visit(Parser::ClearContext* ctx);

  ParsedQuery visit(Parser::DropContext* ctx);

  static std::vector<ParsedQuery> visit(const Parser::CreateContext* ctx);

  // Although only 0 or 1 ParsedQuery are ever returned, the vector makes the
  // interface much simpler.
  std::vector<ParsedQuery> visit(Parser::AddContext* ctx);

  std::vector<ParsedQuery> visit(Parser::MoveContext* ctx);

  std::vector<ParsedQuery> visit(Parser::CopyContext* ctx);

  updateClause::GraphUpdate visit(Parser::InsertDataContext* ctx);

  updateClause::GraphUpdate visit(Parser::DeleteDataContext* ctx);

  ParsedQuery visit(Parser::DeleteWhereContext* ctx);

  ParsedQuery visit(Parser::ModifyContext* ctx);

  Quads visit(Parser::DeleteClauseContext* ctx);

  Quads visit(Parser::InsertClauseContext* ctx);

  GraphOrDefault visit(Parser::GraphOrDefaultContext* ctx);

  GraphRef visit(Parser::GraphRefContext* ctx);

  GraphRefAll visit(Parser::GraphRefAllContext* ctx);

  Quads visit(Parser::QuadPatternContext* ctx);

  Quads visit(Parser::QuadDataContext* ctx);

  Quads visit(Parser::QuadsContext* ctx);

  Quads::GraphBlock visit(Parser::QuadsNotTriplesContext* ctx);

  Triples visit(Parser::TriplesTemplateContext* ctx);

  // Limit the variables in EXISTS expressions of the filter to the ones that
  // are already known outside the EXISTS expression, so that the filter isn't
  // optimized away because not all variables are covered.
  void selectExistsVariables(SparqlFilter& filter) const;

  ParsedQuery::GraphPattern visit(Parser::GroupGraphPatternContext* ctx);

  OperationsAndFilters visit(Parser::GroupGraphPatternSubContext* ctx);

  OperationOrFilterAndMaybeTriples visit(
      Parser::GraphPatternNotTriplesAndMaybeTriplesContext* ctx);

  parsedQuery::BasicGraphPattern visit(Parser::TriplesBlockContext* graphTerm);

  // Filter clauses are no independent graph patterns themselves, but their
  // scope is always the complete graph pattern enclosing them.
  OperationOrFilter visit(Parser::GraphPatternNotTriplesContext* ctx);

  parsedQuery::GraphPatternOperation visit(
      Parser::OptionalGraphPatternContext* ctx);

  parsedQuery::GraphPatternOperation visit(
      Parser::GraphGraphPatternContext* ctx);

  parsedQuery::GraphPatternOperation visit(
      Parser::ServiceGraphPatternContext* ctx);

  // Visitor functions for special builtin features that are triggered via
  // `SERVICE` requests with "magic" IRIs.
  parsedQuery::GraphPatternOperation visitPathQuery(
      Parser::ServiceGraphPatternContext* ctx);

  GraphPatternOperation visitSpatialQuery(
      Parser::ServiceGraphPatternContext* ctx);

  parsedQuery::GraphPatternOperation visitTextSearchQuery(
      Parser::ServiceGraphPatternContext* ctx);

  GraphPatternOperation visitNamedCachedResult(
      const TripleComponent::Iri& target,
      Parser::ServiceGraphPatternContext* ctx);

  // Parse the body of a `MagicServiceQuery`, in particular call
  // `addBasicGraphPattern` and `addGraph` for the contents of the body, and
  // throw an exception if an unsupported element is encountered. This function
  // implements common functionality of `visitNamedCachedResult`,
  // `visitTextQuery`, ... above.
  void parseBodyOfMagicServiceQuery(parsedQuery::MagicServiceQuery& target,
                                    Parser::ServiceGraphPatternContext* ctx,
                                    std::string_view operationName);

  parsedQuery::GraphPatternOperation visit(Parser::BindContext* ctx);

  parsedQuery::GraphPatternOperation visit(Parser::InlineDataContext* ctx);

  parsedQuery::Values visit(Parser::DataBlockContext* ctx);

  parsedQuery::SparqlValues visit(Parser::InlineDataOneVarContext* ctx);

  parsedQuery::SparqlValues visit(Parser::InlineDataFullContext* ctx);

  std::vector<TripleComponent> visit(Parser::DataBlockSingleContext* ctx);

  TripleComponent visit(Parser::DataBlockValueContext* ctx);

  GraphPatternOperation visit(Parser::MinusGraphPatternContext* ctx);

  GraphPatternOperation visit(Parser::GroupOrUnionGraphPatternContext* ctx);

  SparqlFilter visit(Parser::FilterRContext* ctx);

  ExpressionPtr visit(Parser::ConstraintContext* ctx);

  ExpressionPtr visit(Parser::FunctionCallContext* ctx);

  std::vector<ExpressionPtr> visit(Parser::ArgListContext* ctx);

  std::vector<ExpressionPtr> visit(Parser::ExpressionListContext* ctx);

  std::optional<parsedQuery::ConstructClause> visit(
      Parser::ConstructTemplateContext* ctx);

  Triples visit(Parser::ConstructTriplesContext* ctx);

  Triples visit(Parser::TriplesSameSubjectContext* ctx);

  PredicateObjectPairsAndTriples visit(Parser::PropertyListContext* ctx);

  PredicateObjectPairsAndTriples visit(
      Parser::PropertyListNotEmptyContext* ctx);

  GraphTerm visit(Parser::VerbContext* ctx);

  ObjectsAndTriples visit(Parser::ObjectListContext* ctx);

  SubjectOrObjectAndTriples visit(Parser::ObjectRContext* ctx);

  std::vector<TripleWithPropertyPath> visit(
      Parser::TriplesSameSubjectPathContext* ctx);

  std::optional<PathObjectPairsAndTriples> visit(
      Parser::PropertyListPathContext* ctx);

  PathObjectPairsAndTriples visit(Parser::PropertyListPathNotEmptyContext* ctx);

  PropertyPath visit(Parser::VerbPathContext* ctx);

  static Variable visit(Parser::VerbSimpleContext* ctx);

  PathObjectPairsAndTriples visit(Parser::TupleWithoutPathContext* term);

  PathObjectPairsAndTriples visit(Parser::TupleWithPathContext* ctx);

  ad_utility::sparql_types::VarOrPath visit(
      Parser::VerbPathOrSimpleContext* ctx);

  ObjectsAndPathTriples visit(Parser::ObjectListPathContext* ctx);

  SubjectOrObjectAndPathTriples visit(Parser::ObjectPathContext* ctx);

  PropertyPath visit(Parser::PathContext* ctx);

  PropertyPath visit(Parser::PathAlternativeContext* ctx);

  PropertyPath visit(Parser::PathSequenceContext* ctx);

  PropertyPath visit(Parser::PathEltContext* ctx);

  PropertyPath visit(Parser::PathEltOrInverseContext* ctx);

  // Turn the modifiers `+`, `*`, `?` into a pair of integers that indicate the
  // lower and upper bounds of the path length.
  static std::pair<size_t, size_t> visit(Parser::PathModContext* ctx);

  PropertyPath visit(Parser::PathPrimaryContext* ctx);

  PropertyPath visit(Parser::PathNegatedPropertySetContext*);

  PropertyPath visit(Parser::PathOneInPropertySetContext* ctx);

  /// Note that in the SPARQL grammar the INTEGER rule refers to positive
  /// integers without an explicit sign.
  static uint64_t visit(Parser::IntegerContext* ctx);

  SubjectOrObjectAndTriples visit(Parser::TriplesNodeContext* ctx);

  SubjectOrObjectAndTriples visit(Parser::BlankNodePropertyListContext* ctx);

  SubjectOrObjectAndPathTriples visit(Parser::TriplesNodePathContext* ctx);

  SubjectOrObjectAndPathTriples visit(
      Parser::BlankNodePropertyListPathContext* ctx);

  SubjectOrObjectAndTriples visit(Parser::CollectionContext* ctx);

  SubjectOrObjectAndPathTriples visit(Parser::CollectionPathContext* ctx);

  SubjectOrObjectAndTriples visit(Parser::GraphNodeContext* ctx);

  SubjectOrObjectAndPathTriples visit(Parser::GraphNodePathContext* ctx);

  GraphTerm visit(Parser::VarOrTermContext* ctx);

  VarOrIri visit(Parser::VarOrIriContext* ctx);

  static Variable visit(Parser::VarContext* ctx);

  GraphTerm visit(Parser::GraphTermContext* ctx);

  ExpressionPtr visit(Parser::ExpressionContext* ctx);

  ExpressionPtr visit(Parser::ConditionalOrExpressionContext* ctx);

  ExpressionPtr visit(Parser::ConditionalAndExpressionContext* ctx);

  ExpressionPtr visit(Parser::ValueLogicalContext* ctx);

  ExpressionPtr visit(Parser::RelationalExpressionContext* ctx);

  ExpressionPtr visit(Parser::NumericExpressionContext* ctx);

  ExpressionPtr visit(Parser::AdditiveExpressionContext* ctx);

  // Helper structs, needed in the following `visit` functions. Combine an
  // explicit operator (+ - * /) with an expression.
  enum struct Operator { Plus, Minus, Multiply, Divide };
  struct OperatorAndExpression {
    Operator operator_;
    ExpressionPtr expression_;
  };

  OperatorAndExpression visit(
      Parser::MultiplicativeExpressionWithSignContext* ctx);

  OperatorAndExpression visit(Parser::PlusSubexpressionContext* ctx);

  OperatorAndExpression visit(Parser::MinusSubexpressionContext* ctx);

  OperatorAndExpression visit(
      Parser::MultiplicativeExpressionWithLeadingSignButNoSpaceContext* ctx);

  ExpressionPtr visit(Parser::MultiplicativeExpressionContext* ctx);

  OperatorAndExpression visit(Parser::MultiplyOrDivideExpressionContext* ctx);

  OperatorAndExpression visit(Parser::MultiplyExpressionContext* ctx);

  OperatorAndExpression visit(Parser::DivideExpressionContext* ctx);

  ExpressionPtr visit(Parser::UnaryExpressionContext* ctx);

  ExpressionPtr visit(Parser::PrimaryExpressionContext* ctx);

  ExpressionPtr visit(Parser::BrackettedExpressionContext* ctx);

  ExpressionPtr visit(Parser::BuiltInCallContext* ctx);

  ExpressionPtr visit(Parser::RegexExpressionContext* ctx);

  ExpressionPtr visit(Parser::LangExpressionContext* ctx);

  ExpressionPtr visit(Parser::SubstringExpressionContext* ctx);

  ExpressionPtr visit(Parser::StrReplaceExpressionContext* ctx);

  ExpressionPtr visitExists(Parser::GroupGraphPatternContext* pattern,
                            bool negate);

  ExpressionPtr visit(Parser::ExistsFuncContext* ctx);

  ExpressionPtr visit(Parser::NotExistsFuncContext* ctx);

  ExpressionPtr visit(Parser::AggregateContext* ctx);

  ExpressionPtr visit(Parser::IriOrFunctionContext* ctx);

  std::string visit(Parser::RdfLiteralContext* ctx);

  IntOrDouble visit(Parser::NumericLiteralContext* ctx);

  static IntOrDouble visit(Parser::NumericLiteralUnsignedContext* ctx);

  static IntOrDouble visit(Parser::NumericLiteralPositiveContext* ctx);

  static IntOrDouble visit(Parser::NumericLiteralNegativeContext* ctx);

  static bool visit(Parser::BooleanLiteralContext* ctx);

  static RdfEscaping::NormalizedRDFString visit(Parser::StringContext* ctx);

  TripleComponent::Iri visit(Parser::IriContext* ctx);

  std::string visit(Parser::IrirefContext* ctx) const;

  std::string visit(Parser::PrefixedNameContext* ctx);

  GraphTerm visit(Parser::BlankNodeContext* ctx);

  std::string visit(Parser::PnameLnContext* ctx);

  std::string visit(Parser::PnameNsContext* ctx);

  DatasetClause visit(Parser::UsingClauseContext* ctx);

 private:
  // Helper to assign variable `startTime_` a correctly formatted time string.
  static std::string currentTimeAsXsdString();

  // Member starTime_ is needed for the NOW expression. All calls within
  // the query execution reference it. The underlying date time format is e.g.:
  // 2011-01-10T14:45:13.815-05:00
  std::string startTime_ = currentTimeAsXsdString();

  template <typename Visitor, typename Ctx>
  static constexpr bool voidWhenVisited =
      std::is_void_v<decltype(std::declval<Visitor&>().visit(
          std::declval<Ctx*>()))>;

  // Return the next internal variable.
  Variable getNewInternalVariable();

  // Return a callable (not threadsafe) that when being called creates a new
  // internal variable by calling `getNewInternalVariable()` above.
  auto makeInternalVariableGenerator();

  // Create a new generated blank node.
  BlankNode newBlankNode();

  // Create a distinct `GraphTerm` that represents a blank node, when calling
  // this inside of a CONSTRUCT block, and a new variable otherwise, which is
  // required for graph pattern matching inside `WHERE` clauses.
  GraphTerm newBlankNodeOrVariable();

  // Turn a blank node `_:someBlankNode` into an internal variable
  // `?<prefixForInternalVariables>_someBlankNode`. This is required by the
  // SPARQL parser, because blank nodes in the bodies of SPARQL queries behave
  // like variables.
  static Variable blankNodeToInternalVariable(std::string_view blankNode);

  // Get the part of the original input string that pertains to the given
  // context. This is necessary because ANTLR's `getText()` only provides that
  // part with *all* whitespace removed. Preserving the whitespace is important
  // for readability (for example, in an error message), and even more so when
  // using such parts for further processing (like the body of a SERVICE query,
  // which is not valid SPARQL anymore when you remove all whitespace).
  static std::string getOriginalInputForContext(
      const antlr4::ParserRuleContext* context);

  // Process an IRI function call. This is used in both `visitFunctionCall` and
  // `visitIriOrFunction`.
  static ExpressionPtr processIriFunctionCall(
      const TripleComponent::Iri& iri, std::vector<ExpressionPtr> argList,
      const antlr4::ParserRuleContext*);

  void addVisibleVariable(Variable var);

  // Return the `SparqlExpressionPimpl` for a context that returns a
  // `ExpressionPtr` when visited. The descriptor is set automatically on the
  // `SparqlExpressionPimpl`.
  template <typename Context>
  SparqlExpressionPimpl visitExpressionPimpl(Context* ctx);

  template <typename Expr, typename... Children>
  ExpressionPtr createExpression(Children... children) {
    return std::make_unique<Expr>(
        std::array<ExpressionPtr, sizeof...(children)>{std::move(children)...});
  }

  CPP_template(typename Ctx)(
      requires SparqlQleverVisitor::voidWhenVisited<
          SparqlQleverVisitor, Ctx>) void visitVector(const std::vector<Ctx*>&
                                                          childContexts);

  // Call `visit` for each of the `childContexts` and return the results of
  // those calls as a `vector`.
  CPP_template(typename Ctx)(requires CPP_NOT(
      SparqlQleverVisitor::voidWhenVisited<
          SparqlQleverVisitor, Ctx>)) auto visitVector(const std::vector<Ctx*>&
                                                           childContexts)
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

  CPP_template(typename Ctx)(requires SparqlQleverVisitor::voidWhenVisited<
                             SparqlQleverVisitor, Ctx>) void visitIf(Ctx* ctx);

 public:
  [[noreturn]] static void reportError(const antlr4::ParserRuleContext* ctx,
                                       const std::string& msg);

  [[noreturn]] static void reportNotSupported(
      const antlr4::ParserRuleContext* ctx, const std::string& feature);

 private:
  // Parse both `ConstructTriplesContext` and `TriplesTemplateContext` because
  // they have the same structure.
  template <typename Context>
  Triples parseTriplesConstruction(Context* ctx);

  // If the triple is a special triple for the text index (i.e. its predicate is
  // either `ql:contains-word` or `ql:contains-entity`, register the magic
  // variables for the matching word and the score that will be created when
  // processing those triples in the query body, s.t. they can be selected as
  // part of the query result.
  template <typename Context>
  void setMatchingWordAndScoreVisibleIfPresent(
      Context* ctx, const TripleWithPropertyPath& triple);

  // If any of the variables used in `expression` did not appear previously in
  // the query, add a warning or throw an exception (depending on the setting of
  // the corresponding `RuntimeParameter`).
  template <typename Context>
  void warnOrThrowIfUnboundVariables(Context* ctx,
                                     const SparqlExpressionPimpl& expression,
                                     std::string_view clauseName);

  // Convert an instance of `Triples` to a `BasicGraphPattern` so it can be used
  // just like a WHERE clause. Most of the time this just changes the type and
  // stays semantically the same, but for blank nodes, this step converts them
  // into internal variables so they are interpreted correctly by the query
  // planner.
  parsedQuery::BasicGraphPattern toGraphPattern(
      const ad_utility::sparql_types::Triples& triples) const;

  // Set the datasets state of the visitor if `datasetsAreFixed_` is false.
  // `datasetsAreFixed_` controls whether the datasets can be modified from
  // inside the query or update. Then returns the currently active datasets.
  const parsedQuery::DatasetClauses& setAndGetDatasetClauses(
      const std::vector<DatasetClause>& clauses);

  // Construct a `ParsedQuery` that clears the given graph equivalent to
  // `DELETE WHERE { GRAPH graph { ?s ?p ?o } }`.
  ParsedQuery makeClear(const GraphRefAll& graph);

  // Construct a `ParsedQuery` that adds all triples from the source graph to
  // the target graph equivalent to `INSERT { GRAPH target { ?s ?p ?o } } WHERE
  // { GRAPH source { ?s ?p ?o } }`.
  ParsedQuery makeAdd(const GraphOrDefault& source,
                      const GraphOrDefault& target);

  // Construct `ParsedQuery`s that clear the target graph and then copy all
  // triples from the source graph to the target graph.
  std::vector<ParsedQuery> makeCopy(const GraphOrDefault& from,
                                    const GraphOrDefault& to);

  // Check that there are exactly 2 sub-clauses and returned the result from
  // visiting them.
  std::pair<GraphOrDefault, GraphOrDefault> visitFromTo(
      std::vector<Parser::GraphOrDefaultContext*> ctxs);

  FRIEND_TEST(SparqlParser, ensureExceptionOnInvalidGraphTerm);
};

#endif  // QLEVER_SRC_PARSER_SPARQLPARSER_SPARQLQLEVERVISITOR_H
