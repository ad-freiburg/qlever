// Copyright 2021 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@tf.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

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
#include "parser/data/GraphRef.h"
#include "parser/data/Iri.h"
#include "parser/data/SolutionModifiers.h"
#include "parser/data/Types.h"
#undef EOF
#include "parser/sparqlParser/generated/SparqlAutomaticVisitor.h"
#define EOF std::char_traits<char>::eof()
#include "util/HashMap.h"
#include "util/OverloadCallOperator.h"
#include "util/StringUtils.h"

template <typename T>
class Reversed {
  T& _iterable;

 public:
  explicit Reversed(T& iterable) : _iterable(iterable) {}

  auto begin() { return _iterable.rbegin(); };

  auto end() { return _iterable.rend(); }
};

/**
 * This is a visitor that takes the parse tree from ANTLR and transforms it into
 * a `ParsedQuery`.
 */
class SparqlQleverVisitor {
 public:
  using GraphPatternOperation = parsedQuery::GraphPatternOperation;
  using Objects = ad_utility::sparql_types::Objects;
  using PredicateObjectPairs = ad_utility::sparql_types::PredicateObjectPairs;
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
      std::pair<vector<GraphPatternOperation>, vector<SparqlFilter>>;
  using OperationOrFilterAndMaybeTriples =
      std::pair<std::variant<GraphPatternOperation, SparqlFilter>,
                std::optional<parsedQuery::BasicGraphPattern>>;
  using OperationOrFilter = std::variant<GraphPatternOperation, SparqlFilter>;
  using SubQueryAndMaybeValues =
      std::pair<parsedQuery::Subquery, std::optional<parsedQuery::Values>>;
  using PatternAndVisibleVariables =
      std::pair<ParsedQuery::GraphPattern, vector<Variable>>;
  using SparqlExpressionPimpl = sparqlExpression::SparqlExpressionPimpl;
  using PrefixMap = ad_utility::HashMap<string, string>;
  using Parser = SparqlAutomaticParser;
  using ExpressionPtr = sparqlExpression::SparqlExpression::Ptr;
  using IntOrDouble = std::variant<int64_t, double>;

  enum struct DisableSomeChecksOnlyForTesting { False, True };

 private:
  size_t _blankNodeCounter = 0;
  int64_t numInternalVariables_ = 0;
  int64_t numGraphPatterns_ = 0;
  // The visible variables in the order in which they are encountered in the
  // query. This may contain duplicates. A variable is added via
  // `addVisibleVariable`.
  std::vector<Variable> visibleVariables_{};
  PrefixMap prefixMap_{};
  // We need to remember the prologue (prefix declarations) when we encounter it
  // because we need it when we encounter a SERVICE query. When there is no
  // prologue, this string simply remains empty.
  std::string prologueString_;

  DisableSomeChecksOnlyForTesting disableSomeChecksOnlyForTesting_;

  // This is the parsed query so far. Currently, this only contains information
  // about the number of internal variables that have already been assigned.
  ParsedQuery parsedQuery_;

  // This is set to true if and only if we are only inside the template of a
  // CONSTRUCT query (the first {} before the WHERE clause). In this section the
  // meaning of blank and anonymous nodes is different.
  bool isInsideConstructTriples_ = false;

 public:
  SparqlQleverVisitor() = default;
  explicit SparqlQleverVisitor(
      PrefixMap prefixMap,
      DisableSomeChecksOnlyForTesting disableSomeChecksOnlyForTesting =
          DisableSomeChecksOnlyForTesting::False)
      : prefixMap_{std::move(prefixMap)},
        disableSomeChecksOnlyForTesting_{disableSomeChecksOnlyForTesting} {}

  const PrefixMap& prefixMap() const { return prefixMap_; }
  void setPrefixMapManually(PrefixMap map) { prefixMap_ = std::move(map); }

  void setParseModeToInsideConstructTemplateForTesting() {
    isInsideConstructTriples_ = true;
  }

  // ___________________________________________________________________________
  ParsedQuery visit(Parser::QueryOrUpdateContext* ctx);

  // ___________________________________________________________________________
  ParsedQuery visit(Parser::QueryContext* ctx);

  // ___________________________________________________________________________
  void visit(Parser::PrologueContext* ctx);

  // ___________________________________________________________________________
  [[noreturn]] static void visit(const Parser::BaseDeclContext* ctx);

  // ___________________________________________________________________________
  void visit(Parser::PrefixDeclContext* ctx);

  ParsedQuery visit(Parser::SelectQueryContext* ctx);

  SubQueryAndMaybeValues visit(Parser::SubSelectContext* ctx);

  parsedQuery::SelectClause visit(Parser::SelectClauseContext* ctx);

  std::variant<Variable, Alias> visit(Parser::VarOrAliasContext* ctx);

  Alias visit(Parser::AliasContext* ctx);

  Alias visit(Parser::AliasWithoutBracketsContext* ctx);

  ParsedQuery visit(Parser::ConstructQueryContext* ctx);

  // The parser rules for which the visit overload is annotated [[noreturn]]
  // will always throw an exception because the corresponding feature is not
  // (yet) supported by QLever. If they have return types other than void this
  // is to make the usage of abstractions like `visitAlternative` easier.
  [[noreturn]] static ParsedQuery visit(
      const Parser::DescribeQueryContext* ctx);

  [[noreturn]] static ParsedQuery visit(const Parser::AskQueryContext* ctx);

  [[noreturn]] static void visit(const Parser::DatasetClauseContext* ctx);

  [[noreturn]] static void visit(Parser::DefaultGraphClauseContext* ctx);

  [[noreturn]] static void visit(Parser::NamedGraphClauseContext* ctx);

  [[noreturn]] static void visit(Parser::SourceSelectorContext* ctx);

  PatternAndVisibleVariables visit(Parser::WhereClauseContext* ctx);

  SolutionModifiers visit(Parser::SolutionModifierContext* ctx);

  vector<GroupKey> visit(Parser::GroupClauseContext* ctx);

  GroupKey visit(Parser::GroupConditionContext* ctx);

  vector<SparqlFilter> visit(Parser::HavingClauseContext* ctx);

  SparqlFilter visit(Parser::HavingConditionContext* ctx);

  OrderClause visit(Parser::OrderClauseContext* ctx);

  OrderKey visit(Parser::OrderConditionContext* ctx);

  LimitOffsetClause visit(Parser::LimitOffsetClausesContext* ctx);

  static uint64_t visit(Parser::LimitClauseContext* ctx);

  static uint64_t visit(Parser::OffsetClauseContext* ctx);

  static uint64_t visit(Parser::TextLimitClauseContext* ctx);

  std::optional<parsedQuery::Values> visit(Parser::ValuesClauseContext* ctx);

  ParsedQuery visit(Parser::UpdateContext* ctx);

  ParsedQuery visit(Parser::Update1Context* ctx);

  [[noreturn]] void visit(const Parser::LoadContext* ctx) const;

  ParsedQuery visit(Parser::ClearContext* ctx);

  [[noreturn]] void visit(const Parser::DropContext* ctx) const;

  [[noreturn]] void visit(const Parser::CreateContext* ctx) const;

  [[noreturn]] void visit(const Parser::AddContext* ctx) const;

  [[noreturn]] void visit(const Parser::MoveContext* ctx) const;

  [[noreturn]] void visit(const Parser::CopyContext* ctx) const;

  vector<SparqlTripleSimple> visit(Parser::InsertDataContext* ctx);

  vector<SparqlTripleSimple> visit(Parser::DeleteDataContext* ctx);

  std::pair<vector<SparqlTripleSimple>, ParsedQuery::GraphPattern> visit(
      Parser::DeleteWhereContext* ctx);

  ParsedQuery visit(Parser::ModifyContext* ctx);

  vector<SparqlTripleSimple> visit(Parser::DeleteClauseContext* ctx);

  vector<SparqlTripleSimple> visit(Parser::InsertClauseContext* ctx);

  GraphOrDefault visit(Parser::GraphOrDefaultContext* ctx);

  GraphRef visit(Parser::GraphRefContext* ctx);

  GraphRefAll visit(Parser::GraphRefAllContext* ctx);

  vector<SparqlTripleSimple> visit(Parser::QuadPatternContext* ctx);

  vector<SparqlTripleSimple> visit(Parser::QuadDataContext* ctx);

  vector<SparqlTripleSimple> visit(Parser::QuadsContext* ctx);

  Triples visit(Parser::TriplesTemplateContext* ctx);

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

  [[noreturn]] static parsedQuery::GraphPatternOperation visit(
      const Parser::GraphGraphPatternContext* ctx);

  parsedQuery::Service visit(Parser::ServiceGraphPatternContext* ctx);

  parsedQuery::GraphPatternOperation visit(Parser::BindContext* ctx);

  parsedQuery::GraphPatternOperation visit(Parser::InlineDataContext* ctx);

  parsedQuery::Values visit(Parser::DataBlockContext* ctx);

  parsedQuery::SparqlValues visit(Parser::InlineDataOneVarContext* ctx);

  parsedQuery::SparqlValues visit(Parser::InlineDataFullContext* ctx);

  vector<TripleComponent> visit(Parser::DataBlockSingleContext* ctx);

  TripleComponent visit(Parser::DataBlockValueContext* ctx);

  GraphPatternOperation visit(Parser::MinusGraphPatternContext* ctx);

  GraphPatternOperation visit(Parser::GroupOrUnionGraphPatternContext* ctx);

  SparqlFilter visit(Parser::FilterRContext* ctx);

  ExpressionPtr visit(Parser::ConstraintContext* ctx);

  ExpressionPtr visit(Parser::FunctionCallContext* ctx);

  vector<ExpressionPtr> visit(Parser::ArgListContext* ctx);

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

  vector<TripleWithPropertyPath> visit(
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

  [[noreturn]] static void visit(Parser::PathModContext* ctx);

  PropertyPath visit(Parser::PathPrimaryContext* ctx);

  [[noreturn]] static PropertyPath visit(
      const Parser::PathNegatedPropertySetContext*);

  [[noreturn]] static PropertyPath visit(
      Parser::PathOneInPropertySetContext* ctx);

  /// Note that in the SPARQL grammar the INTEGER rule refers to positive
  /// integers without an explicit sign.
  static uint64_t visit(Parser::IntegerContext* ctx);

  SubjectOrObjectAndTriples visit(Parser::TriplesNodeContext* ctx);

  SubjectOrObjectAndTriples visit(Parser::BlankNodePropertyListContext* ctx);

  SubjectOrObjectAndPathTriples visit(Parser::TriplesNodePathContext* ctx);

  SubjectOrObjectAndPathTriples visit(
      Parser::BlankNodePropertyListPathContext* ctx);

  SubjectOrObjectAndTriples visit(Parser::CollectionContext* ctx);

  [[noreturn]] SubjectOrObjectAndPathTriples visit(
      Parser::CollectionPathContext* ctx);

  SubjectOrObjectAndTriples visit(Parser::GraphNodeContext* ctx);

  SubjectOrObjectAndPathTriples visit(Parser::GraphNodePathContext* ctx);

  GraphTerm visit(Parser::VarOrTermContext* ctx);

  GraphTerm visit(Parser::VarOrIriContext* ctx);

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

  [[noreturn]] static void visit(const Parser::ExistsFuncContext* ctx);

  [[noreturn]] static void visit(const Parser::NotExistsFuncContext* ctx);

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

  static string visit(Parser::IrirefContext* ctx);

  string visit(Parser::PrefixedNameContext* ctx);

  GraphTerm visit(Parser::BlankNodeContext* ctx);

  string visit(Parser::PnameLnContext* ctx);

  string visit(Parser::PnameNsContext* ctx);

 private:
  template <typename Visitor, typename Ctx>
  static constexpr bool voidWhenVisited =
      std::is_void_v<decltype(std::declval<Visitor&>().visit(
          std::declval<Ctx*>()))>;

  BlankNode newBlankNode() {
    std::string label = std::to_string(_blankNodeCounter);
    _blankNodeCounter++;
    // true means automatically generated
    return {true, std::move(label)};
  }

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

  [[noreturn]] static void throwCollectionsNotSupported(auto* ctx) {
    reportError(ctx, "( ... ) in triples is not yet supported by QLever.");
  }

  // Return the `SparqlExpressionPimpl` for a context that returns a
  // `ExpressionPtr` when visited. The descriptor is set automatically on the
  // `SparqlExpressionPimpl`.
  SparqlExpressionPimpl visitExpressionPimpl(auto* ctx,
                                             bool allowLanguageFilters = false);

  template <typename Expr>
  ExpressionPtr createExpression(auto... children) {
    return std::make_unique<Expr>(
        std::array<ExpressionPtr, sizeof...(children)>{std::move(children)...});
  }

  template <typename Ctx>
  void visitVector(const std::vector<Ctx*>& childContexts)
      requires voidWhenVisited<SparqlQleverVisitor, Ctx>;

  // Call `visit` for each of the `childContexts` and return the results of
  // those calls as a `vector`.
  template <typename Ctx>
  auto visitVector(const std::vector<Ctx*>& childContexts)
      -> std::vector<decltype(visit(childContexts[0]))>
      requires(!voidWhenVisited<SparqlQleverVisitor, Ctx>);

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

  template <typename Ctx>
  void visitIf(Ctx* ctx) requires voidWhenVisited<SparqlQleverVisitor, Ctx>;

 public:
  [[noreturn]] static void reportError(const antlr4::ParserRuleContext* ctx,
                                       const std::string& msg);

  [[noreturn]] static void reportNotSupported(
      const antlr4::ParserRuleContext* ctx, const std::string& feature);

 private:
  // Throw an exception if the `expression` contains the `LANG()` function. The
  // `context` will be used to create the exception metadata.
  static void checkUnsupportedLangOperation(
      const antlr4::ParserRuleContext* context,
      const SparqlExpressionPimpl& expression);

  // Similar to `checkUnsupportedLangOperation` but doesn't throw for the
  // expression `LANG(?someVariable) = "someLangtag"` which is supported by
  // QLever inside a FILTER clause.
  static void checkUnsupportedLangOperationAllowFilters(
      const antlr4::ParserRuleContext* ctx,
      const SparqlExpressionPimpl& expression);

  // Parse both `ConstructTriplesContext` and `TriplesTemplateContext` because
  // they have the same structure.
  template <typename Context>
  Triples parseTriplesConstruction(Context* ctx);

  // If the triple is a special triple for the text index (i.e. its predicate is
  // either `ql:contains-word` or `ql:contains-entity`, register the magic
  // variables for the matching word and the score that will be created when
  // processing those triples in the query body, s.t. they can be selected as
  // part of the query result.
  void setMatchingWordAndScoreVisibleIfPresent(
      auto* ctx, const TripleWithPropertyPath& triple);

  // Constructs a TripleComponent from a GraphTerm.
  static TripleComponent visitGraphTerm(const GraphTerm& graphTerm);
};
