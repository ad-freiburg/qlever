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
#include "parser/data/Iri.h"
#include "parser/data/SolutionModifiers.h"
#include "parser/data/Types.h"
#include "parser/data/VarOrTerm.h"
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

 public:
  SparqlQleverVisitor() = default;
  SparqlQleverVisitor(
      PrefixMap prefixMap,
      DisableSomeChecksOnlyForTesting disableSomeChecksOnlyForTesting =
          DisableSomeChecksOnlyForTesting::False)
      : prefixMap_{std::move(prefixMap)},
        disableSomeChecksOnlyForTesting_{disableSomeChecksOnlyForTesting} {}

  const PrefixMap& prefixMap() const { return prefixMap_; }
  void setPrefixMapManually(PrefixMap map) { prefixMap_ = std::move(map); }

  // ___________________________________________________________________________
  [[nodiscard]] ParsedQuery visit(Parser::QueryContext* ctx);

  // ___________________________________________________________________________
  void visit(Parser::PrologueContext* ctx);

  // ___________________________________________________________________________
  [[noreturn]] static void visit(const Parser::BaseDeclContext* ctx);

  // ___________________________________________________________________________
  void visit(Parser::PrefixDeclContext* ctx);

  [[nodiscard]] ParsedQuery visit(Parser::SelectQueryContext* ctx);

  [[nodiscard]] SubQueryAndMaybeValues visit(Parser::SubSelectContext* ctx);

  [[nodiscard]] parsedQuery::SelectClause visit(
      Parser::SelectClauseContext* ctx);

  [[nodiscard]] std::variant<Variable, Alias> visit(
      Parser::VarOrAliasContext* ctx);

  [[nodiscard]] Alias visit(Parser::AliasContext* ctx);

  [[nodiscard]] Alias visit(Parser::AliasWithoutBracketsContext* ctx);

  [[nodiscard]] ParsedQuery visit(Parser::ConstructQueryContext* ctx);

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

  [[nodiscard]] PatternAndVisibleVariables visit(
      Parser::WhereClauseContext* ctx);

  [[nodiscard]] SolutionModifiers visit(Parser::SolutionModifierContext* ctx);

  [[nodiscard]] vector<GroupKey> visit(Parser::GroupClauseContext* ctx);

  [[nodiscard]] GroupKey visit(Parser::GroupConditionContext* ctx);

  [[nodiscard]] vector<SparqlFilter> visit(Parser::HavingClauseContext* ctx);

  [[nodiscard]] SparqlFilter visit(Parser::HavingConditionContext* ctx);

  [[nodiscard]] OrderClause visit(Parser::OrderClauseContext* ctx);

  [[nodiscard]] OrderKey visit(Parser::OrderConditionContext* ctx);

  [[nodiscard]] LimitOffsetClause visit(Parser::LimitOffsetClausesContext* ctx);

  [[nodiscard]] static uint64_t visit(Parser::LimitClauseContext* ctx);

  [[nodiscard]] static uint64_t visit(Parser::OffsetClauseContext* ctx);

  [[nodiscard]] static uint64_t visit(Parser::TextLimitClauseContext* ctx);

  [[nodiscard]] std::optional<parsedQuery::Values> visit(
      Parser::ValuesClauseContext* ctx);

  [[nodiscard]] Triples visit(Parser::TriplesTemplateContext* ctx);

  [[nodiscard]] ParsedQuery::GraphPattern visit(
      Parser::GroupGraphPatternContext* ctx);

  [[nodiscard]] OperationsAndFilters visit(
      Parser::GroupGraphPatternSubContext* ctx);

  [[nodiscard]] OperationOrFilterAndMaybeTriples visit(
      Parser::GraphPatternNotTriplesAndMaybeTriplesContext* ctx);

  [[nodiscard]] parsedQuery::BasicGraphPattern visit(
      Parser::TriplesBlockContext* graphTerm);

  // Filter clauses are no independent graph patterns themselves, but their
  // scope is always the complete graph pattern enclosing them.
  [[nodiscard]] OperationOrFilter visit(
      Parser::GraphPatternNotTriplesContext* ctx);

  [[nodiscard]] parsedQuery::GraphPatternOperation visit(
      Parser::OptionalGraphPatternContext* ctx);

  [[noreturn]] static parsedQuery::GraphPatternOperation visit(
      const Parser::GraphGraphPatternContext* ctx);

  [[nodiscard]] parsedQuery::Service visit(
      Parser::ServiceGraphPatternContext* ctx);

  [[nodiscard]] parsedQuery::GraphPatternOperation visit(
      Parser::BindContext* ctx);

  [[nodiscard]] parsedQuery::GraphPatternOperation visit(
      Parser::InlineDataContext* ctx);

  [[nodiscard]] parsedQuery::Values visit(Parser::DataBlockContext* ctx);

  [[nodiscard]] parsedQuery::SparqlValues visit(
      Parser::InlineDataOneVarContext* ctx);

  [[nodiscard]] parsedQuery::SparqlValues visit(
      Parser::InlineDataFullContext* ctx);

  [[nodiscard]] vector<TripleComponent> visit(
      Parser::DataBlockSingleContext* ctx);

  [[nodiscard]] TripleComponent visit(Parser::DataBlockValueContext* ctx);

  [[nodiscard]] GraphPatternOperation visit(
      Parser::MinusGraphPatternContext* ctx);

  [[nodiscard]] GraphPatternOperation visit(
      Parser::GroupOrUnionGraphPatternContext* ctx);

  [[nodiscard]] SparqlFilter visit(Parser::FilterRContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::ConstraintContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::FunctionCallContext* ctx);

  [[nodiscard]] vector<ExpressionPtr> visit(Parser::ArgListContext* ctx);

  std::vector<ExpressionPtr> visit(Parser::ExpressionListContext* ctx);

  [[nodiscard]] std::optional<parsedQuery::ConstructClause> visit(
      Parser::ConstructTemplateContext* ctx);

  [[nodiscard]] Triples visit(Parser::ConstructTriplesContext* ctx);

  [[nodiscard]] Triples visit(Parser::TriplesSameSubjectContext* ctx);

  [[nodiscard]] PropertyList visit(Parser::PropertyListContext* ctx);

  [[nodiscard]] PropertyList visit(Parser::PropertyListNotEmptyContext* ctx);

  [[nodiscard]] VarOrTerm visit(Parser::VerbContext* ctx);

  [[nodiscard]] ObjectList visit(Parser::ObjectListContext* ctx);

  [[nodiscard]] Node visit(Parser::ObjectRContext* ctx);

  [[nodiscard]] vector<TripleWithPropertyPath> visit(
      Parser::TriplesSameSubjectPathContext* ctx);

  [[nodiscard]] std::optional<PathTuples> visit(
      Parser::PropertyListPathContext* ctx);

  [[nodiscard]] PathTuples visit(Parser::PropertyListPathNotEmptyContext* ctx);

  [[nodiscard]] PropertyPath visit(Parser::VerbPathContext* ctx);

  [[nodiscard]] static Variable visit(Parser::VerbSimpleContext* ctx);

  [[nodiscard]] PathTuples visit(Parser::TupleWithoutPathContext* ctx);

  [[nodiscard]] PathTuples visit(Parser::TupleWithPathContext* ctx);

  [[nodiscard]] ad_utility::sparql_types::VarOrPath visit(
      Parser::VerbPathOrSimpleContext* ctx);

  [[nodiscard]] ObjectList visit(Parser::ObjectListPathContext* ctx);

  [[nodiscard]] VarOrTerm visit(Parser::ObjectPathContext* ctx);

  [[nodiscard]] PropertyPath visit(Parser::PathContext* ctx);

  [[nodiscard]] PropertyPath visit(Parser::PathAlternativeContext* ctx);

  [[nodiscard]] PropertyPath visit(Parser::PathSequenceContext* ctx);

  [[nodiscard]] PropertyPath visit(Parser::PathEltContext* ctx);

  [[nodiscard]] PropertyPath visit(Parser::PathEltOrInverseContext* ctx);

  [[noreturn]] static void visit(Parser::PathModContext* ctx);

  [[nodiscard]] PropertyPath visit(Parser::PathPrimaryContext* ctx);

  [[noreturn]] static PropertyPath visit(
      const Parser::PathNegatedPropertySetContext*);

  [[noreturn]] static PropertyPath visit(
      Parser::PathOneInPropertySetContext* ctx);

  /// Note that in the SPARQL grammar the INTEGER rule refers to positive
  /// integers without an explicit sign.
  [[nodiscard]] static uint64_t visit(Parser::IntegerContext* ctx);

  [[nodiscard]] Node visit(Parser::TriplesNodeContext* ctx);

  [[nodiscard]] Node visit(Parser::BlankNodePropertyListContext* ctx);

  [[noreturn]] void visit(Parser::TriplesNodePathContext* ctx);

  [[noreturn]] void visit(Parser::BlankNodePropertyListPathContext* ctx);

  [[nodiscard]] Node visit(Parser::CollectionContext* ctx);

  [[noreturn]] void visit(Parser::CollectionPathContext* ctx);

  [[nodiscard]] Node visit(Parser::GraphNodeContext* ctx);

  [[nodiscard]] VarOrTerm visit(Parser::GraphNodePathContext* ctx);

  [[nodiscard]] VarOrTerm visit(Parser::VarOrTermContext* ctx);

  [[nodiscard]] VarOrTerm visit(Parser::VarOrIriContext* ctx);

  [[nodiscard]] static Variable visit(Parser::VarContext* ctx);

  [[nodiscard]] GraphTerm visit(Parser::GraphTermContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::ExpressionContext* ctx);

  [[nodiscard]] ExpressionPtr visit(
      Parser::ConditionalOrExpressionContext* ctx);

  [[nodiscard]] ExpressionPtr visit(
      Parser::ConditionalAndExpressionContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::ValueLogicalContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::RelationalExpressionContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::NumericExpressionContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::AdditiveExpressionContext* ctx);

  // Helper structs, needed in the following `visit` functions. Combine an
  // explicit operator (+ - * /) with an expression.
  enum struct Operator { Plus, Minus, Multiply, Divide };
  struct OperatorAndExpression {
    Operator operator_;
    ExpressionPtr expression_;
  };

  [[nodiscard]] OperatorAndExpression visit(
      Parser::MultiplicativeExpressionWithSignContext* ctx);

  [[nodiscard]] OperatorAndExpression visit(
      Parser::PlusSubexpressionContext* ctx);

  [[nodiscard]] OperatorAndExpression visit(
      Parser::MinusSubexpressionContext* ctx);

  [[nodiscard]] OperatorAndExpression visit(
      Parser::MultiplicativeExpressionWithLeadingSignButNoSpaceContext* ctx);

  [[nodiscard]] ExpressionPtr visit(
      Parser::MultiplicativeExpressionContext* ctx);

  [[nodiscard]] OperatorAndExpression visit(
      Parser::MultiplyOrDivideExpressionContext* ctx);

  [[nodiscard]] OperatorAndExpression visit(
      Parser::MultiplyExpressionContext* ctx);

  [[nodiscard]] OperatorAndExpression visit(
      Parser::DivideExpressionContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::UnaryExpressionContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::PrimaryExpressionContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::BrackettedExpressionContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::BuiltInCallContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::RegexExpressionContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::LangExpressionContext* ctx);

  ExpressionPtr visit(Parser::SubstringExpressionContext* ctx);

  ExpressionPtr visit(Parser::StrReplaceExpressionContext* ctx);

  [[noreturn]] static void visit(const Parser::ExistsFuncContext* ctx);

  [[noreturn]] static void visit(const Parser::NotExistsFuncContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::AggregateContext* ctx);

  [[nodiscard]] ExpressionPtr visit(Parser::IriOrFunctionContext* ctx);

  [[nodiscard]] std::string visit(Parser::RdfLiteralContext* ctx);

  [[nodiscard]] IntOrDouble visit(Parser::NumericLiteralContext* ctx);

  [[nodiscard]] static IntOrDouble visit(
      Parser::NumericLiteralUnsignedContext* ctx);

  [[nodiscard]] static IntOrDouble visit(
      Parser::NumericLiteralPositiveContext* ctx);

  [[nodiscard]] static IntOrDouble visit(
      Parser::NumericLiteralNegativeContext* ctx);

  [[nodiscard]] static bool visit(Parser::BooleanLiteralContext* ctx);

  [[nodiscard]] static RdfEscaping::NormalizedRDFString visit(
      Parser::StringContext* ctx);

  [[nodiscard]] string visit(Parser::IriContext* ctx);

  [[nodiscard]] static string visit(Parser::IrirefContext* ctx);

  [[nodiscard]] string visit(Parser::PrefixedNameContext* ctx);

  [[nodiscard]] BlankNode visit(Parser::BlankNodeContext* ctx);

  [[nodiscard]] string visit(Parser::PnameLnContext* ctx);

  [[nodiscard]] string visit(Parser::PnameNsContext* ctx);

 private:
  template <typename Visitor, typename Ctx>
  static constexpr bool voidWhenVisited =
      std::is_void_v<decltype(std::declval<Visitor&>().visit(
          std::declval<Ctx*>()))>;

  [[nodiscard]] BlankNode newBlankNode() {
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
  [[nodiscard]] static ExpressionPtr processIriFunctionCall(
      const std::string& iri, std::vector<ExpressionPtr> argList,
      const antlr4::ParserRuleContext*);

  void addVisibleVariable(Variable var);

  [[noreturn]] void throwCollectionsAndBlankNodePathsNotSupported(auto* ctx) {
    reportError(
        ctx, "( ... ) and [ ... ] in triples are not yet supported by QLever.");
  }

  // Return the `SparqlExpressionPimpl` for a context that returns a
  // `ExpressionPtr` when visited. The descriptor is set automatically on the
  // `SparqlExpressionPimpl`.
  [[nodiscard]] SparqlExpressionPimpl visitExpressionPimpl(
      auto* ctx, bool allowLanguageFilters = false);

  template <typename Expr>
  [[nodiscard]] ExpressionPtr createExpression(auto... children) {
    return std::make_unique<Expr>(
        std::array<ExpressionPtr, sizeof...(children)>{std::move(children)...});
  }

  template <typename Ctx>
  void visitVector(const std::vector<Ctx*>& childContexts)
      requires voidWhenVisited<SparqlQleverVisitor, Ctx>;

  // Call `visit` for each of the `childContexts` and return the results of
  // those calls as a `vector`.
  template <typename Ctx>
  [[nodiscard]] auto visitVector(const std::vector<Ctx*>& childContexts)
      -> std::vector<decltype(visit(childContexts[0]))>
      requires(!voidWhenVisited<SparqlQleverVisitor, Ctx>);

  // Check that exactly one of the `ctxs` is not `null`, visit that context,
  // cast the result to `Out` and return it. Requires that for all of the
  // `ctxs`, `visit(ctxs)` is convertible to `Out`.
  template <typename Out, typename... Contexts>
  [[nodiscard]] Out visitAlternative(Contexts*... ctxs);

  // Returns `std::nullopt` if the pointer is the `nullptr`. Otherwise return
  // `visit(ctx)`.
  template <typename Ctx>
  [[nodiscard]] auto visitOptional(Ctx* ctx)
      -> std::optional<decltype(visit(ctx))>;

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
  [[nodiscard]] Triples parseTriplesConstruction(Context* ctx);
};
