
// Generated from SparqlAutomatic.g4 by ANTLR 4.9.2

#pragma once

#include "../../engine/sparqlExpressions/AggregateExpression.h"
#include "../../engine/sparqlExpressions/GroupConcatExpression.h"
#include "../../engine/sparqlExpressions/LiteralExpression.h"
#include "../../engine/sparqlExpressions/NaryExpression.h"
#include "../../engine/sparqlExpressions/SparqlExpressionPimpl.h"
// #include "../../engine/sparqlExpressions/RelationalExpression.h"
#include "../../engine/sparqlExpressions/SampleExpression.h"
#include "../../util/HashMap.h"
#include "../../util/OverloadCallOperator.h"
#include "../../util/StringUtils.h"
#include "../Alias.h"
#include "../ParsedQuery.h"
#include "../RdfEscaping.h"
#include "../data/BlankNode.h"
#include "../data/Iri.h"
#include "../data/Types.h"
#include "../data/VarOrTerm.h"
#include "antlr4-runtime.h"
#include "generated/SparqlAutomaticVisitor.h"

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
class SparqlQleverVisitor : public SparqlAutomaticVisitor {
  using Objects = ad_utility::sparql_types::Objects;
  using Tuples = ad_utility::sparql_types::Tuples;
  using PredicateAndObject = ad_utility::sparql_types::PredicateAndObject;
  using PathTuples = ad_utility::sparql_types::PathTuples;
  using TripleWithPropertyPath =
      ad_utility::sparql_types::TripleWithPropertyPath;
  using Triples = ad_utility::sparql_types::Triples;
  using Node = ad_utility::sparql_types::Node;
  using ObjectList = ad_utility::sparql_types::ObjectList;
  using PropertyList = ad_utility::sparql_types::PropertyList;
  using Any = antlrcpp::Any;
  size_t _blankNodeCounter = 0;

 public:
  using PrefixMap = ad_utility::HashMap<string, string>;
  using Parser = SparqlAutomaticParser;
  const PrefixMap& prefixMap() const { return _prefixMap; }
  SparqlQleverVisitor() = default;
  SparqlQleverVisitor(PrefixMap prefixMap) : _prefixMap{std::move(prefixMap)} {}
  using ExpressionPtr = sparqlExpression::SparqlExpression::Ptr;

  // The inherited default behavior of `visitChildren` does not work with
  // move-only types like `SparqlExpression::Ptr`. This overriding
  // implementation adds std::move, but is otherwise the same as the default.
  Any visitChildren(antlr4::tree::ParseTree* node) override {
    Any result = nullptr;
    size_t n = node->children.size();
    for (size_t i = 0; i < n; i++) {
      Any childResult = node->children[i]->accept(this);
      result = std::move(childResult);
    }

    return result;
  }

  void setPrefixMapManually(PrefixMap map) { _prefixMap = std::move(map); }

 private:
  PrefixMap _prefixMap{};

  template <typename T>
  void appendVector(std::vector<T>& destination, std::vector<T>&& source) {
    destination.insert(destination.end(),
                       std::make_move_iterator(source.begin()),
                       std::make_move_iterator(source.end()));
  }

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

 public:
  // ___________________________________________________________________________
  Any visitQuery(Parser::QueryContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::variant<ParsedQuery, Triples> visitTypesafe(Parser::QueryContext* ctx);

  // ___________________________________________________________________________
  Any visitPrologue(Parser::PrologueContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PrefixMap visitTypesafe(SparqlAutomaticParser::PrologueContext* ctx);

  // ___________________________________________________________________________
  Any visitBaseDecl(Parser::BaseDeclContext* ctx) override {
    return visitTypesafe(ctx);
  }

  SparqlPrefix visitTypesafe(Parser::BaseDeclContext* ctx);

  // ___________________________________________________________________________
  Any visitPrefixDecl(Parser::PrefixDeclContext* ctx) override {
    // This function only changes the state and the return is only for testing.
    return visitTypesafe(ctx);
  }

  SparqlPrefix visitTypesafe(Parser::PrefixDeclContext* ctx);

  Any visitSelectQuery(Parser::SelectQueryContext* ctx) override {
    return visitTypesafe(ctx);
  }

  [[noreturn]] ParsedQuery visitTypesafe(Parser::SelectQueryContext* ctx);

  Any visitSubSelect(Parser::SubSelectContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitSelectClause(Parser::SelectClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ParsedQuery::SelectClause visitTypesafe(Parser::SelectClauseContext* ctx);

  Any visitVarOrAlias(Parser::VarOrAliasContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::variant<Variable, Alias> visitTypesafe(Parser::VarOrAliasContext* ctx);

  Any visitAlias(Parser::AliasContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Alias visitTypesafe(Parser::AliasContext* ctx);

  Any visitAliasWithoutBrackets(
      Parser::AliasWithoutBracketsContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Alias visitTypesafe(Parser::AliasWithoutBracketsContext* ctx);

  Any visitConstructQuery(Parser::ConstructQueryContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Triples visitTypesafe(Parser::ConstructQueryContext* ctx);

  Any visitDescribeQuery(Parser::DescribeQueryContext* ctx) override {
    // TODO: unsupported
    return visitChildren(ctx);
  }

  Any visitAskQuery(Parser::AskQueryContext* ctx) override {
    // TODO: unsupported
    return visitChildren(ctx);
  }

  Any visitDatasetClause(Parser::DatasetClauseContext* ctx) override {
    // TODO: unsupported
    return visitChildren(ctx);
  }

  Any visitDefaultGraphClause(Parser::DefaultGraphClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitNamedGraphClause(Parser::NamedGraphClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitSourceSelector(Parser::SourceSelectorContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitWhereClause(Parser::WhereClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitSolutionModifier(Parser::SolutionModifierContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitGroupClause(Parser::GroupClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<GroupKey> visitTypesafe(Parser::GroupClauseContext* ctx);

  Any visitGroupCondition(Parser::GroupConditionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GroupKey visitTypesafe(Parser::GroupConditionContext* ctx);

  Any visitHavingClause(Parser::HavingClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitHavingCondition(Parser::HavingConditionContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitOrderClause(Parser::OrderClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<OrderKey> visitTypesafe(Parser::OrderClauseContext* ctx);

  Any visitOrderCondition(Parser::OrderConditionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  OrderKey visitTypesafe(Parser::OrderConditionContext* ctx);

  Any visitLimitOffsetClauses(Parser::LimitOffsetClausesContext* ctx) override {
    return visitTypesafe(ctx);
  }

  LimitOffsetClause visitTypesafe(Parser::LimitOffsetClausesContext* ctx);

  Any visitLimitClause(Parser::LimitClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  unsigned long long int visitTypesafe(Parser::LimitClauseContext* ctx);

  Any visitOffsetClause(Parser::OffsetClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  unsigned long long int visitTypesafe(Parser::OffsetClauseContext* ctx);

  Any visitTextLimitClause(Parser::TextLimitClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  unsigned long long int visitTypesafe(Parser::TextLimitClauseContext* ctx);

  Any visitValuesClause(Parser::ValuesClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::optional<GraphPatternOperation::Values> visitTypesafe(
      Parser::ValuesClauseContext* ctx);

  Any visitTriplesTemplate(Parser::TriplesTemplateContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitGroupGraphPattern(Parser::GroupGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitGroupGraphPatternSub(
      Parser::GroupGraphPatternSubContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitTriplesBlock(Parser::TriplesBlockContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitGraphPatternNotTriples(
      Parser::GraphPatternNotTriplesContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitOptionalGraphPattern(
      Parser::OptionalGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitGraphGraphPattern(Parser::GraphGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitServiceGraphPattern(
      Parser::ServiceGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitBind(Parser::BindContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GraphPatternOperation::Bind visitTypesafe(Parser::BindContext* ctx);

  Any visitInlineData(Parser::InlineDataContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GraphPatternOperation::Values visitTypesafe(Parser::InlineDataContext* ctx);

  Any visitDataBlock(Parser::DataBlockContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GraphPatternOperation::Values visitTypesafe(Parser::DataBlockContext* ctx);

  Any visitInlineDataOneVar(Parser::InlineDataOneVarContext* ctx) override {
    return visitTypesafe(ctx);
  }

  SparqlValues visitTypesafe(Parser::InlineDataOneVarContext* ctx);

  Any visitInlineDataFull(Parser::InlineDataFullContext* ctx) override {
    return visitTypesafe(ctx);
  }

  SparqlValues visitTypesafe(Parser::InlineDataFullContext* ctx);

  Any visitDataBlockSingle(Parser::DataBlockSingleContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<std::string> visitTypesafe(Parser::DataBlockSingleContext* ctx);

  Any visitDataBlockValue(Parser::DataBlockValueContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::string visitTypesafe(Parser::DataBlockValueContext* ctx);

  Any visitMinusGraphPattern(Parser::MinusGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitGroupOrUnionGraphPattern(
      Parser::GroupOrUnionGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitFilterR(Parser::FilterRContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitConstraint(Parser::ConstraintContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::ConstraintContext* ctx);

  Any visitFunctionCall(Parser::FunctionCallContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(SparqlAutomaticParser::FunctionCallContext* ctx);

  Any visitArgList(Parser::ArgListContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<ExpressionPtr> visitTypesafe(Parser::ArgListContext* ctx);

  Any visitExpressionList(Parser::ExpressionListContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitConstructTemplate(Parser::ConstructTemplateContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::optional<Triples> visitTypesafe(Parser::ConstructTemplateContext* ctx);

  Any visitConstructTriples(Parser::ConstructTriplesContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Triples visitTypesafe(Parser::ConstructTriplesContext* ctx);

  Any visitTriplesSameSubject(Parser::TriplesSameSubjectContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Triples visitTypesafe(Parser::TriplesSameSubjectContext* ctx);

  Any visitPropertyList(Parser::PropertyListContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyList visitTypesafe(Parser::PropertyListContext* ctx);

  Any visitPropertyListNotEmpty(
      Parser::PropertyListNotEmptyContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyList visitTypesafe(Parser::PropertyListNotEmptyContext* ctx);

  Any visitVerb(Parser::VerbContext* ctx) override {
    return visitTypesafe(ctx);
  }

  VarOrTerm visitTypesafe(Parser::VerbContext* ctx);

  Any visitObjectList(Parser::ObjectListContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ObjectList visitTypesafe(Parser::ObjectListContext* ctx);

  Any visitObjectR(Parser::ObjectRContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Node visitTypesafe(Parser::ObjectRContext* ctx);

  Any visitTriplesSameSubjectPath(
      Parser::TriplesSameSubjectPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<TripleWithPropertyPath> visitTypesafe(
      Parser::TriplesSameSubjectPathContext* ctx);

  Any visitTupleWithoutPath(Parser::TupleWithoutPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PathTuples visitTypesafe(Parser::TupleWithoutPathContext* ctx);

  Any visitTupleWithPath(Parser::TupleWithPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PathTuples visitTypesafe(Parser::TupleWithPathContext* ctx);

  [[noreturn]] void throwCollectionsAndBlankNodePathsNotSupported(auto* ctx) {
    throw ParseException(
        "( ... ) and [ ... ] in triples are not yet supported by QLever. "
        "Got: " +
        ctx->getText());
  }

  Any visitPropertyListPath(Parser::PropertyListPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::optional<PathTuples> visitTypesafe(
      SparqlAutomaticParser::PropertyListPathContext* ctx);

  Any visitPropertyListPathNotEmpty(
      Parser::PropertyListPathNotEmptyContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PathTuples visitTypesafe(Parser::PropertyListPathNotEmptyContext* ctx);

  Any visitVerbPath(Parser::VerbPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::VerbPathContext* ctx);

  Any visitVerbSimple(Parser::VerbSimpleContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Variable visitTypesafe(Parser::VerbSimpleContext* ctx);

  Any visitVerbPathOrSimple(Parser::VerbPathOrSimpleContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ad_utility::sparql_types::VarOrPath visitTypesafe(
      Parser::VerbPathOrSimpleContext* ctx);

  Any visitObjectListPath(Parser::ObjectListPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ObjectList visitTypesafe(Parser::ObjectListPathContext* ctx);

  Any visitObjectPath(Parser::ObjectPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  VarOrTerm visitTypesafe(Parser::ObjectPathContext* ctx);

  Any visitPath(Parser::PathContext* ctx) override {
    // returns PropertyPath
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathContext* ctx);

  Any visitPathAlternative(Parser::PathAlternativeContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathAlternativeContext* ctx);

  Any visitPathSequence(Parser::PathSequenceContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathSequenceContext* ctx);

  Any visitPathElt(Parser::PathEltContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathEltContext* ctx);

  Any visitPathEltOrInverse(Parser::PathEltOrInverseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathEltOrInverseContext* ctx);

  Any visitPathMod(Parser::PathModContext* ctx) override {
    // Handled in visitPathElt.
    throw ParseException(
        "PathMod should be handled by upper clauses. It should not be visited. "
        "Got: " +
        ctx->getText());
  }

  Any visitPathPrimary(Parser::PathPrimaryContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathPrimaryContext* ctx);

  Any visitPathNegatedPropertySet(
      Parser::PathNegatedPropertySetContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathNegatedPropertySetContext*);

  Any visitPathOneInPropertySet(Parser::PathOneInPropertySetContext*) override {
    throw ParseException(
        R"("!" and "^" inside a property path is not supported by QLever.)");
  }

  /// Note that in the SPARQL grammar the INTEGER rule refers to positive
  /// integers without an explicit sign.
  Any visitInteger(Parser::IntegerContext* ctx) override {
    return visitTypesafe(ctx);
  }

  unsigned long long int visitTypesafe(Parser::IntegerContext* ctx);

  Any visitTriplesNode(Parser::TriplesNodeContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Node visitTypesafe(Parser::TriplesNodeContext* ctx);

  Any visitBlankNodePropertyList(
      Parser::BlankNodePropertyListContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Node visitTypesafe(Parser::BlankNodePropertyListContext* ctx);

  Any visitTriplesNodePath(Parser::TriplesNodePathContext* ctx) override {
    // TODO<qup42> use visitAlternative when NotSupported Exceptions and return
    // types are implemented.
    return visitChildren(ctx);
  }

  Any visitBlankNodePropertyListPath(
      Parser::BlankNodePropertyListPathContext* ctx) override {
    throwCollectionsAndBlankNodePathsNotSupported(ctx);
    AD_FAIL()  // Should be unreachable.
  }

  Any visitCollection(Parser::CollectionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Node visitTypesafe(Parser::CollectionContext* ctx);

  Any visitCollectionPath(Parser::CollectionPathContext* ctx) override {
    throwCollectionsAndBlankNodePathsNotSupported(ctx);
    AD_FAIL()  // Should be unreachable.
  }

  Any visitGraphNode(Parser::GraphNodeContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Node visitTypesafe(Parser::GraphNodeContext* ctx);

  Any visitGraphNodePath(Parser::GraphNodePathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  VarOrTerm visitTypesafe(Parser::GraphNodePathContext* ctx);

  Any visitVarOrTerm(Parser::VarOrTermContext* ctx) override {
    return visitTypesafe(ctx);
  }

  VarOrTerm visitTypesafe(Parser::VarOrTermContext* ctx);

  Any visitVarOrIri(Parser::VarOrIriContext* ctx) override {
    return visitTypesafe(ctx);
  }

  VarOrTerm visitTypesafe(Parser::VarOrIriContext* ctx);

  Any visitVar(Parser::VarContext* ctx) override { return visitTypesafe(ctx); }

  Variable visitTypesafe(Parser::VarContext* ctx);

  Any visitGraphTerm(Parser::GraphTermContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GraphTerm visitTypesafe(Parser::GraphTermContext* ctx);

  Any visitExpression(Parser::ExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::ExpressionContext* ctx);

  template <typename Out, typename Ctx>
  std::vector<Out> visitVector(const std::vector<Ctx*>& childContexts) {
    std::vector<Out> children;
    for (const auto& child : childContexts) {
      children.emplace_back(std::move(child->accept(this).template as<Out>()));
    }
    return children;
  }

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

  Any visitConditionalOrExpression(
      Parser::ConditionalOrExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::ConditionalOrExpressionContext* ctx);

  Any visitConditionalAndExpression(
      Parser::ConditionalAndExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::ConditionalAndExpressionContext* ctx);

  Any visitValueLogical(Parser::ValueLogicalContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::ValueLogicalContext* ctx);

  Any visitRelationalExpression(
      Parser::RelationalExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::RelationalExpressionContext* ctx);

  Any visitNumericExpression(Parser::NumericExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::NumericExpressionContext* ctx);

  template <typename Expr>
  ExpressionPtr createExpression(auto... children) {
    return std::make_unique<Expr>(
        std::array<ExpressionPtr, sizeof...(children)>{std::move(children)...});
  }

  Any visitAdditiveExpression(Parser::AdditiveExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::AdditiveExpressionContext* ctx);

  Any visitStrangeMultiplicativeSubexprOfAdditive(
      Parser::StrangeMultiplicativeSubexprOfAdditiveContext* ctx) override {
    throw ParseException(
        "StrangeMultiplicativeSubexprOfAdditiveContext must not be visited. "
        "Got: " +
        ctx->getText());
  }

  Any visitMultiplicativeExpression(
      Parser::MultiplicativeExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::MultiplicativeExpressionContext* ctx);

  Any visitUnaryExpression(Parser::UnaryExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::UnaryExpressionContext* ctx);

  Any visitPrimaryExpression(Parser::PrimaryExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::PrimaryExpressionContext* ctx);

  Any visitBrackettedExpression(
      Parser::BrackettedExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::BrackettedExpressionContext* ctx);

  Any visitBuiltInCall(
      [[maybe_unused]] Parser::BuiltInCallContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::BuiltInCallContext* ctx);

  Any visitRegexExpression(Parser::RegexExpressionContext* context) override {
    return visitChildren(context);
  }

  Any visitSubstringExpression(
      Parser::SubstringExpressionContext* context) override {
    return visitChildren(context);
  }

  Any visitStrReplaceExpression(
      Parser::StrReplaceExpressionContext* context) override {
    return visitChildren(context);
  }

  Any visitExistsFunc(Parser::ExistsFuncContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitNotExistsFunc(Parser::NotExistsFuncContext* ctx) override {
    return visitChildren(ctx);
  }

  Any visitAggregate(Parser::AggregateContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::AggregateContext* ctx);

  Any visitIriOrFunction(Parser::IriOrFunctionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::IriOrFunctionContext* ctx);

  Any visitRdfLiteral(Parser::RdfLiteralContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::string visitTypesafe(Parser::RdfLiteralContext* ctx);

  Any visitNumericLiteral(Parser::NumericLiteralContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::variant<int64_t, double> visitTypesafe(
      Parser::NumericLiteralContext* ctx);

  Any visitNumericLiteralUnsigned(
      Parser::NumericLiteralUnsignedContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::variant<int64_t, double> visitTypesafe(
      Parser::NumericLiteralUnsignedContext* ctx);

  Any visitNumericLiteralPositive(
      Parser::NumericLiteralPositiveContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::variant<int64_t, double> visitTypesafe(
      Parser::NumericLiteralPositiveContext* ctx);

  Any visitNumericLiteralNegative(
      Parser::NumericLiteralNegativeContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::variant<int64_t, double> visitTypesafe(
      Parser::NumericLiteralNegativeContext* ctx);

  Any visitBooleanLiteral(Parser::BooleanLiteralContext* ctx) override {
    return visitTypesafe(ctx);
  }

  bool visitTypesafe(Parser::BooleanLiteralContext* ctx);

  Any visitString(Parser::StringContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(Parser::StringContext* ctx);

  Any visitIri(Parser::IriContext* ctx) override { return visitTypesafe(ctx); }

  string visitTypesafe(Parser::IriContext* ctx);

  Any visitIriref(Parser::IrirefContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(Parser::IrirefContext* ctx);

  Any visitPrefixedName(Parser::PrefixedNameContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(Parser::PrefixedNameContext* ctx);

  Any visitBlankNode(Parser::BlankNodeContext* ctx) override {
    return visitTypesafe(ctx);
  }

  BlankNode visitTypesafe(Parser::BlankNodeContext* ctx);

  Any visitPnameLn(Parser::PnameLnContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(Parser::PnameLnContext* ctx);

  Any visitPnameNs(Parser::PnameNsContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(Parser::PnameNsContext* ctx);

  template <typename Out, bool isRecursive = false, typename FirstContext,
            typename... Context>
  Out visitAlternative(FirstContext* ctx, Context*... ctxs);

  template <typename Ctx>
  auto visitOptional(Ctx* ctx) -> std::optional<decltype(visitTypesafe(ctx))>;

  template <typename Ctx>
  void visitIf(auto* target, Ctx* ctx);
};
