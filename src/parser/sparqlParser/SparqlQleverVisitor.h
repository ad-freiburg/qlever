
// Generated from SparqlAutomatic.g4 by ANTLR 4.9.2

#pragma once

#include <gtest/gtest.h>

#include "../../engine/sparqlExpressions/AggregateExpression.h"
#include "../../engine/sparqlExpressions/GroupConcatExpression.h"
#include "../../engine/sparqlExpressions/LiteralExpression.h"
#include "../../engine/sparqlExpressions/NaryExpression.h"
#include "../../engine/sparqlExpressions/SparqlExpressionPimpl.h"
//#include "../../engine/sparqlExpressions/RelationalExpression.h"
#include "../../engine/sparqlExpressions/SampleExpression.h"
#include "../../util/HashMap.h"
#include "../../util/OverloadCallOperator.h"
#include "../../util/StringUtils.h"
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
  antlrcpp::Any visitChildren(antlr4::tree::ParseTree* node) override {
    antlrcpp::Any result = nullptr;
    size_t n = node->children.size();
    for (size_t i = 0; i < n; i++) {
      antlrcpp::Any childResult = node->children[i]->accept(this);
      result = std::move(childResult);
    }

    return result;
  }

  void setPrefixMapManually(PrefixMap map) { _prefixMap = std::move(map); }

 private:
  // For the unit tests
  PrefixMap& prefixMap() { return _prefixMap; }
  FRIEND_TEST(SparqlParser, Prefix);

  PrefixMap _prefixMap{{"", "<>"}};

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
  antlrcpp::Any processIriFunctionCall(const std::string& iri,
                                       std::vector<ExpressionPtr> argList);

 public:
  // ___________________________________________________________________________
  sparqlExpression::SparqlExpressionPimpl makeExpressionPimpl(
      antlrcpp::Any any) {
    return sparqlExpression::SparqlExpressionPimpl{
        std::move(any.as<ExpressionPtr>())};
  }
  // ___________________________________________________________________________
  sparqlExpression::SparqlExpressionPimpl makeExpressionPimpl(
      ExpressionPtr any) {
    return sparqlExpression::SparqlExpressionPimpl{std::move(any)};
  }
  // ___________________________________________________________________________
  antlrcpp::Any visitQuery(Parser::QueryContext* ctx) override {
    // The prologue (BASE and PREFIX declarations)  only affects the internal
    // state of the visitor.
    visitPrologue(ctx->prologue());
    if (ctx->selectQuery()) {
      return visitSelectQuery(ctx->selectQuery());
    }
    if (ctx->constructQuery()) {
      return ctx->constructQuery()->accept(this);
    }
    throw ParseException{"QLever only supports select and construct queries"};
  }

  // ___________________________________________________________________________
  antlrcpp::Any visitPrologue(Parser::PrologueContext* ctx) override {
    // Default implementation is ok here, simply handle all PREFIX and BASE
    // declarations.
    return visitChildren(ctx);
  }

  // ___________________________________________________________________________
  antlrcpp::Any visitBaseDecl(Parser::BaseDeclContext* ctx) override {
    visitTypesafe(ctx);
    return nullptr;
  }

  void visitTypesafe(Parser::BaseDeclContext* ctx);

  // ___________________________________________________________________________
  antlrcpp::Any visitPrefixDecl(Parser::PrefixDeclContext* ctx) override {
    visitTypesafe(ctx);
    return nullptr;
  }

  void visitTypesafe(Parser::PrefixDeclContext* ctx);

  antlrcpp::Any visitSelectQuery(Parser::SelectQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSubSelect(Parser::SubSelectContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSelectClause(Parser::SelectClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ParsedQuery::SelectClause visitTypesafe(Parser::SelectClauseContext* ctx);

  antlrcpp::Any visitVarOrAlias(Parser::VarOrAliasContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::variant<Variable, ParsedQuery::Alias> visitTypesafe(
      Parser::VarOrAliasContext* ctx);

  antlrcpp::Any visitAlias(Parser::AliasContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ParsedQuery::Alias visitTypesafe(Parser::AliasContext* ctx);

  antlrcpp::Any visitAliasWithoutBrackets(
      Parser::AliasWithoutBracketsContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ParsedQuery::Alias visitTypesafe(Parser::AliasWithoutBracketsContext* ctx);

  antlrcpp::Any visitConstructQuery(
      Parser::ConstructQueryContext* ctx) override {
    if (!ctx->datasetClause().empty()) {
      throw ParseException{"Datasets are not supported"};
    }
    if (ctx->constructTemplate()) {
      return ctx->constructTemplate()->accept(this);
    }
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDescribeQuery(Parser::DescribeQueryContext* ctx) override {
    // TODO: unsupported
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAskQuery(Parser::AskQueryContext* ctx) override {
    // TODO: unsupported
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDatasetClause(Parser::DatasetClauseContext* ctx) override {
    // TODO: unsupported
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDefaultGraphClause(
      Parser::DefaultGraphClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNamedGraphClause(
      Parser::NamedGraphClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSourceSelector(
      Parser::SourceSelectorContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitWhereClause(Parser::WhereClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSolutionModifier(
      Parser::SolutionModifierContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupClause(Parser::GroupClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<GroupKey> visitTypesafe(Parser::GroupClauseContext* ctx);

  antlrcpp::Any visitGroupCondition(
      Parser::GroupConditionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GroupKey visitTypesafe(Parser::GroupConditionContext* ctx);

  antlrcpp::Any visitHavingClause(Parser::HavingClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitHavingCondition(
      Parser::HavingConditionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitOrderClause(Parser::OrderClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<OrderKey> visitTypesafe(Parser::OrderClauseContext* ctx);

  antlrcpp::Any visitOrderCondition(
      Parser::OrderConditionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  OrderKey visitTypesafe(Parser::OrderConditionContext* ctx);

  antlrcpp::Any visitLimitOffsetClauses(
      Parser::LimitOffsetClausesContext* ctx) override {
    return visitTypesafe(ctx);
  }

  LimitOffsetClause visitTypesafe(Parser::LimitOffsetClausesContext* ctx);

  antlrcpp::Any visitLimitClause(Parser::LimitClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  unsigned long long int visitTypesafe(Parser::LimitClauseContext* ctx);

  antlrcpp::Any visitOffsetClause(Parser::OffsetClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  unsigned long long int visitTypesafe(Parser::OffsetClauseContext* ctx);

  antlrcpp::Any visitTextLimitClause(
      Parser::TextLimitClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  unsigned long long int visitTypesafe(Parser::TextLimitClauseContext* ctx);

  antlrcpp::Any visitValuesClause(Parser::ValuesClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::optional<GraphPatternOperation::Values> visitTypesafe(
      Parser::ValuesClauseContext* ctx);

  antlrcpp::Any visitTriplesTemplate(
      Parser::TriplesTemplateContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupGraphPattern(
      Parser::GroupGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupGraphPatternSub(
      Parser::GroupGraphPatternSubContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitTriplesBlock(Parser::TriplesBlockContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphPatternNotTriples(
      Parser::GraphPatternNotTriplesContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitOptionalGraphPattern(
      Parser::OptionalGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphGraphPattern(
      Parser::GraphGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitServiceGraphPattern(
      Parser::ServiceGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBind(Parser::BindContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GraphPatternOperation::Bind visitTypesafe(Parser::BindContext* ctx);

  antlrcpp::Any visitInlineData(Parser::InlineDataContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GraphPatternOperation::Values visitTypesafe(Parser::InlineDataContext* ctx);

  antlrcpp::Any visitDataBlock(Parser::DataBlockContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GraphPatternOperation::Values visitTypesafe(Parser::DataBlockContext* ctx);

  antlrcpp::Any visitInlineDataOneVar(
      Parser::InlineDataOneVarContext* ctx) override {
    return visitTypesafe(ctx);
  }

  SparqlValues visitTypesafe(Parser::InlineDataOneVarContext* ctx);

  antlrcpp::Any visitInlineDataFull(
      Parser::InlineDataFullContext* ctx) override {
    return visitTypesafe(ctx);
  }

  SparqlValues visitTypesafe(Parser::InlineDataFullContext* ctx);

  antlrcpp::Any visitDataBlockSingle(
      Parser::DataBlockSingleContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<std::string> visitTypesafe(Parser::DataBlockSingleContext* ctx);

  antlrcpp::Any visitDataBlockValue(
      Parser::DataBlockValueContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::string visitTypesafe(Parser::DataBlockValueContext* ctx);

  antlrcpp::Any visitMinusGraphPattern(
      Parser::MinusGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupOrUnionGraphPattern(
      Parser::GroupOrUnionGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitFilterR(Parser::FilterRContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConstraint(Parser::ConstraintContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitFunctionCall(Parser::FunctionCallContext* ctx) override {
    return processIriFunctionCall(
        visitIri(ctx->iri()).as<std::string>(),
        std::move(
            visitArgList(ctx->argList()).as<std::vector<ExpressionPtr>>()));
  }

  antlrcpp::Any visitArgList(Parser::ArgListContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<ExpressionPtr> visitTypesafe(Parser::ArgListContext* ctx);

  antlrcpp::Any visitExpressionList(
      Parser::ExpressionListContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConstructTemplate(
      Parser::ConstructTemplateContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Triples visitTypesafe(Parser::ConstructTemplateContext* ctx);

  antlrcpp::Any visitConstructTriples(
      Parser::ConstructTriplesContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Triples visitTypesafe(Parser::ConstructTriplesContext* ctx);

  antlrcpp::Any visitTriplesSameSubject(
      Parser::TriplesSameSubjectContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Triples visitTypesafe(Parser::TriplesSameSubjectContext* ctx);

  antlrcpp::Any visitPropertyList(Parser::PropertyListContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyList visitTypesafe(Parser::PropertyListContext* ctx);

  antlrcpp::Any visitPropertyListNotEmpty(
      Parser::PropertyListNotEmptyContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyList visitTypesafe(Parser::PropertyListNotEmptyContext* ctx);

  antlrcpp::Any visitVerb(Parser::VerbContext* ctx) override {
    return visitTypesafe(ctx);
  }

  VarOrTerm visitTypesafe(Parser::VerbContext* ctx);

  antlrcpp::Any visitObjectList(Parser::ObjectListContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ObjectList visitTypesafe(Parser::ObjectListContext* ctx);

  antlrcpp::Any visitObjectR(Parser::ObjectRContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Node visitTypesafe(Parser::ObjectRContext* ctx);

  antlrcpp::Any visitTriplesSameSubjectPath(
      Parser::TriplesSameSubjectPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<TripleWithPropertyPath> visitTypesafe(
      Parser::TriplesSameSubjectPathContext* ctx);

  antlrcpp::Any visitTupleWithoutPath(
      Parser::TupleWithoutPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PathTuples visitTypesafe(Parser::TupleWithoutPathContext* ctx);

  antlrcpp::Any visitTupleWithPath(Parser::TupleWithPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PathTuples visitTypesafe(Parser::TupleWithPathContext* ctx);

  [[noreturn]] void throwCollectionsAndBlankNodePathsNotSupported(auto* ctx) {
    throw ParseException(
        "( ... ) and [ ... ] in triples are not yet supported by QLever. "
        "Got: " +
        ctx->getText());
  }

  antlrcpp::Any visitPropertyListPath(
      Parser::PropertyListPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::optional<PathTuples> visitTypesafe(
      SparqlAutomaticParser::PropertyListPathContext* ctx);

  antlrcpp::Any visitPropertyListPathNotEmpty(
      Parser::PropertyListPathNotEmptyContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PathTuples visitTypesafe(Parser::PropertyListPathNotEmptyContext* ctx);

  antlrcpp::Any visitVerbPath(Parser::VerbPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::VerbPathContext* ctx);

  antlrcpp::Any visitVerbSimple(Parser::VerbSimpleContext* ctx) override {
    return visitChildren(ctx);
  }

  Variable visitTypesafe(Parser::VerbSimpleContext* ctx);

  antlrcpp::Any visitVerbPathOrSimple(
      Parser::VerbPathOrSimpleContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ad_utility::sparql_types::VarOrPath visitTypesafe(
      Parser::VerbPathOrSimpleContext* ctx);

  antlrcpp::Any visitObjectListPath(
      Parser::ObjectListPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ObjectList visitTypesafe(Parser::ObjectListPathContext* ctx);

  antlrcpp::Any visitObjectPath(Parser::ObjectPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPath(Parser::PathContext* ctx) override {
    // returns PropertyPath
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathContext* ctx);

  antlrcpp::Any visitPathAlternative(
      Parser::PathAlternativeContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathAlternativeContext* ctx);

  antlrcpp::Any visitPathSequence(Parser::PathSequenceContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathSequenceContext* ctx);

  antlrcpp::Any visitPathElt(Parser::PathEltContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathEltContext* ctx);

  antlrcpp::Any visitPathEltOrInverse(
      Parser::PathEltOrInverseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathEltOrInverseContext* ctx);

  antlrcpp::Any visitPathMod(Parser::PathModContext* ctx) override {
    // Handled in visitPathElt.
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathPrimary(Parser::PathPrimaryContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathPrimaryContext* ctx);

  antlrcpp::Any visitPathNegatedPropertySet(
      Parser::PathNegatedPropertySetContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(Parser::PathNegatedPropertySetContext*);

  antlrcpp::Any visitPathOneInPropertySet(
      Parser::PathOneInPropertySetContext*) override {
    throw ParseException(
        R"("!" and "^" inside a property path is not supported by QLever.)");
  }

  /// Note that in the SPARQL grammar the INTEGER rule refers to positive
  /// integers without an explicit sign.
  antlrcpp::Any visitInteger(Parser::IntegerContext* ctx) override {
    return visitTypesafe(ctx);
  }

  unsigned long long int visitTypesafe(Parser::IntegerContext* ctx);

  antlrcpp::Any visitTriplesNode(Parser::TriplesNodeContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBlankNodePropertyList(
      Parser::BlankNodePropertyListContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Node visitTypesafe(Parser::BlankNodePropertyListContext* ctx);

  antlrcpp::Any visitTriplesNodePath(
      Parser::TriplesNodePathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBlankNodePropertyListPath(
      Parser::BlankNodePropertyListPathContext* ctx) override {
    throwCollectionsAndBlankNodePathsNotSupported(ctx);
    AD_FAIL()  // Should be unreachable.
  }

  antlrcpp::Any visitCollection(Parser::CollectionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Node visitTypesafe(Parser::CollectionContext* ctx);

  antlrcpp::Any visitCollectionPath(
      Parser::CollectionPathContext* ctx) override {
    throwCollectionsAndBlankNodePathsNotSupported(ctx);
    AD_FAIL()  // Should be unreachable.
  }

  antlrcpp::Any visitGraphNode(Parser::GraphNodeContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Node visitTypesafe(Parser::GraphNodeContext* ctx);

  antlrcpp::Any visitGraphNodePath(Parser::GraphNodePathContext* ctx) override {
    return visitChildren(ctx);
  }

  VarOrTerm visitTypesafe(Parser::GraphNodePathContext* ctx);

  antlrcpp::Any visitVarOrTerm(Parser::VarOrTermContext* ctx) override {
    return visitTypesafe(ctx);
  }

  VarOrTerm visitTypesafe(Parser::VarOrTermContext* ctx);

  antlrcpp::Any visitVarOrIri(Parser::VarOrIriContext* ctx) override {
    return visitTypesafe(ctx);
  }

  VarOrTerm visitTypesafe(Parser::VarOrIriContext* ctx);

  antlrcpp::Any visitVar(Parser::VarContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Variable visitTypesafe(Parser::VarContext* ctx);

  antlrcpp::Any visitGraphTerm(Parser::GraphTermContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GraphTerm visitTypesafe(Parser::GraphTermContext* ctx);

  antlrcpp::Any visitExpression(Parser::ExpressionContext* ctx) override {
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

  antlrcpp::Any visitConditionalOrExpression(
      Parser::ConditionalOrExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::ConditionalOrExpressionContext* ctx);

  antlrcpp::Any visitConditionalAndExpression(
      Parser::ConditionalAndExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::ConditionalAndExpressionContext* ctx);

  antlrcpp::Any visitValueLogical(Parser::ValueLogicalContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::ValueLogicalContext* ctx);

  antlrcpp::Any visitRelationalExpression(
      Parser::RelationalExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::RelationalExpressionContext* ctx);

  antlrcpp::Any visitNumericExpression(
      Parser::NumericExpressionContext* ctx) override {
    return std::move(visitChildren(ctx).as<ExpressionPtr>());
  }

  ExpressionPtr visitTypesafe(Parser::NumericExpressionContext* ctx);

  template <typename Expr>
  ExpressionPtr createExpression(auto... children) {
    return std::make_unique<Expr>(
        std::array<ExpressionPtr, sizeof...(children)>{std::move(children)...});
  }

  antlrcpp::Any visitAdditiveExpression(
      Parser::AdditiveExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::AdditiveExpressionContext* ctx);

  antlrcpp::Any visitStrangeMultiplicativeSubexprOfAdditive(
      Parser::StrangeMultiplicativeSubexprOfAdditiveContext* context) override {
    return visitChildren(context);
  }

  antlrcpp::Any visitMultiplicativeExpression(
      Parser::MultiplicativeExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::MultiplicativeExpressionContext* ctx);

  antlrcpp::Any visitUnaryExpression(
      Parser::UnaryExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::UnaryExpressionContext* ctx);

  antlrcpp::Any visitPrimaryExpression(
      Parser::PrimaryExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::PrimaryExpressionContext* ctx);

  antlrcpp::Any visitBrackettedExpression(
      Parser::BrackettedExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::BrackettedExpressionContext* ctx);

  antlrcpp::Any visitBuiltInCall(
      [[maybe_unused]] Parser::BuiltInCallContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::BuiltInCallContext* ctx);

  antlrcpp::Any visitRegexExpression(
      Parser::RegexExpressionContext* context) override {
    return visitChildren(context);
  }

  antlrcpp::Any visitSubstringExpression(
      Parser::SubstringExpressionContext* context) override {
    return visitChildren(context);
  }

  antlrcpp::Any visitStrReplaceExpression(
      Parser::StrReplaceExpressionContext* context) override {
    return visitChildren(context);
  }

  antlrcpp::Any visitExistsFunc(Parser::ExistsFuncContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNotExistsFunc(Parser::NotExistsFuncContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAggregate(Parser::AggregateContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::AggregateContext* ctx);

  antlrcpp::Any visitIriOrFunction(Parser::IriOrFunctionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(Parser::IriOrFunctionContext* ctx);

  antlrcpp::Any visitRdfLiteral(Parser::RdfLiteralContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::string visitTypesafe(Parser::RdfLiteralContext* ctx);

  antlrcpp::Any visitNumericLiteral(
      Parser::NumericLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNumericLiteralUnsigned(
      Parser::NumericLiteralUnsignedContext* ctx) override {
    // TODO: refactor to return variant
    if (ctx->INTEGER()) {
      return std::stoull(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  }

  antlrcpp::Any visitNumericLiteralPositive(
      Parser::NumericLiteralPositiveContext* ctx) override {
    if (ctx->INTEGER_POSITIVE()) {
      return std::stoull(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  }

  antlrcpp::Any visitNumericLiteralNegative(
      Parser::NumericLiteralNegativeContext* ctx) override {
    if (ctx->INTEGER_NEGATIVE()) {
      return std::stoll(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  }

  antlrcpp::Any visitBooleanLiteral(
      Parser::BooleanLiteralContext* ctx) override {
    return ctx->getText() == "true";
  }

  bool visitTypesafe(Parser::BooleanLiteralContext* ctx);

  antlrcpp::Any visitString(Parser::StringContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitIri(Parser::IriContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(Parser::IriContext* ctx);

  antlrcpp::Any visitIriref(Parser::IrirefContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(Parser::IrirefContext* ctx);

  antlrcpp::Any visitPrefixedName(Parser::PrefixedNameContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(Parser::PrefixedNameContext* ctx);

  antlrcpp::Any visitBlankNode(Parser::BlankNodeContext* ctx) override {
    return visitTypesafe(ctx);
  }

  BlankNode visitTypesafe(Parser::BlankNodeContext* ctx);

  antlrcpp::Any visitPnameLn(Parser::PnameLnContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(Parser::PnameLnContext* ctx);

  antlrcpp::Any visitPnameNs(Parser::PnameNsContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(Parser::PnameNsContext* ctx);

  template <typename Out, typename FirstContext, typename... Context>
  Out visitAlternative(FirstContext ctx, Context... ctxs);

  template <typename Out>
  Out visitAlternative();
};
