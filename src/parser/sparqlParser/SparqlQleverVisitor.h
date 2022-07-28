
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
  using Triples = ad_utility::sparql_types::Triples;
  using Node = ad_utility::sparql_types::Node;
  using ObjectList = ad_utility::sparql_types::ObjectList;
  using PropertyList = ad_utility::sparql_types::PropertyList;
  size_t _blankNodeCounter = 0;

 public:
  using PrefixMap = ad_utility::HashMap<string, string>;
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
  antlrcpp::Any visitQuery(SparqlAutomaticParser::QueryContext* ctx) override {
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
  antlrcpp::Any visitPrologue(
      SparqlAutomaticParser::PrologueContext* ctx) override {
    // Default implementation is ok here, simply handle all PREFIX and BASE
    // declarations.
    return visitChildren(ctx);
  }

  // ___________________________________________________________________________
  antlrcpp::Any visitBaseDecl(
      SparqlAutomaticParser::BaseDeclContext* ctx) override {
    visitTypesafe(ctx);
    return nullptr;
  }

  void visitTypesafe(SparqlAutomaticParser::BaseDeclContext* ctx);

  // ___________________________________________________________________________
  antlrcpp::Any visitPrefixDecl(
      SparqlAutomaticParser::PrefixDeclContext* ctx) override {
    visitTypesafe(ctx);
    return nullptr;
  }

  void visitTypesafe(SparqlAutomaticParser::PrefixDeclContext* ctx);

  antlrcpp::Any visitSelectQuery(
      SparqlAutomaticParser::SelectQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSubSelect(
      SparqlAutomaticParser::SubSelectContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSelectClause(
      SparqlAutomaticParser::SelectClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ParsedQuery::SelectClause visitTypesafe(
      SparqlAutomaticParser::SelectClauseContext* ctx);

  antlrcpp::Any visitVarOrAlias(
      SparqlAutomaticParser::VarOrAliasContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::variant<Variable, ParsedQuery::Alias> visitTypesafe(
      SparqlAutomaticParser::VarOrAliasContext* ctx);

  antlrcpp::Any visitAlias(SparqlAutomaticParser::AliasContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ParsedQuery::Alias visitTypesafe(SparqlAutomaticParser::AliasContext* ctx);

  antlrcpp::Any visitAliasWithoutBrackets(
      SparqlAutomaticParser::AliasWithoutBracketsContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ParsedQuery::Alias visitTypesafe(
      SparqlAutomaticParser::AliasWithoutBracketsContext* ctx);

  antlrcpp::Any visitConstructQuery(
      SparqlAutomaticParser::ConstructQueryContext* ctx) override {
    if (!ctx->datasetClause().empty()) {
      throw ParseException{"Datasets are not supported"};
    }
    if (ctx->constructTemplate()) {
      return ctx->constructTemplate()->accept(this);
    }
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDescribeQuery(
      SparqlAutomaticParser::DescribeQueryContext* ctx) override {
    // TODO: unsupported
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAskQuery(
      SparqlAutomaticParser::AskQueryContext* ctx) override {
    // TODO: unsupported
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDatasetClause(
      SparqlAutomaticParser::DatasetClauseContext* ctx) override {
    // TODO: unsupported
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDefaultGraphClause(
      SparqlAutomaticParser::DefaultGraphClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNamedGraphClause(
      SparqlAutomaticParser::NamedGraphClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSourceSelector(
      SparqlAutomaticParser::SourceSelectorContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitWhereClause(
      SparqlAutomaticParser::WhereClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSolutionModifier(
      SparqlAutomaticParser::SolutionModifierContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupClause(
      SparqlAutomaticParser::GroupClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<GroupKey> visitTypesafe(
      SparqlAutomaticParser::GroupClauseContext* ctx);

  antlrcpp::Any visitGroupCondition(
      SparqlAutomaticParser::GroupConditionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GroupKey visitTypesafe(SparqlAutomaticParser::GroupConditionContext* ctx);

  antlrcpp::Any visitHavingClause(
      SparqlAutomaticParser::HavingClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitHavingCondition(
      SparqlAutomaticParser::HavingConditionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitOrderClause(
      SparqlAutomaticParser::OrderClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<OrderKey> visitTypesafe(
      SparqlAutomaticParser::OrderClauseContext* ctx);

  antlrcpp::Any visitOrderCondition(
      SparqlAutomaticParser::OrderConditionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  OrderKey visitTypesafe(SparqlAutomaticParser::OrderConditionContext* ctx);

  antlrcpp::Any visitLimitOffsetClauses(
      SparqlAutomaticParser::LimitOffsetClausesContext* ctx) override {
    return visitTypesafe(ctx);
  }

  LimitOffsetClause visitTypesafe(
      SparqlAutomaticParser::LimitOffsetClausesContext* ctx);

  antlrcpp::Any visitLimitClause(
      SparqlAutomaticParser::LimitClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  unsigned long long int visitTypesafe(
      SparqlAutomaticParser::LimitClauseContext* ctx);

  antlrcpp::Any visitOffsetClause(
      SparqlAutomaticParser::OffsetClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  unsigned long long int visitTypesafe(
      SparqlAutomaticParser::OffsetClauseContext* ctx);

  antlrcpp::Any visitTextLimitClause(
      SparqlAutomaticParser::TextLimitClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  unsigned long long int visitTypesafe(
      SparqlAutomaticParser::TextLimitClauseContext* ctx);

  antlrcpp::Any visitValuesClause(
      SparqlAutomaticParser::ValuesClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::optional<GraphPatternOperation::Values> visitTypesafe(
      SparqlAutomaticParser::ValuesClauseContext* ctx);

  antlrcpp::Any visitTriplesTemplate(
      SparqlAutomaticParser::TriplesTemplateContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupGraphPattern(
      SparqlAutomaticParser::GroupGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupGraphPatternSub(
      SparqlAutomaticParser::GroupGraphPatternSubContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitTriplesBlock(
      SparqlAutomaticParser::TriplesBlockContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphPatternNotTriples(
      SparqlAutomaticParser::GraphPatternNotTriplesContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitOptionalGraphPattern(
      SparqlAutomaticParser::OptionalGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphGraphPattern(
      SparqlAutomaticParser::GraphGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitServiceGraphPattern(
      SparqlAutomaticParser::ServiceGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBind(SparqlAutomaticParser::BindContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GraphPatternOperation::Bind visitTypesafe(
      SparqlAutomaticParser::BindContext* ctx);

  antlrcpp::Any visitInlineData(
      SparqlAutomaticParser::InlineDataContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GraphPatternOperation::Values visitTypesafe(
      SparqlAutomaticParser::InlineDataContext* ctx);

  antlrcpp::Any visitDataBlock(
      SparqlAutomaticParser::DataBlockContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GraphPatternOperation::Values visitTypesafe(
      SparqlAutomaticParser::DataBlockContext* ctx);

  antlrcpp::Any visitInlineDataOneVar(
      SparqlAutomaticParser::InlineDataOneVarContext* ctx) override {
    return visitTypesafe(ctx);
  }

  SparqlValues visitTypesafe(
      SparqlAutomaticParser::InlineDataOneVarContext* ctx);

  antlrcpp::Any visitInlineDataFull(
      SparqlAutomaticParser::InlineDataFullContext* ctx) override {
    return visitTypesafe(ctx);
  }

  SparqlValues visitTypesafe(SparqlAutomaticParser::InlineDataFullContext* ctx);

  antlrcpp::Any visitDataBlockSingle(
      SparqlAutomaticParser::DataBlockSingleContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<std::string> visitTypesafe(
      SparqlAutomaticParser::DataBlockSingleContext* ctx);

  antlrcpp::Any visitDataBlockValue(
      SparqlAutomaticParser::DataBlockValueContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::string visitTypesafe(SparqlAutomaticParser::DataBlockValueContext* ctx);

  antlrcpp::Any visitMinusGraphPattern(
      SparqlAutomaticParser::MinusGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupOrUnionGraphPattern(
      SparqlAutomaticParser::GroupOrUnionGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitFilterR(
      SparqlAutomaticParser::FilterRContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConstraint(
      SparqlAutomaticParser::ConstraintContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitFunctionCall(
      SparqlAutomaticParser::FunctionCallContext* ctx) override {
    return processIriFunctionCall(
        visitIri(ctx->iri()).as<std::string>(),
        std::move(
            visitArgList(ctx->argList()).as<std::vector<ExpressionPtr>>()));
  }

  antlrcpp::Any visitArgList(
      SparqlAutomaticParser::ArgListContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<ExpressionPtr> visitTypesafe(
      SparqlAutomaticParser::ArgListContext* ctx);

  antlrcpp::Any visitExpressionList(
      SparqlAutomaticParser::ExpressionListContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConstructTemplate(
      SparqlAutomaticParser::ConstructTemplateContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Triples visitTypesafe(SparqlAutomaticParser::ConstructTemplateContext* ctx);

  antlrcpp::Any visitConstructTriples(
      SparqlAutomaticParser::ConstructTriplesContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Triples visitTypesafe(SparqlAutomaticParser::ConstructTriplesContext* ctx);

  antlrcpp::Any visitTriplesSameSubject(
      SparqlAutomaticParser::TriplesSameSubjectContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Triples visitTypesafe(SparqlAutomaticParser::TriplesSameSubjectContext* ctx);

  antlrcpp::Any visitPropertyList(
      SparqlAutomaticParser::PropertyListContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyList visitTypesafe(SparqlAutomaticParser::PropertyListContext* ctx);

  antlrcpp::Any visitPropertyListNotEmpty(
      SparqlAutomaticParser::PropertyListNotEmptyContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyList visitTypesafe(
      SparqlAutomaticParser::PropertyListNotEmptyContext* ctx);

  antlrcpp::Any visitVerb(SparqlAutomaticParser::VerbContext* ctx) override {
    return visitTypesafe(ctx);
  }

  VarOrTerm visitTypesafe(SparqlAutomaticParser::VerbContext* ctx);

  antlrcpp::Any visitObjectList(
      SparqlAutomaticParser::ObjectListContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ObjectList visitTypesafe(SparqlAutomaticParser::ObjectListContext* ctx);

  antlrcpp::Any visitObjectR(
      SparqlAutomaticParser::ObjectRContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Node visitTypesafe(SparqlAutomaticParser::ObjectRContext* ctx);

  antlrcpp::Any visitTriplesSameSubjectPath(
      SparqlAutomaticParser::TriplesSameSubjectPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPropertyListPath(
      SparqlAutomaticParser::PropertyListPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPropertyListPathNotEmpty(
      SparqlAutomaticParser::PropertyListPathNotEmptyContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVerbPath(
      SparqlAutomaticParser::VerbPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(SparqlAutomaticParser::VerbPathContext* ctx);

  antlrcpp::Any visitVerbSimple(
      SparqlAutomaticParser::VerbSimpleContext* ctx) override {
    return visitChildren(ctx);
  }

  Variable visitTypesafe(SparqlAutomaticParser::VerbSimpleContext* ctx);

  antlrcpp::Any visitVerbPathOrSimple(
      SparqlAutomaticParser::VerbPathOrSimpleContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(
      SparqlAutomaticParser::VerbPathOrSimpleContext* ctx);

  antlrcpp::Any visitObjectListPath(
      SparqlAutomaticParser::ObjectListPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitObjectPath(
      SparqlAutomaticParser::ObjectPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPath(SparqlAutomaticParser::PathContext* ctx) override {
    // returns PropertyPath
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(SparqlAutomaticParser::PathContext* ctx);

  antlrcpp::Any visitPathAlternative(
      SparqlAutomaticParser::PathAlternativeContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(
      SparqlAutomaticParser::PathAlternativeContext* ctx);

  antlrcpp::Any visitPathSequence(
      SparqlAutomaticParser::PathSequenceContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(SparqlAutomaticParser::PathSequenceContext* ctx);

  antlrcpp::Any visitPathElt(
      SparqlAutomaticParser::PathEltContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(SparqlAutomaticParser::PathEltContext* ctx);

  antlrcpp::Any visitPathEltOrInverse(
      SparqlAutomaticParser::PathEltOrInverseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(
      SparqlAutomaticParser::PathEltOrInverseContext* ctx);

  antlrcpp::Any visitPathMod(
      SparqlAutomaticParser::PathModContext* ctx) override {
    // Handled in visitPathElt.
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathPrimary(
      SparqlAutomaticParser::PathPrimaryContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(SparqlAutomaticParser::PathPrimaryContext* ctx);

  antlrcpp::Any visitPathNegatedPropertySet(
      SparqlAutomaticParser::PathNegatedPropertySetContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(
      SparqlAutomaticParser::PathNegatedPropertySetContext*);

  antlrcpp::Any visitPathOneInPropertySet(
      SparqlAutomaticParser::PathOneInPropertySetContext*) override {
    throw ParseException(
        R"("!" and "^" inside a property path is not supported by QLever.)");
  }

  /// Note that in the SPARQL grammar the INTEGER rule refers to positive
  /// integers without an explicit sign.
  antlrcpp::Any visitInteger(
      SparqlAutomaticParser::IntegerContext* ctx) override {
    return visitTypesafe(ctx);
  }

  unsigned long long int visitTypesafe(
      SparqlAutomaticParser::IntegerContext* ctx);

  antlrcpp::Any visitTriplesNode(
      SparqlAutomaticParser::TriplesNodeContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBlankNodePropertyList(
      SparqlAutomaticParser::BlankNodePropertyListContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Node visitTypesafe(SparqlAutomaticParser::BlankNodePropertyListContext* ctx);

  antlrcpp::Any visitTriplesNodePath(
      SparqlAutomaticParser::TriplesNodePathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBlankNodePropertyListPath(
      SparqlAutomaticParser::BlankNodePropertyListPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitCollection(
      SparqlAutomaticParser::CollectionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Node visitTypesafe(SparqlAutomaticParser::CollectionContext* ctx);

  antlrcpp::Any visitCollectionPath(
      SparqlAutomaticParser::CollectionPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphNode(
      SparqlAutomaticParser::GraphNodeContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Node visitTypesafe(SparqlAutomaticParser::GraphNodeContext* ctx);

  antlrcpp::Any visitGraphNodePath(
      SparqlAutomaticParser::GraphNodePathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVarOrTerm(
      SparqlAutomaticParser::VarOrTermContext* ctx) override {
    return visitTypesafe(ctx);
  }

  VarOrTerm visitTypesafe(SparqlAutomaticParser::VarOrTermContext* ctx);

  antlrcpp::Any visitVarOrIri(
      SparqlAutomaticParser::VarOrIriContext* ctx) override {
    return visitTypesafe(ctx);
  }

  VarOrTerm visitTypesafe(SparqlAutomaticParser::VarOrIriContext* ctx);

  antlrcpp::Any visitVar(SparqlAutomaticParser::VarContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Variable visitTypesafe(SparqlAutomaticParser::VarContext* ctx);

  antlrcpp::Any visitGraphTerm(
      SparqlAutomaticParser::GraphTermContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GraphTerm visitTypesafe(SparqlAutomaticParser::GraphTermContext* ctx);

  antlrcpp::Any visitExpression(
      SparqlAutomaticParser::ExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(SparqlAutomaticParser::ExpressionContext* ctx);

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
      SparqlAutomaticParser::ConditionalOrExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(
      SparqlAutomaticParser::ConditionalOrExpressionContext* ctx);

  antlrcpp::Any visitConditionalAndExpression(
      SparqlAutomaticParser::ConditionalAndExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(
      SparqlAutomaticParser::ConditionalAndExpressionContext* ctx);

  antlrcpp::Any visitValueLogical(
      SparqlAutomaticParser::ValueLogicalContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(SparqlAutomaticParser::ValueLogicalContext* ctx);

  antlrcpp::Any visitRelationalExpression(
      SparqlAutomaticParser::RelationalExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(
      SparqlAutomaticParser::RelationalExpressionContext* ctx);

  antlrcpp::Any visitNumericExpression(
      SparqlAutomaticParser::NumericExpressionContext* ctx) override {
    return std::move(visitChildren(ctx).as<ExpressionPtr>());
  }

  ExpressionPtr visitTypesafe(
      SparqlAutomaticParser::NumericExpressionContext* ctx);

  template <typename Expr>
  ExpressionPtr createExpression(auto... children) {
    return std::make_unique<Expr>(
        std::array<ExpressionPtr, sizeof...(children)>{std::move(children)...});
  }

  antlrcpp::Any visitAdditiveExpression(
      SparqlAutomaticParser::AdditiveExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(
      SparqlAutomaticParser::AdditiveExpressionContext* ctx);

  antlrcpp::Any visitStrangeMultiplicativeSubexprOfAdditive(
      SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext*
          context) override {
    return visitChildren(context);
  }

  antlrcpp::Any visitMultiplicativeExpression(
      SparqlAutomaticParser::MultiplicativeExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(
      SparqlAutomaticParser::MultiplicativeExpressionContext* ctx);

  antlrcpp::Any visitUnaryExpression(
      SparqlAutomaticParser::UnaryExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(
      SparqlAutomaticParser::UnaryExpressionContext* ctx);

  antlrcpp::Any visitPrimaryExpression(
      SparqlAutomaticParser::PrimaryExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(
      SparqlAutomaticParser::PrimaryExpressionContext* ctx);

  antlrcpp::Any visitBrackettedExpression(
      SparqlAutomaticParser::BrackettedExpressionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(
      SparqlAutomaticParser::BrackettedExpressionContext* ctx);

  antlrcpp::Any visitBuiltInCall(
      [[maybe_unused]] SparqlAutomaticParser::BuiltInCallContext* ctx)
      override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(SparqlAutomaticParser::BuiltInCallContext* ctx);

  antlrcpp::Any visitRegexExpression(
      SparqlAutomaticParser::RegexExpressionContext* context) override {
    return visitChildren(context);
  }

  antlrcpp::Any visitSubstringExpression(
      SparqlAutomaticParser::SubstringExpressionContext* context) override {
    return visitChildren(context);
  }

  antlrcpp::Any visitStrReplaceExpression(
      SparqlAutomaticParser::StrReplaceExpressionContext* context) override {
    return visitChildren(context);
  }

  antlrcpp::Any visitExistsFunc(
      SparqlAutomaticParser::ExistsFuncContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNotExistsFunc(
      SparqlAutomaticParser::NotExistsFuncContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAggregate(
      SparqlAutomaticParser::AggregateContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(SparqlAutomaticParser::AggregateContext* ctx);

  antlrcpp::Any visitIriOrFunction(
      SparqlAutomaticParser::IriOrFunctionContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ExpressionPtr visitTypesafe(SparqlAutomaticParser::IriOrFunctionContext* ctx);

  antlrcpp::Any visitRdfLiteral(
      SparqlAutomaticParser::RdfLiteralContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::string visitTypesafe(SparqlAutomaticParser::RdfLiteralContext* ctx);

  antlrcpp::Any visitNumericLiteral(
      SparqlAutomaticParser::NumericLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNumericLiteralUnsigned(
      SparqlAutomaticParser::NumericLiteralUnsignedContext* ctx) override {
    // TODO: refactor to return variant
    if (ctx->INTEGER()) {
      return std::stoull(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  }

  antlrcpp::Any visitNumericLiteralPositive(
      SparqlAutomaticParser::NumericLiteralPositiveContext* ctx) override {
    if (ctx->INTEGER_POSITIVE()) {
      return std::stoull(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  }

  antlrcpp::Any visitNumericLiteralNegative(
      SparqlAutomaticParser::NumericLiteralNegativeContext* ctx) override {
    if (ctx->INTEGER_NEGATIVE()) {
      return std::stoll(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  }

  antlrcpp::Any visitBooleanLiteral(
      SparqlAutomaticParser::BooleanLiteralContext* ctx) override {
    return ctx->getText() == "true";
  }

  bool visitTypesafe(SparqlAutomaticParser::BooleanLiteralContext* ctx);

  antlrcpp::Any visitString(
      SparqlAutomaticParser::StringContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitIri(SparqlAutomaticParser::IriContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(SparqlAutomaticParser::IriContext* ctx);

  antlrcpp::Any visitIriref(
      SparqlAutomaticParser::IrirefContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(SparqlAutomaticParser::IrirefContext* ctx);

  antlrcpp::Any visitPrefixedName(
      SparqlAutomaticParser::PrefixedNameContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(SparqlAutomaticParser::PrefixedNameContext* ctx);

  antlrcpp::Any visitBlankNode(
      SparqlAutomaticParser::BlankNodeContext* ctx) override {
    return visitTypesafe(ctx);
  }

  BlankNode visitTypesafe(SparqlAutomaticParser::BlankNodeContext* ctx);

  antlrcpp::Any visitPnameLn(
      SparqlAutomaticParser::PnameLnContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(SparqlAutomaticParser::PnameLnContext* ctx);

  antlrcpp::Any visitPnameNs(
      SparqlAutomaticParser::PnameNsContext* ctx) override {
    return visitTypesafe(ctx);
  }

  string visitTypesafe(SparqlAutomaticParser::PnameNsContext* ctx);
};
