
// Generated from SparqlAutomatic.g4 by ANTLR 4.9.2

#pragma once

#include "SparqlAutomaticVisitor.h"
#include "antlr4-runtime.h"

/**
 * This class provides an empty implementation of SparqlAutomaticVisitor, which
 * can be extended to create a visitor which only needs to handle a subset of
 * the available methods.
 */
class SparqlAutomaticBaseVisitor : public SparqlAutomaticVisitor {
 public:
  virtual antlrcpp::Any visitQuery(
      SparqlAutomaticParser::QueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPrologue(
      SparqlAutomaticParser::PrologueContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBaseDecl(
      SparqlAutomaticParser::BaseDeclContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPrefixDecl(
      SparqlAutomaticParser::PrefixDeclContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSelectQuery(
      SparqlAutomaticParser::SelectQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSubSelect(
      SparqlAutomaticParser::SubSelectContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSelectClause(
      SparqlAutomaticParser::SelectClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitVarOrAlias(
      SparqlAutomaticParser::VarOrAliasContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlias(
      SparqlAutomaticParser::AliasContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAliasWithoutBrackets(
      SparqlAutomaticParser::AliasWithoutBracketsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConstructQuery(
      SparqlAutomaticParser::ConstructQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDescribeQuery(
      SparqlAutomaticParser::DescribeQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAskQuery(
      SparqlAutomaticParser::AskQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDatasetClause(
      SparqlAutomaticParser::DatasetClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDefaultGraphClause(
      SparqlAutomaticParser::DefaultGraphClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNamedGraphClause(
      SparqlAutomaticParser::NamedGraphClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSourceSelector(
      SparqlAutomaticParser::SourceSelectorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitWhereClause(
      SparqlAutomaticParser::WhereClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSolutionModifier(
      SparqlAutomaticParser::SolutionModifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGroupClause(
      SparqlAutomaticParser::GroupClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGroupCondition(
      SparqlAutomaticParser::GroupConditionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitHavingClause(
      SparqlAutomaticParser::HavingClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitHavingCondition(
      SparqlAutomaticParser::HavingConditionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitOrderClause(
      SparqlAutomaticParser::OrderClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitOrderCondition(
      SparqlAutomaticParser::OrderConditionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLimitOffsetClauses(
      SparqlAutomaticParser::LimitOffsetClausesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLimitClause(
      SparqlAutomaticParser::LimitClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitOffsetClause(
      SparqlAutomaticParser::OffsetClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTextLimitClause(
      SparqlAutomaticParser::TextLimitClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitValuesClause(
      SparqlAutomaticParser::ValuesClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTriplesTemplate(
      SparqlAutomaticParser::TriplesTemplateContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGroupGraphPattern(
      SparqlAutomaticParser::GroupGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGroupGraphPatternSub(
      SparqlAutomaticParser::GroupGraphPatternSubContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTriplesBlock(
      SparqlAutomaticParser::TriplesBlockContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGraphPatternNotTriples(
      SparqlAutomaticParser::GraphPatternNotTriplesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitOptionalGraphPattern(
      SparqlAutomaticParser::OptionalGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGraphGraphPattern(
      SparqlAutomaticParser::GraphGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitServiceGraphPattern(
      SparqlAutomaticParser::ServiceGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBind(
      SparqlAutomaticParser::BindContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitInlineData(
      SparqlAutomaticParser::InlineDataContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDataBlock(
      SparqlAutomaticParser::DataBlockContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitInlineDataOneVar(
      SparqlAutomaticParser::InlineDataOneVarContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitInlineDataFull(
      SparqlAutomaticParser::InlineDataFullContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDataBlockSingle(
      SparqlAutomaticParser::DataBlockSingleContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDataBlockValue(
      SparqlAutomaticParser::DataBlockValueContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitMinusGraphPattern(
      SparqlAutomaticParser::MinusGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGroupOrUnionGraphPattern(
      SparqlAutomaticParser::GroupOrUnionGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitFilterR(
      SparqlAutomaticParser::FilterRContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConstraint(
      SparqlAutomaticParser::ConstraintContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitFunctionCall(
      SparqlAutomaticParser::FunctionCallContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitArgList(
      SparqlAutomaticParser::ArgListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpressionList(
      SparqlAutomaticParser::ExpressionListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConstructTemplate(
      SparqlAutomaticParser::ConstructTemplateContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConstructTriples(
      SparqlAutomaticParser::ConstructTriplesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTriplesSameSubject(
      SparqlAutomaticParser::TriplesSameSubjectContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPropertyList(
      SparqlAutomaticParser::PropertyListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPropertyListNotEmpty(
      SparqlAutomaticParser::PropertyListNotEmptyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitVerb(
      SparqlAutomaticParser::VerbContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitObjectList(
      SparqlAutomaticParser::ObjectListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitObjectR(
      SparqlAutomaticParser::ObjectRContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTriplesSameSubjectPath(
      SparqlAutomaticParser::TriplesSameSubjectPathContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPropertyListPath(
      SparqlAutomaticParser::PropertyListPathContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPropertyListPathNotEmpty(
      SparqlAutomaticParser::PropertyListPathNotEmptyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitVerbPath(
      SparqlAutomaticParser::VerbPathContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitVerbSimple(
      SparqlAutomaticParser::VerbSimpleContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTupleWithoutPath(
      SparqlAutomaticParser::TupleWithoutPathContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTupleWithPath(
      SparqlAutomaticParser::TupleWithPathContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitVerbPathOrSimple(
      SparqlAutomaticParser::VerbPathOrSimpleContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitObjectListPath(
      SparqlAutomaticParser::ObjectListPathContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitObjectPath(
      SparqlAutomaticParser::ObjectPathContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPath(
      SparqlAutomaticParser::PathContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPathAlternative(
      SparqlAutomaticParser::PathAlternativeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPathSequence(
      SparqlAutomaticParser::PathSequenceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPathElt(
      SparqlAutomaticParser::PathEltContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPathEltOrInverse(
      SparqlAutomaticParser::PathEltOrInverseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPathMod(
      SparqlAutomaticParser::PathModContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPathPrimary(
      SparqlAutomaticParser::PathPrimaryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPathNegatedPropertySet(
      SparqlAutomaticParser::PathNegatedPropertySetContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPathOneInPropertySet(
      SparqlAutomaticParser::PathOneInPropertySetContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitInteger(
      SparqlAutomaticParser::IntegerContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTriplesNode(
      SparqlAutomaticParser::TriplesNodeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBlankNodePropertyList(
      SparqlAutomaticParser::BlankNodePropertyListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTriplesNodePath(
      SparqlAutomaticParser::TriplesNodePathContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBlankNodePropertyListPath(
      SparqlAutomaticParser::BlankNodePropertyListPathContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCollection(
      SparqlAutomaticParser::CollectionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCollectionPath(
      SparqlAutomaticParser::CollectionPathContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGraphNode(
      SparqlAutomaticParser::GraphNodeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGraphNodePath(
      SparqlAutomaticParser::GraphNodePathContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitVarOrTerm(
      SparqlAutomaticParser::VarOrTermContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitVarOrIri(
      SparqlAutomaticParser::VarOrIriContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitVar(
      SparqlAutomaticParser::VarContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGraphTerm(
      SparqlAutomaticParser::GraphTermContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression(
      SparqlAutomaticParser::ExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConditionalOrExpression(
      SparqlAutomaticParser::ConditionalOrExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConditionalAndExpression(
      SparqlAutomaticParser::ConditionalAndExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitValueLogical(
      SparqlAutomaticParser::ValueLogicalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRelationalExpression(
      SparqlAutomaticParser::RelationalExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNumericExpression(
      SparqlAutomaticParser::NumericExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAdditiveExpression(
      SparqlAutomaticParser::AdditiveExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitStrangeMultiplicativeSubexprOfAdditive(
      SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext* ctx)
      override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitMultiplicativeExpression(
      SparqlAutomaticParser::MultiplicativeExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUnaryExpression(
      SparqlAutomaticParser::UnaryExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPrimaryExpression(
      SparqlAutomaticParser::PrimaryExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBrackettedExpression(
      SparqlAutomaticParser::BrackettedExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBuiltInCall(
      SparqlAutomaticParser::BuiltInCallContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRegexExpression(
      SparqlAutomaticParser::RegexExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSubstringExpression(
      SparqlAutomaticParser::SubstringExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitStrReplaceExpression(
      SparqlAutomaticParser::StrReplaceExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExistsFunc(
      SparqlAutomaticParser::ExistsFuncContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNotExistsFunc(
      SparqlAutomaticParser::NotExistsFuncContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAggregate(
      SparqlAutomaticParser::AggregateContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIriOrFunction(
      SparqlAutomaticParser::IriOrFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRdfLiteral(
      SparqlAutomaticParser::RdfLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNumericLiteral(
      SparqlAutomaticParser::NumericLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNumericLiteralUnsigned(
      SparqlAutomaticParser::NumericLiteralUnsignedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNumericLiteralPositive(
      SparqlAutomaticParser::NumericLiteralPositiveContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNumericLiteralNegative(
      SparqlAutomaticParser::NumericLiteralNegativeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBooleanLiteral(
      SparqlAutomaticParser::BooleanLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitString(
      SparqlAutomaticParser::StringContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIri(
      SparqlAutomaticParser::IriContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPrefixedName(
      SparqlAutomaticParser::PrefixedNameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBlankNode(
      SparqlAutomaticParser::BlankNodeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIriref(
      SparqlAutomaticParser::IrirefContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPnameLn(
      SparqlAutomaticParser::PnameLnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPnameNs(
      SparqlAutomaticParser::PnameNsContext* ctx) override {
    return visitChildren(ctx);
  }
};
