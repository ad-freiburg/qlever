
// Generated from SparqlAutomatic.g4 by ANTLR 4.9.2

#pragma once

#include "SparqlAutomaticParser.h"
#include "antlr4-runtime.h"

/**
 * This class defines an abstract visitor for a parse tree
 * produced by SparqlAutomaticParser.
 */
class SparqlAutomaticVisitor : public antlr4::tree::AbstractParseTreeVisitor {
 public:
  /**
   * Visit parse trees produced by SparqlAutomaticParser.
   */
  virtual antlrcpp::Any visitQuery(
      SparqlAutomaticParser::QueryContext* context) = 0;

  virtual antlrcpp::Any visitPrologue(
      SparqlAutomaticParser::PrologueContext* context) = 0;

  virtual antlrcpp::Any visitBaseDecl(
      SparqlAutomaticParser::BaseDeclContext* context) = 0;

  virtual antlrcpp::Any visitPrefixDecl(
      SparqlAutomaticParser::PrefixDeclContext* context) = 0;

  virtual antlrcpp::Any visitSelectQuery(
      SparqlAutomaticParser::SelectQueryContext* context) = 0;

  virtual antlrcpp::Any visitSubSelect(
      SparqlAutomaticParser::SubSelectContext* context) = 0;

  virtual antlrcpp::Any visitSelectClause(
      SparqlAutomaticParser::SelectClauseContext* context) = 0;

  virtual antlrcpp::Any visitVarOrAlias(
      SparqlAutomaticParser::VarOrAliasContext* context) = 0;

  virtual antlrcpp::Any visitAlias(
      SparqlAutomaticParser::AliasContext* context) = 0;

  virtual antlrcpp::Any visitAliasWithoutBrackets(
      SparqlAutomaticParser::AliasWithoutBracketsContext* context) = 0;

  virtual antlrcpp::Any visitConstructQuery(
      SparqlAutomaticParser::ConstructQueryContext* context) = 0;

  virtual antlrcpp::Any visitDescribeQuery(
      SparqlAutomaticParser::DescribeQueryContext* context) = 0;

  virtual antlrcpp::Any visitAskQuery(
      SparqlAutomaticParser::AskQueryContext* context) = 0;

  virtual antlrcpp::Any visitDatasetClause(
      SparqlAutomaticParser::DatasetClauseContext* context) = 0;

  virtual antlrcpp::Any visitDefaultGraphClause(
      SparqlAutomaticParser::DefaultGraphClauseContext* context) = 0;

  virtual antlrcpp::Any visitNamedGraphClause(
      SparqlAutomaticParser::NamedGraphClauseContext* context) = 0;

  virtual antlrcpp::Any visitSourceSelector(
      SparqlAutomaticParser::SourceSelectorContext* context) = 0;

  virtual antlrcpp::Any visitWhereClause(
      SparqlAutomaticParser::WhereClauseContext* context) = 0;

  virtual antlrcpp::Any visitSolutionModifier(
      SparqlAutomaticParser::SolutionModifierContext* context) = 0;

  virtual antlrcpp::Any visitGroupClause(
      SparqlAutomaticParser::GroupClauseContext* context) = 0;

  virtual antlrcpp::Any visitGroupCondition(
      SparqlAutomaticParser::GroupConditionContext* context) = 0;

  virtual antlrcpp::Any visitHavingClause(
      SparqlAutomaticParser::HavingClauseContext* context) = 0;

  virtual antlrcpp::Any visitHavingCondition(
      SparqlAutomaticParser::HavingConditionContext* context) = 0;

  virtual antlrcpp::Any visitOrderClause(
      SparqlAutomaticParser::OrderClauseContext* context) = 0;

  virtual antlrcpp::Any visitOrderCondition(
      SparqlAutomaticParser::OrderConditionContext* context) = 0;

  virtual antlrcpp::Any visitLimitOffsetClauses(
      SparqlAutomaticParser::LimitOffsetClausesContext* context) = 0;

  virtual antlrcpp::Any visitLimitClause(
      SparqlAutomaticParser::LimitClauseContext* context) = 0;

  virtual antlrcpp::Any visitOffsetClause(
      SparqlAutomaticParser::OffsetClauseContext* context) = 0;

  virtual antlrcpp::Any visitTextLimitClause(
      SparqlAutomaticParser::TextLimitClauseContext* context) = 0;

  virtual antlrcpp::Any visitValuesClause(
      SparqlAutomaticParser::ValuesClauseContext* context) = 0;

  virtual antlrcpp::Any visitTriplesTemplate(
      SparqlAutomaticParser::TriplesTemplateContext* context) = 0;

  virtual antlrcpp::Any visitGroupGraphPattern(
      SparqlAutomaticParser::GroupGraphPatternContext* context) = 0;

  virtual antlrcpp::Any visitGroupGraphPatternSub(
      SparqlAutomaticParser::GroupGraphPatternSubContext* context) = 0;

  virtual antlrcpp::Any visitTriplesBlock(
      SparqlAutomaticParser::TriplesBlockContext* context) = 0;

  virtual antlrcpp::Any visitGraphPatternNotTriples(
      SparqlAutomaticParser::GraphPatternNotTriplesContext* context) = 0;

  virtual antlrcpp::Any visitOptionalGraphPattern(
      SparqlAutomaticParser::OptionalGraphPatternContext* context) = 0;

  virtual antlrcpp::Any visitGraphGraphPattern(
      SparqlAutomaticParser::GraphGraphPatternContext* context) = 0;

  virtual antlrcpp::Any visitServiceGraphPattern(
      SparqlAutomaticParser::ServiceGraphPatternContext* context) = 0;

  virtual antlrcpp::Any visitBind(
      SparqlAutomaticParser::BindContext* context) = 0;

  virtual antlrcpp::Any visitInlineData(
      SparqlAutomaticParser::InlineDataContext* context) = 0;

  virtual antlrcpp::Any visitDataBlock(
      SparqlAutomaticParser::DataBlockContext* context) = 0;

  virtual antlrcpp::Any visitInlineDataOneVar(
      SparqlAutomaticParser::InlineDataOneVarContext* context) = 0;

  virtual antlrcpp::Any visitInlineDataFull(
      SparqlAutomaticParser::InlineDataFullContext* context) = 0;

  virtual antlrcpp::Any visitDataBlockSingle(
      SparqlAutomaticParser::DataBlockSingleContext* context) = 0;

  virtual antlrcpp::Any visitDataBlockValue(
      SparqlAutomaticParser::DataBlockValueContext* context) = 0;

  virtual antlrcpp::Any visitMinusGraphPattern(
      SparqlAutomaticParser::MinusGraphPatternContext* context) = 0;

  virtual antlrcpp::Any visitGroupOrUnionGraphPattern(
      SparqlAutomaticParser::GroupOrUnionGraphPatternContext* context) = 0;

  virtual antlrcpp::Any visitFilterR(
      SparqlAutomaticParser::FilterRContext* context) = 0;

  virtual antlrcpp::Any visitConstraint(
      SparqlAutomaticParser::ConstraintContext* context) = 0;

  virtual antlrcpp::Any visitFunctionCall(
      SparqlAutomaticParser::FunctionCallContext* context) = 0;

  virtual antlrcpp::Any visitArgList(
      SparqlAutomaticParser::ArgListContext* context) = 0;

  virtual antlrcpp::Any visitExpressionList(
      SparqlAutomaticParser::ExpressionListContext* context) = 0;

  virtual antlrcpp::Any visitConstructTemplate(
      SparqlAutomaticParser::ConstructTemplateContext* context) = 0;

  virtual antlrcpp::Any visitConstructTriples(
      SparqlAutomaticParser::ConstructTriplesContext* context) = 0;

  virtual antlrcpp::Any visitTriplesSameSubject(
      SparqlAutomaticParser::TriplesSameSubjectContext* context) = 0;

  virtual antlrcpp::Any visitPropertyList(
      SparqlAutomaticParser::PropertyListContext* context) = 0;

  virtual antlrcpp::Any visitPropertyListNotEmpty(
      SparqlAutomaticParser::PropertyListNotEmptyContext* context) = 0;

  virtual antlrcpp::Any visitVerb(
      SparqlAutomaticParser::VerbContext* context) = 0;

  virtual antlrcpp::Any visitObjectList(
      SparqlAutomaticParser::ObjectListContext* context) = 0;

  virtual antlrcpp::Any visitObjectR(
      SparqlAutomaticParser::ObjectRContext* context) = 0;

  virtual antlrcpp::Any visitTriplesSameSubjectPath(
      SparqlAutomaticParser::TriplesSameSubjectPathContext* context) = 0;

  virtual antlrcpp::Any visitPropertyListPath(
      SparqlAutomaticParser::PropertyListPathContext* context) = 0;

  virtual antlrcpp::Any visitPropertyListPathNotEmpty(
      SparqlAutomaticParser::PropertyListPathNotEmptyContext* context) = 0;

  virtual antlrcpp::Any visitVerbPath(
      SparqlAutomaticParser::VerbPathContext* context) = 0;

  virtual antlrcpp::Any visitVerbSimple(
      SparqlAutomaticParser::VerbSimpleContext* context) = 0;

  virtual antlrcpp::Any visitVerbPathOrSimple(
      SparqlAutomaticParser::VerbPathOrSimpleContext* context) = 0;

  virtual antlrcpp::Any visitObjectListPath(
      SparqlAutomaticParser::ObjectListPathContext* context) = 0;

  virtual antlrcpp::Any visitObjectPath(
      SparqlAutomaticParser::ObjectPathContext* context) = 0;

  virtual antlrcpp::Any visitPath(
      SparqlAutomaticParser::PathContext* context) = 0;

  virtual antlrcpp::Any visitPathAlternative(
      SparqlAutomaticParser::PathAlternativeContext* context) = 0;

  virtual antlrcpp::Any visitPathSequence(
      SparqlAutomaticParser::PathSequenceContext* context) = 0;

  virtual antlrcpp::Any visitPathElt(
      SparqlAutomaticParser::PathEltContext* context) = 0;

  virtual antlrcpp::Any visitPathEltOrInverse(
      SparqlAutomaticParser::PathEltOrInverseContext* context) = 0;

  virtual antlrcpp::Any visitPathMod(
      SparqlAutomaticParser::PathModContext* context) = 0;

  virtual antlrcpp::Any visitPathPrimary(
      SparqlAutomaticParser::PathPrimaryContext* context) = 0;

  virtual antlrcpp::Any visitPathNegatedPropertySet(
      SparqlAutomaticParser::PathNegatedPropertySetContext* context) = 0;

  virtual antlrcpp::Any visitPathOneInPropertySet(
      SparqlAutomaticParser::PathOneInPropertySetContext* context) = 0;

  virtual antlrcpp::Any visitInteger(
      SparqlAutomaticParser::IntegerContext* context) = 0;

  virtual antlrcpp::Any visitTriplesNode(
      SparqlAutomaticParser::TriplesNodeContext* context) = 0;

  virtual antlrcpp::Any visitBlankNodePropertyList(
      SparqlAutomaticParser::BlankNodePropertyListContext* context) = 0;

  virtual antlrcpp::Any visitTriplesNodePath(
      SparqlAutomaticParser::TriplesNodePathContext* context) = 0;

  virtual antlrcpp::Any visitBlankNodePropertyListPath(
      SparqlAutomaticParser::BlankNodePropertyListPathContext* context) = 0;

  virtual antlrcpp::Any visitCollection(
      SparqlAutomaticParser::CollectionContext* context) = 0;

  virtual antlrcpp::Any visitCollectionPath(
      SparqlAutomaticParser::CollectionPathContext* context) = 0;

  virtual antlrcpp::Any visitGraphNode(
      SparqlAutomaticParser::GraphNodeContext* context) = 0;

  virtual antlrcpp::Any visitGraphNodePath(
      SparqlAutomaticParser::GraphNodePathContext* context) = 0;

  virtual antlrcpp::Any visitVarOrTerm(
      SparqlAutomaticParser::VarOrTermContext* context) = 0;

  virtual antlrcpp::Any visitVarOrIri(
      SparqlAutomaticParser::VarOrIriContext* context) = 0;

  virtual antlrcpp::Any visitVar(
      SparqlAutomaticParser::VarContext* context) = 0;

  virtual antlrcpp::Any visitGraphTerm(
      SparqlAutomaticParser::GraphTermContext* context) = 0;

  virtual antlrcpp::Any visitExpression(
      SparqlAutomaticParser::ExpressionContext* context) = 0;

  virtual antlrcpp::Any visitConditionalOrExpression(
      SparqlAutomaticParser::ConditionalOrExpressionContext* context) = 0;

  virtual antlrcpp::Any visitConditionalAndExpression(
      SparqlAutomaticParser::ConditionalAndExpressionContext* context) = 0;

  virtual antlrcpp::Any visitValueLogical(
      SparqlAutomaticParser::ValueLogicalContext* context) = 0;

  virtual antlrcpp::Any visitRelationalExpression(
      SparqlAutomaticParser::RelationalExpressionContext* context) = 0;

  virtual antlrcpp::Any visitNumericExpression(
      SparqlAutomaticParser::NumericExpressionContext* context) = 0;

  virtual antlrcpp::Any visitAdditiveExpression(
      SparqlAutomaticParser::AdditiveExpressionContext* context) = 0;

  virtual antlrcpp::Any visitStrangeMultiplicativeSubexprOfAdditive(
      SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext*
          context) = 0;

  virtual antlrcpp::Any visitMultiplicativeExpression(
      SparqlAutomaticParser::MultiplicativeExpressionContext* context) = 0;

  virtual antlrcpp::Any visitUnaryExpression(
      SparqlAutomaticParser::UnaryExpressionContext* context) = 0;

  virtual antlrcpp::Any visitPrimaryExpression(
      SparqlAutomaticParser::PrimaryExpressionContext* context) = 0;

  virtual antlrcpp::Any visitBrackettedExpression(
      SparqlAutomaticParser::BrackettedExpressionContext* context) = 0;

  virtual antlrcpp::Any visitBuiltInCall(
      SparqlAutomaticParser::BuiltInCallContext* context) = 0;

  virtual antlrcpp::Any visitRegexExpression(
      SparqlAutomaticParser::RegexExpressionContext* context) = 0;

  virtual antlrcpp::Any visitSubstringExpression(
      SparqlAutomaticParser::SubstringExpressionContext* context) = 0;

  virtual antlrcpp::Any visitStrReplaceExpression(
      SparqlAutomaticParser::StrReplaceExpressionContext* context) = 0;

  virtual antlrcpp::Any visitExistsFunc(
      SparqlAutomaticParser::ExistsFuncContext* context) = 0;

  virtual antlrcpp::Any visitNotExistsFunc(
      SparqlAutomaticParser::NotExistsFuncContext* context) = 0;

  virtual antlrcpp::Any visitAggregate(
      SparqlAutomaticParser::AggregateContext* context) = 0;

  virtual antlrcpp::Any visitIriOrFunction(
      SparqlAutomaticParser::IriOrFunctionContext* context) = 0;

  virtual antlrcpp::Any visitRdfLiteral(
      SparqlAutomaticParser::RdfLiteralContext* context) = 0;

  virtual antlrcpp::Any visitNumericLiteral(
      SparqlAutomaticParser::NumericLiteralContext* context) = 0;

  virtual antlrcpp::Any visitNumericLiteralUnsigned(
      SparqlAutomaticParser::NumericLiteralUnsignedContext* context) = 0;

  virtual antlrcpp::Any visitNumericLiteralPositive(
      SparqlAutomaticParser::NumericLiteralPositiveContext* context) = 0;

  virtual antlrcpp::Any visitNumericLiteralNegative(
      SparqlAutomaticParser::NumericLiteralNegativeContext* context) = 0;

  virtual antlrcpp::Any visitBooleanLiteral(
      SparqlAutomaticParser::BooleanLiteralContext* context) = 0;

  virtual antlrcpp::Any visitString(
      SparqlAutomaticParser::StringContext* context) = 0;

  virtual antlrcpp::Any visitIri(
      SparqlAutomaticParser::IriContext* context) = 0;

  virtual antlrcpp::Any visitPrefixedName(
      SparqlAutomaticParser::PrefixedNameContext* context) = 0;

  virtual antlrcpp::Any visitBlankNode(
      SparqlAutomaticParser::BlankNodeContext* context) = 0;

  virtual antlrcpp::Any visitIriref(
      SparqlAutomaticParser::IrirefContext* context) = 0;

  virtual antlrcpp::Any visitPnameLn(
      SparqlAutomaticParser::PnameLnContext* context) = 0;

  virtual antlrcpp::Any visitPnameNs(
      SparqlAutomaticParser::PnameNsContext* context) = 0;
};
