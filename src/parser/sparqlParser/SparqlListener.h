
// Generated from Sparql.g4 by ANTLR 4.9.2

#pragma once


#include "antlr4-runtime.h"
#include "SparqlParser.h"


/**
 * This interface defines an abstract listener for a parse tree produced by SparqlParser.
 */
class  SparqlListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterQuery(SparqlParser::QueryContext *ctx) = 0;
  virtual void exitQuery(SparqlParser::QueryContext *ctx) = 0;

  virtual void enterPrologue(SparqlParser::PrologueContext *ctx) = 0;
  virtual void exitPrologue(SparqlParser::PrologueContext *ctx) = 0;

  virtual void enterBaseDecl(SparqlParser::BaseDeclContext *ctx) = 0;
  virtual void exitBaseDecl(SparqlParser::BaseDeclContext *ctx) = 0;

  virtual void enterPrefixDecl(SparqlParser::PrefixDeclContext *ctx) = 0;
  virtual void exitPrefixDecl(SparqlParser::PrefixDeclContext *ctx) = 0;

  virtual void enterSelectQuery(SparqlParser::SelectQueryContext *ctx) = 0;
  virtual void exitSelectQuery(SparqlParser::SelectQueryContext *ctx) = 0;

  virtual void enterSubSelect(SparqlParser::SubSelectContext *ctx) = 0;
  virtual void exitSubSelect(SparqlParser::SubSelectContext *ctx) = 0;

  virtual void enterSelectClause(SparqlParser::SelectClauseContext *ctx) = 0;
  virtual void exitSelectClause(SparqlParser::SelectClauseContext *ctx) = 0;

  virtual void enterAlias(SparqlParser::AliasContext *ctx) = 0;
  virtual void exitAlias(SparqlParser::AliasContext *ctx) = 0;

  virtual void enterConstructQuery(SparqlParser::ConstructQueryContext *ctx) = 0;
  virtual void exitConstructQuery(SparqlParser::ConstructQueryContext *ctx) = 0;

  virtual void enterDescribeQuery(SparqlParser::DescribeQueryContext *ctx) = 0;
  virtual void exitDescribeQuery(SparqlParser::DescribeQueryContext *ctx) = 0;

  virtual void enterAskQuery(SparqlParser::AskQueryContext *ctx) = 0;
  virtual void exitAskQuery(SparqlParser::AskQueryContext *ctx) = 0;

  virtual void enterDatasetClause(SparqlParser::DatasetClauseContext *ctx) = 0;
  virtual void exitDatasetClause(SparqlParser::DatasetClauseContext *ctx) = 0;

  virtual void enterDefaultGraphClause(SparqlParser::DefaultGraphClauseContext *ctx) = 0;
  virtual void exitDefaultGraphClause(SparqlParser::DefaultGraphClauseContext *ctx) = 0;

  virtual void enterNamedGraphClause(SparqlParser::NamedGraphClauseContext *ctx) = 0;
  virtual void exitNamedGraphClause(SparqlParser::NamedGraphClauseContext *ctx) = 0;

  virtual void enterSourceSelector(SparqlParser::SourceSelectorContext *ctx) = 0;
  virtual void exitSourceSelector(SparqlParser::SourceSelectorContext *ctx) = 0;

  virtual void enterWhereClause(SparqlParser::WhereClauseContext *ctx) = 0;
  virtual void exitWhereClause(SparqlParser::WhereClauseContext *ctx) = 0;

  virtual void enterSolutionModifier(SparqlParser::SolutionModifierContext *ctx) = 0;
  virtual void exitSolutionModifier(SparqlParser::SolutionModifierContext *ctx) = 0;

  virtual void enterGroupClause(SparqlParser::GroupClauseContext *ctx) = 0;
  virtual void exitGroupClause(SparqlParser::GroupClauseContext *ctx) = 0;

  virtual void enterGroupCondition(SparqlParser::GroupConditionContext *ctx) = 0;
  virtual void exitGroupCondition(SparqlParser::GroupConditionContext *ctx) = 0;

  virtual void enterHavingClause(SparqlParser::HavingClauseContext *ctx) = 0;
  virtual void exitHavingClause(SparqlParser::HavingClauseContext *ctx) = 0;

  virtual void enterHavingCondition(SparqlParser::HavingConditionContext *ctx) = 0;
  virtual void exitHavingCondition(SparqlParser::HavingConditionContext *ctx) = 0;

  virtual void enterOrderClause(SparqlParser::OrderClauseContext *ctx) = 0;
  virtual void exitOrderClause(SparqlParser::OrderClauseContext *ctx) = 0;

  virtual void enterOrderCondition(SparqlParser::OrderConditionContext *ctx) = 0;
  virtual void exitOrderCondition(SparqlParser::OrderConditionContext *ctx) = 0;

  virtual void enterLimitOffsetClauses(SparqlParser::LimitOffsetClausesContext *ctx) = 0;
  virtual void exitLimitOffsetClauses(SparqlParser::LimitOffsetClausesContext *ctx) = 0;

  virtual void enterLimitClause(SparqlParser::LimitClauseContext *ctx) = 0;
  virtual void exitLimitClause(SparqlParser::LimitClauseContext *ctx) = 0;

  virtual void enterOffsetClause(SparqlParser::OffsetClauseContext *ctx) = 0;
  virtual void exitOffsetClause(SparqlParser::OffsetClauseContext *ctx) = 0;

  virtual void enterValuesClause(SparqlParser::ValuesClauseContext *ctx) = 0;
  virtual void exitValuesClause(SparqlParser::ValuesClauseContext *ctx) = 0;

  virtual void enterTriplesTemplate(SparqlParser::TriplesTemplateContext *ctx) = 0;
  virtual void exitTriplesTemplate(SparqlParser::TriplesTemplateContext *ctx) = 0;

  virtual void enterGroupGraphPattern(SparqlParser::GroupGraphPatternContext *ctx) = 0;
  virtual void exitGroupGraphPattern(SparqlParser::GroupGraphPatternContext *ctx) = 0;

  virtual void enterGroupGraphPatternSub(SparqlParser::GroupGraphPatternSubContext *ctx) = 0;
  virtual void exitGroupGraphPatternSub(SparqlParser::GroupGraphPatternSubContext *ctx) = 0;

  virtual void enterTriplesBlock(SparqlParser::TriplesBlockContext *ctx) = 0;
  virtual void exitTriplesBlock(SparqlParser::TriplesBlockContext *ctx) = 0;

  virtual void enterGraphPatternNotTriples(SparqlParser::GraphPatternNotTriplesContext *ctx) = 0;
  virtual void exitGraphPatternNotTriples(SparqlParser::GraphPatternNotTriplesContext *ctx) = 0;

  virtual void enterOptionalGraphPattern(SparqlParser::OptionalGraphPatternContext *ctx) = 0;
  virtual void exitOptionalGraphPattern(SparqlParser::OptionalGraphPatternContext *ctx) = 0;

  virtual void enterGraphGraphPattern(SparqlParser::GraphGraphPatternContext *ctx) = 0;
  virtual void exitGraphGraphPattern(SparqlParser::GraphGraphPatternContext *ctx) = 0;

  virtual void enterServiceGraphPattern(SparqlParser::ServiceGraphPatternContext *ctx) = 0;
  virtual void exitServiceGraphPattern(SparqlParser::ServiceGraphPatternContext *ctx) = 0;

  virtual void enterBind(SparqlParser::BindContext *ctx) = 0;
  virtual void exitBind(SparqlParser::BindContext *ctx) = 0;

  virtual void enterInlineData(SparqlParser::InlineDataContext *ctx) = 0;
  virtual void exitInlineData(SparqlParser::InlineDataContext *ctx) = 0;

  virtual void enterDataBlock(SparqlParser::DataBlockContext *ctx) = 0;
  virtual void exitDataBlock(SparqlParser::DataBlockContext *ctx) = 0;

  virtual void enterInlineDataOneVar(SparqlParser::InlineDataOneVarContext *ctx) = 0;
  virtual void exitInlineDataOneVar(SparqlParser::InlineDataOneVarContext *ctx) = 0;

  virtual void enterInlineDataFull(SparqlParser::InlineDataFullContext *ctx) = 0;
  virtual void exitInlineDataFull(SparqlParser::InlineDataFullContext *ctx) = 0;

  virtual void enterDataBlockSingle(SparqlParser::DataBlockSingleContext *ctx) = 0;
  virtual void exitDataBlockSingle(SparqlParser::DataBlockSingleContext *ctx) = 0;

  virtual void enterDataBlockValue(SparqlParser::DataBlockValueContext *ctx) = 0;
  virtual void exitDataBlockValue(SparqlParser::DataBlockValueContext *ctx) = 0;

  virtual void enterMinusGraphPattern(SparqlParser::MinusGraphPatternContext *ctx) = 0;
  virtual void exitMinusGraphPattern(SparqlParser::MinusGraphPatternContext *ctx) = 0;

  virtual void enterGroupOrUnionGraphPattern(SparqlParser::GroupOrUnionGraphPatternContext *ctx) = 0;
  virtual void exitGroupOrUnionGraphPattern(SparqlParser::GroupOrUnionGraphPatternContext *ctx) = 0;

  virtual void enterFilterR(SparqlParser::FilterRContext *ctx) = 0;
  virtual void exitFilterR(SparqlParser::FilterRContext *ctx) = 0;

  virtual void enterConstraint(SparqlParser::ConstraintContext *ctx) = 0;
  virtual void exitConstraint(SparqlParser::ConstraintContext *ctx) = 0;

  virtual void enterFunctionCall(SparqlParser::FunctionCallContext *ctx) = 0;
  virtual void exitFunctionCall(SparqlParser::FunctionCallContext *ctx) = 0;

  virtual void enterArgList(SparqlParser::ArgListContext *ctx) = 0;
  virtual void exitArgList(SparqlParser::ArgListContext *ctx) = 0;

  virtual void enterExpressionList(SparqlParser::ExpressionListContext *ctx) = 0;
  virtual void exitExpressionList(SparqlParser::ExpressionListContext *ctx) = 0;

  virtual void enterConstructTemplate(SparqlParser::ConstructTemplateContext *ctx) = 0;
  virtual void exitConstructTemplate(SparqlParser::ConstructTemplateContext *ctx) = 0;

  virtual void enterConstructTriples(SparqlParser::ConstructTriplesContext *ctx) = 0;
  virtual void exitConstructTriples(SparqlParser::ConstructTriplesContext *ctx) = 0;

  virtual void enterTriplesSameSubject(SparqlParser::TriplesSameSubjectContext *ctx) = 0;
  virtual void exitTriplesSameSubject(SparqlParser::TriplesSameSubjectContext *ctx) = 0;

  virtual void enterPropertyList(SparqlParser::PropertyListContext *ctx) = 0;
  virtual void exitPropertyList(SparqlParser::PropertyListContext *ctx) = 0;

  virtual void enterPropertyListNotEmpty(SparqlParser::PropertyListNotEmptyContext *ctx) = 0;
  virtual void exitPropertyListNotEmpty(SparqlParser::PropertyListNotEmptyContext *ctx) = 0;

  virtual void enterVerb(SparqlParser::VerbContext *ctx) = 0;
  virtual void exitVerb(SparqlParser::VerbContext *ctx) = 0;

  virtual void enterObjectList(SparqlParser::ObjectListContext *ctx) = 0;
  virtual void exitObjectList(SparqlParser::ObjectListContext *ctx) = 0;

  virtual void enterObjectR(SparqlParser::ObjectRContext *ctx) = 0;
  virtual void exitObjectR(SparqlParser::ObjectRContext *ctx) = 0;

  virtual void enterTriplesSameSubjectPath(SparqlParser::TriplesSameSubjectPathContext *ctx) = 0;
  virtual void exitTriplesSameSubjectPath(SparqlParser::TriplesSameSubjectPathContext *ctx) = 0;

  virtual void enterPropertyListPath(SparqlParser::PropertyListPathContext *ctx) = 0;
  virtual void exitPropertyListPath(SparqlParser::PropertyListPathContext *ctx) = 0;

  virtual void enterPropertyListPathNotEmpty(SparqlParser::PropertyListPathNotEmptyContext *ctx) = 0;
  virtual void exitPropertyListPathNotEmpty(SparqlParser::PropertyListPathNotEmptyContext *ctx) = 0;

  virtual void enterVerbPath(SparqlParser::VerbPathContext *ctx) = 0;
  virtual void exitVerbPath(SparqlParser::VerbPathContext *ctx) = 0;

  virtual void enterVerbSimple(SparqlParser::VerbSimpleContext *ctx) = 0;
  virtual void exitVerbSimple(SparqlParser::VerbSimpleContext *ctx) = 0;

  virtual void enterVerbPathOrSimple(SparqlParser::VerbPathOrSimpleContext *ctx) = 0;
  virtual void exitVerbPathOrSimple(SparqlParser::VerbPathOrSimpleContext *ctx) = 0;

  virtual void enterObjectListPath(SparqlParser::ObjectListPathContext *ctx) = 0;
  virtual void exitObjectListPath(SparqlParser::ObjectListPathContext *ctx) = 0;

  virtual void enterObjectPath(SparqlParser::ObjectPathContext *ctx) = 0;
  virtual void exitObjectPath(SparqlParser::ObjectPathContext *ctx) = 0;

  virtual void enterPath(SparqlParser::PathContext *ctx) = 0;
  virtual void exitPath(SparqlParser::PathContext *ctx) = 0;

  virtual void enterPathAlternative(SparqlParser::PathAlternativeContext *ctx) = 0;
  virtual void exitPathAlternative(SparqlParser::PathAlternativeContext *ctx) = 0;

  virtual void enterPathSequence(SparqlParser::PathSequenceContext *ctx) = 0;
  virtual void exitPathSequence(SparqlParser::PathSequenceContext *ctx) = 0;

  virtual void enterPathElt(SparqlParser::PathEltContext *ctx) = 0;
  virtual void exitPathElt(SparqlParser::PathEltContext *ctx) = 0;

  virtual void enterPathEltOrInverse(SparqlParser::PathEltOrInverseContext *ctx) = 0;
  virtual void exitPathEltOrInverse(SparqlParser::PathEltOrInverseContext *ctx) = 0;

  virtual void enterPathMod(SparqlParser::PathModContext *ctx) = 0;
  virtual void exitPathMod(SparqlParser::PathModContext *ctx) = 0;

  virtual void enterPathPrimary(SparqlParser::PathPrimaryContext *ctx) = 0;
  virtual void exitPathPrimary(SparqlParser::PathPrimaryContext *ctx) = 0;

  virtual void enterPathNegatedPropertySet(SparqlParser::PathNegatedPropertySetContext *ctx) = 0;
  virtual void exitPathNegatedPropertySet(SparqlParser::PathNegatedPropertySetContext *ctx) = 0;

  virtual void enterPathOneInPropertySet(SparqlParser::PathOneInPropertySetContext *ctx) = 0;
  virtual void exitPathOneInPropertySet(SparqlParser::PathOneInPropertySetContext *ctx) = 0;

  virtual void enterInteger(SparqlParser::IntegerContext *ctx) = 0;
  virtual void exitInteger(SparqlParser::IntegerContext *ctx) = 0;

  virtual void enterTriplesNode(SparqlParser::TriplesNodeContext *ctx) = 0;
  virtual void exitTriplesNode(SparqlParser::TriplesNodeContext *ctx) = 0;

  virtual void enterBlankNodePropertyList(SparqlParser::BlankNodePropertyListContext *ctx) = 0;
  virtual void exitBlankNodePropertyList(SparqlParser::BlankNodePropertyListContext *ctx) = 0;

  virtual void enterTriplesNodePath(SparqlParser::TriplesNodePathContext *ctx) = 0;
  virtual void exitTriplesNodePath(SparqlParser::TriplesNodePathContext *ctx) = 0;

  virtual void enterBlankNodePropertyListPath(SparqlParser::BlankNodePropertyListPathContext *ctx) = 0;
  virtual void exitBlankNodePropertyListPath(SparqlParser::BlankNodePropertyListPathContext *ctx) = 0;

  virtual void enterCollection(SparqlParser::CollectionContext *ctx) = 0;
  virtual void exitCollection(SparqlParser::CollectionContext *ctx) = 0;

  virtual void enterCollectionPath(SparqlParser::CollectionPathContext *ctx) = 0;
  virtual void exitCollectionPath(SparqlParser::CollectionPathContext *ctx) = 0;

  virtual void enterGraphNode(SparqlParser::GraphNodeContext *ctx) = 0;
  virtual void exitGraphNode(SparqlParser::GraphNodeContext *ctx) = 0;

  virtual void enterGraphNodePath(SparqlParser::GraphNodePathContext *ctx) = 0;
  virtual void exitGraphNodePath(SparqlParser::GraphNodePathContext *ctx) = 0;

  virtual void enterVarOrTerm(SparqlParser::VarOrTermContext *ctx) = 0;
  virtual void exitVarOrTerm(SparqlParser::VarOrTermContext *ctx) = 0;

  virtual void enterVarOrIri(SparqlParser::VarOrIriContext *ctx) = 0;
  virtual void exitVarOrIri(SparqlParser::VarOrIriContext *ctx) = 0;

  virtual void enterVar(SparqlParser::VarContext *ctx) = 0;
  virtual void exitVar(SparqlParser::VarContext *ctx) = 0;

  virtual void enterGraphTerm(SparqlParser::GraphTermContext *ctx) = 0;
  virtual void exitGraphTerm(SparqlParser::GraphTermContext *ctx) = 0;

  virtual void enterExpression(SparqlParser::ExpressionContext *ctx) = 0;
  virtual void exitExpression(SparqlParser::ExpressionContext *ctx) = 0;

  virtual void enterConditionalOrExpression(SparqlParser::ConditionalOrExpressionContext *ctx) = 0;
  virtual void exitConditionalOrExpression(SparqlParser::ConditionalOrExpressionContext *ctx) = 0;

  virtual void enterConditionalAndExpression(SparqlParser::ConditionalAndExpressionContext *ctx) = 0;
  virtual void exitConditionalAndExpression(SparqlParser::ConditionalAndExpressionContext *ctx) = 0;

  virtual void enterValueLogical(SparqlParser::ValueLogicalContext *ctx) = 0;
  virtual void exitValueLogical(SparqlParser::ValueLogicalContext *ctx) = 0;

  virtual void enterRelationalExpression(SparqlParser::RelationalExpressionContext *ctx) = 0;
  virtual void exitRelationalExpression(SparqlParser::RelationalExpressionContext *ctx) = 0;

  virtual void enterNumericExpression(SparqlParser::NumericExpressionContext *ctx) = 0;
  virtual void exitNumericExpression(SparqlParser::NumericExpressionContext *ctx) = 0;

  virtual void enterAdditiveExpression(SparqlParser::AdditiveExpressionContext *ctx) = 0;
  virtual void exitAdditiveExpression(SparqlParser::AdditiveExpressionContext *ctx) = 0;

  virtual void enterMultiplicativeExpression(SparqlParser::MultiplicativeExpressionContext *ctx) = 0;
  virtual void exitMultiplicativeExpression(SparqlParser::MultiplicativeExpressionContext *ctx) = 0;

  virtual void enterUnaryExpression(SparqlParser::UnaryExpressionContext *ctx) = 0;
  virtual void exitUnaryExpression(SparqlParser::UnaryExpressionContext *ctx) = 0;

  virtual void enterPrimaryExpression(SparqlParser::PrimaryExpressionContext *ctx) = 0;
  virtual void exitPrimaryExpression(SparqlParser::PrimaryExpressionContext *ctx) = 0;

  virtual void enterBrackettedExpression(SparqlParser::BrackettedExpressionContext *ctx) = 0;
  virtual void exitBrackettedExpression(SparqlParser::BrackettedExpressionContext *ctx) = 0;

  virtual void enterBuiltInCall(SparqlParser::BuiltInCallContext *ctx) = 0;
  virtual void exitBuiltInCall(SparqlParser::BuiltInCallContext *ctx) = 0;

  virtual void enterRegexExpression(SparqlParser::RegexExpressionContext *ctx) = 0;
  virtual void exitRegexExpression(SparqlParser::RegexExpressionContext *ctx) = 0;

  virtual void enterSubstringExpression(SparqlParser::SubstringExpressionContext *ctx) = 0;
  virtual void exitSubstringExpression(SparqlParser::SubstringExpressionContext *ctx) = 0;

  virtual void enterStrReplaceExpression(SparqlParser::StrReplaceExpressionContext *ctx) = 0;
  virtual void exitStrReplaceExpression(SparqlParser::StrReplaceExpressionContext *ctx) = 0;

  virtual void enterExistsFunc(SparqlParser::ExistsFuncContext *ctx) = 0;
  virtual void exitExistsFunc(SparqlParser::ExistsFuncContext *ctx) = 0;

  virtual void enterNotExistsFunc(SparqlParser::NotExistsFuncContext *ctx) = 0;
  virtual void exitNotExistsFunc(SparqlParser::NotExistsFuncContext *ctx) = 0;

  virtual void enterAggregate(SparqlParser::AggregateContext *ctx) = 0;
  virtual void exitAggregate(SparqlParser::AggregateContext *ctx) = 0;

  virtual void enterIriOrFunction(SparqlParser::IriOrFunctionContext *ctx) = 0;
  virtual void exitIriOrFunction(SparqlParser::IriOrFunctionContext *ctx) = 0;

  virtual void enterRdfLiteral(SparqlParser::RdfLiteralContext *ctx) = 0;
  virtual void exitRdfLiteral(SparqlParser::RdfLiteralContext *ctx) = 0;

  virtual void enterNumericLiteral(SparqlParser::NumericLiteralContext *ctx) = 0;
  virtual void exitNumericLiteral(SparqlParser::NumericLiteralContext *ctx) = 0;

  virtual void enterNumericLiteralUnsigned(SparqlParser::NumericLiteralUnsignedContext *ctx) = 0;
  virtual void exitNumericLiteralUnsigned(SparqlParser::NumericLiteralUnsignedContext *ctx) = 0;

  virtual void enterNumericLiteralPositive(SparqlParser::NumericLiteralPositiveContext *ctx) = 0;
  virtual void exitNumericLiteralPositive(SparqlParser::NumericLiteralPositiveContext *ctx) = 0;

  virtual void enterNumericLiteralNegative(SparqlParser::NumericLiteralNegativeContext *ctx) = 0;
  virtual void exitNumericLiteralNegative(SparqlParser::NumericLiteralNegativeContext *ctx) = 0;

  virtual void enterBooleanLiteral(SparqlParser::BooleanLiteralContext *ctx) = 0;
  virtual void exitBooleanLiteral(SparqlParser::BooleanLiteralContext *ctx) = 0;

  virtual void enterString(SparqlParser::StringContext *ctx) = 0;
  virtual void exitString(SparqlParser::StringContext *ctx) = 0;

  virtual void enterIri(SparqlParser::IriContext *ctx) = 0;
  virtual void exitIri(SparqlParser::IriContext *ctx) = 0;

  virtual void enterPrefixedName(SparqlParser::PrefixedNameContext *ctx) = 0;
  virtual void exitPrefixedName(SparqlParser::PrefixedNameContext *ctx) = 0;

  virtual void enterBlankNode(SparqlParser::BlankNodeContext *ctx) = 0;
  virtual void exitBlankNode(SparqlParser::BlankNodeContext *ctx) = 0;

  virtual void enterIriref(SparqlParser::IrirefContext *ctx) = 0;
  virtual void exitIriref(SparqlParser::IrirefContext *ctx) = 0;

  virtual void enterPnameLn(SparqlParser::PnameLnContext *ctx) = 0;
  virtual void exitPnameLn(SparqlParser::PnameLnContext *ctx) = 0;


};

