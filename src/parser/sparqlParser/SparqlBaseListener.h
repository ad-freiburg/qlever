
// Generated from Sparql.g4 by ANTLR 4.9.2

#pragma once


#include "antlr4-runtime.h"
#include "SparqlListener.h"


/**
 * This class provides an empty implementation of SparqlListener,
 * which can be extended to create a listener which only needs to handle a subset
 * of the available methods.
 */
class  SparqlBaseListener : public SparqlListener {
public:

  virtual void enterQuery(SparqlParser::QueryContext * /*ctx*/) override { }
  virtual void exitQuery(SparqlParser::QueryContext * /*ctx*/) override { }

  virtual void enterPrologue(SparqlParser::PrologueContext * /*ctx*/) override { }
  virtual void exitPrologue(SparqlParser::PrologueContext * /*ctx*/) override { }

  virtual void enterBaseDecl(SparqlParser::BaseDeclContext * /*ctx*/) override { }
  virtual void exitBaseDecl(SparqlParser::BaseDeclContext * /*ctx*/) override { }

  virtual void enterPrefixDecl(SparqlParser::PrefixDeclContext * /*ctx*/) override { }
  virtual void exitPrefixDecl(SparqlParser::PrefixDeclContext * /*ctx*/) override { }

  virtual void enterSelectQuery(SparqlParser::SelectQueryContext * /*ctx*/) override { }
  virtual void exitSelectQuery(SparqlParser::SelectQueryContext * /*ctx*/) override { }

  virtual void enterSubSelect(SparqlParser::SubSelectContext * /*ctx*/) override { }
  virtual void exitSubSelect(SparqlParser::SubSelectContext * /*ctx*/) override { }

  virtual void enterSelectClause(SparqlParser::SelectClauseContext * /*ctx*/) override { }
  virtual void exitSelectClause(SparqlParser::SelectClauseContext * /*ctx*/) override { }

  virtual void enterAlias(SparqlParser::AliasContext * /*ctx*/) override { }
  virtual void exitAlias(SparqlParser::AliasContext * /*ctx*/) override { }

  virtual void enterConstructQuery(SparqlParser::ConstructQueryContext * /*ctx*/) override { }
  virtual void exitConstructQuery(SparqlParser::ConstructQueryContext * /*ctx*/) override { }

  virtual void enterDescribeQuery(SparqlParser::DescribeQueryContext * /*ctx*/) override { }
  virtual void exitDescribeQuery(SparqlParser::DescribeQueryContext * /*ctx*/) override { }

  virtual void enterAskQuery(SparqlParser::AskQueryContext * /*ctx*/) override { }
  virtual void exitAskQuery(SparqlParser::AskQueryContext * /*ctx*/) override { }

  virtual void enterDatasetClause(SparqlParser::DatasetClauseContext * /*ctx*/) override { }
  virtual void exitDatasetClause(SparqlParser::DatasetClauseContext * /*ctx*/) override { }

  virtual void enterDefaultGraphClause(SparqlParser::DefaultGraphClauseContext * /*ctx*/) override { }
  virtual void exitDefaultGraphClause(SparqlParser::DefaultGraphClauseContext * /*ctx*/) override { }

  virtual void enterNamedGraphClause(SparqlParser::NamedGraphClauseContext * /*ctx*/) override { }
  virtual void exitNamedGraphClause(SparqlParser::NamedGraphClauseContext * /*ctx*/) override { }

  virtual void enterSourceSelector(SparqlParser::SourceSelectorContext * /*ctx*/) override { }
  virtual void exitSourceSelector(SparqlParser::SourceSelectorContext * /*ctx*/) override { }

  virtual void enterWhereClause(SparqlParser::WhereClauseContext * /*ctx*/) override { }
  virtual void exitWhereClause(SparqlParser::WhereClauseContext * /*ctx*/) override { }

  virtual void enterSolutionModifier(SparqlParser::SolutionModifierContext * /*ctx*/) override { }
  virtual void exitSolutionModifier(SparqlParser::SolutionModifierContext * /*ctx*/) override { }

  virtual void enterGroupClause(SparqlParser::GroupClauseContext * /*ctx*/) override { }
  virtual void exitGroupClause(SparqlParser::GroupClauseContext * /*ctx*/) override { }

  virtual void enterGroupCondition(SparqlParser::GroupConditionContext * /*ctx*/) override { }
  virtual void exitGroupCondition(SparqlParser::GroupConditionContext * /*ctx*/) override { }

  virtual void enterHavingClause(SparqlParser::HavingClauseContext * /*ctx*/) override { }
  virtual void exitHavingClause(SparqlParser::HavingClauseContext * /*ctx*/) override { }

  virtual void enterHavingCondition(SparqlParser::HavingConditionContext * /*ctx*/) override { }
  virtual void exitHavingCondition(SparqlParser::HavingConditionContext * /*ctx*/) override { }

  virtual void enterOrderClause(SparqlParser::OrderClauseContext * /*ctx*/) override { }
  virtual void exitOrderClause(SparqlParser::OrderClauseContext * /*ctx*/) override { }

  virtual void enterOrderCondition(SparqlParser::OrderConditionContext * /*ctx*/) override { }
  virtual void exitOrderCondition(SparqlParser::OrderConditionContext * /*ctx*/) override { }

  virtual void enterLimitOffsetClauses(SparqlParser::LimitOffsetClausesContext * /*ctx*/) override { }
  virtual void exitLimitOffsetClauses(SparqlParser::LimitOffsetClausesContext * /*ctx*/) override { }

  virtual void enterLimitClause(SparqlParser::LimitClauseContext * /*ctx*/) override { }
  virtual void exitLimitClause(SparqlParser::LimitClauseContext * /*ctx*/) override { }

  virtual void enterOffsetClause(SparqlParser::OffsetClauseContext * /*ctx*/) override { }
  virtual void exitOffsetClause(SparqlParser::OffsetClauseContext * /*ctx*/) override { }

  virtual void enterValuesClause(SparqlParser::ValuesClauseContext * /*ctx*/) override { }
  virtual void exitValuesClause(SparqlParser::ValuesClauseContext * /*ctx*/) override { }

  virtual void enterTriplesTemplate(SparqlParser::TriplesTemplateContext * /*ctx*/) override { }
  virtual void exitTriplesTemplate(SparqlParser::TriplesTemplateContext * /*ctx*/) override { }

  virtual void enterGroupGraphPattern(SparqlParser::GroupGraphPatternContext * /*ctx*/) override { }
  virtual void exitGroupGraphPattern(SparqlParser::GroupGraphPatternContext * /*ctx*/) override { }

  virtual void enterGroupGraphPatternSub(SparqlParser::GroupGraphPatternSubContext * /*ctx*/) override { }
  virtual void exitGroupGraphPatternSub(SparqlParser::GroupGraphPatternSubContext * /*ctx*/) override { }

  virtual void enterTriplesBlock(SparqlParser::TriplesBlockContext * /*ctx*/) override { }
  virtual void exitTriplesBlock(SparqlParser::TriplesBlockContext * /*ctx*/) override { }

  virtual void enterGraphPatternNotTriples(SparqlParser::GraphPatternNotTriplesContext * /*ctx*/) override { }
  virtual void exitGraphPatternNotTriples(SparqlParser::GraphPatternNotTriplesContext * /*ctx*/) override { }

  virtual void enterOptionalGraphPattern(SparqlParser::OptionalGraphPatternContext * /*ctx*/) override { }
  virtual void exitOptionalGraphPattern(SparqlParser::OptionalGraphPatternContext * /*ctx*/) override { }

  virtual void enterGraphGraphPattern(SparqlParser::GraphGraphPatternContext * /*ctx*/) override { }
  virtual void exitGraphGraphPattern(SparqlParser::GraphGraphPatternContext * /*ctx*/) override { }

  virtual void enterServiceGraphPattern(SparqlParser::ServiceGraphPatternContext * /*ctx*/) override { }
  virtual void exitServiceGraphPattern(SparqlParser::ServiceGraphPatternContext * /*ctx*/) override { }

  virtual void enterBind(SparqlParser::BindContext * /*ctx*/) override { }
  virtual void exitBind(SparqlParser::BindContext * /*ctx*/) override { }

  virtual void enterInlineData(SparqlParser::InlineDataContext * /*ctx*/) override { }
  virtual void exitInlineData(SparqlParser::InlineDataContext * /*ctx*/) override { }

  virtual void enterDataBlock(SparqlParser::DataBlockContext * /*ctx*/) override { }
  virtual void exitDataBlock(SparqlParser::DataBlockContext * /*ctx*/) override { }

  virtual void enterInlineDataOneVar(SparqlParser::InlineDataOneVarContext * /*ctx*/) override { }
  virtual void exitInlineDataOneVar(SparqlParser::InlineDataOneVarContext * /*ctx*/) override { }

  virtual void enterInlineDataFull(SparqlParser::InlineDataFullContext * /*ctx*/) override { }
  virtual void exitInlineDataFull(SparqlParser::InlineDataFullContext * /*ctx*/) override { }

  virtual void enterDataBlockSingle(SparqlParser::DataBlockSingleContext * /*ctx*/) override { }
  virtual void exitDataBlockSingle(SparqlParser::DataBlockSingleContext * /*ctx*/) override { }

  virtual void enterDataBlockValue(SparqlParser::DataBlockValueContext * /*ctx*/) override { }
  virtual void exitDataBlockValue(SparqlParser::DataBlockValueContext * /*ctx*/) override { }

  virtual void enterMinusGraphPattern(SparqlParser::MinusGraphPatternContext * /*ctx*/) override { }
  virtual void exitMinusGraphPattern(SparqlParser::MinusGraphPatternContext * /*ctx*/) override { }

  virtual void enterGroupOrUnionGraphPattern(SparqlParser::GroupOrUnionGraphPatternContext * /*ctx*/) override { }
  virtual void exitGroupOrUnionGraphPattern(SparqlParser::GroupOrUnionGraphPatternContext * /*ctx*/) override { }

  virtual void enterFilterR(SparqlParser::FilterRContext * /*ctx*/) override { }
  virtual void exitFilterR(SparqlParser::FilterRContext * /*ctx*/) override { }

  virtual void enterConstraint(SparqlParser::ConstraintContext * /*ctx*/) override { }
  virtual void exitConstraint(SparqlParser::ConstraintContext * /*ctx*/) override { }

  virtual void enterFunctionCall(SparqlParser::FunctionCallContext * /*ctx*/) override { }
  virtual void exitFunctionCall(SparqlParser::FunctionCallContext * /*ctx*/) override { }

  virtual void enterArgList(SparqlParser::ArgListContext * /*ctx*/) override { }
  virtual void exitArgList(SparqlParser::ArgListContext * /*ctx*/) override { }

  virtual void enterExpressionList(SparqlParser::ExpressionListContext * /*ctx*/) override { }
  virtual void exitExpressionList(SparqlParser::ExpressionListContext * /*ctx*/) override { }

  virtual void enterConstructTemplate(SparqlParser::ConstructTemplateContext * /*ctx*/) override { }
  virtual void exitConstructTemplate(SparqlParser::ConstructTemplateContext * /*ctx*/) override { }

  virtual void enterConstructTriples(SparqlParser::ConstructTriplesContext * /*ctx*/) override { }
  virtual void exitConstructTriples(SparqlParser::ConstructTriplesContext * /*ctx*/) override { }

  virtual void enterTriplesSameSubject(SparqlParser::TriplesSameSubjectContext * /*ctx*/) override { }
  virtual void exitTriplesSameSubject(SparqlParser::TriplesSameSubjectContext * /*ctx*/) override { }

  virtual void enterPropertyList(SparqlParser::PropertyListContext * /*ctx*/) override { }
  virtual void exitPropertyList(SparqlParser::PropertyListContext * /*ctx*/) override { }

  virtual void enterPropertyListNotEmpty(SparqlParser::PropertyListNotEmptyContext * /*ctx*/) override { }
  virtual void exitPropertyListNotEmpty(SparqlParser::PropertyListNotEmptyContext * /*ctx*/) override { }

  virtual void enterVerb(SparqlParser::VerbContext * /*ctx*/) override { }
  virtual void exitVerb(SparqlParser::VerbContext * /*ctx*/) override { }

  virtual void enterObjectList(SparqlParser::ObjectListContext * /*ctx*/) override { }
  virtual void exitObjectList(SparqlParser::ObjectListContext * /*ctx*/) override { }

  virtual void enterObjectR(SparqlParser::ObjectRContext * /*ctx*/) override { }
  virtual void exitObjectR(SparqlParser::ObjectRContext * /*ctx*/) override { }

  virtual void enterTriplesSameSubjectPath(SparqlParser::TriplesSameSubjectPathContext * /*ctx*/) override { }
  virtual void exitTriplesSameSubjectPath(SparqlParser::TriplesSameSubjectPathContext * /*ctx*/) override { }

  virtual void enterPropertyListPath(SparqlParser::PropertyListPathContext * /*ctx*/) override { }
  virtual void exitPropertyListPath(SparqlParser::PropertyListPathContext * /*ctx*/) override { }

  virtual void enterPropertyListPathNotEmpty(SparqlParser::PropertyListPathNotEmptyContext * /*ctx*/) override { }
  virtual void exitPropertyListPathNotEmpty(SparqlParser::PropertyListPathNotEmptyContext * /*ctx*/) override { }

  virtual void enterVerbPath(SparqlParser::VerbPathContext * /*ctx*/) override { }
  virtual void exitVerbPath(SparqlParser::VerbPathContext * /*ctx*/) override { }

  virtual void enterVerbSimple(SparqlParser::VerbSimpleContext * /*ctx*/) override { }
  virtual void exitVerbSimple(SparqlParser::VerbSimpleContext * /*ctx*/) override { }

  virtual void enterVerbPathOrSimple(SparqlParser::VerbPathOrSimpleContext * /*ctx*/) override { }
  virtual void exitVerbPathOrSimple(SparqlParser::VerbPathOrSimpleContext * /*ctx*/) override { }

  virtual void enterObjectListPath(SparqlParser::ObjectListPathContext * /*ctx*/) override { }
  virtual void exitObjectListPath(SparqlParser::ObjectListPathContext * /*ctx*/) override { }

  virtual void enterObjectPath(SparqlParser::ObjectPathContext * /*ctx*/) override { }
  virtual void exitObjectPath(SparqlParser::ObjectPathContext * /*ctx*/) override { }

  virtual void enterPath(SparqlParser::PathContext * /*ctx*/) override { }
  virtual void exitPath(SparqlParser::PathContext * /*ctx*/) override { }

  virtual void enterPathAlternative(SparqlParser::PathAlternativeContext * /*ctx*/) override { }
  virtual void exitPathAlternative(SparqlParser::PathAlternativeContext * /*ctx*/) override { }

  virtual void enterPathSequence(SparqlParser::PathSequenceContext * /*ctx*/) override { }
  virtual void exitPathSequence(SparqlParser::PathSequenceContext * /*ctx*/) override { }

  virtual void enterPathElt(SparqlParser::PathEltContext * /*ctx*/) override { }
  virtual void exitPathElt(SparqlParser::PathEltContext * /*ctx*/) override { }

  virtual void enterPathEltOrInverse(SparqlParser::PathEltOrInverseContext * /*ctx*/) override { }
  virtual void exitPathEltOrInverse(SparqlParser::PathEltOrInverseContext * /*ctx*/) override { }

  virtual void enterPathMod(SparqlParser::PathModContext * /*ctx*/) override { }
  virtual void exitPathMod(SparqlParser::PathModContext * /*ctx*/) override { }

  virtual void enterPathPrimary(SparqlParser::PathPrimaryContext * /*ctx*/) override { }
  virtual void exitPathPrimary(SparqlParser::PathPrimaryContext * /*ctx*/) override { }

  virtual void enterPathNegatedPropertySet(SparqlParser::PathNegatedPropertySetContext * /*ctx*/) override { }
  virtual void exitPathNegatedPropertySet(SparqlParser::PathNegatedPropertySetContext * /*ctx*/) override { }

  virtual void enterPathOneInPropertySet(SparqlParser::PathOneInPropertySetContext * /*ctx*/) override { }
  virtual void exitPathOneInPropertySet(SparqlParser::PathOneInPropertySetContext * /*ctx*/) override { }

  virtual void enterInteger(SparqlParser::IntegerContext * /*ctx*/) override { }
  virtual void exitInteger(SparqlParser::IntegerContext * /*ctx*/) override { }

  virtual void enterTriplesNode(SparqlParser::TriplesNodeContext * /*ctx*/) override { }
  virtual void exitTriplesNode(SparqlParser::TriplesNodeContext * /*ctx*/) override { }

  virtual void enterBlankNodePropertyList(SparqlParser::BlankNodePropertyListContext * /*ctx*/) override { }
  virtual void exitBlankNodePropertyList(SparqlParser::BlankNodePropertyListContext * /*ctx*/) override { }

  virtual void enterTriplesNodePath(SparqlParser::TriplesNodePathContext * /*ctx*/) override { }
  virtual void exitTriplesNodePath(SparqlParser::TriplesNodePathContext * /*ctx*/) override { }

  virtual void enterBlankNodePropertyListPath(SparqlParser::BlankNodePropertyListPathContext * /*ctx*/) override { }
  virtual void exitBlankNodePropertyListPath(SparqlParser::BlankNodePropertyListPathContext * /*ctx*/) override { }

  virtual void enterCollection(SparqlParser::CollectionContext * /*ctx*/) override { }
  virtual void exitCollection(SparqlParser::CollectionContext * /*ctx*/) override { }

  virtual void enterCollectionPath(SparqlParser::CollectionPathContext * /*ctx*/) override { }
  virtual void exitCollectionPath(SparqlParser::CollectionPathContext * /*ctx*/) override { }

  virtual void enterGraphNode(SparqlParser::GraphNodeContext * /*ctx*/) override { }
  virtual void exitGraphNode(SparqlParser::GraphNodeContext * /*ctx*/) override { }

  virtual void enterGraphNodePath(SparqlParser::GraphNodePathContext * /*ctx*/) override { }
  virtual void exitGraphNodePath(SparqlParser::GraphNodePathContext * /*ctx*/) override { }

  virtual void enterVarOrTerm(SparqlParser::VarOrTermContext * /*ctx*/) override { }
  virtual void exitVarOrTerm(SparqlParser::VarOrTermContext * /*ctx*/) override { }

  virtual void enterVarOrIri(SparqlParser::VarOrIriContext * /*ctx*/) override { }
  virtual void exitVarOrIri(SparqlParser::VarOrIriContext * /*ctx*/) override { }

  virtual void enterVar(SparqlParser::VarContext * /*ctx*/) override { }
  virtual void exitVar(SparqlParser::VarContext * /*ctx*/) override { }

  virtual void enterGraphTerm(SparqlParser::GraphTermContext * /*ctx*/) override { }
  virtual void exitGraphTerm(SparqlParser::GraphTermContext * /*ctx*/) override { }

  virtual void enterExpression(SparqlParser::ExpressionContext * /*ctx*/) override { }
  virtual void exitExpression(SparqlParser::ExpressionContext * /*ctx*/) override { }

  virtual void enterConditionalOrExpression(SparqlParser::ConditionalOrExpressionContext * /*ctx*/) override { }
  virtual void exitConditionalOrExpression(SparqlParser::ConditionalOrExpressionContext * /*ctx*/) override { }

  virtual void enterConditionalAndExpression(SparqlParser::ConditionalAndExpressionContext * /*ctx*/) override { }
  virtual void exitConditionalAndExpression(SparqlParser::ConditionalAndExpressionContext * /*ctx*/) override { }

  virtual void enterValueLogical(SparqlParser::ValueLogicalContext * /*ctx*/) override { }
  virtual void exitValueLogical(SparqlParser::ValueLogicalContext * /*ctx*/) override { }

  virtual void enterRelationalExpression(SparqlParser::RelationalExpressionContext * /*ctx*/) override { }
  virtual void exitRelationalExpression(SparqlParser::RelationalExpressionContext * /*ctx*/) override { }

  virtual void enterNumericExpression(SparqlParser::NumericExpressionContext * /*ctx*/) override { }
  virtual void exitNumericExpression(SparqlParser::NumericExpressionContext * /*ctx*/) override { }

  virtual void enterAdditiveExpression(SparqlParser::AdditiveExpressionContext * /*ctx*/) override { }
  virtual void exitAdditiveExpression(SparqlParser::AdditiveExpressionContext * /*ctx*/) override { }

  virtual void enterMultiplicativeExpression(SparqlParser::MultiplicativeExpressionContext * /*ctx*/) override { }
  virtual void exitMultiplicativeExpression(SparqlParser::MultiplicativeExpressionContext * /*ctx*/) override { }

  virtual void enterUnaryExpression(SparqlParser::UnaryExpressionContext * /*ctx*/) override { }
  virtual void exitUnaryExpression(SparqlParser::UnaryExpressionContext * /*ctx*/) override { }

  virtual void enterPrimaryExpression(SparqlParser::PrimaryExpressionContext * /*ctx*/) override { }
  virtual void exitPrimaryExpression(SparqlParser::PrimaryExpressionContext * /*ctx*/) override { }

  virtual void enterBrackettedExpression(SparqlParser::BrackettedExpressionContext * /*ctx*/) override { }
  virtual void exitBrackettedExpression(SparqlParser::BrackettedExpressionContext * /*ctx*/) override { }

  virtual void enterBuiltInCall(SparqlParser::BuiltInCallContext * /*ctx*/) override { }
  virtual void exitBuiltInCall(SparqlParser::BuiltInCallContext * /*ctx*/) override { }

  virtual void enterRegexExpression(SparqlParser::RegexExpressionContext * /*ctx*/) override { }
  virtual void exitRegexExpression(SparqlParser::RegexExpressionContext * /*ctx*/) override { }

  virtual void enterSubstringExpression(SparqlParser::SubstringExpressionContext * /*ctx*/) override { }
  virtual void exitSubstringExpression(SparqlParser::SubstringExpressionContext * /*ctx*/) override { }

  virtual void enterStrReplaceExpression(SparqlParser::StrReplaceExpressionContext * /*ctx*/) override { }
  virtual void exitStrReplaceExpression(SparqlParser::StrReplaceExpressionContext * /*ctx*/) override { }

  virtual void enterExistsFunc(SparqlParser::ExistsFuncContext * /*ctx*/) override { }
  virtual void exitExistsFunc(SparqlParser::ExistsFuncContext * /*ctx*/) override { }

  virtual void enterNotExistsFunc(SparqlParser::NotExistsFuncContext * /*ctx*/) override { }
  virtual void exitNotExistsFunc(SparqlParser::NotExistsFuncContext * /*ctx*/) override { }

  virtual void enterAggregate(SparqlParser::AggregateContext * /*ctx*/) override { }
  virtual void exitAggregate(SparqlParser::AggregateContext * /*ctx*/) override { }

  virtual void enterIriOrFunction(SparqlParser::IriOrFunctionContext * /*ctx*/) override { }
  virtual void exitIriOrFunction(SparqlParser::IriOrFunctionContext * /*ctx*/) override { }

  virtual void enterRdfLiteral(SparqlParser::RdfLiteralContext * /*ctx*/) override { }
  virtual void exitRdfLiteral(SparqlParser::RdfLiteralContext * /*ctx*/) override { }

  virtual void enterNumericLiteral(SparqlParser::NumericLiteralContext * /*ctx*/) override { }
  virtual void exitNumericLiteral(SparqlParser::NumericLiteralContext * /*ctx*/) override { }

  virtual void enterNumericLiteralUnsigned(SparqlParser::NumericLiteralUnsignedContext * /*ctx*/) override { }
  virtual void exitNumericLiteralUnsigned(SparqlParser::NumericLiteralUnsignedContext * /*ctx*/) override { }

  virtual void enterNumericLiteralPositive(SparqlParser::NumericLiteralPositiveContext * /*ctx*/) override { }
  virtual void exitNumericLiteralPositive(SparqlParser::NumericLiteralPositiveContext * /*ctx*/) override { }

  virtual void enterNumericLiteralNegative(SparqlParser::NumericLiteralNegativeContext * /*ctx*/) override { }
  virtual void exitNumericLiteralNegative(SparqlParser::NumericLiteralNegativeContext * /*ctx*/) override { }

  virtual void enterBooleanLiteral(SparqlParser::BooleanLiteralContext * /*ctx*/) override { }
  virtual void exitBooleanLiteral(SparqlParser::BooleanLiteralContext * /*ctx*/) override { }

  virtual void enterString(SparqlParser::StringContext * /*ctx*/) override { }
  virtual void exitString(SparqlParser::StringContext * /*ctx*/) override { }

  virtual void enterIri(SparqlParser::IriContext * /*ctx*/) override { }
  virtual void exitIri(SparqlParser::IriContext * /*ctx*/) override { }

  virtual void enterPrefixedName(SparqlParser::PrefixedNameContext * /*ctx*/) override { }
  virtual void exitPrefixedName(SparqlParser::PrefixedNameContext * /*ctx*/) override { }

  virtual void enterBlankNode(SparqlParser::BlankNodeContext * /*ctx*/) override { }
  virtual void exitBlankNode(SparqlParser::BlankNodeContext * /*ctx*/) override { }

  virtual void enterIriref(SparqlParser::IrirefContext * /*ctx*/) override { }
  virtual void exitIriref(SparqlParser::IrirefContext * /*ctx*/) override { }

  virtual void enterPnameLn(SparqlParser::PnameLnContext * /*ctx*/) override { }
  virtual void exitPnameLn(SparqlParser::PnameLnContext * /*ctx*/) override { }

  virtual void enterPnameNs(SparqlParser::PnameNsContext * /*ctx*/) override { }
  virtual void exitPnameNs(SparqlParser::PnameNsContext * /*ctx*/) override { }


  virtual void enterEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void exitEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void visitTerminal(antlr4::tree::TerminalNode * /*node*/) override { }
  virtual void visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override { }

};

