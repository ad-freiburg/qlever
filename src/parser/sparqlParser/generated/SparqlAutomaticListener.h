
// Generated from SparqlAutomatic.g4 by ANTLR 4.11.1

#pragma once


#include "antlr4-runtime.h"
#include "SparqlAutomaticParser.h"


/**
 * This interface defines an abstract listener for a parse tree produced by SparqlAutomaticParser.
 */
class  SparqlAutomaticListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterQueryOrUpdate(SparqlAutomaticParser::QueryOrUpdateContext *ctx) = 0;
  virtual void exitQueryOrUpdate(SparqlAutomaticParser::QueryOrUpdateContext *ctx) = 0;

  virtual void enterQuery(SparqlAutomaticParser::QueryContext *ctx) = 0;
  virtual void exitQuery(SparqlAutomaticParser::QueryContext *ctx) = 0;

  virtual void enterPrologue(SparqlAutomaticParser::PrologueContext *ctx) = 0;
  virtual void exitPrologue(SparqlAutomaticParser::PrologueContext *ctx) = 0;

  virtual void enterBaseDecl(SparqlAutomaticParser::BaseDeclContext *ctx) = 0;
  virtual void exitBaseDecl(SparqlAutomaticParser::BaseDeclContext *ctx) = 0;

  virtual void enterPrefixDecl(SparqlAutomaticParser::PrefixDeclContext *ctx) = 0;
  virtual void exitPrefixDecl(SparqlAutomaticParser::PrefixDeclContext *ctx) = 0;

  virtual void enterSelectQuery(SparqlAutomaticParser::SelectQueryContext *ctx) = 0;
  virtual void exitSelectQuery(SparqlAutomaticParser::SelectQueryContext *ctx) = 0;

  virtual void enterSubSelect(SparqlAutomaticParser::SubSelectContext *ctx) = 0;
  virtual void exitSubSelect(SparqlAutomaticParser::SubSelectContext *ctx) = 0;

  virtual void enterSelectClause(SparqlAutomaticParser::SelectClauseContext *ctx) = 0;
  virtual void exitSelectClause(SparqlAutomaticParser::SelectClauseContext *ctx) = 0;

  virtual void enterVarOrAlias(SparqlAutomaticParser::VarOrAliasContext *ctx) = 0;
  virtual void exitVarOrAlias(SparqlAutomaticParser::VarOrAliasContext *ctx) = 0;

  virtual void enterAlias(SparqlAutomaticParser::AliasContext *ctx) = 0;
  virtual void exitAlias(SparqlAutomaticParser::AliasContext *ctx) = 0;

  virtual void enterAliasWithoutBrackets(SparqlAutomaticParser::AliasWithoutBracketsContext *ctx) = 0;
  virtual void exitAliasWithoutBrackets(SparqlAutomaticParser::AliasWithoutBracketsContext *ctx) = 0;

  virtual void enterConstructQuery(SparqlAutomaticParser::ConstructQueryContext *ctx) = 0;
  virtual void exitConstructQuery(SparqlAutomaticParser::ConstructQueryContext *ctx) = 0;

  virtual void enterDescribeQuery(SparqlAutomaticParser::DescribeQueryContext *ctx) = 0;
  virtual void exitDescribeQuery(SparqlAutomaticParser::DescribeQueryContext *ctx) = 0;

  virtual void enterAskQuery(SparqlAutomaticParser::AskQueryContext *ctx) = 0;
  virtual void exitAskQuery(SparqlAutomaticParser::AskQueryContext *ctx) = 0;

  virtual void enterDatasetClause(SparqlAutomaticParser::DatasetClauseContext *ctx) = 0;
  virtual void exitDatasetClause(SparqlAutomaticParser::DatasetClauseContext *ctx) = 0;

  virtual void enterDefaultGraphClause(SparqlAutomaticParser::DefaultGraphClauseContext *ctx) = 0;
  virtual void exitDefaultGraphClause(SparqlAutomaticParser::DefaultGraphClauseContext *ctx) = 0;

  virtual void enterNamedGraphClause(SparqlAutomaticParser::NamedGraphClauseContext *ctx) = 0;
  virtual void exitNamedGraphClause(SparqlAutomaticParser::NamedGraphClauseContext *ctx) = 0;

  virtual void enterSourceSelector(SparqlAutomaticParser::SourceSelectorContext *ctx) = 0;
  virtual void exitSourceSelector(SparqlAutomaticParser::SourceSelectorContext *ctx) = 0;

  virtual void enterWhereClause(SparqlAutomaticParser::WhereClauseContext *ctx) = 0;
  virtual void exitWhereClause(SparqlAutomaticParser::WhereClauseContext *ctx) = 0;

  virtual void enterSolutionModifier(SparqlAutomaticParser::SolutionModifierContext *ctx) = 0;
  virtual void exitSolutionModifier(SparqlAutomaticParser::SolutionModifierContext *ctx) = 0;

  virtual void enterGroupClause(SparqlAutomaticParser::GroupClauseContext *ctx) = 0;
  virtual void exitGroupClause(SparqlAutomaticParser::GroupClauseContext *ctx) = 0;

  virtual void enterGroupCondition(SparqlAutomaticParser::GroupConditionContext *ctx) = 0;
  virtual void exitGroupCondition(SparqlAutomaticParser::GroupConditionContext *ctx) = 0;

  virtual void enterHavingClause(SparqlAutomaticParser::HavingClauseContext *ctx) = 0;
  virtual void exitHavingClause(SparqlAutomaticParser::HavingClauseContext *ctx) = 0;

  virtual void enterHavingCondition(SparqlAutomaticParser::HavingConditionContext *ctx) = 0;
  virtual void exitHavingCondition(SparqlAutomaticParser::HavingConditionContext *ctx) = 0;

  virtual void enterOrderClause(SparqlAutomaticParser::OrderClauseContext *ctx) = 0;
  virtual void exitOrderClause(SparqlAutomaticParser::OrderClauseContext *ctx) = 0;

  virtual void enterOrderCondition(SparqlAutomaticParser::OrderConditionContext *ctx) = 0;
  virtual void exitOrderCondition(SparqlAutomaticParser::OrderConditionContext *ctx) = 0;

  virtual void enterLimitOffsetClauses(SparqlAutomaticParser::LimitOffsetClausesContext *ctx) = 0;
  virtual void exitLimitOffsetClauses(SparqlAutomaticParser::LimitOffsetClausesContext *ctx) = 0;

  virtual void enterLimitClause(SparqlAutomaticParser::LimitClauseContext *ctx) = 0;
  virtual void exitLimitClause(SparqlAutomaticParser::LimitClauseContext *ctx) = 0;

  virtual void enterOffsetClause(SparqlAutomaticParser::OffsetClauseContext *ctx) = 0;
  virtual void exitOffsetClause(SparqlAutomaticParser::OffsetClauseContext *ctx) = 0;

  virtual void enterTextLimitClause(SparqlAutomaticParser::TextLimitClauseContext *ctx) = 0;
  virtual void exitTextLimitClause(SparqlAutomaticParser::TextLimitClauseContext *ctx) = 0;

  virtual void enterValuesClause(SparqlAutomaticParser::ValuesClauseContext *ctx) = 0;
  virtual void exitValuesClause(SparqlAutomaticParser::ValuesClauseContext *ctx) = 0;

  virtual void enterUpdate(SparqlAutomaticParser::UpdateContext *ctx) = 0;
  virtual void exitUpdate(SparqlAutomaticParser::UpdateContext *ctx) = 0;

  virtual void enterUpdate1(SparqlAutomaticParser::Update1Context *ctx) = 0;
  virtual void exitUpdate1(SparqlAutomaticParser::Update1Context *ctx) = 0;

  virtual void enterLoad(SparqlAutomaticParser::LoadContext *ctx) = 0;
  virtual void exitLoad(SparqlAutomaticParser::LoadContext *ctx) = 0;

  virtual void enterClear(SparqlAutomaticParser::ClearContext *ctx) = 0;
  virtual void exitClear(SparqlAutomaticParser::ClearContext *ctx) = 0;

  virtual void enterDrop(SparqlAutomaticParser::DropContext *ctx) = 0;
  virtual void exitDrop(SparqlAutomaticParser::DropContext *ctx) = 0;

  virtual void enterCreate(SparqlAutomaticParser::CreateContext *ctx) = 0;
  virtual void exitCreate(SparqlAutomaticParser::CreateContext *ctx) = 0;

  virtual void enterAdd(SparqlAutomaticParser::AddContext *ctx) = 0;
  virtual void exitAdd(SparqlAutomaticParser::AddContext *ctx) = 0;

  virtual void enterMove(SparqlAutomaticParser::MoveContext *ctx) = 0;
  virtual void exitMove(SparqlAutomaticParser::MoveContext *ctx) = 0;

  virtual void enterCopy(SparqlAutomaticParser::CopyContext *ctx) = 0;
  virtual void exitCopy(SparqlAutomaticParser::CopyContext *ctx) = 0;

  virtual void enterInsertData(SparqlAutomaticParser::InsertDataContext *ctx) = 0;
  virtual void exitInsertData(SparqlAutomaticParser::InsertDataContext *ctx) = 0;

  virtual void enterDeleteData(SparqlAutomaticParser::DeleteDataContext *ctx) = 0;
  virtual void exitDeleteData(SparqlAutomaticParser::DeleteDataContext *ctx) = 0;

  virtual void enterDeleteWhere(SparqlAutomaticParser::DeleteWhereContext *ctx) = 0;
  virtual void exitDeleteWhere(SparqlAutomaticParser::DeleteWhereContext *ctx) = 0;

  virtual void enterModify(SparqlAutomaticParser::ModifyContext *ctx) = 0;
  virtual void exitModify(SparqlAutomaticParser::ModifyContext *ctx) = 0;

  virtual void enterDeleteClause(SparqlAutomaticParser::DeleteClauseContext *ctx) = 0;
  virtual void exitDeleteClause(SparqlAutomaticParser::DeleteClauseContext *ctx) = 0;

  virtual void enterInsertClause(SparqlAutomaticParser::InsertClauseContext *ctx) = 0;
  virtual void exitInsertClause(SparqlAutomaticParser::InsertClauseContext *ctx) = 0;

  virtual void enterUsingClause(SparqlAutomaticParser::UsingClauseContext *ctx) = 0;
  virtual void exitUsingClause(SparqlAutomaticParser::UsingClauseContext *ctx) = 0;

  virtual void enterGraphOrDefault(SparqlAutomaticParser::GraphOrDefaultContext *ctx) = 0;
  virtual void exitGraphOrDefault(SparqlAutomaticParser::GraphOrDefaultContext *ctx) = 0;

  virtual void enterGraphRef(SparqlAutomaticParser::GraphRefContext *ctx) = 0;
  virtual void exitGraphRef(SparqlAutomaticParser::GraphRefContext *ctx) = 0;

  virtual void enterGraphRefAll(SparqlAutomaticParser::GraphRefAllContext *ctx) = 0;
  virtual void exitGraphRefAll(SparqlAutomaticParser::GraphRefAllContext *ctx) = 0;

  virtual void enterQuadPattern(SparqlAutomaticParser::QuadPatternContext *ctx) = 0;
  virtual void exitQuadPattern(SparqlAutomaticParser::QuadPatternContext *ctx) = 0;

  virtual void enterQuadData(SparqlAutomaticParser::QuadDataContext *ctx) = 0;
  virtual void exitQuadData(SparqlAutomaticParser::QuadDataContext *ctx) = 0;

  virtual void enterQuads(SparqlAutomaticParser::QuadsContext *ctx) = 0;
  virtual void exitQuads(SparqlAutomaticParser::QuadsContext *ctx) = 0;

  virtual void enterQuadsNotTriples(SparqlAutomaticParser::QuadsNotTriplesContext *ctx) = 0;
  virtual void exitQuadsNotTriples(SparqlAutomaticParser::QuadsNotTriplesContext *ctx) = 0;

  virtual void enterTriplesTemplate(SparqlAutomaticParser::TriplesTemplateContext *ctx) = 0;
  virtual void exitTriplesTemplate(SparqlAutomaticParser::TriplesTemplateContext *ctx) = 0;

  virtual void enterGroupGraphPattern(SparqlAutomaticParser::GroupGraphPatternContext *ctx) = 0;
  virtual void exitGroupGraphPattern(SparqlAutomaticParser::GroupGraphPatternContext *ctx) = 0;

  virtual void enterGroupGraphPatternSub(SparqlAutomaticParser::GroupGraphPatternSubContext *ctx) = 0;
  virtual void exitGroupGraphPatternSub(SparqlAutomaticParser::GroupGraphPatternSubContext *ctx) = 0;

  virtual void enterGraphPatternNotTriplesAndMaybeTriples(SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext *ctx) = 0;
  virtual void exitGraphPatternNotTriplesAndMaybeTriples(SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext *ctx) = 0;

  virtual void enterTriplesBlock(SparqlAutomaticParser::TriplesBlockContext *ctx) = 0;
  virtual void exitTriplesBlock(SparqlAutomaticParser::TriplesBlockContext *ctx) = 0;

  virtual void enterGraphPatternNotTriples(SparqlAutomaticParser::GraphPatternNotTriplesContext *ctx) = 0;
  virtual void exitGraphPatternNotTriples(SparqlAutomaticParser::GraphPatternNotTriplesContext *ctx) = 0;

  virtual void enterOptionalGraphPattern(SparqlAutomaticParser::OptionalGraphPatternContext *ctx) = 0;
  virtual void exitOptionalGraphPattern(SparqlAutomaticParser::OptionalGraphPatternContext *ctx) = 0;

  virtual void enterGraphGraphPattern(SparqlAutomaticParser::GraphGraphPatternContext *ctx) = 0;
  virtual void exitGraphGraphPattern(SparqlAutomaticParser::GraphGraphPatternContext *ctx) = 0;

  virtual void enterServiceGraphPattern(SparqlAutomaticParser::ServiceGraphPatternContext *ctx) = 0;
  virtual void exitServiceGraphPattern(SparqlAutomaticParser::ServiceGraphPatternContext *ctx) = 0;

  virtual void enterBind(SparqlAutomaticParser::BindContext *ctx) = 0;
  virtual void exitBind(SparqlAutomaticParser::BindContext *ctx) = 0;

  virtual void enterInlineData(SparqlAutomaticParser::InlineDataContext *ctx) = 0;
  virtual void exitInlineData(SparqlAutomaticParser::InlineDataContext *ctx) = 0;

  virtual void enterDataBlock(SparqlAutomaticParser::DataBlockContext *ctx) = 0;
  virtual void exitDataBlock(SparqlAutomaticParser::DataBlockContext *ctx) = 0;

  virtual void enterInlineDataOneVar(SparqlAutomaticParser::InlineDataOneVarContext *ctx) = 0;
  virtual void exitInlineDataOneVar(SparqlAutomaticParser::InlineDataOneVarContext *ctx) = 0;

  virtual void enterInlineDataFull(SparqlAutomaticParser::InlineDataFullContext *ctx) = 0;
  virtual void exitInlineDataFull(SparqlAutomaticParser::InlineDataFullContext *ctx) = 0;

  virtual void enterDataBlockSingle(SparqlAutomaticParser::DataBlockSingleContext *ctx) = 0;
  virtual void exitDataBlockSingle(SparqlAutomaticParser::DataBlockSingleContext *ctx) = 0;

  virtual void enterDataBlockValue(SparqlAutomaticParser::DataBlockValueContext *ctx) = 0;
  virtual void exitDataBlockValue(SparqlAutomaticParser::DataBlockValueContext *ctx) = 0;

  virtual void enterMinusGraphPattern(SparqlAutomaticParser::MinusGraphPatternContext *ctx) = 0;
  virtual void exitMinusGraphPattern(SparqlAutomaticParser::MinusGraphPatternContext *ctx) = 0;

  virtual void enterGroupOrUnionGraphPattern(SparqlAutomaticParser::GroupOrUnionGraphPatternContext *ctx) = 0;
  virtual void exitGroupOrUnionGraphPattern(SparqlAutomaticParser::GroupOrUnionGraphPatternContext *ctx) = 0;

  virtual void enterFilterR(SparqlAutomaticParser::FilterRContext *ctx) = 0;
  virtual void exitFilterR(SparqlAutomaticParser::FilterRContext *ctx) = 0;

  virtual void enterConstraint(SparqlAutomaticParser::ConstraintContext *ctx) = 0;
  virtual void exitConstraint(SparqlAutomaticParser::ConstraintContext *ctx) = 0;

  virtual void enterFunctionCall(SparqlAutomaticParser::FunctionCallContext *ctx) = 0;
  virtual void exitFunctionCall(SparqlAutomaticParser::FunctionCallContext *ctx) = 0;

  virtual void enterArgList(SparqlAutomaticParser::ArgListContext *ctx) = 0;
  virtual void exitArgList(SparqlAutomaticParser::ArgListContext *ctx) = 0;

  virtual void enterExpressionList(SparqlAutomaticParser::ExpressionListContext *ctx) = 0;
  virtual void exitExpressionList(SparqlAutomaticParser::ExpressionListContext *ctx) = 0;

  virtual void enterConstructTemplate(SparqlAutomaticParser::ConstructTemplateContext *ctx) = 0;
  virtual void exitConstructTemplate(SparqlAutomaticParser::ConstructTemplateContext *ctx) = 0;

  virtual void enterConstructTriples(SparqlAutomaticParser::ConstructTriplesContext *ctx) = 0;
  virtual void exitConstructTriples(SparqlAutomaticParser::ConstructTriplesContext *ctx) = 0;

  virtual void enterTriplesSameSubject(SparqlAutomaticParser::TriplesSameSubjectContext *ctx) = 0;
  virtual void exitTriplesSameSubject(SparqlAutomaticParser::TriplesSameSubjectContext *ctx) = 0;

  virtual void enterPropertyList(SparqlAutomaticParser::PropertyListContext *ctx) = 0;
  virtual void exitPropertyList(SparqlAutomaticParser::PropertyListContext *ctx) = 0;

  virtual void enterPropertyListNotEmpty(SparqlAutomaticParser::PropertyListNotEmptyContext *ctx) = 0;
  virtual void exitPropertyListNotEmpty(SparqlAutomaticParser::PropertyListNotEmptyContext *ctx) = 0;

  virtual void enterVerb(SparqlAutomaticParser::VerbContext *ctx) = 0;
  virtual void exitVerb(SparqlAutomaticParser::VerbContext *ctx) = 0;

  virtual void enterObjectList(SparqlAutomaticParser::ObjectListContext *ctx) = 0;
  virtual void exitObjectList(SparqlAutomaticParser::ObjectListContext *ctx) = 0;

  virtual void enterObjectR(SparqlAutomaticParser::ObjectRContext *ctx) = 0;
  virtual void exitObjectR(SparqlAutomaticParser::ObjectRContext *ctx) = 0;

  virtual void enterTriplesSameSubjectPath(SparqlAutomaticParser::TriplesSameSubjectPathContext *ctx) = 0;
  virtual void exitTriplesSameSubjectPath(SparqlAutomaticParser::TriplesSameSubjectPathContext *ctx) = 0;

  virtual void enterPropertyListPath(SparqlAutomaticParser::PropertyListPathContext *ctx) = 0;
  virtual void exitPropertyListPath(SparqlAutomaticParser::PropertyListPathContext *ctx) = 0;

  virtual void enterPropertyListPathNotEmpty(SparqlAutomaticParser::PropertyListPathNotEmptyContext *ctx) = 0;
  virtual void exitPropertyListPathNotEmpty(SparqlAutomaticParser::PropertyListPathNotEmptyContext *ctx) = 0;

  virtual void enterVerbPath(SparqlAutomaticParser::VerbPathContext *ctx) = 0;
  virtual void exitVerbPath(SparqlAutomaticParser::VerbPathContext *ctx) = 0;

  virtual void enterVerbSimple(SparqlAutomaticParser::VerbSimpleContext *ctx) = 0;
  virtual void exitVerbSimple(SparqlAutomaticParser::VerbSimpleContext *ctx) = 0;

  virtual void enterTupleWithoutPath(SparqlAutomaticParser::TupleWithoutPathContext *ctx) = 0;
  virtual void exitTupleWithoutPath(SparqlAutomaticParser::TupleWithoutPathContext *ctx) = 0;

  virtual void enterTupleWithPath(SparqlAutomaticParser::TupleWithPathContext *ctx) = 0;
  virtual void exitTupleWithPath(SparqlAutomaticParser::TupleWithPathContext *ctx) = 0;

  virtual void enterVerbPathOrSimple(SparqlAutomaticParser::VerbPathOrSimpleContext *ctx) = 0;
  virtual void exitVerbPathOrSimple(SparqlAutomaticParser::VerbPathOrSimpleContext *ctx) = 0;

  virtual void enterObjectListPath(SparqlAutomaticParser::ObjectListPathContext *ctx) = 0;
  virtual void exitObjectListPath(SparqlAutomaticParser::ObjectListPathContext *ctx) = 0;

  virtual void enterObjectPath(SparqlAutomaticParser::ObjectPathContext *ctx) = 0;
  virtual void exitObjectPath(SparqlAutomaticParser::ObjectPathContext *ctx) = 0;

  virtual void enterPath(SparqlAutomaticParser::PathContext *ctx) = 0;
  virtual void exitPath(SparqlAutomaticParser::PathContext *ctx) = 0;

  virtual void enterPathAlternative(SparqlAutomaticParser::PathAlternativeContext *ctx) = 0;
  virtual void exitPathAlternative(SparqlAutomaticParser::PathAlternativeContext *ctx) = 0;

  virtual void enterPathSequence(SparqlAutomaticParser::PathSequenceContext *ctx) = 0;
  virtual void exitPathSequence(SparqlAutomaticParser::PathSequenceContext *ctx) = 0;

  virtual void enterPathElt(SparqlAutomaticParser::PathEltContext *ctx) = 0;
  virtual void exitPathElt(SparqlAutomaticParser::PathEltContext *ctx) = 0;

  virtual void enterPathEltOrInverse(SparqlAutomaticParser::PathEltOrInverseContext *ctx) = 0;
  virtual void exitPathEltOrInverse(SparqlAutomaticParser::PathEltOrInverseContext *ctx) = 0;

  virtual void enterPathMod(SparqlAutomaticParser::PathModContext *ctx) = 0;
  virtual void exitPathMod(SparqlAutomaticParser::PathModContext *ctx) = 0;

  virtual void enterStepsMin(SparqlAutomaticParser::StepsMinContext *ctx) = 0;
  virtual void exitStepsMin(SparqlAutomaticParser::StepsMinContext *ctx) = 0;

  virtual void enterStepsMax(SparqlAutomaticParser::StepsMaxContext *ctx) = 0;
  virtual void exitStepsMax(SparqlAutomaticParser::StepsMaxContext *ctx) = 0;

  virtual void enterPathPrimary(SparqlAutomaticParser::PathPrimaryContext *ctx) = 0;
  virtual void exitPathPrimary(SparqlAutomaticParser::PathPrimaryContext *ctx) = 0;

  virtual void enterPathNegatedPropertySet(SparqlAutomaticParser::PathNegatedPropertySetContext *ctx) = 0;
  virtual void exitPathNegatedPropertySet(SparqlAutomaticParser::PathNegatedPropertySetContext *ctx) = 0;

  virtual void enterPathOneInPropertySet(SparqlAutomaticParser::PathOneInPropertySetContext *ctx) = 0;
  virtual void exitPathOneInPropertySet(SparqlAutomaticParser::PathOneInPropertySetContext *ctx) = 0;

  virtual void enterInteger(SparqlAutomaticParser::IntegerContext *ctx) = 0;
  virtual void exitInteger(SparqlAutomaticParser::IntegerContext *ctx) = 0;

  virtual void enterTriplesNode(SparqlAutomaticParser::TriplesNodeContext *ctx) = 0;
  virtual void exitTriplesNode(SparqlAutomaticParser::TriplesNodeContext *ctx) = 0;

  virtual void enterBlankNodePropertyList(SparqlAutomaticParser::BlankNodePropertyListContext *ctx) = 0;
  virtual void exitBlankNodePropertyList(SparqlAutomaticParser::BlankNodePropertyListContext *ctx) = 0;

  virtual void enterTriplesNodePath(SparqlAutomaticParser::TriplesNodePathContext *ctx) = 0;
  virtual void exitTriplesNodePath(SparqlAutomaticParser::TriplesNodePathContext *ctx) = 0;

  virtual void enterBlankNodePropertyListPath(SparqlAutomaticParser::BlankNodePropertyListPathContext *ctx) = 0;
  virtual void exitBlankNodePropertyListPath(SparqlAutomaticParser::BlankNodePropertyListPathContext *ctx) = 0;

  virtual void enterCollection(SparqlAutomaticParser::CollectionContext *ctx) = 0;
  virtual void exitCollection(SparqlAutomaticParser::CollectionContext *ctx) = 0;

  virtual void enterCollectionPath(SparqlAutomaticParser::CollectionPathContext *ctx) = 0;
  virtual void exitCollectionPath(SparqlAutomaticParser::CollectionPathContext *ctx) = 0;

  virtual void enterGraphNode(SparqlAutomaticParser::GraphNodeContext *ctx) = 0;
  virtual void exitGraphNode(SparqlAutomaticParser::GraphNodeContext *ctx) = 0;

  virtual void enterGraphNodePath(SparqlAutomaticParser::GraphNodePathContext *ctx) = 0;
  virtual void exitGraphNodePath(SparqlAutomaticParser::GraphNodePathContext *ctx) = 0;

  virtual void enterVarOrTerm(SparqlAutomaticParser::VarOrTermContext *ctx) = 0;
  virtual void exitVarOrTerm(SparqlAutomaticParser::VarOrTermContext *ctx) = 0;

  virtual void enterVarOrIri(SparqlAutomaticParser::VarOrIriContext *ctx) = 0;
  virtual void exitVarOrIri(SparqlAutomaticParser::VarOrIriContext *ctx) = 0;

  virtual void enterVar(SparqlAutomaticParser::VarContext *ctx) = 0;
  virtual void exitVar(SparqlAutomaticParser::VarContext *ctx) = 0;

  virtual void enterGraphTerm(SparqlAutomaticParser::GraphTermContext *ctx) = 0;
  virtual void exitGraphTerm(SparqlAutomaticParser::GraphTermContext *ctx) = 0;

  virtual void enterExpression(SparqlAutomaticParser::ExpressionContext *ctx) = 0;
  virtual void exitExpression(SparqlAutomaticParser::ExpressionContext *ctx) = 0;

  virtual void enterConditionalOrExpression(SparqlAutomaticParser::ConditionalOrExpressionContext *ctx) = 0;
  virtual void exitConditionalOrExpression(SparqlAutomaticParser::ConditionalOrExpressionContext *ctx) = 0;

  virtual void enterConditionalAndExpression(SparqlAutomaticParser::ConditionalAndExpressionContext *ctx) = 0;
  virtual void exitConditionalAndExpression(SparqlAutomaticParser::ConditionalAndExpressionContext *ctx) = 0;

  virtual void enterValueLogical(SparqlAutomaticParser::ValueLogicalContext *ctx) = 0;
  virtual void exitValueLogical(SparqlAutomaticParser::ValueLogicalContext *ctx) = 0;

  virtual void enterRelationalExpression(SparqlAutomaticParser::RelationalExpressionContext *ctx) = 0;
  virtual void exitRelationalExpression(SparqlAutomaticParser::RelationalExpressionContext *ctx) = 0;

  virtual void enterNumericExpression(SparqlAutomaticParser::NumericExpressionContext *ctx) = 0;
  virtual void exitNumericExpression(SparqlAutomaticParser::NumericExpressionContext *ctx) = 0;

  virtual void enterAdditiveExpression(SparqlAutomaticParser::AdditiveExpressionContext *ctx) = 0;
  virtual void exitAdditiveExpression(SparqlAutomaticParser::AdditiveExpressionContext *ctx) = 0;

  virtual void enterMultiplicativeExpressionWithSign(SparqlAutomaticParser::MultiplicativeExpressionWithSignContext *ctx) = 0;
  virtual void exitMultiplicativeExpressionWithSign(SparqlAutomaticParser::MultiplicativeExpressionWithSignContext *ctx) = 0;

  virtual void enterPlusSubexpression(SparqlAutomaticParser::PlusSubexpressionContext *ctx) = 0;
  virtual void exitPlusSubexpression(SparqlAutomaticParser::PlusSubexpressionContext *ctx) = 0;

  virtual void enterMinusSubexpression(SparqlAutomaticParser::MinusSubexpressionContext *ctx) = 0;
  virtual void exitMinusSubexpression(SparqlAutomaticParser::MinusSubexpressionContext *ctx) = 0;

  virtual void enterMultiplicativeExpressionWithLeadingSignButNoSpace(SparqlAutomaticParser::MultiplicativeExpressionWithLeadingSignButNoSpaceContext *ctx) = 0;
  virtual void exitMultiplicativeExpressionWithLeadingSignButNoSpace(SparqlAutomaticParser::MultiplicativeExpressionWithLeadingSignButNoSpaceContext *ctx) = 0;

  virtual void enterMultiplicativeExpression(SparqlAutomaticParser::MultiplicativeExpressionContext *ctx) = 0;
  virtual void exitMultiplicativeExpression(SparqlAutomaticParser::MultiplicativeExpressionContext *ctx) = 0;

  virtual void enterMultiplyOrDivideExpression(SparqlAutomaticParser::MultiplyOrDivideExpressionContext *ctx) = 0;
  virtual void exitMultiplyOrDivideExpression(SparqlAutomaticParser::MultiplyOrDivideExpressionContext *ctx) = 0;

  virtual void enterMultiplyExpression(SparqlAutomaticParser::MultiplyExpressionContext *ctx) = 0;
  virtual void exitMultiplyExpression(SparqlAutomaticParser::MultiplyExpressionContext *ctx) = 0;

  virtual void enterDivideExpression(SparqlAutomaticParser::DivideExpressionContext *ctx) = 0;
  virtual void exitDivideExpression(SparqlAutomaticParser::DivideExpressionContext *ctx) = 0;

  virtual void enterUnaryExpression(SparqlAutomaticParser::UnaryExpressionContext *ctx) = 0;
  virtual void exitUnaryExpression(SparqlAutomaticParser::UnaryExpressionContext *ctx) = 0;

  virtual void enterPrimaryExpression(SparqlAutomaticParser::PrimaryExpressionContext *ctx) = 0;
  virtual void exitPrimaryExpression(SparqlAutomaticParser::PrimaryExpressionContext *ctx) = 0;

  virtual void enterBrackettedExpression(SparqlAutomaticParser::BrackettedExpressionContext *ctx) = 0;
  virtual void exitBrackettedExpression(SparqlAutomaticParser::BrackettedExpressionContext *ctx) = 0;

  virtual void enterBuiltInCall(SparqlAutomaticParser::BuiltInCallContext *ctx) = 0;
  virtual void exitBuiltInCall(SparqlAutomaticParser::BuiltInCallContext *ctx) = 0;

  virtual void enterRegexExpression(SparqlAutomaticParser::RegexExpressionContext *ctx) = 0;
  virtual void exitRegexExpression(SparqlAutomaticParser::RegexExpressionContext *ctx) = 0;

  virtual void enterLangExpression(SparqlAutomaticParser::LangExpressionContext *ctx) = 0;
  virtual void exitLangExpression(SparqlAutomaticParser::LangExpressionContext *ctx) = 0;

  virtual void enterSubstringExpression(SparqlAutomaticParser::SubstringExpressionContext *ctx) = 0;
  virtual void exitSubstringExpression(SparqlAutomaticParser::SubstringExpressionContext *ctx) = 0;

  virtual void enterStrReplaceExpression(SparqlAutomaticParser::StrReplaceExpressionContext *ctx) = 0;
  virtual void exitStrReplaceExpression(SparqlAutomaticParser::StrReplaceExpressionContext *ctx) = 0;

  virtual void enterExistsFunc(SparqlAutomaticParser::ExistsFuncContext *ctx) = 0;
  virtual void exitExistsFunc(SparqlAutomaticParser::ExistsFuncContext *ctx) = 0;

  virtual void enterNotExistsFunc(SparqlAutomaticParser::NotExistsFuncContext *ctx) = 0;
  virtual void exitNotExistsFunc(SparqlAutomaticParser::NotExistsFuncContext *ctx) = 0;

  virtual void enterAggregate(SparqlAutomaticParser::AggregateContext *ctx) = 0;
  virtual void exitAggregate(SparqlAutomaticParser::AggregateContext *ctx) = 0;

  virtual void enterIriOrFunction(SparqlAutomaticParser::IriOrFunctionContext *ctx) = 0;
  virtual void exitIriOrFunction(SparqlAutomaticParser::IriOrFunctionContext *ctx) = 0;

  virtual void enterRdfLiteral(SparqlAutomaticParser::RdfLiteralContext *ctx) = 0;
  virtual void exitRdfLiteral(SparqlAutomaticParser::RdfLiteralContext *ctx) = 0;

  virtual void enterNumericLiteral(SparqlAutomaticParser::NumericLiteralContext *ctx) = 0;
  virtual void exitNumericLiteral(SparqlAutomaticParser::NumericLiteralContext *ctx) = 0;

  virtual void enterNumericLiteralUnsigned(SparqlAutomaticParser::NumericLiteralUnsignedContext *ctx) = 0;
  virtual void exitNumericLiteralUnsigned(SparqlAutomaticParser::NumericLiteralUnsignedContext *ctx) = 0;

  virtual void enterNumericLiteralPositive(SparqlAutomaticParser::NumericLiteralPositiveContext *ctx) = 0;
  virtual void exitNumericLiteralPositive(SparqlAutomaticParser::NumericLiteralPositiveContext *ctx) = 0;

  virtual void enterNumericLiteralNegative(SparqlAutomaticParser::NumericLiteralNegativeContext *ctx) = 0;
  virtual void exitNumericLiteralNegative(SparqlAutomaticParser::NumericLiteralNegativeContext *ctx) = 0;

  virtual void enterBooleanLiteral(SparqlAutomaticParser::BooleanLiteralContext *ctx) = 0;
  virtual void exitBooleanLiteral(SparqlAutomaticParser::BooleanLiteralContext *ctx) = 0;

  virtual void enterString(SparqlAutomaticParser::StringContext *ctx) = 0;
  virtual void exitString(SparqlAutomaticParser::StringContext *ctx) = 0;

  virtual void enterIri(SparqlAutomaticParser::IriContext *ctx) = 0;
  virtual void exitIri(SparqlAutomaticParser::IriContext *ctx) = 0;

  virtual void enterPrefixedName(SparqlAutomaticParser::PrefixedNameContext *ctx) = 0;
  virtual void exitPrefixedName(SparqlAutomaticParser::PrefixedNameContext *ctx) = 0;

  virtual void enterBlankNode(SparqlAutomaticParser::BlankNodeContext *ctx) = 0;
  virtual void exitBlankNode(SparqlAutomaticParser::BlankNodeContext *ctx) = 0;

  virtual void enterIriref(SparqlAutomaticParser::IrirefContext *ctx) = 0;
  virtual void exitIriref(SparqlAutomaticParser::IrirefContext *ctx) = 0;

  virtual void enterPnameLn(SparqlAutomaticParser::PnameLnContext *ctx) = 0;
  virtual void exitPnameLn(SparqlAutomaticParser::PnameLnContext *ctx) = 0;

  virtual void enterPnameNs(SparqlAutomaticParser::PnameNsContext *ctx) = 0;
  virtual void exitPnameNs(SparqlAutomaticParser::PnameNsContext *ctx) = 0;


};

