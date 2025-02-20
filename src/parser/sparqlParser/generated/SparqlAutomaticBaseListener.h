
// Generated from SparqlAutomatic.g4 by ANTLR 4.11.1

#pragma once


#include "antlr4-runtime.h"
#include "SparqlAutomaticListener.h"


/**
 * This class provides an empty implementation of SparqlAutomaticListener,
 * which can be extended to create a listener which only needs to handle a subset
 * of the available methods.
 */
class  SparqlAutomaticBaseListener : public SparqlAutomaticListener {
public:

  virtual void enterQueryOrUpdate(SparqlAutomaticParser::QueryOrUpdateContext * /*ctx*/) override { }
  virtual void exitQueryOrUpdate(SparqlAutomaticParser::QueryOrUpdateContext * /*ctx*/) override { }

  virtual void enterQuery(SparqlAutomaticParser::QueryContext * /*ctx*/) override { }
  virtual void exitQuery(SparqlAutomaticParser::QueryContext * /*ctx*/) override { }

  virtual void enterPrologue(SparqlAutomaticParser::PrologueContext * /*ctx*/) override { }
  virtual void exitPrologue(SparqlAutomaticParser::PrologueContext * /*ctx*/) override { }

  virtual void enterBaseDecl(SparqlAutomaticParser::BaseDeclContext * /*ctx*/) override { }
  virtual void exitBaseDecl(SparqlAutomaticParser::BaseDeclContext * /*ctx*/) override { }

  virtual void enterPrefixDecl(SparqlAutomaticParser::PrefixDeclContext * /*ctx*/) override { }
  virtual void exitPrefixDecl(SparqlAutomaticParser::PrefixDeclContext * /*ctx*/) override { }

  virtual void enterSelectQuery(SparqlAutomaticParser::SelectQueryContext * /*ctx*/) override { }
  virtual void exitSelectQuery(SparqlAutomaticParser::SelectQueryContext * /*ctx*/) override { }

  virtual void enterSubSelect(SparqlAutomaticParser::SubSelectContext * /*ctx*/) override { }
  virtual void exitSubSelect(SparqlAutomaticParser::SubSelectContext * /*ctx*/) override { }

  virtual void enterSelectClause(SparqlAutomaticParser::SelectClauseContext * /*ctx*/) override { }
  virtual void exitSelectClause(SparqlAutomaticParser::SelectClauseContext * /*ctx*/) override { }

  virtual void enterVarOrAlias(SparqlAutomaticParser::VarOrAliasContext * /*ctx*/) override { }
  virtual void exitVarOrAlias(SparqlAutomaticParser::VarOrAliasContext * /*ctx*/) override { }

  virtual void enterAlias(SparqlAutomaticParser::AliasContext * /*ctx*/) override { }
  virtual void exitAlias(SparqlAutomaticParser::AliasContext * /*ctx*/) override { }

  virtual void enterAliasWithoutBrackets(SparqlAutomaticParser::AliasWithoutBracketsContext * /*ctx*/) override { }
  virtual void exitAliasWithoutBrackets(SparqlAutomaticParser::AliasWithoutBracketsContext * /*ctx*/) override { }

  virtual void enterConstructQuery(SparqlAutomaticParser::ConstructQueryContext * /*ctx*/) override { }
  virtual void exitConstructQuery(SparqlAutomaticParser::ConstructQueryContext * /*ctx*/) override { }

  virtual void enterDescribeQuery(SparqlAutomaticParser::DescribeQueryContext * /*ctx*/) override { }
  virtual void exitDescribeQuery(SparqlAutomaticParser::DescribeQueryContext * /*ctx*/) override { }

  virtual void enterAskQuery(SparqlAutomaticParser::AskQueryContext * /*ctx*/) override { }
  virtual void exitAskQuery(SparqlAutomaticParser::AskQueryContext * /*ctx*/) override { }

  virtual void enterDatasetClause(SparqlAutomaticParser::DatasetClauseContext * /*ctx*/) override { }
  virtual void exitDatasetClause(SparqlAutomaticParser::DatasetClauseContext * /*ctx*/) override { }

  virtual void enterDefaultGraphClause(SparqlAutomaticParser::DefaultGraphClauseContext * /*ctx*/) override { }
  virtual void exitDefaultGraphClause(SparqlAutomaticParser::DefaultGraphClauseContext * /*ctx*/) override { }

  virtual void enterNamedGraphClause(SparqlAutomaticParser::NamedGraphClauseContext * /*ctx*/) override { }
  virtual void exitNamedGraphClause(SparqlAutomaticParser::NamedGraphClauseContext * /*ctx*/) override { }

  virtual void enterSourceSelector(SparqlAutomaticParser::SourceSelectorContext * /*ctx*/) override { }
  virtual void exitSourceSelector(SparqlAutomaticParser::SourceSelectorContext * /*ctx*/) override { }

  virtual void enterWhereClause(SparqlAutomaticParser::WhereClauseContext * /*ctx*/) override { }
  virtual void exitWhereClause(SparqlAutomaticParser::WhereClauseContext * /*ctx*/) override { }

  virtual void enterSolutionModifier(SparqlAutomaticParser::SolutionModifierContext * /*ctx*/) override { }
  virtual void exitSolutionModifier(SparqlAutomaticParser::SolutionModifierContext * /*ctx*/) override { }

  virtual void enterGroupClause(SparqlAutomaticParser::GroupClauseContext * /*ctx*/) override { }
  virtual void exitGroupClause(SparqlAutomaticParser::GroupClauseContext * /*ctx*/) override { }

  virtual void enterGroupCondition(SparqlAutomaticParser::GroupConditionContext * /*ctx*/) override { }
  virtual void exitGroupCondition(SparqlAutomaticParser::GroupConditionContext * /*ctx*/) override { }

  virtual void enterHavingClause(SparqlAutomaticParser::HavingClauseContext * /*ctx*/) override { }
  virtual void exitHavingClause(SparqlAutomaticParser::HavingClauseContext * /*ctx*/) override { }

  virtual void enterHavingCondition(SparqlAutomaticParser::HavingConditionContext * /*ctx*/) override { }
  virtual void exitHavingCondition(SparqlAutomaticParser::HavingConditionContext * /*ctx*/) override { }

  virtual void enterOrderClause(SparqlAutomaticParser::OrderClauseContext * /*ctx*/) override { }
  virtual void exitOrderClause(SparqlAutomaticParser::OrderClauseContext * /*ctx*/) override { }

  virtual void enterOrderCondition(SparqlAutomaticParser::OrderConditionContext * /*ctx*/) override { }
  virtual void exitOrderCondition(SparqlAutomaticParser::OrderConditionContext * /*ctx*/) override { }

  virtual void enterLimitOffsetClauses(SparqlAutomaticParser::LimitOffsetClausesContext * /*ctx*/) override { }
  virtual void exitLimitOffsetClauses(SparqlAutomaticParser::LimitOffsetClausesContext * /*ctx*/) override { }

  virtual void enterLimitClause(SparqlAutomaticParser::LimitClauseContext * /*ctx*/) override { }
  virtual void exitLimitClause(SparqlAutomaticParser::LimitClauseContext * /*ctx*/) override { }

  virtual void enterOffsetClause(SparqlAutomaticParser::OffsetClauseContext * /*ctx*/) override { }
  virtual void exitOffsetClause(SparqlAutomaticParser::OffsetClauseContext * /*ctx*/) override { }

  virtual void enterTextLimitClause(SparqlAutomaticParser::TextLimitClauseContext * /*ctx*/) override { }
  virtual void exitTextLimitClause(SparqlAutomaticParser::TextLimitClauseContext * /*ctx*/) override { }

  virtual void enterValuesClause(SparqlAutomaticParser::ValuesClauseContext * /*ctx*/) override { }
  virtual void exitValuesClause(SparqlAutomaticParser::ValuesClauseContext * /*ctx*/) override { }

  virtual void enterUpdate(SparqlAutomaticParser::UpdateContext * /*ctx*/) override { }
  virtual void exitUpdate(SparqlAutomaticParser::UpdateContext * /*ctx*/) override { }

  virtual void enterUpdate1(SparqlAutomaticParser::Update1Context * /*ctx*/) override { }
  virtual void exitUpdate1(SparqlAutomaticParser::Update1Context * /*ctx*/) override { }

  virtual void enterLoad(SparqlAutomaticParser::LoadContext * /*ctx*/) override { }
  virtual void exitLoad(SparqlAutomaticParser::LoadContext * /*ctx*/) override { }

  virtual void enterClear(SparqlAutomaticParser::ClearContext * /*ctx*/) override { }
  virtual void exitClear(SparqlAutomaticParser::ClearContext * /*ctx*/) override { }

  virtual void enterDrop(SparqlAutomaticParser::DropContext * /*ctx*/) override { }
  virtual void exitDrop(SparqlAutomaticParser::DropContext * /*ctx*/) override { }

  virtual void enterCreate(SparqlAutomaticParser::CreateContext * /*ctx*/) override { }
  virtual void exitCreate(SparqlAutomaticParser::CreateContext * /*ctx*/) override { }

  virtual void enterAdd(SparqlAutomaticParser::AddContext * /*ctx*/) override { }
  virtual void exitAdd(SparqlAutomaticParser::AddContext * /*ctx*/) override { }

  virtual void enterMove(SparqlAutomaticParser::MoveContext * /*ctx*/) override { }
  virtual void exitMove(SparqlAutomaticParser::MoveContext * /*ctx*/) override { }

  virtual void enterCopy(SparqlAutomaticParser::CopyContext * /*ctx*/) override { }
  virtual void exitCopy(SparqlAutomaticParser::CopyContext * /*ctx*/) override { }

  virtual void enterInsertData(SparqlAutomaticParser::InsertDataContext * /*ctx*/) override { }
  virtual void exitInsertData(SparqlAutomaticParser::InsertDataContext * /*ctx*/) override { }

  virtual void enterDeleteData(SparqlAutomaticParser::DeleteDataContext * /*ctx*/) override { }
  virtual void exitDeleteData(SparqlAutomaticParser::DeleteDataContext * /*ctx*/) override { }

  virtual void enterDeleteWhere(SparqlAutomaticParser::DeleteWhereContext * /*ctx*/) override { }
  virtual void exitDeleteWhere(SparqlAutomaticParser::DeleteWhereContext * /*ctx*/) override { }

  virtual void enterModify(SparqlAutomaticParser::ModifyContext * /*ctx*/) override { }
  virtual void exitModify(SparqlAutomaticParser::ModifyContext * /*ctx*/) override { }

  virtual void enterDeleteClause(SparqlAutomaticParser::DeleteClauseContext * /*ctx*/) override { }
  virtual void exitDeleteClause(SparqlAutomaticParser::DeleteClauseContext * /*ctx*/) override { }

  virtual void enterInsertClause(SparqlAutomaticParser::InsertClauseContext * /*ctx*/) override { }
  virtual void exitInsertClause(SparqlAutomaticParser::InsertClauseContext * /*ctx*/) override { }

  virtual void enterUsingClause(SparqlAutomaticParser::UsingClauseContext * /*ctx*/) override { }
  virtual void exitUsingClause(SparqlAutomaticParser::UsingClauseContext * /*ctx*/) override { }

  virtual void enterGraphOrDefault(SparqlAutomaticParser::GraphOrDefaultContext * /*ctx*/) override { }
  virtual void exitGraphOrDefault(SparqlAutomaticParser::GraphOrDefaultContext * /*ctx*/) override { }

  virtual void enterGraphRef(SparqlAutomaticParser::GraphRefContext * /*ctx*/) override { }
  virtual void exitGraphRef(SparqlAutomaticParser::GraphRefContext * /*ctx*/) override { }

  virtual void enterGraphRefAll(SparqlAutomaticParser::GraphRefAllContext * /*ctx*/) override { }
  virtual void exitGraphRefAll(SparqlAutomaticParser::GraphRefAllContext * /*ctx*/) override { }

  virtual void enterQuadPattern(SparqlAutomaticParser::QuadPatternContext * /*ctx*/) override { }
  virtual void exitQuadPattern(SparqlAutomaticParser::QuadPatternContext * /*ctx*/) override { }

  virtual void enterQuadData(SparqlAutomaticParser::QuadDataContext * /*ctx*/) override { }
  virtual void exitQuadData(SparqlAutomaticParser::QuadDataContext * /*ctx*/) override { }

  virtual void enterQuads(SparqlAutomaticParser::QuadsContext * /*ctx*/) override { }
  virtual void exitQuads(SparqlAutomaticParser::QuadsContext * /*ctx*/) override { }

  virtual void enterQuadsNotTriples(SparqlAutomaticParser::QuadsNotTriplesContext * /*ctx*/) override { }
  virtual void exitQuadsNotTriples(SparqlAutomaticParser::QuadsNotTriplesContext * /*ctx*/) override { }

  virtual void enterTriplesTemplate(SparqlAutomaticParser::TriplesTemplateContext * /*ctx*/) override { }
  virtual void exitTriplesTemplate(SparqlAutomaticParser::TriplesTemplateContext * /*ctx*/) override { }

  virtual void enterGroupGraphPattern(SparqlAutomaticParser::GroupGraphPatternContext * /*ctx*/) override { }
  virtual void exitGroupGraphPattern(SparqlAutomaticParser::GroupGraphPatternContext * /*ctx*/) override { }

  virtual void enterGroupGraphPatternSub(SparqlAutomaticParser::GroupGraphPatternSubContext * /*ctx*/) override { }
  virtual void exitGroupGraphPatternSub(SparqlAutomaticParser::GroupGraphPatternSubContext * /*ctx*/) override { }

  virtual void enterGraphPatternNotTriplesAndMaybeTriples(SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext * /*ctx*/) override { }
  virtual void exitGraphPatternNotTriplesAndMaybeTriples(SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext * /*ctx*/) override { }

  virtual void enterTriplesBlock(SparqlAutomaticParser::TriplesBlockContext * /*ctx*/) override { }
  virtual void exitTriplesBlock(SparqlAutomaticParser::TriplesBlockContext * /*ctx*/) override { }

  virtual void enterGraphPatternNotTriples(SparqlAutomaticParser::GraphPatternNotTriplesContext * /*ctx*/) override { }
  virtual void exitGraphPatternNotTriples(SparqlAutomaticParser::GraphPatternNotTriplesContext * /*ctx*/) override { }

  virtual void enterOptionalGraphPattern(SparqlAutomaticParser::OptionalGraphPatternContext * /*ctx*/) override { }
  virtual void exitOptionalGraphPattern(SparqlAutomaticParser::OptionalGraphPatternContext * /*ctx*/) override { }

  virtual void enterGraphGraphPattern(SparqlAutomaticParser::GraphGraphPatternContext * /*ctx*/) override { }
  virtual void exitGraphGraphPattern(SparqlAutomaticParser::GraphGraphPatternContext * /*ctx*/) override { }

  virtual void enterServiceGraphPattern(SparqlAutomaticParser::ServiceGraphPatternContext * /*ctx*/) override { }
  virtual void exitServiceGraphPattern(SparqlAutomaticParser::ServiceGraphPatternContext * /*ctx*/) override { }

  virtual void enterBind(SparqlAutomaticParser::BindContext * /*ctx*/) override { }
  virtual void exitBind(SparqlAutomaticParser::BindContext * /*ctx*/) override { }

  virtual void enterInlineData(SparqlAutomaticParser::InlineDataContext * /*ctx*/) override { }
  virtual void exitInlineData(SparqlAutomaticParser::InlineDataContext * /*ctx*/) override { }

  virtual void enterDataBlock(SparqlAutomaticParser::DataBlockContext * /*ctx*/) override { }
  virtual void exitDataBlock(SparqlAutomaticParser::DataBlockContext * /*ctx*/) override { }

  virtual void enterInlineDataOneVar(SparqlAutomaticParser::InlineDataOneVarContext * /*ctx*/) override { }
  virtual void exitInlineDataOneVar(SparqlAutomaticParser::InlineDataOneVarContext * /*ctx*/) override { }

  virtual void enterInlineDataFull(SparqlAutomaticParser::InlineDataFullContext * /*ctx*/) override { }
  virtual void exitInlineDataFull(SparqlAutomaticParser::InlineDataFullContext * /*ctx*/) override { }

  virtual void enterDataBlockSingle(SparqlAutomaticParser::DataBlockSingleContext * /*ctx*/) override { }
  virtual void exitDataBlockSingle(SparqlAutomaticParser::DataBlockSingleContext * /*ctx*/) override { }

  virtual void enterDataBlockValue(SparqlAutomaticParser::DataBlockValueContext * /*ctx*/) override { }
  virtual void exitDataBlockValue(SparqlAutomaticParser::DataBlockValueContext * /*ctx*/) override { }

  virtual void enterMinusGraphPattern(SparqlAutomaticParser::MinusGraphPatternContext * /*ctx*/) override { }
  virtual void exitMinusGraphPattern(SparqlAutomaticParser::MinusGraphPatternContext * /*ctx*/) override { }

  virtual void enterGroupOrUnionGraphPattern(SparqlAutomaticParser::GroupOrUnionGraphPatternContext * /*ctx*/) override { }
  virtual void exitGroupOrUnionGraphPattern(SparqlAutomaticParser::GroupOrUnionGraphPatternContext * /*ctx*/) override { }

  virtual void enterFilterR(SparqlAutomaticParser::FilterRContext * /*ctx*/) override { }
  virtual void exitFilterR(SparqlAutomaticParser::FilterRContext * /*ctx*/) override { }

  virtual void enterConstraint(SparqlAutomaticParser::ConstraintContext * /*ctx*/) override { }
  virtual void exitConstraint(SparqlAutomaticParser::ConstraintContext * /*ctx*/) override { }

  virtual void enterFunctionCall(SparqlAutomaticParser::FunctionCallContext * /*ctx*/) override { }
  virtual void exitFunctionCall(SparqlAutomaticParser::FunctionCallContext * /*ctx*/) override { }

  virtual void enterArgList(SparqlAutomaticParser::ArgListContext * /*ctx*/) override { }
  virtual void exitArgList(SparqlAutomaticParser::ArgListContext * /*ctx*/) override { }

  virtual void enterExpressionList(SparqlAutomaticParser::ExpressionListContext * /*ctx*/) override { }
  virtual void exitExpressionList(SparqlAutomaticParser::ExpressionListContext * /*ctx*/) override { }

  virtual void enterConstructTemplate(SparqlAutomaticParser::ConstructTemplateContext * /*ctx*/) override { }
  virtual void exitConstructTemplate(SparqlAutomaticParser::ConstructTemplateContext * /*ctx*/) override { }

  virtual void enterConstructTriples(SparqlAutomaticParser::ConstructTriplesContext * /*ctx*/) override { }
  virtual void exitConstructTriples(SparqlAutomaticParser::ConstructTriplesContext * /*ctx*/) override { }

  virtual void enterTriplesSameSubject(SparqlAutomaticParser::TriplesSameSubjectContext * /*ctx*/) override { }
  virtual void exitTriplesSameSubject(SparqlAutomaticParser::TriplesSameSubjectContext * /*ctx*/) override { }

  virtual void enterPropertyList(SparqlAutomaticParser::PropertyListContext * /*ctx*/) override { }
  virtual void exitPropertyList(SparqlAutomaticParser::PropertyListContext * /*ctx*/) override { }

  virtual void enterPropertyListNotEmpty(SparqlAutomaticParser::PropertyListNotEmptyContext * /*ctx*/) override { }
  virtual void exitPropertyListNotEmpty(SparqlAutomaticParser::PropertyListNotEmptyContext * /*ctx*/) override { }

  virtual void enterVerb(SparqlAutomaticParser::VerbContext * /*ctx*/) override { }
  virtual void exitVerb(SparqlAutomaticParser::VerbContext * /*ctx*/) override { }

  virtual void enterObjectList(SparqlAutomaticParser::ObjectListContext * /*ctx*/) override { }
  virtual void exitObjectList(SparqlAutomaticParser::ObjectListContext * /*ctx*/) override { }

  virtual void enterObjectR(SparqlAutomaticParser::ObjectRContext * /*ctx*/) override { }
  virtual void exitObjectR(SparqlAutomaticParser::ObjectRContext * /*ctx*/) override { }

  virtual void enterTriplesSameSubjectPath(SparqlAutomaticParser::TriplesSameSubjectPathContext * /*ctx*/) override { }
  virtual void exitTriplesSameSubjectPath(SparqlAutomaticParser::TriplesSameSubjectPathContext * /*ctx*/) override { }

  virtual void enterPropertyListPath(SparqlAutomaticParser::PropertyListPathContext * /*ctx*/) override { }
  virtual void exitPropertyListPath(SparqlAutomaticParser::PropertyListPathContext * /*ctx*/) override { }

  virtual void enterPropertyListPathNotEmpty(SparqlAutomaticParser::PropertyListPathNotEmptyContext * /*ctx*/) override { }
  virtual void exitPropertyListPathNotEmpty(SparqlAutomaticParser::PropertyListPathNotEmptyContext * /*ctx*/) override { }

  virtual void enterVerbPath(SparqlAutomaticParser::VerbPathContext * /*ctx*/) override { }
  virtual void exitVerbPath(SparqlAutomaticParser::VerbPathContext * /*ctx*/) override { }

  virtual void enterVerbSimple(SparqlAutomaticParser::VerbSimpleContext * /*ctx*/) override { }
  virtual void exitVerbSimple(SparqlAutomaticParser::VerbSimpleContext * /*ctx*/) override { }

  virtual void enterTupleWithoutPath(SparqlAutomaticParser::TupleWithoutPathContext * /*ctx*/) override { }
  virtual void exitTupleWithoutPath(SparqlAutomaticParser::TupleWithoutPathContext * /*ctx*/) override { }

  virtual void enterTupleWithPath(SparqlAutomaticParser::TupleWithPathContext * /*ctx*/) override { }
  virtual void exitTupleWithPath(SparqlAutomaticParser::TupleWithPathContext * /*ctx*/) override { }

  virtual void enterVerbPathOrSimple(SparqlAutomaticParser::VerbPathOrSimpleContext * /*ctx*/) override { }
  virtual void exitVerbPathOrSimple(SparqlAutomaticParser::VerbPathOrSimpleContext * /*ctx*/) override { }

  virtual void enterObjectListPath(SparqlAutomaticParser::ObjectListPathContext * /*ctx*/) override { }
  virtual void exitObjectListPath(SparqlAutomaticParser::ObjectListPathContext * /*ctx*/) override { }

  virtual void enterObjectPath(SparqlAutomaticParser::ObjectPathContext * /*ctx*/) override { }
  virtual void exitObjectPath(SparqlAutomaticParser::ObjectPathContext * /*ctx*/) override { }

  virtual void enterPath(SparqlAutomaticParser::PathContext * /*ctx*/) override { }
  virtual void exitPath(SparqlAutomaticParser::PathContext * /*ctx*/) override { }

  virtual void enterPathAlternative(SparqlAutomaticParser::PathAlternativeContext * /*ctx*/) override { }
  virtual void exitPathAlternative(SparqlAutomaticParser::PathAlternativeContext * /*ctx*/) override { }

  virtual void enterPathSequence(SparqlAutomaticParser::PathSequenceContext * /*ctx*/) override { }
  virtual void exitPathSequence(SparqlAutomaticParser::PathSequenceContext * /*ctx*/) override { }

  virtual void enterPathElt(SparqlAutomaticParser::PathEltContext * /*ctx*/) override { }
  virtual void exitPathElt(SparqlAutomaticParser::PathEltContext * /*ctx*/) override { }

  virtual void enterPathEltOrInverse(SparqlAutomaticParser::PathEltOrInverseContext * /*ctx*/) override { }
  virtual void exitPathEltOrInverse(SparqlAutomaticParser::PathEltOrInverseContext * /*ctx*/) override { }

  virtual void enterPathMod(SparqlAutomaticParser::PathModContext * /*ctx*/) override { }
  virtual void exitPathMod(SparqlAutomaticParser::PathModContext * /*ctx*/) override { }

  virtual void enterStepsMin(SparqlAutomaticParser::StepsMinContext * /*ctx*/) override { }
  virtual void exitStepsMin(SparqlAutomaticParser::StepsMinContext * /*ctx*/) override { }

  virtual void enterStepsMax(SparqlAutomaticParser::StepsMaxContext * /*ctx*/) override { }
  virtual void exitStepsMax(SparqlAutomaticParser::StepsMaxContext * /*ctx*/) override { }

  virtual void enterPathPrimary(SparqlAutomaticParser::PathPrimaryContext * /*ctx*/) override { }
  virtual void exitPathPrimary(SparqlAutomaticParser::PathPrimaryContext * /*ctx*/) override { }

  virtual void enterPathNegatedPropertySet(SparqlAutomaticParser::PathNegatedPropertySetContext * /*ctx*/) override { }
  virtual void exitPathNegatedPropertySet(SparqlAutomaticParser::PathNegatedPropertySetContext * /*ctx*/) override { }

  virtual void enterPathOneInPropertySet(SparqlAutomaticParser::PathOneInPropertySetContext * /*ctx*/) override { }
  virtual void exitPathOneInPropertySet(SparqlAutomaticParser::PathOneInPropertySetContext * /*ctx*/) override { }

  virtual void enterInteger(SparqlAutomaticParser::IntegerContext * /*ctx*/) override { }
  virtual void exitInteger(SparqlAutomaticParser::IntegerContext * /*ctx*/) override { }

  virtual void enterTriplesNode(SparqlAutomaticParser::TriplesNodeContext * /*ctx*/) override { }
  virtual void exitTriplesNode(SparqlAutomaticParser::TriplesNodeContext * /*ctx*/) override { }

  virtual void enterBlankNodePropertyList(SparqlAutomaticParser::BlankNodePropertyListContext * /*ctx*/) override { }
  virtual void exitBlankNodePropertyList(SparqlAutomaticParser::BlankNodePropertyListContext * /*ctx*/) override { }

  virtual void enterTriplesNodePath(SparqlAutomaticParser::TriplesNodePathContext * /*ctx*/) override { }
  virtual void exitTriplesNodePath(SparqlAutomaticParser::TriplesNodePathContext * /*ctx*/) override { }

  virtual void enterBlankNodePropertyListPath(SparqlAutomaticParser::BlankNodePropertyListPathContext * /*ctx*/) override { }
  virtual void exitBlankNodePropertyListPath(SparqlAutomaticParser::BlankNodePropertyListPathContext * /*ctx*/) override { }

  virtual void enterCollection(SparqlAutomaticParser::CollectionContext * /*ctx*/) override { }
  virtual void exitCollection(SparqlAutomaticParser::CollectionContext * /*ctx*/) override { }

  virtual void enterCollectionPath(SparqlAutomaticParser::CollectionPathContext * /*ctx*/) override { }
  virtual void exitCollectionPath(SparqlAutomaticParser::CollectionPathContext * /*ctx*/) override { }

  virtual void enterGraphNode(SparqlAutomaticParser::GraphNodeContext * /*ctx*/) override { }
  virtual void exitGraphNode(SparqlAutomaticParser::GraphNodeContext * /*ctx*/) override { }

  virtual void enterGraphNodePath(SparqlAutomaticParser::GraphNodePathContext * /*ctx*/) override { }
  virtual void exitGraphNodePath(SparqlAutomaticParser::GraphNodePathContext * /*ctx*/) override { }

  virtual void enterVarOrTerm(SparqlAutomaticParser::VarOrTermContext * /*ctx*/) override { }
  virtual void exitVarOrTerm(SparqlAutomaticParser::VarOrTermContext * /*ctx*/) override { }

  virtual void enterVarOrIri(SparqlAutomaticParser::VarOrIriContext * /*ctx*/) override { }
  virtual void exitVarOrIri(SparqlAutomaticParser::VarOrIriContext * /*ctx*/) override { }

  virtual void enterVar(SparqlAutomaticParser::VarContext * /*ctx*/) override { }
  virtual void exitVar(SparqlAutomaticParser::VarContext * /*ctx*/) override { }

  virtual void enterGraphTerm(SparqlAutomaticParser::GraphTermContext * /*ctx*/) override { }
  virtual void exitGraphTerm(SparqlAutomaticParser::GraphTermContext * /*ctx*/) override { }

  virtual void enterExpression(SparqlAutomaticParser::ExpressionContext * /*ctx*/) override { }
  virtual void exitExpression(SparqlAutomaticParser::ExpressionContext * /*ctx*/) override { }

  virtual void enterConditionalOrExpression(SparqlAutomaticParser::ConditionalOrExpressionContext * /*ctx*/) override { }
  virtual void exitConditionalOrExpression(SparqlAutomaticParser::ConditionalOrExpressionContext * /*ctx*/) override { }

  virtual void enterConditionalAndExpression(SparqlAutomaticParser::ConditionalAndExpressionContext * /*ctx*/) override { }
  virtual void exitConditionalAndExpression(SparqlAutomaticParser::ConditionalAndExpressionContext * /*ctx*/) override { }

  virtual void enterValueLogical(SparqlAutomaticParser::ValueLogicalContext * /*ctx*/) override { }
  virtual void exitValueLogical(SparqlAutomaticParser::ValueLogicalContext * /*ctx*/) override { }

  virtual void enterRelationalExpression(SparqlAutomaticParser::RelationalExpressionContext * /*ctx*/) override { }
  virtual void exitRelationalExpression(SparqlAutomaticParser::RelationalExpressionContext * /*ctx*/) override { }

  virtual void enterNumericExpression(SparqlAutomaticParser::NumericExpressionContext * /*ctx*/) override { }
  virtual void exitNumericExpression(SparqlAutomaticParser::NumericExpressionContext * /*ctx*/) override { }

  virtual void enterAdditiveExpression(SparqlAutomaticParser::AdditiveExpressionContext * /*ctx*/) override { }
  virtual void exitAdditiveExpression(SparqlAutomaticParser::AdditiveExpressionContext * /*ctx*/) override { }

  virtual void enterMultiplicativeExpressionWithSign(SparqlAutomaticParser::MultiplicativeExpressionWithSignContext * /*ctx*/) override { }
  virtual void exitMultiplicativeExpressionWithSign(SparqlAutomaticParser::MultiplicativeExpressionWithSignContext * /*ctx*/) override { }

  virtual void enterPlusSubexpression(SparqlAutomaticParser::PlusSubexpressionContext * /*ctx*/) override { }
  virtual void exitPlusSubexpression(SparqlAutomaticParser::PlusSubexpressionContext * /*ctx*/) override { }

  virtual void enterMinusSubexpression(SparqlAutomaticParser::MinusSubexpressionContext * /*ctx*/) override { }
  virtual void exitMinusSubexpression(SparqlAutomaticParser::MinusSubexpressionContext * /*ctx*/) override { }

  virtual void enterMultiplicativeExpressionWithLeadingSignButNoSpace(SparqlAutomaticParser::MultiplicativeExpressionWithLeadingSignButNoSpaceContext * /*ctx*/) override { }
  virtual void exitMultiplicativeExpressionWithLeadingSignButNoSpace(SparqlAutomaticParser::MultiplicativeExpressionWithLeadingSignButNoSpaceContext * /*ctx*/) override { }

  virtual void enterMultiplicativeExpression(SparqlAutomaticParser::MultiplicativeExpressionContext * /*ctx*/) override { }
  virtual void exitMultiplicativeExpression(SparqlAutomaticParser::MultiplicativeExpressionContext * /*ctx*/) override { }

  virtual void enterMultiplyOrDivideExpression(SparqlAutomaticParser::MultiplyOrDivideExpressionContext * /*ctx*/) override { }
  virtual void exitMultiplyOrDivideExpression(SparqlAutomaticParser::MultiplyOrDivideExpressionContext * /*ctx*/) override { }

  virtual void enterMultiplyExpression(SparqlAutomaticParser::MultiplyExpressionContext * /*ctx*/) override { }
  virtual void exitMultiplyExpression(SparqlAutomaticParser::MultiplyExpressionContext * /*ctx*/) override { }

  virtual void enterDivideExpression(SparqlAutomaticParser::DivideExpressionContext * /*ctx*/) override { }
  virtual void exitDivideExpression(SparqlAutomaticParser::DivideExpressionContext * /*ctx*/) override { }

  virtual void enterUnaryExpression(SparqlAutomaticParser::UnaryExpressionContext * /*ctx*/) override { }
  virtual void exitUnaryExpression(SparqlAutomaticParser::UnaryExpressionContext * /*ctx*/) override { }

  virtual void enterPrimaryExpression(SparqlAutomaticParser::PrimaryExpressionContext * /*ctx*/) override { }
  virtual void exitPrimaryExpression(SparqlAutomaticParser::PrimaryExpressionContext * /*ctx*/) override { }

  virtual void enterBrackettedExpression(SparqlAutomaticParser::BrackettedExpressionContext * /*ctx*/) override { }
  virtual void exitBrackettedExpression(SparqlAutomaticParser::BrackettedExpressionContext * /*ctx*/) override { }

  virtual void enterBuiltInCall(SparqlAutomaticParser::BuiltInCallContext * /*ctx*/) override { }
  virtual void exitBuiltInCall(SparqlAutomaticParser::BuiltInCallContext * /*ctx*/) override { }

  virtual void enterRegexExpression(SparqlAutomaticParser::RegexExpressionContext * /*ctx*/) override { }
  virtual void exitRegexExpression(SparqlAutomaticParser::RegexExpressionContext * /*ctx*/) override { }

  virtual void enterLangExpression(SparqlAutomaticParser::LangExpressionContext * /*ctx*/) override { }
  virtual void exitLangExpression(SparqlAutomaticParser::LangExpressionContext * /*ctx*/) override { }

  virtual void enterSubstringExpression(SparqlAutomaticParser::SubstringExpressionContext * /*ctx*/) override { }
  virtual void exitSubstringExpression(SparqlAutomaticParser::SubstringExpressionContext * /*ctx*/) override { }

  virtual void enterStrReplaceExpression(SparqlAutomaticParser::StrReplaceExpressionContext * /*ctx*/) override { }
  virtual void exitStrReplaceExpression(SparqlAutomaticParser::StrReplaceExpressionContext * /*ctx*/) override { }

  virtual void enterExistsFunc(SparqlAutomaticParser::ExistsFuncContext * /*ctx*/) override { }
  virtual void exitExistsFunc(SparqlAutomaticParser::ExistsFuncContext * /*ctx*/) override { }

  virtual void enterNotExistsFunc(SparqlAutomaticParser::NotExistsFuncContext * /*ctx*/) override { }
  virtual void exitNotExistsFunc(SparqlAutomaticParser::NotExistsFuncContext * /*ctx*/) override { }

  virtual void enterAggregate(SparqlAutomaticParser::AggregateContext * /*ctx*/) override { }
  virtual void exitAggregate(SparqlAutomaticParser::AggregateContext * /*ctx*/) override { }

  virtual void enterIriOrFunction(SparqlAutomaticParser::IriOrFunctionContext * /*ctx*/) override { }
  virtual void exitIriOrFunction(SparqlAutomaticParser::IriOrFunctionContext * /*ctx*/) override { }

  virtual void enterRdfLiteral(SparqlAutomaticParser::RdfLiteralContext * /*ctx*/) override { }
  virtual void exitRdfLiteral(SparqlAutomaticParser::RdfLiteralContext * /*ctx*/) override { }

  virtual void enterNumericLiteral(SparqlAutomaticParser::NumericLiteralContext * /*ctx*/) override { }
  virtual void exitNumericLiteral(SparqlAutomaticParser::NumericLiteralContext * /*ctx*/) override { }

  virtual void enterNumericLiteralUnsigned(SparqlAutomaticParser::NumericLiteralUnsignedContext * /*ctx*/) override { }
  virtual void exitNumericLiteralUnsigned(SparqlAutomaticParser::NumericLiteralUnsignedContext * /*ctx*/) override { }

  virtual void enterNumericLiteralPositive(SparqlAutomaticParser::NumericLiteralPositiveContext * /*ctx*/) override { }
  virtual void exitNumericLiteralPositive(SparqlAutomaticParser::NumericLiteralPositiveContext * /*ctx*/) override { }

  virtual void enterNumericLiteralNegative(SparqlAutomaticParser::NumericLiteralNegativeContext * /*ctx*/) override { }
  virtual void exitNumericLiteralNegative(SparqlAutomaticParser::NumericLiteralNegativeContext * /*ctx*/) override { }

  virtual void enterBooleanLiteral(SparqlAutomaticParser::BooleanLiteralContext * /*ctx*/) override { }
  virtual void exitBooleanLiteral(SparqlAutomaticParser::BooleanLiteralContext * /*ctx*/) override { }

  virtual void enterString(SparqlAutomaticParser::StringContext * /*ctx*/) override { }
  virtual void exitString(SparqlAutomaticParser::StringContext * /*ctx*/) override { }

  virtual void enterIri(SparqlAutomaticParser::IriContext * /*ctx*/) override { }
  virtual void exitIri(SparqlAutomaticParser::IriContext * /*ctx*/) override { }

  virtual void enterPrefixedName(SparqlAutomaticParser::PrefixedNameContext * /*ctx*/) override { }
  virtual void exitPrefixedName(SparqlAutomaticParser::PrefixedNameContext * /*ctx*/) override { }

  virtual void enterBlankNode(SparqlAutomaticParser::BlankNodeContext * /*ctx*/) override { }
  virtual void exitBlankNode(SparqlAutomaticParser::BlankNodeContext * /*ctx*/) override { }

  virtual void enterIriref(SparqlAutomaticParser::IrirefContext * /*ctx*/) override { }
  virtual void exitIriref(SparqlAutomaticParser::IrirefContext * /*ctx*/) override { }

  virtual void enterPnameLn(SparqlAutomaticParser::PnameLnContext * /*ctx*/) override { }
  virtual void exitPnameLn(SparqlAutomaticParser::PnameLnContext * /*ctx*/) override { }

  virtual void enterPnameNs(SparqlAutomaticParser::PnameNsContext * /*ctx*/) override { }
  virtual void exitPnameNs(SparqlAutomaticParser::PnameNsContext * /*ctx*/) override { }


  virtual void enterEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void exitEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void visitTerminal(antlr4::tree::TerminalNode * /*node*/) override { }
  virtual void visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override { }

};

