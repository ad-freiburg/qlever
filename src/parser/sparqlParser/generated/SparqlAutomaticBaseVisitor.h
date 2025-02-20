
// Generated from SparqlAutomatic.g4 by ANTLR 4.11.1

#pragma once


#include "antlr4-runtime.h"
#include "SparqlAutomaticVisitor.h"


/**
 * This class provides an empty implementation of SparqlAutomaticVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the available methods.
 */
class  SparqlAutomaticBaseVisitor : public SparqlAutomaticVisitor {
public:

  virtual std::any visitQueryOrUpdate(SparqlAutomaticParser::QueryOrUpdateContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuery(SparqlAutomaticParser::QueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPrologue(SparqlAutomaticParser::PrologueContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBaseDecl(SparqlAutomaticParser::BaseDeclContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPrefixDecl(SparqlAutomaticParser::PrefixDeclContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSelectQuery(SparqlAutomaticParser::SelectQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSubSelect(SparqlAutomaticParser::SubSelectContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSelectClause(SparqlAutomaticParser::SelectClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVarOrAlias(SparqlAutomaticParser::VarOrAliasContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAlias(SparqlAutomaticParser::AliasContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAliasWithoutBrackets(SparqlAutomaticParser::AliasWithoutBracketsContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstructQuery(SparqlAutomaticParser::ConstructQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDescribeQuery(SparqlAutomaticParser::DescribeQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAskQuery(SparqlAutomaticParser::AskQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDatasetClause(SparqlAutomaticParser::DatasetClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDefaultGraphClause(SparqlAutomaticParser::DefaultGraphClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamedGraphClause(SparqlAutomaticParser::NamedGraphClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSourceSelector(SparqlAutomaticParser::SourceSelectorContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWhereClause(SparqlAutomaticParser::WhereClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSolutionModifier(SparqlAutomaticParser::SolutionModifierContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupClause(SparqlAutomaticParser::GroupClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupCondition(SparqlAutomaticParser::GroupConditionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitHavingClause(SparqlAutomaticParser::HavingClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitHavingCondition(SparqlAutomaticParser::HavingConditionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOrderClause(SparqlAutomaticParser::OrderClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOrderCondition(SparqlAutomaticParser::OrderConditionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLimitOffsetClauses(SparqlAutomaticParser::LimitOffsetClausesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLimitClause(SparqlAutomaticParser::LimitClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOffsetClause(SparqlAutomaticParser::OffsetClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTextLimitClause(SparqlAutomaticParser::TextLimitClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitValuesClause(SparqlAutomaticParser::ValuesClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUpdate(SparqlAutomaticParser::UpdateContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUpdate1(SparqlAutomaticParser::Update1Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLoad(SparqlAutomaticParser::LoadContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitClear(SparqlAutomaticParser::ClearContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDrop(SparqlAutomaticParser::DropContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreate(SparqlAutomaticParser::CreateContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAdd(SparqlAutomaticParser::AddContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMove(SparqlAutomaticParser::MoveContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCopy(SparqlAutomaticParser::CopyContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInsertData(SparqlAutomaticParser::InsertDataContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDeleteData(SparqlAutomaticParser::DeleteDataContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDeleteWhere(SparqlAutomaticParser::DeleteWhereContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitModify(SparqlAutomaticParser::ModifyContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDeleteClause(SparqlAutomaticParser::DeleteClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInsertClause(SparqlAutomaticParser::InsertClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUsingClause(SparqlAutomaticParser::UsingClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGraphOrDefault(SparqlAutomaticParser::GraphOrDefaultContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGraphRef(SparqlAutomaticParser::GraphRefContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGraphRefAll(SparqlAutomaticParser::GraphRefAllContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuadPattern(SparqlAutomaticParser::QuadPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuadData(SparqlAutomaticParser::QuadDataContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuads(SparqlAutomaticParser::QuadsContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuadsNotTriples(SparqlAutomaticParser::QuadsNotTriplesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTriplesTemplate(SparqlAutomaticParser::TriplesTemplateContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupGraphPattern(SparqlAutomaticParser::GroupGraphPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupGraphPatternSub(SparqlAutomaticParser::GroupGraphPatternSubContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGraphPatternNotTriplesAndMaybeTriples(SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTriplesBlock(SparqlAutomaticParser::TriplesBlockContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGraphPatternNotTriples(SparqlAutomaticParser::GraphPatternNotTriplesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOptionalGraphPattern(SparqlAutomaticParser::OptionalGraphPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGraphGraphPattern(SparqlAutomaticParser::GraphGraphPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitServiceGraphPattern(SparqlAutomaticParser::ServiceGraphPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBind(SparqlAutomaticParser::BindContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInlineData(SparqlAutomaticParser::InlineDataContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDataBlock(SparqlAutomaticParser::DataBlockContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInlineDataOneVar(SparqlAutomaticParser::InlineDataOneVarContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInlineDataFull(SparqlAutomaticParser::InlineDataFullContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDataBlockSingle(SparqlAutomaticParser::DataBlockSingleContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDataBlockValue(SparqlAutomaticParser::DataBlockValueContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMinusGraphPattern(SparqlAutomaticParser::MinusGraphPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupOrUnionGraphPattern(SparqlAutomaticParser::GroupOrUnionGraphPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFilterR(SparqlAutomaticParser::FilterRContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstraint(SparqlAutomaticParser::ConstraintContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFunctionCall(SparqlAutomaticParser::FunctionCallContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitArgList(SparqlAutomaticParser::ArgListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpressionList(SparqlAutomaticParser::ExpressionListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstructTemplate(SparqlAutomaticParser::ConstructTemplateContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstructTriples(SparqlAutomaticParser::ConstructTriplesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTriplesSameSubject(SparqlAutomaticParser::TriplesSameSubjectContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPropertyList(SparqlAutomaticParser::PropertyListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPropertyListNotEmpty(SparqlAutomaticParser::PropertyListNotEmptyContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVerb(SparqlAutomaticParser::VerbContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitObjectList(SparqlAutomaticParser::ObjectListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitObjectR(SparqlAutomaticParser::ObjectRContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTriplesSameSubjectPath(SparqlAutomaticParser::TriplesSameSubjectPathContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPropertyListPath(SparqlAutomaticParser::PropertyListPathContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPropertyListPathNotEmpty(SparqlAutomaticParser::PropertyListPathNotEmptyContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVerbPath(SparqlAutomaticParser::VerbPathContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVerbSimple(SparqlAutomaticParser::VerbSimpleContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTupleWithoutPath(SparqlAutomaticParser::TupleWithoutPathContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTupleWithPath(SparqlAutomaticParser::TupleWithPathContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVerbPathOrSimple(SparqlAutomaticParser::VerbPathOrSimpleContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitObjectListPath(SparqlAutomaticParser::ObjectListPathContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitObjectPath(SparqlAutomaticParser::ObjectPathContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPath(SparqlAutomaticParser::PathContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPathAlternative(SparqlAutomaticParser::PathAlternativeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPathSequence(SparqlAutomaticParser::PathSequenceContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPathElt(SparqlAutomaticParser::PathEltContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPathEltOrInverse(SparqlAutomaticParser::PathEltOrInverseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPathMod(SparqlAutomaticParser::PathModContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStepsMin(SparqlAutomaticParser::StepsMinContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStepsMax(SparqlAutomaticParser::StepsMaxContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPathPrimary(SparqlAutomaticParser::PathPrimaryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPathNegatedPropertySet(SparqlAutomaticParser::PathNegatedPropertySetContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPathOneInPropertySet(SparqlAutomaticParser::PathOneInPropertySetContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInteger(SparqlAutomaticParser::IntegerContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTriplesNode(SparqlAutomaticParser::TriplesNodeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBlankNodePropertyList(SparqlAutomaticParser::BlankNodePropertyListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTriplesNodePath(SparqlAutomaticParser::TriplesNodePathContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBlankNodePropertyListPath(SparqlAutomaticParser::BlankNodePropertyListPathContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCollection(SparqlAutomaticParser::CollectionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCollectionPath(SparqlAutomaticParser::CollectionPathContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGraphNode(SparqlAutomaticParser::GraphNodeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGraphNodePath(SparqlAutomaticParser::GraphNodePathContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVarOrTerm(SparqlAutomaticParser::VarOrTermContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVarOrIri(SparqlAutomaticParser::VarOrIriContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVar(SparqlAutomaticParser::VarContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGraphTerm(SparqlAutomaticParser::GraphTermContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression(SparqlAutomaticParser::ExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConditionalOrExpression(SparqlAutomaticParser::ConditionalOrExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConditionalAndExpression(SparqlAutomaticParser::ConditionalAndExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitValueLogical(SparqlAutomaticParser::ValueLogicalContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRelationalExpression(SparqlAutomaticParser::RelationalExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNumericExpression(SparqlAutomaticParser::NumericExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAdditiveExpression(SparqlAutomaticParser::AdditiveExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultiplicativeExpressionWithSign(SparqlAutomaticParser::MultiplicativeExpressionWithSignContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPlusSubexpression(SparqlAutomaticParser::PlusSubexpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMinusSubexpression(SparqlAutomaticParser::MinusSubexpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultiplicativeExpressionWithLeadingSignButNoSpace(SparqlAutomaticParser::MultiplicativeExpressionWithLeadingSignButNoSpaceContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultiplicativeExpression(SparqlAutomaticParser::MultiplicativeExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultiplyOrDivideExpression(SparqlAutomaticParser::MultiplyOrDivideExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultiplyExpression(SparqlAutomaticParser::MultiplyExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDivideExpression(SparqlAutomaticParser::DivideExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnaryExpression(SparqlAutomaticParser::UnaryExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPrimaryExpression(SparqlAutomaticParser::PrimaryExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBrackettedExpression(SparqlAutomaticParser::BrackettedExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBuiltInCall(SparqlAutomaticParser::BuiltInCallContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRegexExpression(SparqlAutomaticParser::RegexExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLangExpression(SparqlAutomaticParser::LangExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSubstringExpression(SparqlAutomaticParser::SubstringExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStrReplaceExpression(SparqlAutomaticParser::StrReplaceExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExistsFunc(SparqlAutomaticParser::ExistsFuncContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNotExistsFunc(SparqlAutomaticParser::NotExistsFuncContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAggregate(SparqlAutomaticParser::AggregateContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIriOrFunction(SparqlAutomaticParser::IriOrFunctionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRdfLiteral(SparqlAutomaticParser::RdfLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNumericLiteral(SparqlAutomaticParser::NumericLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNumericLiteralUnsigned(SparqlAutomaticParser::NumericLiteralUnsignedContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNumericLiteralPositive(SparqlAutomaticParser::NumericLiteralPositiveContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNumericLiteralNegative(SparqlAutomaticParser::NumericLiteralNegativeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBooleanLiteral(SparqlAutomaticParser::BooleanLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitString(SparqlAutomaticParser::StringContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIri(SparqlAutomaticParser::IriContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPrefixedName(SparqlAutomaticParser::PrefixedNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBlankNode(SparqlAutomaticParser::BlankNodeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIriref(SparqlAutomaticParser::IrirefContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPnameLn(SparqlAutomaticParser::PnameLnContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPnameNs(SparqlAutomaticParser::PnameNsContext *ctx) override {
    return visitChildren(ctx);
  }


};

