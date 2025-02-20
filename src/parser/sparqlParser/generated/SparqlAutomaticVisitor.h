
// Generated from SparqlAutomatic.g4 by ANTLR 4.11.1

#pragma once


#include "antlr4-runtime.h"
#include "SparqlAutomaticParser.h"



/**
 * This class defines an abstract visitor for a parse tree
 * produced by SparqlAutomaticParser.
 */
class  SparqlAutomaticVisitor : public antlr4::tree::AbstractParseTreeVisitor {
public:

  /**
   * Visit parse trees produced by SparqlAutomaticParser.
   */
    virtual std::any visitQueryOrUpdate(SparqlAutomaticParser::QueryOrUpdateContext *context) = 0;

    virtual std::any visitQuery(SparqlAutomaticParser::QueryContext *context) = 0;

    virtual std::any visitPrologue(SparqlAutomaticParser::PrologueContext *context) = 0;

    virtual std::any visitBaseDecl(SparqlAutomaticParser::BaseDeclContext *context) = 0;

    virtual std::any visitPrefixDecl(SparqlAutomaticParser::PrefixDeclContext *context) = 0;

    virtual std::any visitSelectQuery(SparqlAutomaticParser::SelectQueryContext *context) = 0;

    virtual std::any visitSubSelect(SparqlAutomaticParser::SubSelectContext *context) = 0;

    virtual std::any visitSelectClause(SparqlAutomaticParser::SelectClauseContext *context) = 0;

    virtual std::any visitVarOrAlias(SparqlAutomaticParser::VarOrAliasContext *context) = 0;

    virtual std::any visitAlias(SparqlAutomaticParser::AliasContext *context) = 0;

    virtual std::any visitAliasWithoutBrackets(SparqlAutomaticParser::AliasWithoutBracketsContext *context) = 0;

    virtual std::any visitConstructQuery(SparqlAutomaticParser::ConstructQueryContext *context) = 0;

    virtual std::any visitDescribeQuery(SparqlAutomaticParser::DescribeQueryContext *context) = 0;

    virtual std::any visitAskQuery(SparqlAutomaticParser::AskQueryContext *context) = 0;

    virtual std::any visitDatasetClause(SparqlAutomaticParser::DatasetClauseContext *context) = 0;

    virtual std::any visitDefaultGraphClause(SparqlAutomaticParser::DefaultGraphClauseContext *context) = 0;

    virtual std::any visitNamedGraphClause(SparqlAutomaticParser::NamedGraphClauseContext *context) = 0;

    virtual std::any visitSourceSelector(SparqlAutomaticParser::SourceSelectorContext *context) = 0;

    virtual std::any visitWhereClause(SparqlAutomaticParser::WhereClauseContext *context) = 0;

    virtual std::any visitSolutionModifier(SparqlAutomaticParser::SolutionModifierContext *context) = 0;

    virtual std::any visitGroupClause(SparqlAutomaticParser::GroupClauseContext *context) = 0;

    virtual std::any visitGroupCondition(SparqlAutomaticParser::GroupConditionContext *context) = 0;

    virtual std::any visitHavingClause(SparqlAutomaticParser::HavingClauseContext *context) = 0;

    virtual std::any visitHavingCondition(SparqlAutomaticParser::HavingConditionContext *context) = 0;

    virtual std::any visitOrderClause(SparqlAutomaticParser::OrderClauseContext *context) = 0;

    virtual std::any visitOrderCondition(SparqlAutomaticParser::OrderConditionContext *context) = 0;

    virtual std::any visitLimitOffsetClauses(SparqlAutomaticParser::LimitOffsetClausesContext *context) = 0;

    virtual std::any visitLimitClause(SparqlAutomaticParser::LimitClauseContext *context) = 0;

    virtual std::any visitOffsetClause(SparqlAutomaticParser::OffsetClauseContext *context) = 0;

    virtual std::any visitTextLimitClause(SparqlAutomaticParser::TextLimitClauseContext *context) = 0;

    virtual std::any visitValuesClause(SparqlAutomaticParser::ValuesClauseContext *context) = 0;

    virtual std::any visitUpdate(SparqlAutomaticParser::UpdateContext *context) = 0;

    virtual std::any visitUpdate1(SparqlAutomaticParser::Update1Context *context) = 0;

    virtual std::any visitLoad(SparqlAutomaticParser::LoadContext *context) = 0;

    virtual std::any visitClear(SparqlAutomaticParser::ClearContext *context) = 0;

    virtual std::any visitDrop(SparqlAutomaticParser::DropContext *context) = 0;

    virtual std::any visitCreate(SparqlAutomaticParser::CreateContext *context) = 0;

    virtual std::any visitAdd(SparqlAutomaticParser::AddContext *context) = 0;

    virtual std::any visitMove(SparqlAutomaticParser::MoveContext *context) = 0;

    virtual std::any visitCopy(SparqlAutomaticParser::CopyContext *context) = 0;

    virtual std::any visitInsertData(SparqlAutomaticParser::InsertDataContext *context) = 0;

    virtual std::any visitDeleteData(SparqlAutomaticParser::DeleteDataContext *context) = 0;

    virtual std::any visitDeleteWhere(SparqlAutomaticParser::DeleteWhereContext *context) = 0;

    virtual std::any visitModify(SparqlAutomaticParser::ModifyContext *context) = 0;

    virtual std::any visitDeleteClause(SparqlAutomaticParser::DeleteClauseContext *context) = 0;

    virtual std::any visitInsertClause(SparqlAutomaticParser::InsertClauseContext *context) = 0;

    virtual std::any visitUsingClause(SparqlAutomaticParser::UsingClauseContext *context) = 0;

    virtual std::any visitGraphOrDefault(SparqlAutomaticParser::GraphOrDefaultContext *context) = 0;

    virtual std::any visitGraphRef(SparqlAutomaticParser::GraphRefContext *context) = 0;

    virtual std::any visitGraphRefAll(SparqlAutomaticParser::GraphRefAllContext *context) = 0;

    virtual std::any visitQuadPattern(SparqlAutomaticParser::QuadPatternContext *context) = 0;

    virtual std::any visitQuadData(SparqlAutomaticParser::QuadDataContext *context) = 0;

    virtual std::any visitQuads(SparqlAutomaticParser::QuadsContext *context) = 0;

    virtual std::any visitQuadsNotTriples(SparqlAutomaticParser::QuadsNotTriplesContext *context) = 0;

    virtual std::any visitTriplesTemplate(SparqlAutomaticParser::TriplesTemplateContext *context) = 0;

    virtual std::any visitGroupGraphPattern(SparqlAutomaticParser::GroupGraphPatternContext *context) = 0;

    virtual std::any visitGroupGraphPatternSub(SparqlAutomaticParser::GroupGraphPatternSubContext *context) = 0;

    virtual std::any visitGraphPatternNotTriplesAndMaybeTriples(SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext *context) = 0;

    virtual std::any visitTriplesBlock(SparqlAutomaticParser::TriplesBlockContext *context) = 0;

    virtual std::any visitGraphPatternNotTriples(SparqlAutomaticParser::GraphPatternNotTriplesContext *context) = 0;

    virtual std::any visitOptionalGraphPattern(SparqlAutomaticParser::OptionalGraphPatternContext *context) = 0;

    virtual std::any visitGraphGraphPattern(SparqlAutomaticParser::GraphGraphPatternContext *context) = 0;

    virtual std::any visitServiceGraphPattern(SparqlAutomaticParser::ServiceGraphPatternContext *context) = 0;

    virtual std::any visitBind(SparqlAutomaticParser::BindContext *context) = 0;

    virtual std::any visitInlineData(SparqlAutomaticParser::InlineDataContext *context) = 0;

    virtual std::any visitDataBlock(SparqlAutomaticParser::DataBlockContext *context) = 0;

    virtual std::any visitInlineDataOneVar(SparqlAutomaticParser::InlineDataOneVarContext *context) = 0;

    virtual std::any visitInlineDataFull(SparqlAutomaticParser::InlineDataFullContext *context) = 0;

    virtual std::any visitDataBlockSingle(SparqlAutomaticParser::DataBlockSingleContext *context) = 0;

    virtual std::any visitDataBlockValue(SparqlAutomaticParser::DataBlockValueContext *context) = 0;

    virtual std::any visitMinusGraphPattern(SparqlAutomaticParser::MinusGraphPatternContext *context) = 0;

    virtual std::any visitGroupOrUnionGraphPattern(SparqlAutomaticParser::GroupOrUnionGraphPatternContext *context) = 0;

    virtual std::any visitFilterR(SparqlAutomaticParser::FilterRContext *context) = 0;

    virtual std::any visitConstraint(SparqlAutomaticParser::ConstraintContext *context) = 0;

    virtual std::any visitFunctionCall(SparqlAutomaticParser::FunctionCallContext *context) = 0;

    virtual std::any visitArgList(SparqlAutomaticParser::ArgListContext *context) = 0;

    virtual std::any visitExpressionList(SparqlAutomaticParser::ExpressionListContext *context) = 0;

    virtual std::any visitConstructTemplate(SparqlAutomaticParser::ConstructTemplateContext *context) = 0;

    virtual std::any visitConstructTriples(SparqlAutomaticParser::ConstructTriplesContext *context) = 0;

    virtual std::any visitTriplesSameSubject(SparqlAutomaticParser::TriplesSameSubjectContext *context) = 0;

    virtual std::any visitPropertyList(SparqlAutomaticParser::PropertyListContext *context) = 0;

    virtual std::any visitPropertyListNotEmpty(SparqlAutomaticParser::PropertyListNotEmptyContext *context) = 0;

    virtual std::any visitVerb(SparqlAutomaticParser::VerbContext *context) = 0;

    virtual std::any visitObjectList(SparqlAutomaticParser::ObjectListContext *context) = 0;

    virtual std::any visitObjectR(SparqlAutomaticParser::ObjectRContext *context) = 0;

    virtual std::any visitTriplesSameSubjectPath(SparqlAutomaticParser::TriplesSameSubjectPathContext *context) = 0;

    virtual std::any visitPropertyListPath(SparqlAutomaticParser::PropertyListPathContext *context) = 0;

    virtual std::any visitPropertyListPathNotEmpty(SparqlAutomaticParser::PropertyListPathNotEmptyContext *context) = 0;

    virtual std::any visitVerbPath(SparqlAutomaticParser::VerbPathContext *context) = 0;

    virtual std::any visitVerbSimple(SparqlAutomaticParser::VerbSimpleContext *context) = 0;

    virtual std::any visitTupleWithoutPath(SparqlAutomaticParser::TupleWithoutPathContext *context) = 0;

    virtual std::any visitTupleWithPath(SparqlAutomaticParser::TupleWithPathContext *context) = 0;

    virtual std::any visitVerbPathOrSimple(SparqlAutomaticParser::VerbPathOrSimpleContext *context) = 0;

    virtual std::any visitObjectListPath(SparqlAutomaticParser::ObjectListPathContext *context) = 0;

    virtual std::any visitObjectPath(SparqlAutomaticParser::ObjectPathContext *context) = 0;

    virtual std::any visitPath(SparqlAutomaticParser::PathContext *context) = 0;

    virtual std::any visitPathAlternative(SparqlAutomaticParser::PathAlternativeContext *context) = 0;

    virtual std::any visitPathSequence(SparqlAutomaticParser::PathSequenceContext *context) = 0;

    virtual std::any visitPathElt(SparqlAutomaticParser::PathEltContext *context) = 0;

    virtual std::any visitPathEltOrInverse(SparqlAutomaticParser::PathEltOrInverseContext *context) = 0;

    virtual std::any visitPathMod(SparqlAutomaticParser::PathModContext *context) = 0;

    virtual std::any visitStepsMin(SparqlAutomaticParser::StepsMinContext *context) = 0;

    virtual std::any visitStepsMax(SparqlAutomaticParser::StepsMaxContext *context) = 0;

    virtual std::any visitPathPrimary(SparqlAutomaticParser::PathPrimaryContext *context) = 0;

    virtual std::any visitPathNegatedPropertySet(SparqlAutomaticParser::PathNegatedPropertySetContext *context) = 0;

    virtual std::any visitPathOneInPropertySet(SparqlAutomaticParser::PathOneInPropertySetContext *context) = 0;

    virtual std::any visitInteger(SparqlAutomaticParser::IntegerContext *context) = 0;

    virtual std::any visitTriplesNode(SparqlAutomaticParser::TriplesNodeContext *context) = 0;

    virtual std::any visitBlankNodePropertyList(SparqlAutomaticParser::BlankNodePropertyListContext *context) = 0;

    virtual std::any visitTriplesNodePath(SparqlAutomaticParser::TriplesNodePathContext *context) = 0;

    virtual std::any visitBlankNodePropertyListPath(SparqlAutomaticParser::BlankNodePropertyListPathContext *context) = 0;

    virtual std::any visitCollection(SparqlAutomaticParser::CollectionContext *context) = 0;

    virtual std::any visitCollectionPath(SparqlAutomaticParser::CollectionPathContext *context) = 0;

    virtual std::any visitGraphNode(SparqlAutomaticParser::GraphNodeContext *context) = 0;

    virtual std::any visitGraphNodePath(SparqlAutomaticParser::GraphNodePathContext *context) = 0;

    virtual std::any visitVarOrTerm(SparqlAutomaticParser::VarOrTermContext *context) = 0;

    virtual std::any visitVarOrIri(SparqlAutomaticParser::VarOrIriContext *context) = 0;

    virtual std::any visitVar(SparqlAutomaticParser::VarContext *context) = 0;

    virtual std::any visitGraphTerm(SparqlAutomaticParser::GraphTermContext *context) = 0;

    virtual std::any visitExpression(SparqlAutomaticParser::ExpressionContext *context) = 0;

    virtual std::any visitConditionalOrExpression(SparqlAutomaticParser::ConditionalOrExpressionContext *context) = 0;

    virtual std::any visitConditionalAndExpression(SparqlAutomaticParser::ConditionalAndExpressionContext *context) = 0;

    virtual std::any visitValueLogical(SparqlAutomaticParser::ValueLogicalContext *context) = 0;

    virtual std::any visitRelationalExpression(SparqlAutomaticParser::RelationalExpressionContext *context) = 0;

    virtual std::any visitNumericExpression(SparqlAutomaticParser::NumericExpressionContext *context) = 0;

    virtual std::any visitAdditiveExpression(SparqlAutomaticParser::AdditiveExpressionContext *context) = 0;

    virtual std::any visitMultiplicativeExpressionWithSign(SparqlAutomaticParser::MultiplicativeExpressionWithSignContext *context) = 0;

    virtual std::any visitPlusSubexpression(SparqlAutomaticParser::PlusSubexpressionContext *context) = 0;

    virtual std::any visitMinusSubexpression(SparqlAutomaticParser::MinusSubexpressionContext *context) = 0;

    virtual std::any visitMultiplicativeExpressionWithLeadingSignButNoSpace(SparqlAutomaticParser::MultiplicativeExpressionWithLeadingSignButNoSpaceContext *context) = 0;

    virtual std::any visitMultiplicativeExpression(SparqlAutomaticParser::MultiplicativeExpressionContext *context) = 0;

    virtual std::any visitMultiplyOrDivideExpression(SparqlAutomaticParser::MultiplyOrDivideExpressionContext *context) = 0;

    virtual std::any visitMultiplyExpression(SparqlAutomaticParser::MultiplyExpressionContext *context) = 0;

    virtual std::any visitDivideExpression(SparqlAutomaticParser::DivideExpressionContext *context) = 0;

    virtual std::any visitUnaryExpression(SparqlAutomaticParser::UnaryExpressionContext *context) = 0;

    virtual std::any visitPrimaryExpression(SparqlAutomaticParser::PrimaryExpressionContext *context) = 0;

    virtual std::any visitBrackettedExpression(SparqlAutomaticParser::BrackettedExpressionContext *context) = 0;

    virtual std::any visitBuiltInCall(SparqlAutomaticParser::BuiltInCallContext *context) = 0;

    virtual std::any visitRegexExpression(SparqlAutomaticParser::RegexExpressionContext *context) = 0;

    virtual std::any visitLangExpression(SparqlAutomaticParser::LangExpressionContext *context) = 0;

    virtual std::any visitSubstringExpression(SparqlAutomaticParser::SubstringExpressionContext *context) = 0;

    virtual std::any visitStrReplaceExpression(SparqlAutomaticParser::StrReplaceExpressionContext *context) = 0;

    virtual std::any visitExistsFunc(SparqlAutomaticParser::ExistsFuncContext *context) = 0;

    virtual std::any visitNotExistsFunc(SparqlAutomaticParser::NotExistsFuncContext *context) = 0;

    virtual std::any visitAggregate(SparqlAutomaticParser::AggregateContext *context) = 0;

    virtual std::any visitIriOrFunction(SparqlAutomaticParser::IriOrFunctionContext *context) = 0;

    virtual std::any visitRdfLiteral(SparqlAutomaticParser::RdfLiteralContext *context) = 0;

    virtual std::any visitNumericLiteral(SparqlAutomaticParser::NumericLiteralContext *context) = 0;

    virtual std::any visitNumericLiteralUnsigned(SparqlAutomaticParser::NumericLiteralUnsignedContext *context) = 0;

    virtual std::any visitNumericLiteralPositive(SparqlAutomaticParser::NumericLiteralPositiveContext *context) = 0;

    virtual std::any visitNumericLiteralNegative(SparqlAutomaticParser::NumericLiteralNegativeContext *context) = 0;

    virtual std::any visitBooleanLiteral(SparqlAutomaticParser::BooleanLiteralContext *context) = 0;

    virtual std::any visitString(SparqlAutomaticParser::StringContext *context) = 0;

    virtual std::any visitIri(SparqlAutomaticParser::IriContext *context) = 0;

    virtual std::any visitPrefixedName(SparqlAutomaticParser::PrefixedNameContext *context) = 0;

    virtual std::any visitBlankNode(SparqlAutomaticParser::BlankNodeContext *context) = 0;

    virtual std::any visitIriref(SparqlAutomaticParser::IrirefContext *context) = 0;

    virtual std::any visitPnameLn(SparqlAutomaticParser::PnameLnContext *context) = 0;

    virtual std::any visitPnameNs(SparqlAutomaticParser::PnameNsContext *context) = 0;


};

