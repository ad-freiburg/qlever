
// Generated from Sparql.g4 by ANTLR 4.9.2

#pragma once

#include "SparqlVisitor.h"
#include "antlr4-runtime.h"
#include "../RdfEscaping.h"
#include "../../util/HashMap.h"

/**
 * This class provides an empty implementation of SparqlVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the
 * available methods.
 */
class SparqlQleverVisitor : public SparqlVisitor {
 private:
 public:
  antlrcpp::Any visitQuery(SparqlParser::QueryContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPrologue(SparqlParser::PrologueContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBaseDecl(SparqlParser::BaseDeclContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPrefixDecl(SparqlParser::PrefixDeclContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSelectQuery(
      SparqlParser::SelectQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSubSelect(SparqlParser::SubSelectContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSelectClause(
      SparqlParser::SelectClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAlias(SparqlParser::AliasContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConstructQuery(
      SparqlParser::ConstructQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDescribeQuery(
      SparqlParser::DescribeQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAskQuery(SparqlParser::AskQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDatasetClause(
      SparqlParser::DatasetClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDefaultGraphClause(
      SparqlParser::DefaultGraphClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNamedGraphClause(
      SparqlParser::NamedGraphClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSourceSelector(
      SparqlParser::SourceSelectorContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitWhereClause(
      SparqlParser::WhereClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSolutionModifier(
      SparqlParser::SolutionModifierContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupClause(
      SparqlParser::GroupClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupCondition(
      SparqlParser::GroupConditionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitHavingClause(
      SparqlParser::HavingClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitHavingCondition(
      SparqlParser::HavingConditionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitOrderClause(
      SparqlParser::OrderClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitOrderCondition(
      SparqlParser::OrderConditionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitLimitOffsetClauses(
      SparqlParser::LimitOffsetClausesContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitLimitClause(
      SparqlParser::LimitClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitOffsetClause(
      SparqlParser::OffsetClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitValuesClause(
      SparqlParser::ValuesClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitTriplesTemplate(
      SparqlParser::TriplesTemplateContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupGraphPattern(
      SparqlParser::GroupGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupGraphPatternSub(
      SparqlParser::GroupGraphPatternSubContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitTriplesBlock(
      SparqlParser::TriplesBlockContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphPatternNotTriples(
      SparqlParser::GraphPatternNotTriplesContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitOptionalGraphPattern(
      SparqlParser::OptionalGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphGraphPattern(
      SparqlParser::GraphGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitServiceGraphPattern(
      SparqlParser::ServiceGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBind(SparqlParser::BindContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitInlineData(SparqlParser::InlineDataContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDataBlock(SparqlParser::DataBlockContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitInlineDataOneVar(
      SparqlParser::InlineDataOneVarContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitInlineDataFull(
      SparqlParser::InlineDataFullContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDataBlockSingle(
      SparqlParser::DataBlockSingleContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDataBlockValue(
      SparqlParser::DataBlockValueContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitMinusGraphPattern(
      SparqlParser::MinusGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupOrUnionGraphPattern(
      SparqlParser::GroupOrUnionGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitFilterR(SparqlParser::FilterRContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConstraint(SparqlParser::ConstraintContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitFunctionCall(
      SparqlParser::FunctionCallContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitArgList(SparqlParser::ArgListContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitExpressionList(
      SparqlParser::ExpressionListContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConstructTemplate(
      SparqlParser::ConstructTemplateContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConstructTriples(
      SparqlParser::ConstructTriplesContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitTriplesSameSubject(
      SparqlParser::TriplesSameSubjectContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPropertyList(
      SparqlParser::PropertyListContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPropertyListNotEmpty(
      SparqlParser::PropertyListNotEmptyContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVerb(SparqlParser::VerbContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitObjectList(SparqlParser::ObjectListContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitObjectR(SparqlParser::ObjectRContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitTriplesSameSubjectPath(
      SparqlParser::TriplesSameSubjectPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPropertyListPath(
      SparqlParser::PropertyListPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPropertyListPathNotEmpty(
      SparqlParser::PropertyListPathNotEmptyContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVerbPath(SparqlParser::VerbPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVerbSimple(SparqlParser::VerbSimpleContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVerbPathOrSimple(
      SparqlParser::VerbPathOrSimpleContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitObjectListPath(
      SparqlParser::ObjectListPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitObjectPath(SparqlParser::ObjectPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPath(SparqlParser::PathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathAlternative(
      SparqlParser::PathAlternativeContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathSequence(
      SparqlParser::PathSequenceContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathElt(SparqlParser::PathEltContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathEltOrInverse(
      SparqlParser::PathEltOrInverseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathMod(SparqlParser::PathModContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathPrimary(
      SparqlParser::PathPrimaryContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathNegatedPropertySet(
      SparqlParser::PathNegatedPropertySetContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathOneInPropertySet(
      SparqlParser::PathOneInPropertySetContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitInteger(SparqlParser::IntegerContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitTriplesNode(
      SparqlParser::TriplesNodeContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBlankNodePropertyList(
      SparqlParser::BlankNodePropertyListContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitTriplesNodePath(
      SparqlParser::TriplesNodePathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBlankNodePropertyListPath(
      SparqlParser::BlankNodePropertyListPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitCollection(SparqlParser::CollectionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitCollectionPath(
      SparqlParser::CollectionPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphNode(SparqlParser::GraphNodeContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphNodePath(
      SparqlParser::GraphNodePathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVarOrTerm(SparqlParser::VarOrTermContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVarOrIri(SparqlParser::VarOrIriContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVar(SparqlParser::VarContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphTerm(SparqlParser::GraphTermContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitExpression(SparqlParser::ExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConditionalOrExpression(
      SparqlParser::ConditionalOrExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConditionalAndExpression(
      SparqlParser::ConditionalAndExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitValueLogical(
      SparqlParser::ValueLogicalContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitRelationalExpression(
      SparqlParser::RelationalExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNumericExpression(
      SparqlParser::NumericExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAdditiveExpression(
      SparqlParser::AdditiveExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitMultiplicativeExpression(
      SparqlParser::MultiplicativeExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitUnaryExpression(
      SparqlParser::UnaryExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPrimaryExpression(
      SparqlParser::PrimaryExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBrackettedExpression(
      SparqlParser::BrackettedExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBuiltInCall(
      SparqlParser::BuiltInCallContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitRegexExpression(
      SparqlParser::RegexExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSubstringExpression(
      SparqlParser::SubstringExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitStrReplaceExpression(
      SparqlParser::StrReplaceExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitExistsFunc(SparqlParser::ExistsFuncContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNotExistsFunc(
      SparqlParser::NotExistsFuncContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAggregate(SparqlParser::AggregateContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitIriOrFunction(
      SparqlParser::IriOrFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitRdfLiteral(SparqlParser::RdfLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNumericLiteral(
      SparqlParser::NumericLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNumericLiteralUnsigned(
      SparqlParser::NumericLiteralUnsignedContext* ctx) override {
    if (ctx->INTEGER()) {
      return std::stoull(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  }

  antlrcpp::Any visitNumericLiteralPositive(
      SparqlParser::NumericLiteralPositiveContext* ctx) override {
    if (ctx->INTEGER_POSITIVE()) {
      return std::stoull(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  }

  antlrcpp::Any visitNumericLiteralNegative(
      SparqlParser::NumericLiteralNegativeContext* ctx) override {
    if (ctx->INTEGER_NEGATIVE()) {
      return std::stoll(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  }

  antlrcpp::Any visitBooleanLiteral(
      SparqlParser::BooleanLiteralContext* ctx) override {
    return ctx->getText() == "true";
  }

  antlrcpp::Any visitString(SparqlParser::StringContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitIri(SparqlParser::IriContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitIriref(SparqlParser::IrirefContext* ctx) override {
    return RdfEscaping::unescapeIriref(ctx->getText());
  }

  antlrcpp::Any visitPrefixedName(
      SparqlParser::PrefixedNameContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBlankNode(SparqlParser::BlankNodeContext* ctx) override {
    return visitChildren(ctx);
  }

   antlrcpp::Any visitPnameLn(SparqlParser::PnameLnContext *ctx) override {
     return visitChildren(ctx);
   }
};
