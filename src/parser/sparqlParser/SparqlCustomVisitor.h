//
// Created by johannes on 29.10.20.
//

#ifndef QLEVER_SPARQLCUSTOMVISITOR_H
#define QLEVER_SPARQLCUSTOMVISITOR_H

#include <antlr4-runtime/antlr4-runtime.h>
#include "SparqlParser.h"


class SparqlCustomVisitor : public antlr4::tree::AbstractParseTreeVisitor {


  void visitConditionalOrExpression(SparqlParser::ConditionalOrExpressionContext* ctx) {
    if (ctx->conditionalAndExpression().size() != 1) {
      throw std::runtime_error("Or (||) in Sparql Expressions is not yet implemented");
    }
    return visitConditionalAndExpression(ctx->conditionalAndExpression(0));
  }

  void visitConditionalAndExpression(SparqlParser::ConditionalAndExpressionContext* ctx) {
    if (ctx->valueLogical().size() != 1) {
      throw std::runtime_error("And (&&) in Sparql Expressions is not yet implemented");
    }

    return visitValueLogical(ctx->valueLogical(0));
  }

  void visitValueLogical(SparqlParser::ValueLogicalContext* ctx) {
    return visitRelationalExpression(ctx->relationalExpression());
  }

  void visitRelationalExpression(SparqlParser::RelationalExpressionContext* ctx) {
    if (ctx->numericExpression().size() != 1 || ! ctx->expressionList()->isEmpty()) {
      throw std::runtime_error("Relational Expressions (<=> etc) are not yet implemented");
    }

  }

  void visitNumericExpression(SparqlParser::NumericExpressionContext* ctx) {
    return visitAdditiveExpression(ctx->additiveExpression());
  }

  void visitAdditiveExpression(SparqlParser::AdditiveExpressionContext* ctx) {

  }
};

#endif  // QLEVER_SPARQLCUSTOMVISITOR_H
