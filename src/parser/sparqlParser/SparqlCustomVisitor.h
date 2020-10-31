//
// Created by johannes on 29.10.20.
//

#ifndef QLEVER_SPARQLCUSTOMVISITOR_H
#define QLEVER_SPARQLCUSTOMVISITOR_H

#include <antlr4-runtime/antlr4-runtime.h>
#include "SparqlParser.h"
#include "../../engine/expressionModel/ExpressionTree.h"

#include <memory>


class SparqlCustomVisitor : public antlr4::tree::AbstractParseTreeVisitor {

 public:

  std::unique_ptr<ExpressionTree> visitConditionalOrExpression(SparqlParser::ConditionalOrExpressionContext* ctx) {
    if (ctx->conditionalAndExpression().size() != 1) {
      throw std::runtime_error("Or (||) in Sparql Expressions is not yet implemented");
    }
    return visitConditionalAndExpression(ctx->conditionalAndExpression(0));
  }

  std::unique_ptr<ExpressionTree> visitConditionalAndExpression(SparqlParser::ConditionalAndExpressionContext* ctx) {
    if (ctx->valueLogical().size() != 1) {
      throw std::runtime_error("And (&&) in Sparql Expressions is not yet implemented");
    }

    return visitValueLogical(ctx->valueLogical(0));
  }

  std::unique_ptr<ExpressionTree> visitValueLogical(SparqlParser::ValueLogicalContext* ctx) {
    return visitRelationalExpression(ctx->relationalExpression());
  }

  std::unique_ptr<ExpressionTree> visitRelationalExpression(SparqlParser::RelationalExpressionContext* ctx);

  std::unique_ptr<ExpressionTree> visitNumericExpression(SparqlParser::NumericExpressionContext* ctx) {
    return visitAdditiveExpression(ctx->additiveExpression());
  }

  std::unique_ptr<ExpressionTree> visitAdditiveExpression(SparqlParser::AdditiveExpressionContext* ctx) {
    if (ctx->children.size() > 1) {
      throw std::runtime_error("Additive Expressions are not yet implemented");
    }
    return visitMultiplicativeExpression(ctx->multiplicativeExpression(0));


  }

   std::unique_ptr<ExpressionTree> visitMultiplicativeExpression(SparqlParser::MultiplicativeExpressionContext* ctx) {
     auto a = visitUnaryExpression(ctx->unaryExpression(0));
     size_t i = 1;
     while (i < ctx->children.size()) {
       AD_CHECK(i+1 < ctx->children.size());
       auto b = visitUnaryExpression(ctx->unaryExpression((i + 1)/2));
       if (ctx->children[i]->getText() == "*") {
         a = std::make_unique<MultiplyExpression>(std::move(a), std::move(b));
       } else if (ctx->children[i]->getText() == "/") {
         a = std::make_unique<DivideExpression>(std::move(a), std::move(b));
       }

       i += 2;
     }

     return a;

   }

  std::unique_ptr<ExpressionTree> visitUnaryExpression(SparqlParser::UnaryExpressionContext* ctx) {
     if (ctx->children.size() > 1) {
       throw std::runtime_error("Signs or negations before unary expressions are not yet supported");
     }

     return visitPrimaryExpression(ctx->primaryExpression());

   }

   std::unique_ptr<ExpressionTree> visitPrimaryExpression(SparqlParser::PrimaryExpressionContext* ctx) {
     if (!ctx->var()) {
       throw std::runtime_error("Only Variables are currently implemented as building blocks of primary Expressions");
     }
     return std::make_unique<VariableExpression>(ctx->var()->getText());
   }

};

#endif  // QLEVER_SPARQLCUSTOMVISITOR_H
