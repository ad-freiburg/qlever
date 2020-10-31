//
// Created by johannes on 29.10.20.
//

#include "SparqlCustomVisitor.h"

// ___________________________________________________________________________________
std::unique_ptr<ExpressionTree> SparqlCustomVisitor::visitRelationalExpression(
    SparqlParser::RelationalExpressionContext* ctx) {
  if (ctx->numericExpression().size() != 1 || (ctx->expressionList() && !ctx->expressionList()->isEmpty())) {
    throw std::runtime_error("Relational Expressions (<=> etc) are not yet implemented");
  }

  return visitNumericExpression(ctx->numericExpression(0));
}
