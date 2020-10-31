//
// Created by johannes on 31.10.20.
//


#include <gtest/gtest.h>
#include "../src/parser/sparqlParser/SparqlCustomVisitor.h"
#include "../src/parser/sparqlParser/SparqlLexer.h"

using namespace antlr4;

TEST(SparqlExpressionParser, Basic) {

  ANTLRInputStream input("?x * ?y");
  SparqlLexer lexer(&input);
  CommonTokenStream tokens(&lexer);
  SparqlParser parser(&tokens);
  auto tree = parser.conditionalOrExpression();
  SparqlCustomVisitor visitor;
  auto expr = visitor.visitConditionalOrExpression(tree);

  auto root = dynamic_cast<const MultiplyExpression*>(expr.get());
  auto a = dynamic_cast<const VariableExpression*>(root->a());
  auto b = dynamic_cast<const VariableExpression*>(root->b());

  ASSERT_EQ(a->variable(), "?x"s);
  ASSERT_EQ(b->variable(), "?y"s);


}