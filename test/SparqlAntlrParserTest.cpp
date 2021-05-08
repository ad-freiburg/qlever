//
// Created by johannes on 08.05.21.
//

#include <antlr4-runtime.h>
#include <gtest/gtest.h>

#include <type_traits>

#include "../../src/parser/sparqlParser/SparqlLexer.h"
#include "../../src/parser/sparqlParser/SparqlParser.h"
#include "../../src/parser/sparqlParser/SparqlQleverVisitor.h"

using namespace antlr4;

template <typename T>
void testNumericLiteral(const std::string& input, T target) {
  ANTLRInputStream stream{input};
  SparqlLexer lexer{&stream};
  CommonTokenStream tokens{&lexer};
  SparqlParser parser{&tokens};

  auto literalContext = parser.numericLiteral();

  SparqlQleverVisitor visitor;
  auto result = visitor.visitNumericLiteral(literalContext).as<T>();

  if constexpr (std::is_floating_point_v<T>) {
    ASSERT_FLOAT_EQ(target, result);
  } else {
    ASSERT_EQ(target, result);
  }
}

TEST(SparqlParser, NumericLiterals) {
  testNumericLiteral("3.0", 3.0);
  testNumericLiteral("3.0e2", 300.0);
  testNumericLiteral("3.0e-2", 0.030);
  testNumericLiteral("3", 3ull);
  testNumericLiteral("-3.0", -3.0);
  testNumericLiteral("-3", -3ll);

  // TODO<joka921> : Unit tests with random numbers
}