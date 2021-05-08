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

struct ParserAndVisitor {
 private:
  string input;
  ANTLRInputStream stream{input};
  SparqlLexer lexer{&stream};
  CommonTokenStream tokens{&lexer};

 public:
  SparqlParser parser{&tokens};
  SparqlQleverVisitor visitor;
  explicit ParserAndVisitor(string toParse) : input{std::move(toParse)} {}
};

template <typename T>
void testNumericLiteral(const std::string& input, T target) {
  ParserAndVisitor p(input);
  auto literalContext = p.parser.numericLiteral();
  auto result = p.visitor.visitNumericLiteral(literalContext).as<T>();

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

TEST(SparqlParser, Prefix) {
  string s = "PREFIX wd: <www.wikidata.org>";
  ParserAndVisitor p{s};
  auto context = p.parser.prefixDecl();
  p.visitor.visitPrefixDecl(context);
  const auto& m = p.visitor.prefixMap();
  ASSERT_EQ(1ul, m.size());
  ASSERT_TRUE(m.contains("wd:"));
  ASSERT_TRUE(m.at("wd:") == "<www.wikidata.org>");
}