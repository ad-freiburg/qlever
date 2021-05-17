//
// Created by johannes on 16.05.21.
//

#include "SparqlParserHelpers.h"
#include "sparqlParser/SparqlQleverVisitor.h"
#include "sparqlParser/generated/SparqlAutomaticLexer.h"

namespace sparqlParserHelpers {
using std::string;

struct ParserAndVisitor {
 private:
  string input;
  antlr4::ANTLRInputStream stream{input};
  SparqlAutomaticLexer lexer{&stream};
  antlr4::CommonTokenStream tokens{&lexer};

 public:
  SparqlAutomaticParser parser{&tokens};
  SparqlQleverVisitor visitor;
  explicit ParserAndVisitor(string toParse) : input{std::move(toParse)} {}
  explicit ParserAndVisitor(string toParse,
                            SparqlQleverVisitor::PrefixMap prefixMap)
      : input{std::move(toParse)}, visitor{std::move(prefixMap)} {}

  template <typename ResultType, typename ContextType>
  auto parseStuff(const std::string& input,
                  ContextType* (SparqlAutomaticParser::*F)(void))

  {
    auto context = (parser.*F)();
    auto remainingString =
        input.substr(parser.getCurrentToken()->getStartIndex());
    auto result = std::move(context->accept(&(visitor)).template as<ResultType>());
    return ParseResultAndRemainingText(std::move(result),
                                       std::move(remainingString));
  }
};

ParseResultAndRemainingText<
    sparqlExpression::SparqlExpressionWrapper>
parseExpression(const std::string& input) {
  ParserAndVisitor p{input};
  auto actualResult = p.parseStuff<sparqlExpression::SparqlExpression::Ptr>(input, &SparqlAutomaticParser::expression);

  return ParseResultAndRemainingText{
      sparqlExpression::SparqlExpressionWrapper{std::move(actualResult._parseResult)}, actualResult._remainingText};
}

// ______________________________________________________________________________
std::pair<SparqlQleverVisitor::PrefixMap, size_t> parsePrologue(
    const string& input) {
  ParserAndVisitor p{input};
  auto context = p.parser.prologue();
  auto parsedSize = context->getText().size();
  p.visitor.visitPrologue(context);
  const auto& constVisitor = p.visitor;
  return {constVisitor.prefixMap(), parsedSize};
}

// _____________________________________________________________________________
std::pair<string, size_t> parseIri(const string& input,
                                   SparqlQleverVisitor::PrefixMap prefixMap) {
  ParserAndVisitor p{input, std::move(prefixMap)};
  auto context = p.parser.iri();
  auto parsedSize = context->getText().size();
  auto resultString = p.visitor.visitIri(context).as<string>();
  // const auto& constVisitor = p.visitor;
  return {std::move(resultString), parsedSize};
}

} // namespace sparqlParserHelpers