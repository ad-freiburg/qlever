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
  string _input;
  antlr4::ANTLRInputStream _stream{_input};
  SparqlAutomaticLexer _lexer{&_stream};
  antlr4::CommonTokenStream _tokens{&_lexer};

 public:
  SparqlAutomaticParser _parser{&_tokens};
  SparqlQleverVisitor _visitor;
  explicit ParserAndVisitor(string input) : _input{std::move(input)} {}
  explicit ParserAndVisitor(string input,
                            SparqlQleverVisitor::PrefixMap prefixMap)
      : _input{std::move(input)}, _visitor{std::move(prefixMap)} {}

  template <typename ResultType, typename ContextType>
  auto parse(const std::string& input,
             ContextType* (SparqlAutomaticParser::*F)(void))

  {
    auto context = (_parser.*F)();
    auto resultOfParse =
        std::move(context->accept(&(_visitor)).template as<ResultType>());

    auto remainingString =
        input.substr(_parser.getCurrentToken()->getStartIndex());
    return ResultOfParseAndRemainingText{std::move(resultOfParse),
                                         std::move(remainingString)};
  }
};

// ____________________________________________________________________________
ResultOfParseAndRemainingText<sparqlExpression::SparqlExpressionPimpl>
parseExpression(const std::string& input) {
  ParserAndVisitor p{input};
  auto resultOfParseAndRemainingText =
      p.parse<sparqlExpression::SparqlExpression::Ptr>(
          input, &SparqlAutomaticParser::expression);

  return ResultOfParseAndRemainingText{
      sparqlExpression::SparqlExpressionPimpl{
          std::move(resultOfParseAndRemainingText._resultOfParse)},
      std::move(resultOfParseAndRemainingText._remainingText)};
}

// ____________________________________________________________________________
ResultOfParseAndRemainingText<ParsedQuery::Alias> parseAlias(
    const std::string& input) {
  ParserAndVisitor p{input};
  return p.parse<ParsedQuery::Alias>(
      input, &SparqlAutomaticParser::aliasWithouBrackes);
}

// ______________________________________________________________________________
std::pair<SparqlQleverVisitor::PrefixMap, size_t> parsePrologue(
    const string& input) {
  ParserAndVisitor p{input};
  auto prologueContext = p._parser.prologue();
  auto prologueSize = prologueContext->getText().size();
  p._visitor.visitPrologue(prologueContext);
  const auto& constVisitor = p._visitor;
  return {constVisitor.prefixMap(), prologueSize};
}

// Parse a prefix of `input` as an iri, return the iri and the number of bytes
// that were consumed from the `input`.
std::pair<string, size_t> parseIri(const string& input,
                                   SparqlQleverVisitor::PrefixMap prefixMap) {
  ParserAndVisitor p{input, std::move(prefixMap)};
  auto iriContext = p._parser.iri();
  auto iriSize = iriContext->getText().size();
  auto resultString = p._visitor.visitIri(iriContext).as<string>();
  // const auto& constVisitor = p.visitor;
  return {std::move(resultString), iriSize};
}

}  // namespace sparqlParserHelpers