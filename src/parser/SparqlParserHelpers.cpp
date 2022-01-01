//
// Created by johannes on 16.05.21.
//

#include "SparqlParserHelpers.h"

#include "../util/antlr/ThrowingErrorStrategy.h"
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
  explicit ParserAndVisitor(string input) : _input{std::move(input)} {
    _parser.setErrorHandler(std::make_shared<ThrowingErrorStrategy>());
  }

  template <typename ResultType, typename ContextType>
  auto parse(const std::string_view input, const std::string_view name,
             ContextType* (SparqlAutomaticParser::*F)(void)) {
    try {
      auto context = (_parser.*F)();
      auto resultOfParse =
          std::move(context->accept(&(_visitor)).template as<ResultType>());

      auto remainingString =
          input.substr(_parser.getCurrentToken()->getStartIndex());
      return ResultOfParseAndRemainingText{std::move(resultOfParse),
                                           std::string{remainingString}};
    } catch (const antlr4::ParseCancellationException& e) {
      throw std::runtime_error{"Failed to parse " + name + ": " + e.what()};
    }
  }
};

// ____________________________________________________________________________
ResultOfParseAndRemainingText<sparqlExpression::SparqlExpressionPimpl>
parseExpression(const std::string& input) {
  ParserAndVisitor p{input};
  auto resultOfParseAndRemainingText =
      p.parse<sparqlExpression::SparqlExpression::Ptr>(
          input, "expression", &SparqlAutomaticParser::expression);

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
      input, "alias", &SparqlAutomaticParser::aliasWithouBrackes);
}
// _____________________________________________________________________________

ResultOfParseAndRemainingText<std::vector<std::array<VarOrTerm, 3>>>
parseConstructTemplate(const std::string& input) {
  ParserAndVisitor p{input};
  return p.parse<std::vector<std::array<VarOrTerm, 3>>>(
      input, "construct template", &SparqlAutomaticParser::constructTemplate);
}
}  // namespace sparqlParserHelpers
