//
// Created by johannes on 16.05.21.
//

#ifndef QLEVER_SPARQLPARSERHELPERS_H
#define QLEVER_SPARQLPARSERHELPERS_H

#include <memory>
#include <string>

#include "../engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "./ParsedQuery.h"
#include "sparqlParser/SparqlQleverVisitor.h"
#include "sparqlParser/generated/SparqlAutomaticLexer.h"

namespace sparqlParserHelpers {

template <typename ResultOfParse>
struct ResultOfParseAndRemainingText {
  ResultOfParseAndRemainingText(ResultOfParse&& resultOfParse,
                                std::string&& remainingText)
      : _resultOfParse{std::move(resultOfParse)},
        _remainingText{std::move(remainingText)} {}
  ResultOfParse _resultOfParse;
  std::string _remainingText;
};

struct ParserAndVisitor {
 private:
  string _input;
  antlr4::ANTLRInputStream _stream{_input};
  SparqlAutomaticLexer _lexer{&_stream};
  antlr4::CommonTokenStream _tokens{&_lexer};

 public:
  SparqlAutomaticParser _parser{&_tokens};
  SparqlQleverVisitor _visitor;
  explicit ParserAndVisitor(string input,
                            SparqlQleverVisitor::PrefixMap prefixes = {});

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
parseExpression(const std::string& input,
                SparqlQleverVisitor::PrefixMap prefixMap);

// An `alias` in Sparql have the form (<expression> as ?variable).
ResultOfParseAndRemainingText<ParsedQuery::Alias> parseAlias(
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixes);

ResultOfParseAndRemainingText<GraphPatternOperation::Bind> parseBind(
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixMap);

ResultOfParseAndRemainingText<ad_utility::sparql_types::Triples>
parseConstructTemplate(const std::string& input,
                       SparqlQleverVisitor::PrefixMap prefixes);

ResultOfParseAndRemainingText<LimitOffsetClause> parseLimitOffsetClause(
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixes);

ResultOfParseAndRemainingText<vector<OrderKey>> parseOrderClause(
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixes);
}  // namespace sparqlParserHelpers

#endif  // QLEVER_SPARQLPARSERHELPERS_H
