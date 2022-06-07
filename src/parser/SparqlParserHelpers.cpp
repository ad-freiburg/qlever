//
// Created by johannes on 16.05.21.
//

#include "SparqlParserHelpers.h"

#include "../util/antlr/ThrowingErrorStrategy.h"
#include "sparqlParser/generated/SparqlAutomaticLexer.h"

namespace sparqlParserHelpers {
using std::string;

ParserAndVisitor::ParserAndVisitor(string input,
                                   SparqlQleverVisitor::PrefixMap prefixes)
    : _input{std::move(input)}, _visitor{std::move(prefixes)} {
  _parser.setErrorHandler(std::make_shared<ThrowingErrorStrategy>());
}

// ____________________________________________________________________________
ResultOfParseAndRemainingText<sparqlExpression::SparqlExpressionPimpl>
parseExpression(const std::string& input,
                SparqlQleverVisitor::PrefixMap prefixMap) {
  ParserAndVisitor p{input, std::move(prefixMap)};
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
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixMap) {
  ParserAndVisitor p{input, std::move(prefixMap)};
  return p.parse<ParsedQuery::Alias>(
      input, "alias", &SparqlAutomaticParser::aliasWithouBrackes);
}
// ____________________________________________________________________________
ResultOfParseAndRemainingText<GraphPatternOperation::Bind> parseBind(
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixMap) {
  ParserAndVisitor p{input, std::move(prefixMap)};
  return p.parse<GraphPatternOperation::Bind>(input, "bind",
                                              &SparqlAutomaticParser::bind);
}
// _____________________________________________________________________________

ResultOfParseAndRemainingText<ad_utility::sparql_types::Triples>
parseConstructTemplate(const std::string& input,
                       SparqlQleverVisitor::PrefixMap prefixes) {
  ParserAndVisitor p{input, std::move(prefixes)};
  return p.parse<ad_utility::sparql_types::Triples>(
      input, "construct template", &SparqlAutomaticParser::constructTemplate);
}
// _____________________________________________________________________________

ResultOfParseAndRemainingText<LimitOffsetClause> parseLimitOffsetClause(
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixes) {
  ParserAndVisitor p{input, std::move(prefixes)};
  return p.parse<LimitOffsetClause>(input, "limit offset clause",
                                    &SparqlAutomaticParser::limitOffsetClauses);
}
// _____________________________________________________________________________

ResultOfParseAndRemainingText<vector<OrderKey>> parseOrderClause(
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixes) {
  ParserAndVisitor p{input, std::move(prefixes)};
  return p.parse<vector<OrderKey>>(input, "order clause",
                                   &SparqlAutomaticParser::orderClause);
}
}  // namespace sparqlParserHelpers
