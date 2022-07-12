//
// Created by johannes on 16.05.21.
//

#include "SparqlParserHelpers.h"

#include "sparqlParser/generated/SparqlAutomaticLexer.h"

namespace sparqlParserHelpers {
using std::string;

// _____________________________________________________________________________
ParserAndVisitor::ParserAndVisitor(string input)
    : input_{std::move(input)}, visitor_{} {
  parser_.setErrorHandler(std::make_shared<ThrowingErrorStrategy>());
  // The default in ANTLR is to log all errors to the console and to continue
  // the parsing. We need to turn parse errors into exceptions instead to
  // propagate them to the user.
  parser_.removeErrorListeners();
  parser_.addErrorListener(&errorListener_);
  lexer_.removeErrorListeners();
  lexer_.addErrorListener(&errorListener_);
}

// _____________________________________________________________________________
ParserAndVisitor::ParserAndVisitor(string input,
                                   SparqlQleverVisitor::PrefixMap prefixes)
    : ParserAndVisitor{std::move(input)} {
  visitor_.setPrefixMapManually(std::move(prefixes));
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
          std::move(resultOfParseAndRemainingText.resultOfParse_)},
      std::move(resultOfParseAndRemainingText.remainingText_)};
}

// ____________________________________________________________________________
ResultOfParseAndRemainingText<ParsedQuery::Alias> parseAlias(
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixMap) {
  ParserAndVisitor p{input, std::move(prefixMap)};
  return p.parse<ParsedQuery::Alias>(
      input, "alias", &SparqlAutomaticParser::aliasWithoutBrackets);
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
// _____________________________________________________________________________

ResultOfParseAndRemainingText<vector<GroupKey>> parseGroupClause(
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixes) {
  ParserAndVisitor p{input, std::move(prefixes)};
  return p.parse<vector<GroupKey>>(input, "group clause",
                                   &SparqlAutomaticParser::groupClause);
}
// _____________________________________________________________________________

ResultOfParseAndRemainingText<std::optional<GraphPatternOperation::Values>>
parseValuesClause(const std::string& input,
                  SparqlQleverVisitor::PrefixMap prefixes) {
  ParserAndVisitor p{input, std::move(prefixes)};
  return p.parseTypesafe(input, "values clause",
                         &SparqlAutomaticParser::valuesClause);
}
// _____________________________________________________________________________

ResultOfParseAndRemainingText<PropertyPath> parseVerbPathOrSimple(
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixes) {
  ParserAndVisitor p{input, std::move(prefixes)};
  return p.parseTypesafe(input, "verb path or simple",
                         &SparqlAutomaticParser::verbPathOrSimple);
}
// _____________________________________________________________________________

ResultOfParseAndRemainingText<ParsedQuery::SelectClause> parseSelectClause(
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixes) {
  ParserAndVisitor p{input, std::move(prefixes)};
  return p.parseTypesafe(input, "selectClause",
                         &SparqlAutomaticParser::selectClause);
}
}  // namespace sparqlParserHelpers
