//
// Created by johannes on 16.05.21.
//

#ifndef QLEVER_SPARQLPARSERHELPERS_H
#define QLEVER_SPARQLPARSERHELPERS_H

#include <memory>
#include <string>

#include "../engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "../util/antlr/ANTLRErrorHandling.h"
#include "./ParsedQuery.h"
#include "sparqlParser/SparqlQleverVisitor.h"
#include "sparqlParser/generated/SparqlAutomaticLexer.h"

namespace sparqlParserHelpers {

template <typename ResultOfParse>
struct ResultOfParseAndRemainingText {
  ResultOfParse resultOfParse_;
  std::string remainingText_;
  ResultOfParseAndRemainingText(ResultOfParse&& resultOfParse,
                                std::string&& remainingText)
      : resultOfParse_{std::move(resultOfParse)},
        remainingText_{std::move(remainingText)} {}
};

struct ParserAndVisitor {
 private:
  string input_;
  antlr4::ANTLRInputStream stream_{input_};
  SparqlAutomaticLexer lexer_{&stream_};
  antlr4::CommonTokenStream tokens_{&lexer_};
  ThrowingErrorListener errorListener_{};

 public:
  SparqlAutomaticParser parser_{&tokens_};
  SparqlQleverVisitor visitor_;
  explicit ParserAndVisitor(string input);
  ParserAndVisitor(string input, SparqlQleverVisitor::PrefixMap prefixes);

  template <typename ResultType, typename ContextType>
  auto parse(const std::string_view input, const std::string_view name,
             ContextType* (SparqlAutomaticParser::*F)(void)) {
    try {
      auto context = (parser_.*F)();
      auto resultOfParse =
          std::move(context->accept(&(visitor_)).template as<ResultType>());

      auto remainingString =
          input.substr(parser_.getCurrentToken()->getStartIndex());
      return ResultOfParseAndRemainingText{std::move(resultOfParse),
                                           std::string{remainingString}};
    } catch (const antlr4::ParseCancellationException& e) {
      throw std::runtime_error{"Failed to parse " + name + ": " + e.what()};
    }
  }

  template <typename ContextType>
  auto parseTypesafe(const std::string_view input, const std::string_view name,
                     ContextType* (SparqlAutomaticParser::*F)(void)) {
    try {
      auto resultOfParse = visitor_.visitTypesafe(std::invoke(F, parser_));

      auto remainingString =
          input.substr(parser_.getCurrentToken()->getStartIndex());
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

ResultOfParseAndRemainingText<vector<GroupKey>> parseGroupClause(
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixes);

ResultOfParseAndRemainingText<std::optional<GraphPatternOperation::Values>>
parseValuesClause(const std::string& input,
                  SparqlQleverVisitor::PrefixMap prefixes);

ResultOfParseAndRemainingText<PropertyPath> parseVerbPathOrSimple(
    const std::string& input, SparqlQleverVisitor::PrefixMap prefixes);
}  // namespace sparqlParserHelpers

#endif  // QLEVER_SPARQLPARSERHELPERS_H
