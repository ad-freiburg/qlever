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
  auto parse(const std::string_view input,
             ContextType* (SparqlAutomaticParser::*F)(void)) {
    auto context = (parser_.*F)();
    auto resultOfParse =
        std::move(context->accept(&(visitor_)).template as<ResultType>());

    auto remainingString =
        input.substr(parser_.getCurrentToken()->getStartIndex());
    return ResultOfParseAndRemainingText{std::move(resultOfParse),
                                         std::string{remainingString}};
  }

  template <typename ContextType>
  auto parseTypesafe(const std::string_view input,
                     ContextType* (SparqlAutomaticParser::*F)(void)) {
    auto resultOfParse = visitor_.visitTypesafe(std::invoke(F, parser_));

    auto remainingString =
        input.substr(parser_.getCurrentToken()->getStartIndex());
    return ResultOfParseAndRemainingText{std::move(resultOfParse),
                                         std::string{remainingString}};
  }
};

// ____________________________________________________________________________
inline auto parseFront = []<typename ContextType>(
                             ContextType* (SparqlAutomaticParser::*F)(void),
                             const std::string& input,
                             SparqlQleverVisitor::PrefixMap prefixes = {}) {
  ParserAndVisitor p{input, std::move(prefixes)};
  return p.parseTypesafe(input, F);
};

inline auto parseInlineData =
    std::bind_front(parseFront, &SparqlAutomaticParser::inlineData);

inline auto parseExpression =
    std::bind_front(parseFront, &SparqlAutomaticParser::expression);

inline auto parseBind =
    std::bind_front(parseFront, &SparqlAutomaticParser::bind);

inline auto parseConstructTemplate =
    std::bind_front(parseFront, &SparqlAutomaticParser::constructTemplate);

inline auto parseLimitOffsetClause =
    std::bind_front(parseFront, &SparqlAutomaticParser::limitOffsetClauses);

inline auto parseOrderClause =
    std::bind_front(parseFront, &SparqlAutomaticParser::orderClause);

inline auto parseGroupClause =
    std::bind_front(parseFront, &SparqlAutomaticParser::groupClause);

inline auto parseDataBlock =
    std::bind_front(parseFront, &SparqlAutomaticParser::dataBlock);

inline auto parseVerbPathOrSimple =
    std::bind_front(parseFront, &SparqlAutomaticParser::verbPathOrSimple);

inline auto parseSelectClause =
    std::bind_front(parseFront, &SparqlAutomaticParser::selectClause);

inline auto parsePropertyListPathNotEmpty = std::bind_front(
    parseFront, &SparqlAutomaticParser::propertyListPathNotEmpty);

inline auto parseTriplesSameSubjectPath =
    std::bind_front(parseFront, &SparqlAutomaticParser::triplesSameSubjectPath);

inline auto parsePrologue =
    std::bind_front(parseFront, &SparqlAutomaticParser::prologue);

inline auto parsePrefixDecl =
    std::bind_front(parseFront, &SparqlAutomaticParser::prefixDecl);

inline auto parsePnameLn =
    std::bind_front(parseFront, &SparqlAutomaticParser::pnameLn);

inline auto parsePnameNs =
    std::bind_front(parseFront, &SparqlAutomaticParser::pnameNs);

inline auto parsePrefixedName =
    std::bind_front(parseFront, &SparqlAutomaticParser::prefixedName);

inline auto parseIriref =
    std::bind_front(parseFront, &SparqlAutomaticParser::iriref);

inline auto parseNumericLiteral =
    std::bind_front(parseFront, &SparqlAutomaticParser::numericLiteral);

}  // namespace sparqlParserHelpers

#endif  // QLEVER_SPARQLPARSERHELPERS_H
