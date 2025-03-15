// Copyright 2022, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include "./SparqlParser.h"

#include "parser/SparqlParserHelpers.h"

using AntlrParser = SparqlAutomaticParser;

// _____________________________________________________________________________
template <typename ContextType>
auto parseOperation(ContextType* (SparqlAutomaticParser::*F)(void),
                    std::string operation) {
  // The second argument is the `PrefixMap` for QLever's internal IRIs.
  using S = std::string;
  sparqlParserHelpers::ParserAndVisitor p{
      std::move(operation),
      {{S{QLEVER_INTERNAL_PREFIX_NAME}, S{QLEVER_INTERNAL_PREFIX_IRI}}}};
  // Note: `AntlrParser::query` is a method of `AntlrParser` (which is an alias
  // for `SparqlAutomaticParser`) that returns the `QueryContext*` for the whole
  // query.
  auto resultOfParseAndRemainingText = p.parseTypesafe(F);
  // The query rule ends with <EOF> so the parse always has to consume the whole
  // input. If this is not the case a ParseException should have been thrown at
  // an earlier point.
  AD_CONTRACT_CHECK(resultOfParseAndRemainingText.remainingText_.empty());
  return std::move(resultOfParseAndRemainingText.resultOfParse_);
}

// _____________________________________________________________________________
ParsedQuery SparqlParser::parseQuery(std::string query) {
  return parseOperation(&AntlrParser::query, std::move(query));
}

// _____________________________________________________________________________
std::vector<ParsedQuery> SparqlParser::parseUpdate(std::string update) {
  return parseOperation(&AntlrParser::update, std::move(update));
}

// _____________________________________________________________________________
ParsedQuery SparqlParser::parseQuery(
    std::string query, const std::vector<DatasetClause>& datasets) {
  auto parsedQuery = parseQuery(std::move(query));
  // SPARQL Protocol 2.1.4 specifies that the dataset from the query
  // parameters overrides the dataset from the query itself.
  if (!datasets.empty()) {
    parsedQuery.datasetClauses_ =
        parsedQuery::DatasetClauses::fromClauses(datasets);
  }
  return parsedQuery;
}

// _____________________________________________________________________________
std::vector<ParsedQuery> SparqlParser::parseUpdate(
    std::string update, const std::vector<DatasetClause>& datasets) {
  auto parsedUpdates = parseUpdate(std::move(update));
  // SPARQL Protocol 2.1.4 specifies that the dataset from the query
  // parameters overrides the dataset from the query itself.
  if (!datasets.empty()) {
    auto datasetClauses = parsedQuery::DatasetClauses::fromClauses(datasets);
    for (auto& parsedOperation : parsedUpdates) {
      parsedOperation.datasetClauses_ = datasetClauses;
    }
  }
  return parsedUpdates;
}
