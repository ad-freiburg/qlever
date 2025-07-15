// Copyright 2022, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include "./SparqlParser.h"

#include "parser/SparqlParserHelpers.h"

using AntlrParser = SparqlAutomaticParser;

namespace {
// _____________________________________________________________________________
// Parse the given string as the given clause. If the datasets are not empty,
// then they are fixed during the parsing and cannot be changed by the SPARQL.
template <typename ContextType>
auto parseOperation(ContextType* (SparqlAutomaticParser::*F)(void),
                    std::string operation,
                    const std::vector<DatasetClause>& datasets) {
  using S = std::string;
  // The second argument is the `PrefixMap` for QLever's internal IRIs.
  // The third argument are the datasets from outside the query, which override
  // any datasets in the query.
  sparqlParserHelpers::ParserAndVisitor p{
      std::move(operation),
      {{S{QLEVER_INTERNAL_PREFIX_NAME}, S{QLEVER_INTERNAL_PREFIX_IRI}}},
      datasets.empty()
          ? std::nullopt
          : std::optional(parsedQuery::DatasetClauses::fromClauses(datasets))};
  auto resultOfParseAndRemainingText = p.parseTypesafe(F);
  // The query rule ends with <EOF> so the parse always has to consume the whole
  // input. If this is not the case a ParseException should have been thrown at
  // an earlier point.
  AD_CONTRACT_CHECK(resultOfParseAndRemainingText.remainingText_.empty());
  return std::move(resultOfParseAndRemainingText.resultOfParse_);
}
}  // namespace

// _____________________________________________________________________________
ParsedQuery SparqlParser::parseQuery(
    std::string query, const std::vector<DatasetClause>& datasets) {
  return parseOperation(&AntlrParser::query, std::move(query), datasets);
}

// _____________________________________________________________________________
std::vector<ParsedQuery> SparqlParser::parseUpdate(
    std::string update, const std::vector<DatasetClause>& datasets) {
  return parseOperation(&AntlrParser::update, std::move(update), datasets);
}
