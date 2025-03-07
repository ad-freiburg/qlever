// Copyright 2022, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include "./SparqlParser.h"

#include "parser/SparqlParserHelpers.h"

using AntlrParser = SparqlAutomaticParser;

// _____________________________________________________________________________
ParsedQuery SparqlParser::parseQuery(std::string operation,
                                     std::vector<DatasetClause> datasets) {
  using S = std::string;
  std::optional<ParsedQuery::DatasetClauses> parsedDatasets = std::nullopt;
  if (!datasets.empty()) {
    parsedDatasets = parsedQuery::DatasetClauses::fromClauses(datasets);
  }
  // The second argument is the `PrefixMap` for QLever's internal IRIs.
  // The third argument are the datasets from outside the query, which override
  // any datasets in the query.
  sparqlParserHelpers::ParserAndVisitor p{
      std::move(operation),
      {{S{QLEVER_INTERNAL_PREFIX_NAME}, S{QLEVER_INTERNAL_PREFIX_IRI}}},
      std::move(parsedDatasets)};
  // Note: `AntlrParser::queryOrUpdate` is a method of `AntlrParser` (which is
  // an alias for `SparqlAutomaticParser`) that returns the
  // `QueryOrUpdateContext*` for the whole query or update.
  auto resultOfParseAndRemainingText =
      p.parseTypesafe(&AntlrParser::queryOrUpdate);
  // The query rule ends with <EOF> so the parse always has to consume the whole
  // input. If this is not the case a ParseException should have been thrown at
  // an earlier point.
  AD_CONTRACT_CHECK(resultOfParseAndRemainingText.remainingText_.empty());
  return std::move(resultOfParseAndRemainingText.resultOfParse_);
}
