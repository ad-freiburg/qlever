// Copyright 2022, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include "./SparqlParser.h"

#include "parser/SparqlParserHelpers.h"

using AntlrParser = SparqlAutomaticParser;

// _____________________________________________________________________________
ParsedQuery SparqlParser::parseQuery(std::string query) {
  // The second argument is the `PrefixMap` for QLever's internal IRIs.
  using S = std::string;
  sparqlParserHelpers::ParserAndVisitor p{
      std::move(query),
      {{S{QLEVER_INTERNAL_PREFIX_NAME}, S{QLEVER_INTERNAL_PREFIX_IRI}}}};
  // Note: `AntlrParser::query` is a method of `AntlrParser` (which is an alias
  // for `SparqlAutomaticParser`) that returns the `QueryContext*` for the whole
  // query.
  auto resultOfParseAndRemainingText =
      p.parseTypesafe(&AntlrParser::queryOrUpdate);
  // The query rule ends with <EOF> so the parse always has to consume the whole
  // input. If this is not the case a ParseException should have been thrown at
  // an earlier point.
  AD_CONTRACT_CHECK(resultOfParseAndRemainingText.remainingText_.empty());
  return std::move(resultOfParseAndRemainingText.resultOfParse_);
}

// _____________________________________________________________________________
ParsedQuery SparqlParser::parseQuery(
    std::string operation, const std::vector<DatasetClause>& datasets) {
  auto parsedOperation = parseQuery(std::move(operation));
  // SPARQL Protocol 2.1.4 specifies that the dataset from the query
  // parameters overrides the dataset from the query itself.
  if (!datasets.empty()) {
    parsedOperation.datasetClauses_ =
        parsedQuery::DatasetClauses::fromClauses(datasets);
  }
  return parsedOperation;
}
