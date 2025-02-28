// Copyright 2022, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include "./SparqlParser.h"

#include "parser/SparqlParserHelpers.h"

using AntlrParser = SparqlAutomaticParser;

// _____________________________________________________________________________
ParsedQuery SparqlParser::parseQuery(
    std::string query, const std::vector<DatasetClause>& datasets) {
  // The second argument is the `PrefixMap` for QLever's internal IRIs.
  using S = std::string;
  sparqlParserHelpers::ParserAndVisitor p{
      std::move(query),
      {{S{QLEVER_INTERNAL_PREFIX_NAME}, S{QLEVER_INTERNAL_PREFIX_IRI}}},
      parsedQuery::DatasetClauses::fromClauses(datasets)};
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
