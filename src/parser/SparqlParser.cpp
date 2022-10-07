// Copyright 2022, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include "./SparqlParser.h"

#include "parser/SparqlParserHelpers.h"

using AntlrParser = SparqlAutomaticParser;

// _____________________________________________________________________________
ParsedQuery SparqlParser::parseQuery(std::string query) {
  sparqlParserHelpers::ParserAndVisitor p{
      std::move(query),
      {{INTERNAL_PREDICATE_PREFIX_NAME, INTERNAL_PREDICATE_PREFIX_IRI}}};
  auto resultOfParseAndRemainingText = p.parseTypesafe(&AntlrParser::query);
  // The query rule ends with <EOF> so the parse always has to consume the whole
  // input. If this is not the case a ParseException should have been thrown at
  // an earlier point.
  AD_CHECK(resultOfParseAndRemainingText.remainingText_.empty());
  return std::move(resultOfParseAndRemainingText.resultOfParse_);
}
