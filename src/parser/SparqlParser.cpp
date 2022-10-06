// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "parser/SparqlParser.h"

#include <unordered_set>
#include <variant>

#include "parser/Alias.h"
#include "parser/data/Types.h"
#include "parser/sparqlParser/SparqlQleverVisitor.h"
#include "util/Conversions.h"

using namespace std::literals::string_literals;
using AntlrParser = SparqlAutomaticParser;

SparqlParser::SparqlParser(const string& query) : lexer_(query), query_(query) {
  LOG(DEBUG) << "Parsing " << query << std::endl;
}

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
