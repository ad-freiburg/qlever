// Copyright 2022, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include "./SparqlParser.h"

#include "parser/SparqlParserHelpers.h"

using AntlrParser = SparqlAutomaticParser;

using BnodeMgr = ad_utility::BlankNodeManager*;

namespace {
// _____________________________________________________________________________
// Parse the given string as the given clause. If the datasets are not empty,
// then they are fixed during the parsing and cannot be changed by the SPARQL.
template <typename ContextType>
auto parseOperation(BnodeMgr bnodeMgr,
                    const EncodedIriManager* encodedIriManager,
                    ContextType* (SparqlAutomaticParser::*f)(void),
                    std::string operation,
                    const std::vector<DatasetClause>& datasets) {
  using S = std::string;
  // The second argument is the `PrefixMap` for QLever's internal IRIs.
  // The third argument are the datasets from outside the query, which override
  // any datasets in the query.
  sparqlParserHelpers::ParserAndVisitor p{
      bnodeMgr,
      encodedIriManager,
      std::move(operation),
      {{S{QLEVER_INTERNAL_PREFIX_NAME}, S{QLEVER_INTERNAL_PREFIX_IRI}}},
      datasets.empty()
          ? std::nullopt
          : std::optional(parsedQuery::DatasetClauses::fromClauses(datasets))};
  auto resultOfParseAndRemainingText = p.parseTypesafe(f);
  // The query rule ends with <EOF> so the parse always has to consume the whole
  // input. If this is not the case a ParseException should have been thrown at
  // an earlier point.
  AD_CONTRACT_CHECK(resultOfParseAndRemainingText.remainingText_.empty());
  return std::move(resultOfParseAndRemainingText.resultOfParse_);
}
}  // namespace

// _____________________________________________________________________________
ParsedQuery SparqlParser::parseQuery(
    const EncodedIriManager* encodedIriManager, std::string query,
    const std::vector<DatasetClause>& datasets) {
  ad_utility::BlankNodeManager bnodeMgr;
  auto res = parseOperation(&bnodeMgr, encodedIriManager, &AntlrParser::query,
                            std::move(query), datasets);
  // Queries never contain blank nodes in the body since they are always turned
  // into internal variables.
  AD_CORRECTNESS_CHECK(bnodeMgr.numBlocksUsed() == 0);
  return res;
}

// _____________________________________________________________________________
std::vector<ParsedQuery> SparqlParser::parseUpdate(
    BnodeMgr bnodeMgr, const EncodedIriManager* encodedIriManager,
    std::string update, const std::vector<DatasetClause>& datasets) {
  return parseOperation(bnodeMgr, encodedIriManager, &AntlrParser::update,
                        std::move(update), datasets);
}
