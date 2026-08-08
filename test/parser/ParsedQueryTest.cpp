// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Tomas Damek <tomas.damek@email.uni-freiburg.de>

#include <gmock/gmock.h>

#include <optional>

#include "../util/GTestHelpers.h"
#include "../util/ParsedQueryTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "parser/ParsedQuery.h"

using ad_utility::testing::parseQuery;

// _____________________________________________________________________________
TEST(ParsedQueryTest, updateExportLimit) {
  using enum ad_utility::MediaType;
  auto expectExportLimit =
      [](ad_utility::MediaType mediaType, std::optional<uint64_t> limit,
         std::string operation =
             "SELECT * WHERE { <a> <b> ?c } LIMIT 10 OFFSET 15",
         std::optional<uint64_t> sendLimit = 12,
         ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(l);
        ParsedQuery pq = parseQuery(std::move(operation));
        pq.updateExportLimit(mediaType, sendLimit);
        EXPECT_THAT(pq._limitOffset.exportLimit_, testing::Eq(limit));
      };
  // Use different queries with and without LIMIT/OFFSET to ensure that the
  // export limit is not affected by query-specific LIMIT/OFFSET clauses.
  std::string complexQuery{
      "SELECT * WHERE { ?a ?b ?c . FILTER(LANG(?a) = 'en') . "
      "BIND(RAND() as ?r) . } OFFSET 5"};

  // Check that the export limit is set for `qlever-results+json`.
  expectExportLimit(qleverJson, 12);
  expectExportLimit(qleverJson, 13, "SELECT * WHERE { <a> <b> ?c }", 13);
  expectExportLimit(qleverJson, 13, complexQuery, 13);
  // Check that the export limit is set for `sparql-results+json` if and
  // only if the runtime parameter `sparql-results-json-with-time`  is set.
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::sparqlResultsJsonWithTime_>(true);
    expectExportLimit(sparqlJson, 12);
  }
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::sparqlResultsJsonWithTime_>(false);
    expectExportLimit(sparqlJson, std::nullopt);
  }
  // Check that no export limit is set for other media types.
  expectExportLimit(csv, std::nullopt);
  expectExportLimit(csv, std::nullopt, complexQuery);
  expectExportLimit(tsv, std::nullopt);
}
