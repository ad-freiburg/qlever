// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Tomas Damek <tomas.damek@email.uni-freiburg.de>

#include <gmock/gmock.h>

#include <optional>

#include "../util/GTestHelpers.h"
#include "../util/ParsedQueryTestHelpers.h"
#include "parser/ParsedQuery.h"

using ad_utility::testing::parseQuery;

// _____________________________________________________________________________
TEST(ParsedQueryTest, updateExportLimit) {
  auto expectExportLimit =
      [](std::optional<uint64_t> limit,
         std::string operation =
             "SELECT * WHERE { <a> <b> ?c } LIMIT 10 OFFSET 15",
         std::optional<uint64_t> sendLimit = 12,
         ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(l);
        ParsedQuery pq = parseQuery(std::move(operation));
        pq.updateExportLimit(sendLimit);
        EXPECT_THAT(pq._limitOffset.exportLimit_, testing::Eq(limit));
      };
  // Use different queries with and without LIMIT/OFFSET to ensure that the
  // export limit is not affected by query-specific LIMIT/OFFSET clauses.
  std::string complexQuery{
      "SELECT * WHERE { ?a ?b ?c . FILTER(LANG(?a) = 'en') . "
      "BIND(RAND() as ?r) . } OFFSET 5"};

  // The export limit is set to `sendLimit` when it is given.
  expectExportLimit(12);
  expectExportLimit(13, "SELECT * WHERE { <a> <b> ?c }", 13);
  expectExportLimit(13, complexQuery, 13);

  // No export limit is set when no send limit is given.
  expectExportLimit(std::nullopt, "SELECT * WHERE { <a> <b> ?c }",
                    std::nullopt);
}
