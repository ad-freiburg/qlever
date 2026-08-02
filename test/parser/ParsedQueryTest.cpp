#include <gmock/gmock.h>

#include <optional>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "engine/QueryPlanner.h"
#include "libqlever/QleverTypes.h"
#include "parser/ParsedQuery.h"
#include "parser/SparqlParser.h"

namespace {
using namespace ad_utility::url_parser;
using namespace ad_utility::url_parser::sparqlOperation;
using namespace ad_utility::testing;

constexpr auto encodedIriManager = []() -> const EncodedIriManager* {
  static EncodedIriManager encodedIriManager_;
  return &encodedIriManager_;
};
auto parseQuery(std::string query,
                const std::vector<DatasetClause>& datasets = {}) {
  return SparqlParser::parseQuery(encodedIriManager(), std::move(query),
                                  datasets);
}
}  // namespace

// _____________________________________________________________________________
TEST(ParsedQueryTest, adjustLimitOffset) {
  using enum ad_utility::MediaType;
  auto makePlannedQuery = [](std::string operation) -> qlever::PlannedQuery {
    ParsedQuery parsed = parseQuery(std::move(operation));
    auto* qec = ad_utility::testing::getQec();
    QueryExecutionTree qet =
        QueryPlanner{qec, std::make_shared<ad_utility::CancellationHandle<>>()}
            .createExecutionTree(parsed);
    return {std::move(parsed), std::move(qet), *qec};
  };
  auto expectExportLimit =
      [&makePlannedQuery](
          ad_utility::MediaType mediaType, std::optional<uint64_t> limit,
          std::string operation =
              "SELECT * WHERE { <a> <b> ?c } LIMIT 10 OFFSET 15",
          std::optional<uint64_t> sendLimit = std::optional<uint64_t>{12},
          ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(l);
        auto pq = makePlannedQuery(std::move(operation));
        pq.parsedQuery().adjustLimitOffset(mediaType, sendLimit);
        EXPECT_THAT(pq.parsedQuery()._limitOffset.exportLimit_,
                    testing::Eq(limit));
      };

  std::string complexQuery{
      "SELECT * WHERE { ?a ?b ?c . FILTER(LANG(?a) = 'en') . "
      "BIND(RAND() as ?r) . } OFFSET 5"};

  // Check that the export limit is set for `qlever-results+json`.
  expectExportLimit(qleverJson, 12);
  expectExportLimit(qleverJson, 13, "SELECT * WHERE { <a> <b> ?c }",
                    std::optional<uint64_t>{13});
  expectExportLimit(qleverJson, 13, complexQuery, std::optional<uint64_t>{13});
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
