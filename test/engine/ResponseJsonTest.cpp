// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tomas Damek <tomas.damek@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/IndexTestHelpers.h"
#include "engine/NamedResultCache.h"
#include "engine/QueryExecutionContext.h"
#include "engine/ResponseJson.h"
#include "global/Constants.h"
#include "util/ParseException.h"
#include "util/Timer.h"
#include "util/json.h"

using nlohmann::json;

// _____________________________________________________________________________
TEST(ResponseJsonTest, composeStats) {
  const Index& index = ad_utility::testing::getQec("<a> <b> <c>")->getIndex();
  json expectedJson{{"git-hash-index", "git short hash not set"},
                    {"git-hash-server", "git short hash not set"},
                    {"version-server", "project version not set"},
                    {"name-index", ""},
                    {"name-text-index", ""},
                    {"num-entity-occurrences", 0},
                    {"num-objects-internal", 0},
                    {"num-objects-normal", 1},
                    {"num-permutations", 6},
                    {"num-predicates-internal", 1},
                    {"num-predicates-normal", 1},
                    {"num-subjects-internal", 0},
                    {"num-subjects-normal", 1},
                    {"num-text-records", 0},
                    {"num-triples-internal", 1},
                    {"num-triples-normal", 1},
                    {"num-word-occurrences", 0}};
  EXPECT_THAT(responseJson::composeStats(index), testing::Eq(expectedJson));
}

// _____________________________________________________________________________
TEST(ResponseJsonTest, composeCacheStats) {
  QueryResultCache cache;
  NamedResultCache namedResultCache;
  json expectedJson{{"num-results-unpinned", 0},
                    {"num-results-pinned-unnamed", 0},
                    {"num-results-pinned-named", 0},
                    {"cache-size-unpinned", 0},
                    {"cache-size-pinned", 0}};
  EXPECT_THAT(responseJson::composeCacheStats(cache, namedResultCache),
              testing::Eq(expectedJson));
}

// _____________________________________________________________________________
TEST(ResponseJsonTest, composeErrorWithoutMetadata) {
  ad_utility::Timer requestTimer{ad_utility::Timer::Stopped};
  json expectedJson{{"query", "SELECT * WHERE { ?a ?b ?c }"},
                    {"status", "ERROR"},
                    {"resultsize", 0},
                    {"time", {{"total", 0}, {"computeResult", 0}}},
                    {"exception", "some error message"}};
  EXPECT_THAT(responseJson::composeError("SELECT * WHERE { ?a ?b ?c }",
                                         "some error message", requestTimer),
              testing::Eq(expectedJson));
}

// _____________________________________________________________________________
TEST(ResponseJsonTest, composeErrorWithMetadata) {
  ad_utility::Timer requestTimer{ad_utility::Timer::Stopped};
  ExceptionMetadata metadata{"SELECT * WHERE { ?a ?b ?c }", 7, 10, 1, 7};
  json expectedJson{{"query", "SELECT * WHERE { ?a ?b ?c }"},
                    {"status", "ERROR"},
                    {"resultsize", 0},
                    {"time", {{"total", 0}, {"computeResult", 0}}},
                    {"exception", "some error message"},
                    {"metadata",
                     {{"startIndex", 7},
                      {"stopIndex", 10},
                      {"line", 1},
                      {"positionInLine", 7}}}};
  EXPECT_THAT(
      responseJson::composeError("SELECT * WHERE { ?a ?b ?c }",
                                 "some error message", requestTimer, metadata),
      testing::Eq(expectedJson));
}

// _____________________________________________________________________________
TEST(ResponseJsonTest, composeErrorWithTruncatedMetadataOmitsLocation) {
  ad_utility::Timer requestTimer{ad_utility::Timer::Stopped};
  // `stopIndex_` at or beyond `MAX_LENGTH_OPERATION_ECHO` means the location
  // falls outside the (truncated) echoed query, so it must not be sent.
  ExceptionMetadata metadata{"SELECT * WHERE { ?a ?b ?c }", 0,
                             MAX_LENGTH_OPERATION_ECHO, 1, 0};
  json expectedJson{{"query", "SELECT * WHERE { ?a ?b ?c }"},
                    {"status", "ERROR"},
                    {"resultsize", 0},
                    {"time", {{"total", 0}, {"computeResult", 0}}},
                    {"exception", "some error message"}};
  EXPECT_THAT(
      responseJson::composeError("SELECT * WHERE { ?a ?b ?c }",
                                 "some error message", requestTimer, metadata),
      testing::Eq(expectedJson));
}
