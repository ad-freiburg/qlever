//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include <sstream>

#include "util/Log.h"
#include "util/http/websocket/QueryId.h"

using ad_utility::websocket::OwningQueryId;
using ad_utility::websocket::QueryId;
using ad_utility::websocket::QueryRegistry;
using ::testing::Field;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

TEST(QueryId, checkIdEqualityRelation) {
  auto queryIdOne = QueryId::idFromString("some-id");
  auto queryIdTwo = QueryId::idFromString("some-id");
  auto queryIdThree = QueryId::idFromString("other-id");

  EXPECT_EQ(queryIdOne, queryIdOne);
  EXPECT_EQ(queryIdOne, queryIdTwo);
  EXPECT_EQ(queryIdTwo, queryIdOne);

  EXPECT_NE(queryIdOne, queryIdThree);
  EXPECT_NE(queryIdTwo, queryIdThree);
  EXPECT_NE(queryIdThree, queryIdOne);
  EXPECT_NE(queryIdThree, queryIdTwo);
}

TEST(QueryId, checkEmptyIdDisallowedByConstruction) {
  EXPECT_THROW(QueryId::idFromString(""), ad_utility::Exception);
}

// _____________________________________________________________________________

// Note there's no guarantee for std::string to be empty after being
// moved out of, but we check for it anyway to provide a reference case
TEST(QueryId, checkEmptyAfterMove) {
  auto queryId = QueryId::idFromString("53.32794768794578, -2.230040905974742");
  { auto temporary = std::move(queryId); }
  EXPECT_TRUE(queryId.empty());
}

// _____________________________________________________________________________

TEST(QueryId, veriyToJsonWorks) {
  nlohmann::json json = QueryId::idFromString("test-id");
  EXPECT_EQ(json.get<std::string_view>(), "test-id");
}

// _____________________________________________________________________________

TEST(QueryRegistry, verifyActiveQueryInfoToJsonWorks) {
  using ActiveQueryInfo = QueryRegistry::ActiveQueryInfo;
  auto startedAt = std::chrono::system_clock::time_point{} +
                   std::chrono::milliseconds{1700000000123};
  ActiveQueryInfo info{"my-query", startedAt};

  nlohmann::json json = info;

  EXPECT_EQ(json.at("query").get<std::string_view>(), "my-query");
  EXPECT_EQ(json.at("started-at").get<int64_t>(), 1700000000123);
}

// _____________________________________________________________________________

TEST(QueryRegistry, verifyUniqueIdProvidesUniqueIds) {
  QueryRegistry registry{};
  auto queryIdOne = registry.uniqueId("my-query");
  auto queryIdTwo = registry.uniqueId("my-query");

  EXPECT_NE(queryIdOne.toQueryId(), queryIdTwo.toQueryId());
}

// _____________________________________________________________________________

TEST(QueryRegistry, verifyUniqueIdFromStringEnforcesUniqueness) {
  QueryRegistry registry{};
  auto optionalQueryIdOne =
      registry.uniqueIdFromString("01123581321345589144", "my-query");
  auto optionalQueryIdTwo =
      registry.uniqueIdFromString("01123581321345589144", "my-query");

  EXPECT_TRUE(optionalQueryIdOne.has_value());
  EXPECT_FALSE(optionalQueryIdTwo.has_value());
}

// _____________________________________________________________________________

TEST(QueryRegistry, verifyIdIsUnregisteredAfterUse) {
  QueryRegistry registry{};
  {
    auto optionalQueryId =
        registry.uniqueIdFromString("01123581321345589144", "my-query");
    EXPECT_TRUE(optionalQueryId.has_value());
  }
  {
    auto optionalQueryId =
        registry.uniqueIdFromString("01123581321345589144", "my-query");
    EXPECT_TRUE(optionalQueryId.has_value());
  }
}

// _____________________________________________________________________________

TEST(QueryRegistry, demonstrateRegistryLocalUniqueness) {
  QueryRegistry registryOne{};
  QueryRegistry registryTwo{};
  auto optQidOne =
      registryOne.uniqueIdFromString("01123581321345589144", "my-query");
  auto optQidTwo =
      registryTwo.uniqueIdFromString("01123581321345589144", "my-query");
  ASSERT_TRUE(optQidOne.has_value());
  ASSERT_TRUE(optQidTwo.has_value());
  // The QueryId object doesn't know anything about registries,
  // so the values should equal
  EXPECT_EQ(optQidOne->toQueryId(), optQidTwo->toQueryId());
}

// _____________________________________________________________________________

// The code should guard against the case where the ids outlive their registries
// so this should not segfault or raise any address sanitizer errors
TEST(QueryRegistry, performCleanupFromDestroyedRegistry) {
  std::unique_ptr<OwningQueryId> holder;
  {
    QueryRegistry registry{};
    holder = std::make_unique<OwningQueryId>(registry.uniqueId("my-query"));
  }
}

// _____________________________________________________________________________

TEST(QueryRegistry, verifyCancellationHandleIsCreated) {
  QueryRegistry registry{};
  auto queryId = registry.uniqueId("my-query");

  auto handle1 = registry.getCancellationHandle(queryId.toQueryId());
  auto handle2 = registry.getCancellationHandle(queryId.toQueryId());

  EXPECT_EQ(handle1, handle2);
  EXPECT_NE(handle1, nullptr);
  EXPECT_NE(handle2, nullptr);
}

// _____________________________________________________________________________

TEST(QueryRegistry, verifyCancellationHandleIsNullptrIfNotPresent) {
  QueryRegistry registry{};

  auto handle =
      registry.getCancellationHandle(QueryId::idFromString("does not exist"));

  EXPECT_EQ(handle, nullptr);
}

// _____________________________________________________________________________

TEST(QueryRegistry, verifyGetActiveQueriesReturnsAllActiveQueries) {
  using ActiveQueryInfo = QueryRegistry::ActiveQueryInfo;
  QueryRegistry registry{};

  EXPECT_THAT(registry.getActiveQueries(), IsEmpty());

  {
    auto queryId1 = registry.uniqueId("my-query");

    EXPECT_THAT(registry.getActiveQueries(),
                UnorderedElementsAre(
                    Pair(queryId1.toQueryId(),
                         Field(&ActiveQueryInfo::query_, "my-query"))));

    {
      auto queryId2 = registry.uniqueId("other-query");

      EXPECT_THAT(registry.getActiveQueries(),
                  UnorderedElementsAre(
                      Pair(queryId1.toQueryId(),
                           Field(&ActiveQueryInfo::query_, "my-query")),
                      Pair(queryId2.toQueryId(),
                           Field(&ActiveQueryInfo::query_, "other-query"))));
    }

    EXPECT_THAT(registry.getActiveQueries(),
                UnorderedElementsAre(
                    Pair(queryId1.toQueryId(),
                         Field(&ActiveQueryInfo::query_, "my-query"))));
  }

  EXPECT_THAT(registry.getActiveQueries(), IsEmpty());
}

// _____________________________________________________________________________

namespace {
// Extracts every JSON payload from log lines that carry the " METRIC: " tag.
// Each METRIC line in the captured log has the shape:
//   "<timestamp> - METRIC: <json>\n"
// so we split on newlines, find the tag, and parse the trailing JSON.
std::vector<nlohmann::json> extractMetricLines(const std::string& log) {
  std::vector<nlohmann::json> out;
  static constexpr std::string_view kTag = " METRIC: ";
  size_t pos = 0;
  while (pos < log.size()) {
    size_t newline = log.find('\n', pos);
    auto line = log.substr(pos, newline - pos);
    auto tagAt = line.find(kTag);
    if (tagAt != std::string::npos) {
      out.push_back(nlohmann::json::parse(line.substr(tagAt + kTag.size())));
    }
    if (newline == std::string::npos) break;
    pos = newline + 1;
  }
  return out;
}
}  // namespace

// _____________________________________________________________________________

TEST(QueryRegistry, registrationEmitsStartMetricLine) {
  absl::Cleanup cleanup{
      []() { ad_utility::setGlobalLoggingStream(&std::cout); }};
  std::ostringstream logStream;
  ad_utility::setGlobalLoggingStream(&logStream);

  QueryRegistry registry{};
  auto owned =
      registry.uniqueIdFromString("01123581321345589144", "SELECT * WHERE {}");
  ASSERT_TRUE(owned.has_value());

  // The `started-at` we logged should match what `getActiveQueries()` reports
  // for this query, so the live and historic views the TUI consumes agree.
  auto active = registry.getActiveQueries();
  auto activeIt = active.find(owned->toQueryId());
  ASSERT_NE(activeIt, active.end());
  auto expectedStartedAt =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          activeIt->second.startedAt_.time_since_epoch())
          .count();

  auto metrics = extractMetricLines(logStream.str());
  // Only the start line should have been emitted at this point.
  ASSERT_EQ(metrics.size(), 1u);
  const auto& start = metrics.front();
  EXPECT_EQ(start.at("event").get<std::string>(), "start");
  EXPECT_EQ(start.at("query-id").get<std::string>(), "01123581321345589144");
  EXPECT_EQ(start.at("started-at").get<int64_t>(), expectedStartedAt);
  EXPECT_EQ(start.at("query").get<std::string>(), "SELECT * WHERE {}");
}

// _____________________________________________________________________________

TEST(QueryRegistry, destructionEmitsEndMetricLine) {
  absl::Cleanup cleanup{
      []() { ad_utility::setGlobalLoggingStream(&std::cout); }};
  std::ostringstream logStream;
  ad_utility::setGlobalLoggingStream(&logStream);

  QueryRegistry registry{};
  int64_t startedAtMs = 0;
  {
    auto owned = registry.uniqueIdFromString("01123581321345589144",
                                             "SELECT * WHERE {}");
    ASSERT_TRUE(owned.has_value());
    auto active = registry.getActiveQueries();
    startedAtMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            active.at(owned->toQueryId()).startedAt_.time_since_epoch())
            .count();
    // `owned` goes out of scope at the closing brace, triggering the
    // unregister lambda which should emit the end metric line.
  }

  auto metrics = extractMetricLines(logStream.str());
  ASSERT_EQ(metrics.size(), 2u);
  const auto& end = metrics.back();
  EXPECT_EQ(end.at("event").get<std::string>(), "end");
  EXPECT_EQ(end.at("query-id").get<std::string>(), "01123581321345589144");
  EXPECT_GE(end.at("ended-at").get<int64_t>(), startedAtMs);
}

// _____________________________________________________________________________

TEST(QueryRegistry, duplicateIdDoesNotEmitStartMetricLine) {
  absl::Cleanup cleanup{
      []() { ad_utility::setGlobalLoggingStream(&std::cout); }};
  std::ostringstream logStream;
  ad_utility::setGlobalLoggingStream(&logStream);

  QueryRegistry registry{};
  auto first =
      registry.uniqueIdFromString("01123581321345589144", "first-query");
  ASSERT_TRUE(first.has_value());
  auto second =
      registry.uniqueIdFromString("01123581321345589144", "second-query");
  EXPECT_FALSE(second.has_value());

  auto metrics = extractMetricLines(logStream.str());
  // Exactly one start line — for the successful first registration. The
  // failed second registration must not emit anything.
  ASSERT_EQ(metrics.size(), 1u);
  EXPECT_EQ(metrics.front().at("event").get<std::string>(), "start");
  EXPECT_EQ(metrics.front().at("query").get<std::string>(), "first-query");
}
