//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

#include "./util/FileTestHelpers.h"
#include "util/QueryEventLog.h"
#include "util/http/websocket/QueryId.h"

using ad_utility::websocket::OwningQueryId;
using ad_utility::websocket::QueryId;
using ad_utility::websocket::QueryRegistry;
using ad_utility::websocket::QueryStatus;
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
namespace fs = std::filesystem;
using ad_utility::QueryEventLog;
using ad_utility::testing::readLines;

std::vector<nlohmann::json> parseAll(const std::vector<std::string>& lines) {
  std::vector<nlohmann::json> out;
  out.reserve(lines.size());
  for (const auto& line : lines) {
    out.push_back(nlohmann::json::parse(line));
  }
  return out;
}

// Acceptance invariant: every JSONL line begins with the literal
// `{"ts_ms":` so the TUI can byte-slice the timestamp without a JSON
// parse.
void expectAllStartWithTsMs(const std::vector<std::string>& lines) {
  static constexpr std::string_view kPrefix = "{\"ts_ms\":";
  for (const auto& line : lines) {
    EXPECT_EQ(line.rfind(kPrefix, 0), 0u) << "bad line: " << line;
  }
}
}  // namespace

// _____________________________________________________________________________

TEST(QueryRegistry, registrationEmitsStartEventLine) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  int64_t expectedStartedAt = 0;
  {
    QueryEventLog log;
    log.setOutputFile(path);
    {
      // Registry holds `&log` and must die before `log`.
      QueryRegistry registry{&log};
      auto owned = registry.uniqueIdFromString("01123581321345589144",
                                               "SELECT * WHERE {}");
      ASSERT_TRUE(owned.has_value());
      auto active = registry.getActiveQueries();
      auto activeIt = active.find(owned->toQueryId());
      ASSERT_NE(activeIt, active.end());
      expectedStartedAt = std::chrono::duration_cast<std::chrono::milliseconds>(
                              activeIt->second.startedAt_.time_since_epoch())
                              .count();
    }  // OwningQueryId + registry destroyed → end event also pushed.
  }    // log destroyed → queue drained, file closed.

  auto lines = readLines(path);
  expectAllStartWithTsMs(lines);
  auto events = parseAll(lines);
  ASSERT_EQ(events.size(), 2u);
  const auto& start = events.front();
  EXPECT_EQ(start.at("event").get<std::string>(), "start");
  EXPECT_EQ(start.at("qid").get<std::string>(), "01123581321345589144");
  EXPECT_EQ(start.at("ts_ms").get<int64_t>(), expectedStartedAt);
  EXPECT_EQ(start.at("client_ip").get<std::string>(), "");
  EXPECT_EQ(start.at("query").get<std::string>(), "SELECT * WHERE {}");
}

// _____________________________________________________________________________

TEST(QueryRegistry, destructionEmitsEndEventLine) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  int64_t startedAtMs = 0;
  {
    QueryEventLog log;
    log.setOutputFile(path);
    {
      QueryRegistry registry{&log};
      {
        auto owned = registry.uniqueIdFromString("01123581321345589144",
                                                 "SELECT * WHERE {}");
        ASSERT_TRUE(owned.has_value());
        startedAtMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                          registry.getActiveQueries()
                              .at(owned->toQueryId())
                              .startedAt_.time_since_epoch())
                          .count();
        // `owned` goes out of scope here, firing the unregister lambda
        // which pushes the end event before the registry erases the entry.
      }
    }
  }

  auto lines = readLines(path);
  expectAllStartWithTsMs(lines);
  auto events = parseAll(lines);
  ASSERT_EQ(events.size(), 2u);
  const auto& end = events.back();
  EXPECT_EQ(end.at("event").get<std::string>(), "end");
  EXPECT_EQ(end.at("qid").get<std::string>(), "01123581321345589144");
  EXPECT_GE(end.at("ts_ms").get<int64_t>(), startedAtMs);
  // `setStatus` was never called, so the end event must carry the
  // sentinel `"unknown"` rather than be missing the field.
  EXPECT_EQ(end.at("status").get<std::string>(), "unknown");
}

// _____________________________________________________________________________

TEST(QueryRegistry, statusDefaultsToUnknown) {
  QueryRegistry registry{};
  auto owned = registry.uniqueId("my-query");
  EXPECT_EQ(owned.status(), QueryStatus::Unknown);
}

// _____________________________________________________________________________

TEST(QueryRegistry, setStatusIsObservable) {
  QueryRegistry registry{};
  auto owned = registry.uniqueId("my-query");
  ASSERT_EQ(owned.status(), QueryStatus::Unknown);

  owned.setStatus(QueryStatus::Ok);
  EXPECT_EQ(owned.status(), QueryStatus::Ok);

  owned.setStatus(QueryStatus::Failed);
  EXPECT_EQ(owned.status(), QueryStatus::Failed);
}

// _____________________________________________________________________________

// The status field must survive a move construction of `OwningQueryId`.
TEST(QueryRegistry, statusSurvivesMove) {
  QueryRegistry registry{};
  auto owned = registry.uniqueId("my-query");
  owned.setStatus(QueryStatus::Cancelled);

  OwningQueryId moved = std::move(owned);
  EXPECT_EQ(moved.status(), QueryStatus::Cancelled);
}

// _____________________________________________________________________________

TEST(QueryRegistry, duplicateIdDoesNotEmitStartEventLine) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  {
    QueryEventLog log;
    log.setOutputFile(path);
    {
      QueryRegistry registry{&log};
      auto first =
          registry.uniqueIdFromString("01123581321345589144", "first-query");
      ASSERT_TRUE(first.has_value());
      auto second =
          registry.uniqueIdFromString("01123581321345589144", "second-query");
      EXPECT_FALSE(second.has_value());
      // `first` goes out of scope at the end of this block, producing
      // one end event. Total events on disk: 1 start + 1 end = 2. If the
      // failed registration had leaked a start, we'd see 3.
    }
  }

  auto lines = readLines(path);
  expectAllStartWithTsMs(lines);
  auto events = parseAll(lines);
  ASSERT_EQ(events.size(), 2u);
  EXPECT_EQ(events[0].at("event").get<std::string>(), "start");
  EXPECT_EQ(events[0].at("query").get<std::string>(), "first-query");
  EXPECT_EQ(events[1].at("event").get<std::string>(), "end");
}

// _____________________________________________________________________________

// Status set on `OwningQueryId` must reach the end event through the
// shared-pointer indirection captured by the unregister lambda.
TEST(QueryRegistry, endEventCarriesStatusSetByCaller) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  {
    QueryEventLog log;
    log.setOutputFile(path);
    {
      QueryRegistry registry{&log};
      auto owned = registry.uniqueIdFromString("qid-with-status", "q");
      ASSERT_TRUE(owned.has_value());
      owned->setStatus(QueryStatus::Cancelled);
    }
  }

  auto events = parseAll(readLines(path));
  ASSERT_EQ(events.size(), 2u);
  EXPECT_EQ(events.back().at("event").get<std::string>(), "end");
  EXPECT_EQ(events.back().at("status").get<std::string>(), "cancelled");
}

// _____________________________________________________________________________

namespace {
/// Drive one start/end cycle through a local `QueryEventLog` while
/// applying `setStatus` to the `OwningQueryId`, then return the value of
/// the `status` field on the resulting `end` event line.
std::string runOneCycleAndReadEndStatus(QueryStatus status) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  {
    QueryEventLog log;
    log.setOutputFile(path);
    {
      QueryRegistry registry{&log};
      auto owned = registry.uniqueIdFromString("qid", "q");
      EXPECT_TRUE(owned.has_value());
      owned->setStatus(status);
    }
  }
  auto events = parseAll(readLines(path));
  EXPECT_EQ(events.size(), 2u);
  EXPECT_EQ(events.back().at("event").get<std::string>(), "end");
  return events.back().at("status").get<std::string>();
}
}  // namespace

TEST(QueryRegistry, endEventStatusOk) {
  EXPECT_EQ(runOneCycleAndReadEndStatus(QueryStatus::Ok), "ok");
}

TEST(QueryRegistry, endEventStatusFailed) {
  EXPECT_EQ(runOneCycleAndReadEndStatus(QueryStatus::Failed), "failed");
}

TEST(QueryRegistry, endEventStatusTimeout) {
  EXPECT_EQ(runOneCycleAndReadEndStatus(QueryStatus::Timeout), "timeout");
}

// _____________________________________________________________________________

/// `sharedStatus()` returns a handle that writes through to the same
/// atomic the unregister lambda will read. Simulates a third party (e.g.
/// `Server::processOperation`'s catch block) publishing the status after
/// the `OwningQueryId` has been moved out of its original scope.
TEST(QueryRegistry, sharedStatusHandlePropagatesToEndEvent) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  {
    QueryEventLog log;
    log.setOutputFile(path);
    {
      QueryRegistry registry{&log};
      auto owned = registry.uniqueIdFromString("qid-shared", "q");
      ASSERT_TRUE(owned.has_value());
      auto handle = owned->sharedStatus();
      // Move the `OwningQueryId` into a different scope; the handle
      // must still address the same atomic.
      OwningQueryId movedAway = std::move(owned.value());
      handle->store(QueryStatus::Failed);
      // `movedAway` destroyed here → end event reads through `handle`.
    }
  }

  auto events = parseAll(readLines(path));
  ASSERT_EQ(events.size(), 2u);
  EXPECT_EQ(events.back().at("status").get<std::string>(), "failed");
}
