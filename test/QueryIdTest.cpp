//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

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

// An out-of-range value exercises the trailing fallback after the exhaustive
// switch in `toString`, which is otherwise unreachable.
TEST(QueryStatus, toStringFallbackForUnknownValue) {
  EXPECT_EQ(ad_utility::websocket::toString(static_cast<QueryStatus>(42)),
            "unknown");
}

// _____________________________________________________________________________

// The registry fires registered callbacks; the tests below record what each
// callback receives and assert on it directly.

namespace {
// `StartInfo`/`EndInfo` carry references/`string_view`s, so copy the values out
// while the callback runs and they are still valid.
struct StartRecord {
  QueryId queryId_;
  std::string query_;
  std::string clientIp_;
  std::chrono::system_clock::time_point startedAt_;
};
struct EndRecord {
  QueryId queryId_;
  QueryStatus status_;
  std::chrono::system_clock::time_point endedAt_;
};

StartRecord toRecord(const QueryRegistry::StartInfo& info) {
  return {info.queryId_, std::string{info.query_}, std::string{info.clientIp_},
          info.startedAt_};
}
EndRecord toRecord(const QueryRegistry::EndInfo& info) {
  return {info.queryId_, info.status_, info.endedAt_};
}
}  // namespace

// _____________________________________________________________________________

// A successful registration fires the start callback once with matching fields;
// `startedAt_` equals the instant recorded for the active query.
TEST(QueryRegistry, onStartFiresWithQueryDetails) {
  QueryRegistry registry{};
  std::vector<StartRecord> starts;
  registry.addOnStart([&starts](const QueryRegistry::StartInfo& info) {
    starts.push_back(toRecord(info));
  });

  auto owned = registry.uniqueIdFromString("01123581321345589144",
                                           "SELECT * WHERE {}", "10.0.0.5");
  ASSERT_TRUE(owned.has_value());

  ASSERT_EQ(starts.size(), 1u);
  EXPECT_EQ(starts.at(0).queryId_, owned->toQueryId());
  EXPECT_EQ(starts.at(0).query_, "SELECT * WHERE {}");
  EXPECT_EQ(starts.at(0).clientIp_, "10.0.0.5");
  auto active = registry.getActiveQueries();
  EXPECT_EQ(epochMillis(starts.at(0).startedAt_),
            epochMillis(active.at(owned->toQueryId()).startedAt_));
}

// _____________________________________________________________________________

// A rejected duplicate id returns early, before firing the start callback.
TEST(QueryRegistry, onStartNotFiredForDuplicateId) {
  QueryRegistry registry{};
  int starts = 0;
  registry.addOnStart([&starts](const QueryRegistry::StartInfo&) { ++starts; });

  auto first = registry.uniqueIdFromString("01123581321345589144", "first");
  ASSERT_TRUE(first.has_value());
  auto second = registry.uniqueIdFromString("01123581321345589144", "second");
  EXPECT_FALSE(second.has_value());

  EXPECT_EQ(starts, 1);
}

// _____________________________________________________________________________

// Destroying the `OwningQueryId` fires the end callback once, with the qid and
// the default `Unknown` status (no `setStatus` was called).
TEST(QueryRegistry, onEndFiresAtDestructionWithDefaultStatus) {
  QueryRegistry registry{};
  std::vector<EndRecord> ends;
  registry.addOnEnd([&ends](const QueryRegistry::EndInfo& info) {
    ends.push_back(toRecord(info));
  });

  {
    auto owned = registry.uniqueIdFromString("01123581321345589144", "q");
    ASSERT_TRUE(owned.has_value());
    EXPECT_THAT(ends, IsEmpty());
  }

  ASSERT_EQ(ends.size(), 1u);
  EXPECT_EQ(ends.at(0).queryId_, QueryId::idFromString("01123581321345589144"));
  EXPECT_EQ(ends.at(0).status_, QueryStatus::Unknown);
}

// _____________________________________________________________________________

namespace {
// Run one register -> setStatus -> destroy cycle; return the status the end
// callback observed.
QueryStatus runCycleCaptureEndStatus(QueryStatus toSet) {
  QueryRegistry registry{};
  std::optional<QueryStatus> captured;
  registry.addOnEnd([&captured](const QueryRegistry::EndInfo& info) {
    captured = info.status_;
  });
  {
    auto owned = registry.uniqueIdFromString("qid", "q");
    EXPECT_TRUE(owned.has_value());
    owned->setStatus(toSet);
  }
  EXPECT_TRUE(captured.has_value());
  return captured.value_or(QueryStatus::Unknown);
}
}  // namespace

// The status set on the `OwningQueryId` reaches `EndInfo.status_`.
TEST(QueryRegistry, onEndStatusReflectsSetStatusOk) {
  EXPECT_EQ(runCycleCaptureEndStatus(QueryStatus::Ok), QueryStatus::Ok);
}
TEST(QueryRegistry, onEndStatusReflectsSetStatusFailed) {
  EXPECT_EQ(runCycleCaptureEndStatus(QueryStatus::Failed), QueryStatus::Failed);
}
TEST(QueryRegistry, onEndStatusReflectsSetStatusCancelled) {
  EXPECT_EQ(runCycleCaptureEndStatus(QueryStatus::Cancelled),
            QueryStatus::Cancelled);
}
TEST(QueryRegistry, onEndStatusReflectsSetStatusTimeout) {
  EXPECT_EQ(runCycleCaptureEndStatus(QueryStatus::Timeout),
            QueryStatus::Timeout);
}

// _____________________________________________________________________________

// The unregister lambda holds the end callbacks via `shared_ptr`, so the end
// event fires even if the registry is destroyed before the `OwningQueryId`.
TEST(QueryRegistry, onEndFiresEvenWhenRegistryDestroyedFirst) {
  int ends = 0;
  std::optional<QueryRegistry> registry{std::in_place};
  registry->addOnEnd([&ends](const QueryRegistry::EndInfo&) { ++ends; });

  auto owned = registry->uniqueIdFromString("01123581321345589144", "q");
  ASSERT_TRUE(owned.has_value());

  registry.reset();  // registry gone; the weak_ptr in the lambda expires
  EXPECT_EQ(ends, 0);
  owned.reset();  // end callback still fires
  EXPECT_EQ(ends, 1);
}

// _____________________________________________________________________________

// `sharedStatus()` returns a handle to the same atomic the end callback reads,
// so a write through it reaches the end event even after the id is moved.
TEST(QueryRegistry, sharedStatusReachesEndCallback) {
  QueryRegistry registry{};
  std::optional<QueryStatus> captured;
  registry.addOnEnd([&captured](const QueryRegistry::EndInfo& info) {
    captured = info.status_;
  });

  {
    auto owned = registry.uniqueIdFromString("qid-shared", "q");
    ASSERT_TRUE(owned.has_value());
    auto handle = owned->sharedStatus();
    OwningQueryId movedAway = std::move(owned.value());
    handle->store(QueryStatus::Failed);
  }

  ASSERT_TRUE(captured.has_value());
  EXPECT_EQ(captured.value(), QueryStatus::Failed);
}

// _____________________________________________________________________________

// All registered start and end callbacks fire, not just the first.
TEST(QueryRegistry, multipleCallbacksAllFire) {
  QueryRegistry registry{};
  int starts = 0;
  int ends = 0;
  registry.addOnStart([&starts](const QueryRegistry::StartInfo&) { ++starts; });
  registry.addOnStart([&starts](const QueryRegistry::StartInfo&) { ++starts; });
  registry.addOnEnd([&ends](const QueryRegistry::EndInfo&) { ++ends; });
  registry.addOnEnd([&ends](const QueryRegistry::EndInfo&) { ++ends; });

  {
    auto owned = registry.uniqueIdFromString("01123581321345589144", "q");
    ASSERT_TRUE(owned.has_value());
    EXPECT_EQ(starts, 2);
    EXPECT_EQ(ends, 0);
  }
  EXPECT_EQ(ends, 2);
}

// _____________________________________________________________________________

// Two successful registrations produce two starts and two ends; the rejected
// duplicate produces neither.
TEST(QueryRegistry, exactlyOneEndPerStart) {
  QueryRegistry registry{};
  int starts = 0;
  int ends = 0;
  registry.addOnStart([&starts](const QueryRegistry::StartInfo&) { ++starts; });
  registry.addOnEnd([&ends](const QueryRegistry::EndInfo&) { ++ends; });

  {
    auto a = registry.uniqueIdFromString("id-a", "q");
    auto b = registry.uniqueIdFromString("id-b", "q");
    auto dup = registry.uniqueIdFromString("id-a", "q");
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    ASSERT_FALSE(dup.has_value());
  }

  EXPECT_EQ(starts, 2);
  EXPECT_EQ(ends, 2);
}
