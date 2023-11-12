//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "util/http/websocket/QueryId.h"

using ad_utility::websocket::OwningQueryId;
using ad_utility::websocket::QueryId;
using ad_utility::websocket::QueryRegistry;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
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

TEST(QueryRegistry, verifyUniqueIdProvidesUniqueIds) {
  QueryRegistry registry{};
  auto queryIdOne = registry.uniqueId();
  auto queryIdTwo = registry.uniqueId();

  EXPECT_NE(queryIdOne.toQueryId(), queryIdTwo.toQueryId());
}

// _____________________________________________________________________________

TEST(QueryRegistry, verifyUniqueIdFromStringEnforcesUniqueness) {
  QueryRegistry registry{};
  auto optionalQueryIdOne = registry.uniqueIdFromString("01123581321345589144");
  auto optionalQueryIdTwo = registry.uniqueIdFromString("01123581321345589144");

  EXPECT_TRUE(optionalQueryIdOne.has_value());
  EXPECT_FALSE(optionalQueryIdTwo.has_value());
}

// _____________________________________________________________________________

TEST(QueryRegistry, verifyIdIsUnregisteredAfterUse) {
  QueryRegistry registry{};
  {
    auto optionalQueryId = registry.uniqueIdFromString("01123581321345589144");
    EXPECT_TRUE(optionalQueryId.has_value());
  }
  {
    auto optionalQueryId = registry.uniqueIdFromString("01123581321345589144");
    EXPECT_TRUE(optionalQueryId.has_value());
  }
}

// _____________________________________________________________________________

TEST(QueryRegistry, demonstrateRegistryLocalUniqueness) {
  QueryRegistry registryOne{};
  QueryRegistry registryTwo{};
  auto optQidOne = registryOne.uniqueIdFromString("01123581321345589144");
  auto optQidTwo = registryTwo.uniqueIdFromString("01123581321345589144");
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
    holder = std::make_unique<OwningQueryId>(registry.uniqueId());
  }
}

// _____________________________________________________________________________

TEST(QueryRegistry, verifyCancellationHandleIsCreated) {
  QueryRegistry registry{};
  auto queryId = registry.uniqueId();

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
  QueryRegistry registry{};

  EXPECT_THAT(registry.getActiveQueries(), IsEmpty());

  {
    auto queryId1 = registry.uniqueId();

    EXPECT_THAT(registry.getActiveQueries(), ElementsAre(queryId1.toQueryId()));

    {
      auto queryId2 = registry.uniqueId();

      EXPECT_THAT(
          registry.getActiveQueries(),
          UnorderedElementsAre(queryId1.toQueryId(), queryId2.toQueryId()));
    }

    EXPECT_THAT(registry.getActiveQueries(), ElementsAre(queryId1.toQueryId()));
  }

  EXPECT_THAT(registry.getActiveQueries(), IsEmpty());
}
