// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gtest/gtest.h>

#include "../MaterializedViewsTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "index/GraphManager.h"
#include "util/HashSet.h"

class GraphManagerTest : public ::testing::Test {
 protected:
  Index index = ad_utility::testing::makeTestIndex(
      "GraphManagerTest", ad_utility::testing::TestIndexConfig{});
  LocalVocab localVocab;
  std::function<Id(const std::string&)> getId =
      ad_utility::testing::makeGetId(index);

  Id lvIri(std::string_view iri) {
    return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
        LocalVocabEntry(ad_utility::triple_component::Iri::fromIriref(iri))));
  }
};

// ============================================================================
// GraphManager tests
// ============================================================================

TEST_F(GraphManagerTest, FromExistingGraphs) {
  ad_utility::HashSet<Id> graphs{getId("<x>"), getId("<y>"), getId("<z>")};
  auto gm = GraphManager::fromExistingGraphs(graphs);

  EXPECT_FALSE(gm.graphDoesntExist(getId("<x>")));
  EXPECT_FALSE(gm.graphDoesntExist(getId("<x>")));
  EXPECT_FALSE(gm.graphDoesntExist(getId("<z>")));
  EXPECT_TRUE(gm.graphDoesntExist(getId("<zz>")));
}

TEST_F(GraphManagerTest, AddGraphsWithVocabIndex) {
  auto gm = GraphManager::fromExistingGraphs({});
  EXPECT_TRUE(gm.graphDoesntExist(getId("<x>")));

  gm.addGraphs({getId("<x>"), getId("<y>")});

  EXPECT_FALSE(gm.graphDoesntExist(getId("<x>")));
  EXPECT_FALSE(gm.graphDoesntExist(getId("<y>")));
  EXPECT_TRUE(gm.graphDoesntExist(getId("<z>")));
}

TEST_F(GraphManagerTest, GraphExists) {
  auto lvi1 = lvIri("<bar>");

  auto gm = GraphManager::fromExistingGraphs({});
  EXPECT_TRUE(gm.graphDoesntExist(getId("<x>")));
  EXPECT_TRUE(gm.graphDoesntExist(getId("<y>")));
  EXPECT_TRUE(gm.graphDoesntExist(lvi1));

  gm.addGraphs({getId("<x>")});
  EXPECT_FALSE(gm.graphDoesntExist(getId("<x>")));
  EXPECT_TRUE(gm.graphDoesntExist(getId("<y>")));
  EXPECT_TRUE(gm.graphDoesntExist(lvi1));

  gm.addGraphs({lvi1});
  EXPECT_FALSE(gm.graphDoesntExist(getId("<x>")));
  EXPECT_TRUE(gm.graphDoesntExist(getId("<y>")));
  EXPECT_FALSE(gm.graphDoesntExist(lvi1));
}

TEST_F(GraphManagerTest, GetGraphs) {
  ad_utility::HashSet<Id> expected{getId("<x>"), getId("<y>")};
  auto gm = GraphManager::fromExistingGraphs(expected);

  auto locked = gm.getGraphs();
  EXPECT_EQ(*locked, expected);
}

TEST_F(GraphManagerTest, GetNamespaceManagerUninitialized) {
  auto gm = GraphManager::fromExistingGraphs({});
  EXPECT_ANY_THROW(gm.getNamespaceManager());
}

// ============================================================================
// GraphNamespaceManager tests
// ============================================================================

TEST(GraphNamespaceManager, AllocateNewGraph) {
  GraphManager::GraphNamespaceManager nsm("<http://example.org/graph/", 0);

  auto iri = nsm.allocateNewGraph();
  EXPECT_EQ(iri.toStringRepresentation(), "<http://example.org/graph/0>");
}

TEST(GraphNamespaceManager, AllocateMultipleGraphs) {
  GraphManager::GraphNamespaceManager nsm("<http://example.org/g/", 0);

  auto iri0 = nsm.allocateNewGraph();
  auto iri1 = nsm.allocateNewGraph();
  auto iri2 = nsm.allocateNewGraph();

  EXPECT_EQ(iri0.toStringRepresentation(), "<http://example.org/g/0>");
  EXPECT_EQ(iri1.toStringRepresentation(), "<http://example.org/g/1>");
  EXPECT_EQ(iri2.toStringRepresentation(), "<http://example.org/g/2>");
}

// ============================================================================
// Serialization (JSON round-trip) tests
// ============================================================================

TEST(GraphNamespaceManager, JsonRoundTrip) {
  GraphManager::GraphNamespaceManager original("<http://example.org/ns/", 42);

  nlohmann::json j;
  to_json(j, original);

  GraphManager::GraphNamespaceManager restored;
  from_json(j, restored);

  auto iri = restored.allocateNewGraph();
  EXPECT_EQ(iri.toStringRepresentation(), "<http://example.org/ns/42>");
}

TEST_F(GraphManagerTest, JsonRoundTrip) {
  ad_utility::HashSet<Id> graphs{getId("<x>"), getId("<y>")};
  auto gm = GraphManager::fromExistingGraphs(graphs);
  gm.initializeNamespaceManager("<http://example.org/ns/", index.getVocab());

  nlohmann::json j;
  to_json(j, gm);

  auto restored = GraphManager::fromExistingGraphs({});
  from_json(j, restored);

  EXPECT_EQ(*restored.getGraphs(), graphs);
  EXPECT_EQ(restored.getNamespaceManager()
                .allocateNewGraph()
                .toStringRepresentation(),
            "<http://example.org/ns/0>");
}
