//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Raymond Schätzle <schaetzr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "engine/TransitivePathGraphSearch.h"
#include "engine/TransitivePathHashMap.h"
#include "util/AllocatorTestHelpers.h"

using namespace qlever::graphSearch;
using namespace ::testing;

// Test fixture which prepares TransitivePathGraphSearch testing.
class GraphSearchTest : public Test {
 protected:
  using AdjacencyList = std::unordered_map<size_t, std::vector<size_t>>;

  const ad_utility::AllocatorWithLimit<Id> allocator_ =
      ad_utility::testing::makeAllocator();
  GraphSearchExecutionParams ep_ = {
      std::make_shared<ad_utility::CancellationHandle<>>(), allocator_};

  // The `GraphSearch` functions are templated and work with e.g.
  // `HashMapWrapper` as well as `BinarySearchWrapper`. Those themselves are
  // tested in other tests, so we can assume they are working correctly and
  // just one here for testing the `GraphSearch` functions.
  // We pre-initialize a graph since every test of this fixture will need
  // one.
  std::vector<HashMapWrapper> graphs_;

  // Create a set that contains some Ids with the given size_t values.
  Set initializeSet(std::vector<size_t> values) {
    Set returned{allocator_};
    for (size_t idx : values) {
      returned.insert(Id::makeFromEncodedVal(idx));
    }
    return returned;
  }

  GraphSearchTest() {
    // Initialize the list of Graphs we want to test with.
    const std::vector<AdjacencyList> graphsAdjListRepresentation = {
        // Empty graph.
        {},
        // Single node, no edges.
        {{0, {}}},
        // Minimal loop.
        {{0, {0}}},
        // Loop using two nodes.
        {{0, {1}}, {1, {0}}},
        // Two disconnected nodes, each one looping with itself.
        {{0, {0}}, {1, {1}}},
        // "Regular" connected graph with some loops and some not reachable
        // nodes (from 0).
        {{0, {1}},
         {1, {3, 4}},
         {2, {1}},
         {3, {5}},
         {4, {1, 6}},
         {5, {2}},
         {6, {5, 7}},
         {7, {7}},
         {8, {1}}},
        // Graph with skippable node (1).
        {{0, {2, 1}}, {1, {2}}},
        // Graph two paths to a potential target, having different lengths.
        {{0, {1, 4}}, {1, {2}}, {2, {3}}, {4, {3}}},
        // Similar to graph 6, but the potential target node is one after
        // node 2.
        {{0, {1, 2}}, {1, {2}}, {2, {3}}, {3, {}}}};
    for (AdjacencyList adjList : graphsAdjListRepresentation) {
      HashMapWrapper::Map map(allocator_);
      for (const auto& pair : adjList) {
        map.insert_or_assign(Id::makeFromEncodedVal(pair.first),
                             initializeSet(pair.second));
      }
      graphs_.push_back(HashMapWrapper(map, allocator_));
    }
  }
};

// _____________________________________________________________________________
TEST_F(GraphSearchTest, binaryFirstSearch) {
  // For all graph examples, construct the expected returned sets.
  std::vector<std::vector<size_t>> expected = {
      {0}, {0}, {0}, {0, 1}, {0}, {0, 1, 2, 3, 4, 5, 6, 7}, {0, 1, 2}};

  // Iterate over all graphs and check if binarySearch will return the right
  // values.
  for (size_t i = 0; i < expected.size(); i++) {
    GraphSearchProblem<HashMapWrapper> gsp(
        graphs_.at(i), Id::makeFromEncodedVal(0), std::optional<Id>(), 0,
        std::numeric_limits<size_t>::max());
    EXPECT_THAT(runOptimalGraphSearch(gsp, ep_), initializeSet(expected.at(i)));
  }
}

// _____________________________________________________________________________
TEST_F(GraphSearchTest, binaryFirstSearchWithLimit) {
  // Organize each test case into a neat struct to hold the expected values.
  struct testVal {
    size_t graphNumber_;
    size_t minDist_;
    size_t maxDist_;
    std::vector<size_t> expected_;
  };
  std::array<testVal, 11> tests = {testVal(0, 0, 100, {0}),
                                   testVal(1, 0, 100, {0}),
                                   testVal(1, 1, 10, {}),
                                   testVal(2, 0, 10, {0}),
                                   testVal(2, 10, 11, {0}),
                                   testVal(3, 0, 1, {1, 0}),
                                   testVal(3, 1, 1, {1}),
                                   testVal(4, 0, 100, {0}),
                                   testVal(5, 1, 2, {1, 4, 3}),
                                   testVal(5, 10, 100, {1, 2, 3, 4, 5, 6, 7}),
                                   testVal(2, 10001, 1000001, {0})};

  for (testVal test : tests) {
    GraphSearchProblem<HashMapWrapper> gsp(
        graphs_.at(test.graphNumber_), Id::makeFromEncodedVal(0),
        std::optional<Id>(), test.minDist_, test.maxDist_);

    EXPECT_THAT(runOptimalGraphSearch(gsp, ep_), initializeSet(test.expected_));
  }
}

// _____________________________________________________________________________
TEST_F(GraphSearchTest, depthFirstSearch) {
  struct testVal {
    size_t graphNumber_;
    size_t target_;
    std::vector<size_t> expected_;
  };
  std::array<testVal, 10> tests = {testVal(0, 0, {0}), testVal(0, 1, {}),
                                   testVal(1, 0, {0}), testVal(1, 1, {}),
                                   testVal(2, 0, {0}), testVal(3, 0, {0}),
                                   testVal(3, 1, {1}), testVal(4, 1, {}),
                                   testVal(5, 7, {7}), testVal(5, 8, {})};

  for (testVal test : tests) {
    GraphSearchProblem<HashMapWrapper> gsp(
        graphs_.at(test.graphNumber_), Id::makeFromEncodedVal(0),
        Id::makeFromEncodedVal(test.target_), 0,
        std::numeric_limits<size_t>::max());

    EXPECT_THAT(runOptimalGraphSearch(gsp, ep_), initializeSet(test.expected_))
        << "Failure at graph " << test.graphNumber_ << ", trying to find node "
        << test.target_ << ".";
  }
}

// _____________________________________________________________________________
TEST_F(GraphSearchTest, depthFirstSearchWithLimit) {
  struct testVal {
    size_t graphNumber_;
    size_t target_;
    size_t minDist_;
    size_t maxDist_;
    std::vector<size_t> expected_;
  };
  std::array<testVal, 16> tests = {
      testVal(0, 0, 0, 10, {0}),    testVal(0, 0, 10, 100, {}),
      testVal(1, 0, 0, 100, {0}),   testVal(1, 1, 0, 100, {}),
      testVal(2, 0, 100, 200, {0}), testVal(3, 1, 0, 0, {}),
      testVal(3, 0, 100, 100, {0}), testVal(4, 1, 0, 1000, {}),
      testVal(5, 8, 0, 10000, {}),  testVal(5, 7, 100, 999, {7}),
      testVal(5, 0, 1, 100, {}),    testVal(5, 4, 5, 1000, {4}),
      testVal(6, 2, 0, 1, {2}),     testVal(7, 3, 0, 2, {3}),
      testVal(2, 0, 100, 100, {0}), testVal(8, 3, 0, 2, {3})};

  for (testVal test : tests) {
    GraphSearchProblem<HashMapWrapper> gsp(
        graphs_.at(test.graphNumber_), Id::makeFromEncodedVal(0),
        Id::makeFromEncodedVal(test.target_), test.minDist_, test.maxDist_);

    EXPECT_THAT(runOptimalGraphSearch(gsp, ep_), initializeSet(test.expected_))
        << "Failure at graph " << test.graphNumber_ << ",trying to find node "
        << test.target_ << " in distance limits " << test.minDist_ << " to "
        << test.maxDist_ << ".";
  }
}

// ___________________________________________________________________________
TEST(GraphSearchTestExtraTests, wronglyCalledDFSWithNoTarget) {
  // Test that DFS, if no target node given, just
  // skips searching altogether and returns an
  // empty set. Normally, the
  // `runOptimalGraphSearch` function which rules
  // out such cases should be used so this is only
  // implemented and tested for completeness reasons.
  auto allocator = ad_utility::testing::makeAllocator();
  qlever::graphSearch::GraphSearchExecutionParams ep(
      std::make_shared<ad_utility::CancellationHandle<>>(), allocator);

  auto graph = HashMapWrapper{HashMapWrapper::Map(allocator), allocator};
  qlever::graphSearch::GraphSearchProblem gsp(graph, Id::makeFromInt(0),
                                              std::optional<Id>(), 0, 100);

  {  // Unlimited DFS.
    Set actual = qlever::graphSearch::depthFirstSearch(gsp, ep);
    EXPECT_EQ(actual, Set{allocator});
  }
  {  // Limited DFS.
    Set actual = qlever::graphSearch::depthFirstSearchWithLimit(gsp, ep);
    EXPECT_EQ(actual, Set{allocator});
  }
}
