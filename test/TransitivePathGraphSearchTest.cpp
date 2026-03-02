//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Raymond Schätzle <schaetzr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <chrono>
#include <exception>
#include <memory>
#include <string>
#include <thread>

#include "engine/TransitivePathBinSearch.h"
#include "engine/TransitivePathGraphSearch.h"
#include "engine/TransitivePathHashMap.h"
#include "global/Constants.h"
#include "util/AllocatorTestHelpers.h"
#include "util/AllocatorWithLimit.h"
#include "util/CancellationHandle.h"
#include "util/Log.h"

using namespace qlever::graphSearch;
using namespace ::testing;

// Test fixture which prepares TransitivePathGraphSearch testing.
template <typename T>
class GraphSearchTest : public Test {
 protected:
  using AdjacencyList = std::unordered_map<size_t, std::vector<size_t>>;

  const ad_utility::AllocatorWithLimit<Id> allocator_ =
      ad_utility::testing::makeAllocator();
  GraphSearchExecutionParams ep_ = {
      std::make_shared<ad_utility::CancellationHandle<>>(), allocator_};

  std::vector<T> graphs_;
  // When testing using BinSearchMap, store the data for the startIds and
  // targetIds spans here.
  std::vector<std::unique_ptr<std::vector<Id>>> binSearchMapStartIds_;
  std::vector<std::unique_ptr<std::vector<Id>>> binSearchMapTargetIds_;

  // Easy-to-read-and-change representation of the graphs that will be tested
  // on. Will be converted to template type T and stored in graphs_ in the
  // initializeGraphWrappers() method.
  const std::vector<AdjacencyList> graphsAdjListRepresentation_ = {
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

  // Create a set that contains some Ids with the given size_t values.
  Set initializeSet(const std::vector<size_t>& values) {
    Set returned{allocator_};
    for (size_t idx : values) {
      returned.insert(Id::makeFromInt(idx));
    }
    return returned;
  }

  GraphSearchTest() { initializeGraphsWrappers(); }

 private:
  // Initialize the graphs_ list, depending on which type is currently used for
  // template T.
  void initializeGraphsWrappers() {
    // If a third wrapper (next to HashMapWrapper and BinSearchMap) is
    // introduced, specialized creation thereof will be necessary here.
    if constexpr (std::is_same_v<T, HashMapWrapper>) {
      for (const AdjacencyList& adjList : graphsAdjListRepresentation_) {
        HashMapWrapper::Map map(allocator_);
        for (const std::pair<size_t, std::vector<size_t>> pair : adjList) {
          map.insert_or_assign(Id::makeFromInt(pair.first),
                               this->initializeSet(pair.second));
        }
        graphs_.push_back(HashMapWrapper(map, allocator_));
      }
    } else {  // BinSearchMap
      for (const AdjacencyList& adjList : graphsAdjListRepresentation_) {
        // Create new storage on the heap for a new BinSearchMap's startId and
        // targetId spans.
        binSearchMapStartIds_.push_back(std::make_unique<std::vector<Id>>());
        binSearchMapTargetIds_.push_back(std::make_unique<std::vector<Id>>());
        auto& startIds = *binSearchMapStartIds_.back();
        auto& targetIds = *binSearchMapTargetIds_.back();

        std::vector<size_t> keys;
        for (const std::pair<size_t, std::vector<size_t>> pair : adjList) {
          keys.emplace_back(pair.first);
        }
        std::sort(keys.begin(), keys.end());

        for (const size_t startNode : keys) {
          for (const size_t targetNode : adjList.at(startNode)) {
            startIds.emplace_back(Id::makeFromInt(startNode));
            targetIds.emplace_back(Id::makeFromInt(targetNode));
          }
        }
        graphs_.push_back(BinSearchMap(ql::span<const Id>(startIds),
                                       ql::span<const Id>(targetIds)));
      }
    }
  }
};

// If another wrapper for graphs is to be introduced, add it here as well as
// in GraphSearchTest::initializeGraphsWrappers().
using graphSearchTestTypes = Types<HashMapWrapper, BinSearchMap>;
TYPED_TEST_SUITE(GraphSearchTest, graphSearchTestTypes);

// _____________________________________________________________________________
TYPED_TEST(GraphSearchTest, binaryFirstSearch) {
  // For all graph examples, construct the expected returned sets.
  std::vector<std::vector<size_t>> expected = {
      {0}, {0}, {0}, {0, 1}, {0}, {0, 1, 2, 3, 4, 5, 6, 7}, {0, 1, 2}};

  // Iterate over all graphs and check if binarySearch will return the right
  // values.
  for (size_t i = 0; i < expected.size(); i++) {
    GraphSearchProblem<TypeParam> gsp(this->graphs_.at(i), Id::makeFromInt(0),
                                      std::optional<Id>(), 0,
                                      std::numeric_limits<size_t>::max());
    EXPECT_THAT(runOptimalGraphSearch(gsp, this->ep_),
                this->initializeSet(expected.at(i)));
  }
}

// _____________________________________________________________________________
TYPED_TEST(GraphSearchTest, binaryFirstSearchWithLimit) {
  // Organize each test case into a neat struct to hold the expected values.
  struct TestVal {
    size_t graphNumber_;
    size_t minDist_;
    size_t maxDist_;
    std::vector<size_t> expected_;
  };
  std::array<TestVal, 11> tests = {TestVal(0, 0, 100, {0}),
                                   TestVal(1, 0, 100, {0}),
                                   TestVal(1, 1, 10, {}),
                                   TestVal(2, 0, 10, {0}),
                                   TestVal(2, 10, 11, {0}),
                                   TestVal(3, 0, 1, {1, 0}),
                                   TestVal(3, 1, 1, {1}),
                                   TestVal(4, 0, 100, {0}),
                                   TestVal(5, 1, 2, {1, 4, 3}),
                                   TestVal(5, 10, 100, {1, 2, 3, 4, 5, 6, 7}),
                                   TestVal(2, 10001, 1000001, {0})};

  for (const TestVal& test : tests) {
    GraphSearchProblem<TypeParam> gsp(this->graphs_.at(test.graphNumber_),
                                      Id::makeFromInt(0), std::optional<Id>(),
                                      test.minDist_, test.maxDist_);

    EXPECT_THAT(runOptimalGraphSearch(gsp, this->ep_),
                this->initializeSet(test.expected_));
  }
}

// _____________________________________________________________________________
TYPED_TEST(GraphSearchTest, depthFirstSearch) {
  struct TestVal {
    size_t graphNumber_;
    size_t target_;
    std::vector<size_t> expected_;
  };
  std::array<TestVal, 10> tests = {TestVal(0, 0, {0}), TestVal(0, 1, {}),
                                   TestVal(1, 0, {0}), TestVal(1, 1, {}),
                                   TestVal(2, 0, {0}), TestVal(3, 0, {0}),
                                   TestVal(3, 1, {1}), TestVal(4, 1, {}),
                                   TestVal(5, 7, {7}), TestVal(5, 8, {})};

  for (const TestVal& test : tests) {
    GraphSearchProblem<TypeParam> gsp(
        this->graphs_.at(test.graphNumber_), Id::makeFromInt(0),
        Id::makeFromInt(test.target_), 0, std::numeric_limits<size_t>::max());

    EXPECT_THAT(runOptimalGraphSearch(gsp, this->ep_),
                this->initializeSet(test.expected_))
        << "Failure at graph " << test.graphNumber_ << ", trying to find node "
        << test.target_ << ".";
  }
}

// _____________________________________________________________________________
TYPED_TEST(GraphSearchTest, depthFirstSearchWithLimit) {
  struct TestVal {
    size_t graphNumber_;
    size_t target_;
    size_t minDist_;
    size_t maxDist_;
    std::vector<size_t> expected_;
  };
  std::array<TestVal, 16> tests = {
      TestVal(0, 0, 0, 10, {0}),    TestVal(0, 0, 10, 100, {}),
      TestVal(1, 0, 0, 100, {0}),   TestVal(1, 1, 0, 100, {}),
      TestVal(2, 0, 100, 200, {0}), TestVal(3, 1, 0, 0, {}),
      TestVal(3, 0, 100, 100, {0}), TestVal(4, 1, 0, 1000, {}),
      TestVal(5, 8, 0, 10000, {}),  TestVal(5, 7, 100, 999, {7}),
      TestVal(5, 0, 1, 100, {}),    TestVal(5, 4, 5, 1000, {4}),
      TestVal(6, 2, 0, 1, {2}),     TestVal(7, 3, 0, 2, {3}),
      TestVal(2, 0, 100, 100, {0}), TestVal(8, 3, 0, 2, {3})};

  for (const TestVal& test : tests) {
    GraphSearchProblem<TypeParam> gsp(
        this->graphs_.at(test.graphNumber_), Id::makeFromInt(0),
        Id::makeFromInt(test.target_), test.minDist_, test.maxDist_);

    EXPECT_THAT(runOptimalGraphSearch(gsp, this->ep_),
                this->initializeSet(test.expected_))
        << "Failure at graph " << test.graphNumber_ << ", trying to find node "
        << test.target_ << " in distance limits " << test.minDist_ << " to "
        << test.maxDist_ << ".";
  }
}

// ___________________________________________________________________________
TEST(GraphSearchTestExtraTests, cancellationCheck) {
  // Test that the log message created in
  // GraphSearchExecutionParams.checkCancellation() when a cancellation is
  // received will be logged.

  const ad_utility::AllocatorWithLimit<Id> allocator =
      ad_utility::testing::makeAllocator();
  GraphSearchExecutionParams ep(
      std::make_shared<ad_utility::CancellationHandle<>>(), allocator);

  // Tell absl to reset the logging stream after scope exits.
  absl::Cleanup cleanup{
      []() { ad_utility::setGlobalLoggingStream(&std::cout); }};

  // Redirect logging stream to a stream object which we can test on.
  std::stringstream stream;
  ad_utility::setGlobalLoggingStream(&stream);

  // Trigger a CHECK_WINDOW_MISSED cancellation state which will make the
  // handle's watchdog write logs containing the algorithmName specified in
  // checkCancellation.
  ep.cancellationHandle_->startWatchDog();
  std::this_thread::sleep_for(2 * DESIRED_CANCELLATION_CHECK_INTERVAL);
  ep.checkCancellation("TEST");

  EXPECT_THAT(
      stream.str().c_str(),
      HasSubstr(
          "The TEST graph search algorithm received a cancellation signal."));
}
