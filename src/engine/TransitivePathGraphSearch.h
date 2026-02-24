// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Raymond Schätzle (schaetzr at tf.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHSEARCHSTRATEGY_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHSEARCHSTRATEGY_H

#include <limits>
#include <optional>
#include <unordered_set>

#include "global/ValueId.h"
#include "util/AllocatorWithLimit.h"
#include "util/CancellationHandle.h"

using Id = ValueId;
using Set = std::unordered_set<Id, absl::Hash<Id>, std::equal_to<Id>,
                               ad_utility::AllocatorWithLimit<Id>>;

// Helper struct that combines all possible values necessary for the
// Graph search algorithms in the `TransitivePathGraphSearch` class. Improves
// readability of functions' signatures.
template <typename T>
struct GraphSearchProblem {
  // Adjacency-list representation of the graph.
  T& edges;

  // The node where the graph search shall start from. It must be
  // contained in `edges`, if not, it will be implicitly used as if it was.
  Id startNode;

  // Optional object which may contain a target to which a transitive path shall
  // be found.
  std::optional<Id> targetNode;

  // The minimum distance which shall be between startNode and targetNode
  // (inclusive).
  size_t minDist;

  // The maximum distance which shall be between startNode and
  // targetNode (inclusive).
  size_t maxDist;
};

// Helper class which allows extracting the minimal parts of an
// `Operation`s inheritor to be used in external algorithms which do not inherit
// from `Operation` by recreating the necessary member function's behavior.
class TransPathExecutionParams {
 private:
  // Used to communicate cancellation signals between objects.
  const ad_utility::SharedCancellationHandle cancellationHandle_;
  // Used to allocate limited memory for new data structures.
  const ad_utility::AllocatorWithLimit<Id>& allocator_;
  // Holds human-readable information about the caller object.
  std::string descriptor_;

 public:
  // Constructor, save all parameters to object.
  TransPathExecutionParams(
      ad_utility::SharedCancellationHandle cancellationHandle,
      const ad_utility::AllocatorWithLimit<Id>& allocator,
      std::string descriptor)
      : cancellationHandle_(cancellationHandle),
        allocator_(allocator),
        descriptor_(descriptor) {}

  // Allocate limited memory for new data structures.
  const ad_utility::AllocatorWithLimit<Id>& allocator() { return allocator_; }

  // Check if a signal to cancel computation was sent, and if yes, throw
  // an CancellationException.
  void checkCancellation(
      ad_utility::source_location location = AD_CURRENT_SOURCE_LOC()) {
    cancellationHandle_->throwIfCancelled(location,
                                          [this]() { return descriptor_; });
  }
};

// A class containint only static members which run DFS/BFS search
// algorithms for finding the connected nodes of a transitive path operation.
template <typename T>
class TransitivePathGraphSearch {
 public:
  // Breadth first search without any distance constraints.
  // Returns the set of all nodes connected to the startNode as given in `gsp`.
  static Set breadthFirstSearch(GraphSearchProblem<T> gsp,
                                TransPathExecutionParams ep) {
    std::queue<Id> queue{};
    Set connectedNodes{ep.allocator()};

    queue.push(gsp.startNode);

    while (!queue.empty()) {
      ep.checkCancellation();

      // Get next node from queue.
      auto node = queue.front();
      queue.pop();
      connectedNodes.insert(node);

      // Enqueue all successors of currently handled node.
      const auto& successors = gsp.edges.successors(node);
      for (auto successor : successors) {
        // Do not re-add already discovered nodes (skip loops).
        if (!connectedNodes.contains(successor)) {
          queue.push(successor);
        }
      }
    }
    return connectedNodes;
  }

  // Breadth first search respecting the minimum and maximum distance
  // constraints from the `gsp` parameter.
  // Returns The set of all nodes connected to the startNode as given in `gsp`,
  // with respect to the limit.
  static Set breadthFirstSearchWithLimit(GraphSearchProblem<T> gsp,
                                         TransPathExecutionParams ep) {
    size_t traversalDepth = 0;
    size_t nodesUntilNextDepthIncrease = 1;
    std::queue<Id> queue;
    Set connectedNodes{ep.allocator()};

    queue.push(gsp.startNode);
    while (!queue.empty() && traversalDepth <= gsp.maxDist ) {
      ep.checkCancellation();

      // Get next node from queue.
      auto node = queue.front();
      queue.pop();
      if (traversalDepth >= gsp.minDist) {
        connectedNodes.insert(node);
      }
      nodesUntilNextDepthIncrease--;

      // Enqueue all successors of currently handled node.
      const auto& successors = gsp.edges.successors(node);
      for (auto successor : successors) {
        // Do not re-add already discovered nodes (skip loops).
        if (!connectedNodes.contains(successor)) {
          queue.push(successor);
        }
      }

      // Another layer has been fully discovered.
      if (nodesUntilNextDepthIncrease == 0) {
        traversalDepth++;
        // At this point, the queue contains exactly all
        // undiscovered nodes from the next layer.
        nodesUntilNextDepthIncrease = queue.size();
      }
    }
    return connectedNodes;
  }

  // Depth first search for a given target node inside the given graph.
  // Returns a set containing the target node, if a path from the startNode to
  // it was found in the graph.
  static Set depthFirstSearch(GraphSearchProblem<T> gsp,
                              TransPathExecutionParams ep) {
    std::vector<Id> stack;
    ad_utility::HashSetWithMemoryLimit<Id> marks{ep.allocator()};
    Set connectedNodes{ep.allocator()};

    stack.emplace_back(gsp.startNode);

    while (!stack.empty()) {
      ep.checkCancellation();
      Id node = stack.back();
      stack.pop_back();

      if (marks.contains(node)) {
        continue;
      }

      marks.insert(node);
      if (node == gsp.targetNode.value()) {
        connectedNodes.insert(node);
        // Stop the DFS once target found, no further processing necessary.
        break;
      }

      const auto& successors = gsp.edges.successors(node);
      for (auto successor : successors) {
        // Only add unmarked/new nodes.
        if (!marks.contains(successor)) {
          stack.emplace_back(successor);
        }
      }
    }
    return connectedNodes;
  }

  // Depth first search for a given target node inside the given graph,
  // respecting minimum and maximum distance constraints.
  // Returns a set containing the target node, if the graph contains a path
  // from the start node to it and which fits inside the distance constraints..
  static Set depthFirstSearchWithLimit(GraphSearchProblem<T> gsp,
                                       TransPathExecutionParams ep) {
    std::vector<std::pair<Id, size_t>> stack;
    ad_utility::HashSetWithMemoryLimit<Id> marks{ep.allocator()};
    Set connectedNodes{ep.allocator()};
    stack.emplace_back(gsp.startNode, 0);

    while (!stack.empty()) {
      ep.checkCancellation();
      auto [node, steps] = stack.back();
      stack.pop_back();

      if (steps > gsp.maxDist || marks.contains(node)) {
        continue;
      }

      if (steps >= gsp.minDist) {
        // Marked nodes are guaranteed to be reachable inside the distance
        // constraints.
        marks.insert(node);
        if (node == gsp.targetNode) {
          connectedNodes.insert(node);
          // Stop the DFS once target found, no further processing necessary.
          break;
        }
      }

      const auto& successors = gsp.edges.successors(node);
      for (auto successor : successors) {
        if (!marks.contains(successor)) {
          stack.emplace_back(successor, steps + 1);
        }
      }
    }
    return connectedNodes;
  }

  // Check the given graph search problem and run the appropriate
  // algorithm (BFS/DFS, with or without limits.).
  // Returns a set containing the target node, if it was given and is
  // reachable. Otherwise, all reachable nodes. If limits were given, only
  // nodes inside that limit are contained.
  static Set runOptimalGraphSearch(GraphSearchProblem<T> gsp,
                                   TransPathExecutionParams ep) {
    // Select limited versions of graph search algorithms if limits
    // are not size limits of size_t (as used when parsing input).
    bool usesLimits =
        gsp.minDist != 0 || gsp.maxDist != std::numeric_limits<size_t>::max();
    bool targetHasValue = gsp.targetNode.has_value();
    return usesLimits ? (targetHasValue ? depthFirstSearchWithLimit(gsp, ep)
                                        : breadthFirstSearchWithLimit(gsp, ep))
                      : (targetHasValue ? depthFirstSearch(gsp, ep)
                                        : breadthFirstSearch(gsp, ep));
  }
};

#endif  // QLEVER_SRC_ENGINE_TRANSITIVEPATHSEARCHSTRATEGY_H

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
