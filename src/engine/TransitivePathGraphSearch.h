//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Raymond Schätzle <schaetzr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHSEARCHSTRATEGY_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHSEARCHSTRATEGY_H

#include <limits>
#include <optional>
#include <unordered_set>

#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/CancellationHandle.h"

namespace qlever::graphSearch {
using Set = std::unordered_set<Id, absl::Hash<Id>, std::equal_to<Id>,
                               ad_utility::AllocatorWithLimit<Id>>;
using Queue = std::deque<Id, ad_utility::AllocatorWithLimit<Id>>;

// Helper struct that combines all possible values necessary for the
// Graph search algorithms contained in this namespace. Improves
// readability of functions' signatures.
template <typename T>
struct GraphSearchProblem {
  // Adjacency-list representation of the graph.
  T& edges_;

  // The node where the graph search shall start from.
  Id startNode_;

  // Optional object which may contain a target to which a transitive path shall
  // be found.
  std::optional<Id> targetNode_;

  // The minimum distance which shall be between startNode and targetNode
  // (inclusive).
  size_t minDist_;

  // The maximum distance which shall be between startNode and
  // targetNode (inclusive).
  size_t maxDist_;
};

// Helper struct which allows extracting the minimal parts of an
// `Operation`s inheritor to be used in external algorithms which do not inherit
// from `Operation` by recreating the necessary member function's behavior.
struct GraphSearchExecutionParams {
  // Used to communicate cancellation signals between objects.
  ad_utility::SharedCancellationHandle cancellationHandle_;
  // Used to allocate limited memory for new data structures.
  const ad_utility::AllocatorWithLimit<Id>& allocator_;

  // Check if a signal to cancel computation was sent, and if yes, throw
  // an CancellationException.
  void checkCancellation(
      std::string algorithmName,
      ad_utility::source_location location = AD_CURRENT_SOURCE_LOC()) {
    cancellationHandle_->throwIfCancelled(location, [algorithmName]() {
      return std::string(
          "The " + algorithmName +
          " graph search algorithm received a cancellation signal.");
    });
  }
};

// Breadth first search without any distance constraints.
// Returns the set of all nodes connected to the startNode as given in `gsp`.
template <typename T>
static Set breadthFirstSearch(GraphSearchProblem<T>& gsp,
                              GraphSearchExecutionParams& ep) {
  // TODO<schaetzr>: Check if there are advantages to making our own minimal
  // implementation of a FIFO queue.
  Queue queue{ep.allocator_};
  Set connectedNodes{ep.allocator_};

  queue.push_back(gsp.startNode_);

  while (!queue.empty()) {
    ep.checkCancellation("Breadth first search");

    // Get next node from queue.
    auto node = queue.front();
    queue.pop_front();
    connectedNodes.insert(node);

    // Enqueue all successors of currently handled node.
    const auto& successors = gsp.edges_.successors(node);
    for (Id successor : successors) {
      // Do not re-add already discovered nodes (skip loops).
      if (!connectedNodes.contains(successor)) {
        queue.push_back(successor);
      }
    }
  }
  return connectedNodes;
}

// Breadth first search respecting the minimum and maximum distance
// constraints from the `gsp` parameter.
// Returns The set of all nodes connected to the startNode as given in `gsp`,
// with respect to the limit.
template <typename T>
static Set breadthFirstSearchWithLimit(GraphSearchProblem<T>& gsp,
                                       GraphSearchExecutionParams& ep) {
  size_t traversalDepth = 0;
  size_t nodesUntilNextDepthIncrease = 1;
  // TODO<schaetzr>: Check if there are advantages to making our own minimal
  // implementations of a FIFO queue.
  Queue queue{ep.allocator_};
  Set connectedNodes{ep.allocator_};

  queue.push_back(gsp.startNode_);
  while (!queue.empty() && traversalDepth <= gsp.maxDist_) {
    ep.checkCancellation("Breadth first search (limted version)");

    // Get next node from queue.
    auto node = queue.front();
    queue.pop_front();
    if (traversalDepth >= gsp.minDist_) {
      connectedNodes.insert(node);
    }
    nodesUntilNextDepthIncrease--;

    // Enqueue all successors of currently handled node.
    const auto& successors = gsp.edges_.successors(node);
    for (Id successor : successors) {
      // Do not re-add already discovered nodes (skip loops).
      if (!connectedNodes.contains(successor)) {
        queue.push_back(successor);
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
template <typename T>
static Set depthFirstSearch(GraphSearchProblem<T>& gsp,
                            GraphSearchExecutionParams& ep) {
  Set connectedNodes{ep.allocator_};

  std::vector<Id, ad_utility::AllocatorWithLimit<Id>> stack{ep.allocator_};
  ad_utility::HashSetWithMemoryLimit<Id> marks{ep.allocator_};

  stack.emplace_back(gsp.startNode_);

  while (!stack.empty()) {
    ep.checkCancellation("Depth first search");
    Id node = stack.back();
    stack.pop_back();

    if (marks.contains(node)) {
      continue;
    }

    marks.insert(node);
    if (node == gsp.targetNode_.value()) {
      connectedNodes.insert(node);
      // Stop the DFS once target found, no further processing necessary.
      break;
    }

    const auto& successors = gsp.edges_.successors(node);
    for (Id successor : successors) {
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
template <typename T>
static Set depthFirstSearchWithLimit(GraphSearchProblem<T>& gsp,
                                     GraphSearchExecutionParams& ep) {
  Set connectedNodes{ep.allocator_};
  // TODO<rs505> Find out how to correctly use the allocator for std::pair.
  std::vector<std::pair<Id, size_t>> stack;
  ad_utility::HashSetWithMemoryLimit<Id> marks{ep.allocator_};

  stack.emplace_back(gsp.startNode_, 0);

  while (!stack.empty()) {
    ep.checkCancellation("Depth first search (limted version)");
    auto [node, steps] = stack.back();
    stack.pop_back();

    if (steps > gsp.maxDist_ || marks.contains(node)) {
      continue;
    }

    if (steps >= gsp.minDist_) {
      // Marked nodes are guaranteed to be reachable inside the distance
      // constraints.
      marks.insert(node);
      if (node == gsp.targetNode_) {
        connectedNodes.insert(node);
        // Stop the DFS once target found, no further processing necessary.
        break;
      }
    }

    const auto& successors = gsp.edges_.successors(node);
    for (Id successor : successors) {
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
template <typename T>
static Set runOptimalGraphSearch(GraphSearchProblem<T>& gsp,
                                 GraphSearchExecutionParams& ep) {
  // Select limited versions of graph search algorithms if limits
  // are not size limits of size_t (as used when parsing input).
  bool usesLimits =
      gsp.minDist_ != 0 || gsp.maxDist_ != std::numeric_limits<size_t>::max();
  bool targetHasValue = gsp.targetNode_.has_value();

  if (usesLimits) {
    if (targetHasValue) {
      return depthFirstSearchWithLimit(gsp, ep);
    }
    return breadthFirstSearchWithLimit(gsp, ep);
  }
  if (targetHasValue) {
    return depthFirstSearch(gsp, ep);
  }
  return breadthFirstSearch(gsp, ep);
}
};  // namespace qlever::graphSearch

#endif  // QLEVER_SRC_ENGINE_TRANSITIVEPATHSEARCHSTRATEGY_H
