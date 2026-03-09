//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Raymond Schätzle <schaetzr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHGRAPHSEARCH_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHGRAPHSEARCH_H

#include <limits>
#include <optional>
#include <unordered_set>

#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/CancellationHandle.h"

namespace qlever::graphSearch {
using Set = std::unordered_set<Id, absl::Hash<Id>, std::equal_to<Id>,
                               ad_utility::AllocatorWithLimit<Id>>;
using Queue = std::deque<Id, ad_utility::AllocatorWithLimit<Id>>;

// Helper struct that combines all possible values necessary for the
// graph search algorithms contained in this namespace. Improves
// readability of functions' signatures.
template <typename T>
struct GraphSearchProblem {
  // Adjacency-list representation of the graph.
  T& edges_;

  // The node where the graph search shall start from.
  Id startNode_;

  // Optional `Id` which may contain a target to which a transitive path shall
  // be found.
  std::optional<Id> targetNode_;

  // The minimum distance which shall be between `startNode_` and `targetNode_`
  // (inclusive).
  size_t minDist_;

  // The maximum distance which shall be between `startNode_` and
  // `targetNode_` (inclusive).
  size_t maxDist_;
};

// Store allocator and cancellationHandle which need to be passed to the graph
// search algorithms by their caller.
struct GraphSearchExecutionParams {
  // Used to communicate cancellation signals between objects.
  ad_utility::SharedCancellationHandle cancellationHandle_;
  // Used to allocate limited memory for new data structures.
  const ad_utility::AllocatorWithLimit<Id>& allocator_;

  // Check if a signal to cancel computation was sent, and if yes, throw
  // a CancellationException containing info about the currently running
  // algorithm.
  void checkCancellation(
      std::string_view algorithmName,
      ad_utility::source_location location = AD_CURRENT_SOURCE_LOC()) const {
    cancellationHandle_->throwIfCancelled(location, [algorithmName]() {
      return absl::StrCat(
          "The ", algorithmName,
          " graph search algorithm received a cancellation signal.");
    });
  }
};

// Depth-first search for a given target node inside the given graph.
// Returns a set containing the target node, if a path from the start node to
// it was found in the graph.
template <typename T, bool searchForTarget>
Set depthFirstSearch(const GraphSearchProblem<T>& gsp,
                     const GraphSearchExecutionParams& ep,
                     bool skipStartNodeInitially = false) {
  Set connectedNodes{ep.allocator_};

  // Saving the value of `gsp.targetNode_` removes the `.has_value()` check each
  // iteration and therefore improves runtime.
  Id targetNode;
  if constexpr (searchForTarget) {
    AD_CORRECTNESS_CHECK(gsp.targetNode_.has_value());
    targetNode = gsp.targetNode_.value();
  }

  sparqlExpression::VectorWithMemoryLimit<Id> stack{ep.allocator_};
  ad_utility::HashSetWithMemoryLimit<Id> marks{ep.allocator_};

  // Skip start node, improves performance for "+" operator.
  if (skipStartNodeInitially) {
    for (const Id& successor : gsp.edges_.successors(gsp.startNode_)) {
      stack.emplace_back(successor);
    }
  } else {
    stack.emplace_back(gsp.startNode_);
  }

  while (!stack.empty()) {
    ep.checkCancellation("Depth-first search");
    Id node = stack.back();
    stack.pop_back();

    if (marks.contains(node)) {
      continue;
    }

    marks.insert(node);
    if constexpr (searchForTarget) {
      if (node == targetNode) {
        connectedNodes.reserve(1);
        connectedNodes.insert(node);
        // Stop the search once target found, no further processing necessary.
        break;
      }
    } else {
      connectedNodes.insert(node);
    }

    const auto& successors = gsp.edges_.successors(node);
    for (Id successor : successors) {
      stack.emplace_back(successor);
    }
  }
  return connectedNodes;
}

// Depth-first search for a given target node inside the given graph,
// respecting minimum and maximum distance constraints.
// Returns a set containing the target node, if the graph contains a path
// from the start node to it and which fits inside the distance constraints.
template <typename T, bool searchForTarget>
Set depthFirstSearchWithLimit(const GraphSearchProblem<T>& gsp,
                              const GraphSearchExecutionParams& ep) {
  Set connectedNodes{ep.allocator_};

  // Saving the value of `gsp.targetNode_` removes the `.has_value()` check each
  // iteration and therefore improves runtime.
  Id targetNode;
  if constexpr (searchForTarget) {
    AD_CORRECTNESS_CHECK(gsp.targetNode_.has_value());
    targetNode = gsp.targetNode_.value();
  }

  sparqlExpression::VectorWithMemoryLimit<std::pair<Id, size_t>> stack{
      ep.allocator_.as<std::pair<Id, size_t>>()};
  ad_utility::HashMapWithMemoryLimit<Id, size_t> marks{ep.allocator_};

  stack.emplace_back(gsp.startNode_, 0);

  while (!stack.empty()) {
    ep.checkCancellation("Depth-first search (with limits)");

    auto [node, dist] = stack.back();
    stack.pop_back();

    if (gsp.minDist_ <= dist && gsp.maxDist_ >= dist) {
      marks.emplace(node, dist);
      if constexpr (searchForTarget) {
        if (targetNode == node) {
          // Target found, we can terminate DFS.
          connectedNodes.reserve(1);
          connectedNodes.insert(node);
          break;
        }
      } else {
        connectedNodes.insert(node);
      }
    }

    const auto& successors = gsp.edges_.successors(node);
    for (const Id& successor : successors) {
      // We re-check already marked nodes if we visit them at a shorter distance
      // than before.
      if (!marks.contains(successor) || marks.at(successor) > dist) {
        stack.emplace_back(successor, dist + 1);
      }
    }
  }
  return connectedNodes;
}

// Check the given graph search problem and run the appropriate
// algorithm.
// Return a set containing the target node, if it was given and is
// reachable. Otherwise, return all reachable nodes. If limits were given, only
// nodes inside that limit are contained.
template <typename T>
Set runOptimalGraphSearch(const GraphSearchProblem<T>& gsp,
                          const GraphSearchExecutionParams& ep) {
  // Select limited versions of graph search algorithms if limits
  // are not size limits of size_t (as used when parsing input).
  bool usesLimits =
      gsp.minDist_ != 0 || gsp.maxDist_ != std::numeric_limits<size_t>::max();
  bool targetHasValue = gsp.targetNode_.has_value();
  bool skipStartNodeInitially =
      gsp.minDist_ == 1 && gsp.maxDist_ == std::numeric_limits<size_t>::max();

  if (usesLimits && !skipStartNodeInitially) {
    if (targetHasValue) {
      return depthFirstSearchWithLimit<T, true>(gsp, ep);
    } else {
      return depthFirstSearchWithLimit<T, false>(gsp, ep);
    }
  }
  if (targetHasValue) {
    return depthFirstSearch<T, true>(gsp, ep, skipStartNodeInitially);
  }
  return depthFirstSearch<T, false>(gsp, ep, skipStartNodeInitially);
}
};  // namespace qlever::graphSearch

#endif  // QLEVER_SRC_ENGINE_TRANSITIVEPATHGRAPHSEARCH_H
