// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#pragma once

#include <memory>
#include <span>
#include <unordered_map>
#include <vector>

#include "engine/Operation.h"
#include "engine/VariableToColumnMap.h"
#include "engine/PathSearchVisitors.h"
#include "global/Id.h"
#include "index/Vocabulary.h"

// We deliberately use the `std::` variants of a hash set and hash map because
// `absl`s types are not exception safe.
struct IdHash {
  auto operator()(Id id) const { return std::hash<uint64_t>{}(id.getBits()); }
};

using IdToNodeMap = std::unordered_map<
    Id, size_t, IdHash, std::equal_to<Id>,
    ad_utility::AllocatorWithLimit<std::pair<const Id, size_t>>>;

enum PathSearchAlgorithm { ALL_PATHS, SHORTEST_PATHS };

/**
 * @brief Struct to hold configuration parameters for the path search.
 */
struct PathSearchConfiguration {
  // The path search algorithm to use.
  PathSearchAlgorithm algorithm_;
  // The source node ID.
  Id source_;
  // A list of target node IDs.
  std::vector<Id> targets_; 
  // Variable representing the start column in the result.
  Variable start_; 
  // Variable representing the end column in the result.
  Variable end_; 
  // Variable representing the path column in the result.
  Variable pathColumn_; 
  // Variable representing the edge column in the result.
  Variable edgeColumn_; 
  // Variables representing edge property columns.
  std::vector<Variable> edgeProperties_;
};

/**
 * @brief Class to perform various path search algorithms on a graph.
 */
class PathSearch : public Operation {
  std::shared_ptr<QueryExecutionTree> subtree_;
  size_t resultWidth_;
  VariableToColumnMap variableColumns_;

  // The graph on which the path search is performed.
  Graph graph_;
  // Configuration for the path search.
  PathSearchConfiguration config_;
  Id source_;
  std::vector<Id> targets_;

  std::vector<Id> indexToId_;
  IdToNodeMap idToIndex_;

 public:
  PathSearch(QueryExecutionContext* qec,
             std::shared_ptr<QueryExecutionTree> subtree,
             PathSearchConfiguration config);

  std::vector<QueryExecutionTree*> getChildren() override;
  const Id& getSource() const { return config_.source_; }
  const std::vector<Id>& getTargets() const { return config_.targets_; }

  const PathSearchConfiguration& getConfig() const { return config_; }

  ColumnIndex getStartIndex() const { return variableColumns_.at(config_.start_).columnIndex_; }
  ColumnIndex getEndIndex() const { return variableColumns_.at(config_.end_).columnIndex_; }
  ColumnIndex getPathIndex() const { return variableColumns_.at(config_.pathColumn_).columnIndex_; }
  ColumnIndex getEdgeIndex() const { return variableColumns_.at(config_.edgeColumn_).columnIndex_; }


  string getCacheKeyImpl() const override;
  string getDescriptor() const override;
  size_t getResultWidth() const override;

  size_t getCostEstimate() override;

  uint64_t getSizeEstimateBeforeLimit() override;
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

  vector<ColumnIndex> resultSortedOn() const override;

  Result computeResult([[maybe_unused]] bool requestLaziness) override;
  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  /**
   * @brief Builds the graph from the given nodes and edge properties.
   * @param startNodes A span of start nodes.
   * @param endNodes A span of end nodes.
   * @param edgePropertyLists A span of edge property lists.
   */
  void buildGraph(std::span<const Id> startNodes, std::span<const Id> endNodes,
                  std::span<std::span<const Id>> edgePropertyLists);

  /**
   * @brief Builds the mapping from node IDs to indices.
   * @param startNodes A span of start nodes.
   * @param endNodes A span of end nodes.
   */
  void buildMapping(std::span<const Id> startNodes,
                    std::span<const Id> endNodes);

  /**
   * @brief Finds paths based on the configured algorithm.
   * @return A vector of paths.
   */
  std::vector<Path> findPaths() const;

  /**
   * @brief Finds all paths in the graph.
   * @return A vector of all paths.
   */
  std::vector<Path> allPaths() const;

  /**
   * @brief Finds the shortest paths in the graph.
   * @return A vector of the shortest paths.
   */
  std::vector<Path> shortestPaths() const;

  /**
   * @brief Converts paths to a result table with a specified width.
   * @tparam WIDTH The width of the result table.
   * @param tableDyn The dynamic table to store the results.
   * @param paths The vector of paths to convert.
   */
  template <size_t WIDTH>
  void pathsToResultTable(IdTable& tableDyn, std::vector<Path>& paths) const;
};
