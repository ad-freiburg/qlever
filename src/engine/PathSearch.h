// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#pragma once

#include <memory>
#include <optional>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "engine/Operation.h"
#include "engine/PathSearchVisitors.h"
#include "engine/VariableToColumnMap.h"
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

using TreeAndCol = std::pair<std::shared_ptr<QueryExecutionTree>, size_t>;
using SearchSide = std::variant<Variable, std::vector<Id>>;

/**
 * @brief Struct to hold configuration parameters for the path search.
 */
struct PathSearchConfiguration {
  // The path search algorithm to use.
  PathSearchAlgorithm algorithm_;
  // The source node ID.
  SearchSide sources_;
  // A list of target node IDs.
  SearchSide targets_;
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

  bool sourceIsVariable() const {
    return std::holds_alternative<Variable>(sources_);
  }
  bool targetIsVariable() const {
    return std::holds_alternative<Variable>(targets_);
  }

  std::string searchSideToString(const SearchSide& side) const {
    if (std::holds_alternative<Variable>(side)) {
      return std::get<Variable>(side).toSparql();
    }
    std::ostringstream os;
    for (auto id : std::get<std::vector<Id>>(side)) {
      os << id << ", ";
    }
    return std::move(os).str();
  }

  std::string toString() const {
    std::ostringstream os;
    switch (algorithm_) {
      case ALL_PATHS:
        os << "Algorithm: All paths" << '\n';
        break;
      case SHORTEST_PATHS:
        os << "Algorithm: Shortest paths" << '\n';
        break;
    }

    os << "Source: " << searchSideToString(sources_) << '\n';
    os << "Target: " << searchSideToString(targets_) << '\n';

    os << "Start: " << start_.toSparql() << '\n';
    os << "End: " << end_.toSparql() << '\n';
    os << "PathColumn: " << pathColumn_.toSparql() << '\n';
    os << "EdgeColumn: " << edgeColumn_.toSparql() << '\n';

    os << "EdgeProperties:" << '\n';
    for (auto edgeProperty : edgeProperties_) {
      os << "  " << edgeProperty.toSparql() << '\n';
    }

    return std::move(os).str();
  }
};

class BinSearchWrapper {
  const IdTable& table_;
  size_t startCol_;
  size_t endCol_;
  std::vector<size_t> edgeCols_;
  std::unordered_map<uint64_t, std::vector<Path>> pathCache_;

 public:
  BinSearchWrapper(const IdTable& table, size_t startCol, size_t endCol, std::vector<size_t> edgeCols);

  std::vector<Edge> outgoingEdes(const Id node) const;

  std::vector<Path> findPaths(const Id& source, const std::unordered_set<uint64_t>& targets);

  bool isTarget(const Id node) const;

private:
  const Edge makeEdgeFromRow(size_t row) const;
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

  std::vector<Id> indexToId_;
  IdToNodeMap idToIndex_;

  std::optional<TreeAndCol> boundSources_;
  std::optional<TreeAndCol> boundTargets_;

 public:
  PathSearch(QueryExecutionContext* qec,
             std::shared_ptr<QueryExecutionTree> subtree,
             PathSearchConfiguration config);

  std::vector<QueryExecutionTree*> getChildren() override;

  const PathSearchConfiguration& getConfig() const { return config_; }

  ColumnIndex getStartIndex() const {
    return variableColumns_.at(config_.start_).columnIndex_;
  }
  ColumnIndex getEndIndex() const {
    return variableColumns_.at(config_.end_).columnIndex_;
  }
  ColumnIndex getPathIndex() const {
    return variableColumns_.at(config_.pathColumn_).columnIndex_;
  }
  ColumnIndex getEdgeIndex() const {
    return variableColumns_.at(config_.edgeColumn_).columnIndex_;
  }

  string getCacheKeyImpl() const override;
  string getDescriptor() const override;
  size_t getResultWidth() const override;

  size_t getCostEstimate() override;

  uint64_t getSizeEstimateBeforeLimit() override;
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

  vector<ColumnIndex> resultSortedOn() const override;

  void bindSourceSide(std::shared_ptr<QueryExecutionTree> sourcesOp,
                      size_t inputCol);
  void bindTargetSide(std::shared_ptr<QueryExecutionTree> targetsOp,
                      size_t inputCol);

  bool isSourceBound() const {
    return boundSources_.has_value() || !config_.sourceIsVariable();
  }

  bool isTargetBound() const {
    return boundTargets_.has_value() || !config_.targetIsVariable();
  }

  std::optional<size_t> getSourceColumn() const {
    if (!config_.sourceIsVariable()) {
      return std::nullopt;
    }

    return variableColumns_.at(std::get<Variable>(config_.sources_))
        .columnIndex_;
  }

  std::optional<size_t> getTargetColumn() const {
    if (!config_.targetIsVariable()) {
      return std::nullopt;
    }

    return variableColumns_.at(std::get<Variable>(config_.targets_))
        .columnIndex_;
  }

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

  std::span<const Id> handleSearchSide(
      const SearchSide& side, const std::optional<TreeAndCol>& binding) const;

  /**
   * @brief Finds paths based on the configured algorithm.
   * @return A vector of paths.
   */
  std::vector<Path> findPaths(std::span<const Id> sources,
                              std::span<const Id> targets, BinSearchWrapper& binSearch) const;

  /**
   * @brief Finds all paths in the graph.
   * @return A vector of all paths.
   */
  std::vector<Path> allPaths(std::span<const Id> sources,
                             std::span<const Id> targets, BinSearchWrapper& binSearch) const;

  /**
   * @brief Finds the shortest paths in the graph.
   * @return A vector of the shortest paths.
   */
  std::vector<Path> shortestPaths(std::span<const Id> sources,
                                  std::span<const Id> targets) const;

  std::vector<Path> reconstructPaths(uint64_t source, uint64_t target,
                                     PredecessorMap predecessors) const;

  /**
   * @brief Converts paths to a result table with a specified width.
   * @tparam WIDTH The width of the result table.
   * @param tableDyn The dynamic table to store the results.
   * @param paths The vector of paths to convert.
   */
  template <size_t WIDTH>
  void pathsToResultTable(IdTable& tableDyn, std::vector<Path>& paths) const;
};
