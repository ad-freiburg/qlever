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
#include "engine/VariableToColumnMap.h"
#include "global/Id.h"
#include "index/Vocabulary.h"

enum class PathSearchAlgorithm { ALL_PATHS };

using TreeAndCol = std::pair<std::shared_ptr<QueryExecutionTree>, size_t>;
using SearchSide = std::variant<Variable, std::vector<Id>>;

/**
 * @brief Represents an edge in the graph.
 */
struct Edge {
  // The starting node ID.
  Id start_;

  // The ending node ID.
  Id end_;

  // Properties associated with the edge.
  std::vector<Id> edgeProperties_;
};

/**
 * @brief Represents a path consisting of multiple edges.
 */
struct Path {
  // The edges that make up the path.
  std::vector<Edge> edges_;

  /**
   * @brief Checks if the path is empty.
   * @return True if the path is empty, false otherwise.
   */
  bool empty() const { return edges_.empty(); }

  /**
   * @brief Returns the number of edges in the path.
   * @return The number of edges in the path.
   */
  size_t size() const { return edges_.size(); }

  /**
   * @brief Adds an edge to the end of the path.
   * @param edge The edge to add.
   */
  void push_back(const Edge& edge) { edges_.push_back(edge); }

  void pop_back() { edges_.pop_back(); }

  /**
   * @brief Reverses the order of the edges in the path.
   */
  void reverse() { std::ranges::reverse(edges_); }

  Path concat(const Path& other) const {
    Path path;
    path.edges_ = edges_;
    path.edges_.insert(path.edges_.end(), other.edges_.begin(),
                       other.edges_.end());
    return path;
  }

  const Id& end() { return edges_.back().end_; }
  const Id& first() { return edges_.front().start_; }

  Path startingAt(size_t index) const {
    std::vector<Edge> edges;
    for (size_t i = index; i < edges_.size(); i++) {
      edges.push_back(edges_[i]);
    }
    return Path{edges};
  }
};

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
      case PathSearchAlgorithm::ALL_PATHS:
        os << "Algorithm: All paths" << '\n';
        break;
    }

    os << "Source: " << searchSideToString(sources_) << '\n';
    os << "Target: " << searchSideToString(targets_) << '\n';

    os << "Start: " << start_.toSparql() << '\n';
    os << "End: " << end_.toSparql() << '\n';
    os << "PathColumn: " << pathColumn_.toSparql() << '\n';
    os << "EdgeColumn: " << edgeColumn_.toSparql() << '\n';

    os << "EdgeProperties:" << '\n';
    for (const auto& edgeProperty : edgeProperties_) {
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
  BinSearchWrapper(const IdTable& table, size_t startCol, size_t endCol,
                   std::vector<size_t> edgeCols);

  std::vector<Edge> outgoingEdes(const Id node) const;

  std::vector<Path> findPaths(const Id& source,
                              const std::unordered_set<uint64_t>& targets);

  std::span<const Id> getSources() const;

 private:
  Edge makeEdgeFromRow(size_t row) const;
};

/**
 * @brief Class to perform various path search algorithms on a graph.
 */
class PathSearch : public Operation {
  std::shared_ptr<QueryExecutionTree> subtree_;
  size_t resultWidth_;
  VariableToColumnMap variableColumns_;

  // Configuration for the path search.
  PathSearchConfiguration config_;

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
  std::span<const Id> handleSearchSide(
      const SearchSide& side, const std::optional<TreeAndCol>& binding) const;

  /**
   * @brief Finds paths based on the configured algorithm.
   * @return A vector of paths.
   */
  std::vector<Path> findPaths(const Id source,
                              const std::unordered_set<uint64_t>& targets,
                              const BinSearchWrapper& binSearch) const;

  /**
   * @brief Finds all paths in the graph.
   * @return A vector of all paths.
   */
  std::vector<Path> allPaths(std::span<const Id> sources,
                             std::span<const Id> targets,
                             BinSearchWrapper& binSearch) const;

  /**
   * @brief Converts paths to a result table with a specified width.
   * @tparam WIDTH The width of the result table.
   * @param tableDyn The dynamic table to store the results.
   * @param paths The vector of paths to convert.
   */
  template <size_t WIDTH>
  void pathsToResultTable(IdTable& tableDyn, std::vector<Path>& paths) const;
};
