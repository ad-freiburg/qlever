// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#pragma once

#include <memory>
#include <optional>
#include <span>
#include <variant>
#include <vector>

#include "engine/Operation.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"

enum class PathSearchAlgorithm { ALL_PATHS };

/**
 * @brief Represents the source or target side of a PathSearch.
 * The side can either be a variable or a list of Ids.
 */
using SearchSide = std::variant<Variable, std::vector<Id>>;

namespace pathSearch {
struct Edge {
  Id start_;

  Id end_;

  size_t edgeRow_;
};

using EdgesLimited = std::vector<Edge, ad_utility::AllocatorWithLimit<Edge>>;

struct Path {
  EdgesLimited edges_;

  bool empty() const { return edges_.empty(); }

  size_t size() const { return edges_.size(); }

  void push_back(const Edge& edge) { edges_.push_back(edge); }

  void pop_back() { edges_.pop_back(); }

  const Id& end() { return edges_.back().end_; }
};

using PathsLimited = std::vector<Path, ad_utility::AllocatorWithLimit<Path>>;

/**
 * @class BinSearchWrapper
 * @brief Encapsulates logic for binary search of edges in
 * an IdTable. It provides methods to find outgoing edges from
 * a node and retrie
 *
 */
class BinSearchWrapper {
  const IdTable& table_;
  size_t startCol_;
  size_t endCol_;
  std::vector<size_t> edgeCols_;

 public:
  BinSearchWrapper(const IdTable& table, size_t startCol, size_t endCol,
                   std::vector<size_t> edgeCols);

  /**
   * @brief Return all outgoing edges of a node
   *
   * @param node The start node of the outgoing edges
   */
  std::vector<Edge> outgoingEdes(const Id node) const;

  /**
   * @brief Returns the start nodes of all edges.
   * In case the sources field for the path search is empty,
   * the search starts from all possible sources (i.e. all
   * start nodes). Returns only unique start nodes.
   */
  std::vector<Id> getSources() const;

  std::vector<Id> getEdgeProperties(const Edge& edge) const;

 private:
  Edge makeEdgeFromRow(size_t row) const;
};
}  // namespace pathSearch

struct PathSearchConfiguration {
  PathSearchAlgorithm algorithm_;
  SearchSide sources_;
  SearchSide targets_;
  Variable start_;
  Variable end_;
  Variable pathColumn_;
  Variable edgeColumn_;
  std::vector<Variable> edgeProperties_;
  bool cartesian_ = true;

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
    if (algorithm_ == PathSearchAlgorithm::ALL_PATHS) {
      os << "Algorithm: All paths" << '\n';
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

/**
 * @class PathSearch
 * @brief Main class implementing the path search operation.
 * It manages the configuration, executes the search and
 * builds the ResultTable.
 *
 */
class PathSearch : public Operation {
  std::shared_ptr<QueryExecutionTree> subtree_;
  size_t resultWidth_;
  VariableToColumnMap variableColumns_;

  PathSearchConfiguration config_;

  // The following optional fields are filled, depending
  // on how the PathSearch is bound.
  std::optional<size_t> sourceCol_;
  std::optional<size_t> targetCol_;

  std::optional<std::shared_ptr<QueryExecutionTree>> sourceTree_;
  std::optional<std::shared_ptr<QueryExecutionTree>> targetTree_;
  std::optional<std::shared_ptr<QueryExecutionTree>> sourceAndTargetTree_;

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
  std::optional<ColumnIndex> getSourceIndex() const {
    if (!config_.sourceIsVariable()) {
      return std::nullopt;
    }
    const auto& sourceVar = std::get<Variable>(config_.sources_);
    return variableColumns_.at(sourceVar).columnIndex_;
  }
  std::optional<ColumnIndex> getTargetIndex() const {
    if (!config_.targetIsVariable()) {
      return std::nullopt;
    }
    const auto& targetVar = std::get<Variable>(config_.targets_);
    return variableColumns_.at(targetVar).columnIndex_;
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

  void bindSourceAndTargetSide(
      std::shared_ptr<QueryExecutionTree> sourceAndTargetOp, size_t sourceCol,
      size_t targetCol);

  bool isSourceBound() const {
    return sourceTree_.has_value() || sourceAndTargetTree_.has_value() ||
           !config_.sourceIsVariable();
  }

  bool isTargetBound() const {
    return targetTree_.has_value() || sourceAndTargetTree_.has_value() ||
           !config_.targetIsVariable();
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
  std::pair<std::span<const Id>, std::span<const Id>> handleSearchSides() const;

  /**
   * @brief Finds paths based on the configured algorithm.
   * @return A vector of paths.
   */
  pathSearch::PathsLimited findPaths(
      const Id& source, const std::unordered_set<uint64_t>& targets,
      const pathSearch::BinSearchWrapper& binSearch) const;

  /**
   * @brief Finds all paths in the graph.
   * @return A vector of all paths.
   */
  pathSearch::PathsLimited allPaths(
      std::span<const Id> sources, std::span<const Id> targets,
      const pathSearch::BinSearchWrapper& binSearch, bool cartesian) const;

  /**
   * @brief Converts paths to a result table with a specified width.
   * @tparam WIDTH The width of the result table.
   * @param tableDyn The dynamic table to store the results.
   * @param paths The vector of paths to convert.
   */
  template <size_t WIDTH>
  void pathsToResultTable(IdTable& tableDyn, pathSearch::PathsLimited& paths,
                          const pathSearch::BinSearchWrapper& binSearch) const;
};
