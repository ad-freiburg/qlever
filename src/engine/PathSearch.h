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

struct PathSearchConfiguration {
  PathSearchAlgorithm algorithm_;
  Id source_;
  std::vector<Id> targets_;
  Variable start_;
  Variable end_;
  Variable pathColumn_;
  Variable edgeColumn_;
  std::vector<Variable> edgeProperties_;
};

class PathSearch : public Operation {
  std::shared_ptr<QueryExecutionTree> subtree_;
  size_t resultWidth_;
  VariableToColumnMap variableColumns_;

  Graph graph_;
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
  const Id& getSource() const { return source_; }
  const std::vector<Id>& getTargets() const { return targets_; }

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

  ResultTable computeResult() override;
  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  void buildGraph(std::span<const Id> startNodes, std::span<const Id> endNodes,
                  std::span<std::span<const Id>> edgePropertyLists);
  void buildMapping(std::span<const Id> startNodes,
                    std::span<const Id> endNodes);
  std::vector<Path> findPaths() const;
  std::vector<Path> allPaths() const;
  std::vector<Path> shortestPaths() const;
  template <size_t WIDTH>
  void pathsToResultTable(IdTable& tableDyn, std::vector<Path>& paths) const;
};
