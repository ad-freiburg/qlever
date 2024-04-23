// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/depth_first_search.hpp>
#include <boost/graph/graph_selectors.hpp>
#include <boost/graph/graph_traits.hpp>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "global/Id.h"

struct Edge {
  uint64_t start_;
  uint64_t end_;

  std::pair<Id, Id> toIds() const {
    return {Id::fromBits(start_), Id::fromBits(end_)};
  }
};

struct Path {
  std::vector<Edge> edges_;

  bool empty() const { return edges_.empty(); }

  size_t size() const { return edges_.size(); }

  std::optional<uint64_t> firstNode() const {
    return !empty() ? std::optional<uint64_t>{edges_.front().start_}
                    : std::nullopt;
  }

  std::optional<uint64_t> lastNode() const {
    return !empty() ? std::optional<uint64_t>{edges_.back().end_}
                    : std::nullopt;
  }
};

typedef boost::adjacency_list<boost::vecS, boost::vecS, boost::directedS,
                              boost::no_property, Edge>
    Graph;
typedef boost::graph_traits<Graph>::vertex_descriptor VertexDescriptor;
typedef boost::graph_traits<Graph>::edge_descriptor EdgeDescriptor;

class AllPathsVisitor : public boost::default_dfs_visitor {
  VertexDescriptor target_;
  VertexDescriptor lastVertex_;
  Path& currentPath_;
  std::vector<Path>& allPaths_;

 public:
  AllPathsVisitor(VertexDescriptor target, Path& path, std::vector<Path>& paths)
      : target_(target), currentPath_(path), allPaths_(paths) {}

  void discover_vertex(VertexDescriptor vertex, const Graph& graph) {
    (void)graph;
    lastVertex_ = vertex;
  }

  void tree_edge(EdgeDescriptor edgeDesc, const Graph& graph) {
    const Edge& edge = boost::get(boost::edge_bundle, graph)[edgeDesc];
    currentPath_.edges_.push_back(edge);
  }

  void finish_vertex(VertexDescriptor vertex, const Graph& graph) {
    (void)graph;
    if (vertex == target_) {
      allPaths_.push_back(currentPath_);
    }
    if (!currentPath_.empty()) {
      if (currentPath_.lastNode() == vertex) {
        currentPath_.edges_.pop_back();
      }
    }
  }
};

enum PathSearchAlgorithm {
  ALL_PATHS,
};

struct PathSearchConfiguration {
  PathSearchAlgorithm algorithm_;
  Id source_;
  Id destination_;
  size_t startColumn_;
  size_t endColumn_;
  size_t pathIndexColumn_;
  size_t edgeIndexColumn_;
};

class PathSearch : public Operation {
  std::shared_ptr<QueryExecutionTree> subtree_;
  size_t resultWidth_;
  VariableToColumnMap variableColumns_;

  Graph graph_;
  PathSearchConfiguration config_;

 public:
  PathSearch(QueryExecutionContext* qec,
             std::shared_ptr<QueryExecutionTree> subtree,
             PathSearchConfiguration config);

  std::vector<QueryExecutionTree*> getChildren() override;

  string getCacheKeyImpl() const override;
  string getDescriptor() const override;
  size_t getResultWidth() const override;
  void setTextLimit(size_t limit) override;

  size_t getCostEstimate() override;

  uint64_t getSizeEstimateBeforeLimit() override;
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

  vector<ColumnIndex> resultSortedOn() const override;

  ResultTable computeResult() override;
  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  void buildGraph(std::span<const Id> startNodes, std::span<const Id> endNodes);
  std::vector<Path> findPaths() const;
  std::vector<Path> allPaths() const;
  template <size_t WIDTH>
  void normalizePaths(IdTable& tableDyn, std::vector<Path> paths) const;
};
