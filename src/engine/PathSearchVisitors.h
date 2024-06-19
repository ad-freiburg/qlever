// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#pragma once

#include <optional>
#include <algorithm>

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/depth_first_search.hpp>
#include <boost/graph/dijkstra_shortest_paths.hpp>
#include <boost/graph/graph_selectors.hpp>
#include <boost/graph/graph_traits.hpp>


struct Edge {
  uint64_t start_;
  uint64_t end_;
  std::vector<Id> edgeProperties_;
  double weight_ = 1;

  std::pair<Id, Id> toIds() const {
    return {Id::fromBits(start_), Id::fromBits(end_)};
  }
};

struct Path {
  std::vector<Edge> edges_;

  bool empty() const { return edges_.empty(); }

  size_t size() const { return edges_.size(); }

  void push_back(Edge edge) { edges_.push_back(edge); }

  void reverse() { std::reverse(edges_.begin(), edges_.end()); }

  std::optional<uint64_t> firstNode() const {
    return !empty() ? std::optional<uint64_t>{edges_.front().start_}
                    : std::nullopt;
  }

  std::optional<uint64_t> lastNode() const {
    return !empty() ? std::optional<uint64_t>{edges_.back().end_}
                    : std::nullopt;
  }

  bool ends_with(uint64_t node) const {
    return (!empty() && node == lastNode().value());
  }
};


typedef boost::adjacency_list<boost::vecS, boost::vecS, boost::directedS,
                              boost::no_property, Edge>
    Graph;
typedef boost::graph_traits<Graph>::vertex_descriptor VertexDescriptor;
typedef boost::graph_traits<Graph>::edge_descriptor EdgeDescriptor;

class AllPathsVisitor : public boost::default_dfs_visitor {
  std::unordered_set<uint64_t> targets_;
  Path& currentPath_;
  std::vector<Path>& allPaths_;

  const std::vector<Id>& indexToId_;

 public:
  AllPathsVisitor(std::unordered_set<uint64_t> targets, Path& path,
                  std::vector<Path>& paths, const std::vector<Id>& indexToId)
      : targets_(std::move(targets)),
        currentPath_(path),
        allPaths_(paths),
        indexToId_(indexToId) {}

  void examine_edge(EdgeDescriptor edgeDesc, const Graph& graph) {
    const Edge& edge = graph[edgeDesc];
    if (targets_.empty() || (currentPath_.ends_with(edge.start_) && targets_.find(edge.end_) != targets_.end())) {
      auto pathCopy = currentPath_;
      pathCopy.push_back(edge);
      allPaths_.push_back(pathCopy);
    }
  }

  void tree_edge(EdgeDescriptor edgeDesc, const Graph& graph) {
    const Edge& edge = graph[edgeDesc];
    currentPath_.edges_.push_back(edge);
  }

  void finish_vertex(VertexDescriptor vertex, const Graph& graph) {
    (void)graph;
    if (!currentPath_.empty() && Id::fromBits(currentPath_.lastNode().value()) == indexToId_[vertex]) {
      currentPath_.edges_.pop_back();
    }
  }
};

class DijkstraAllPathsVisitor : public boost::default_dijkstra_visitor {
  VertexDescriptor source_;
  std::unordered_set<uint64_t> targets_;
  Path& currentPath_;
  std::vector<Path>& allPaths_;
  std::vector<VertexDescriptor>& predecessors_;
  std::vector<double>& distances_;

 public:
  DijkstraAllPathsVisitor(VertexDescriptor source,
                          std::unordered_set<uint64_t> targets, Path& path,
                          std::vector<Path>& paths,
                          std::vector<VertexDescriptor>& predecessors,
                          std::vector<double>& distances)
      : source_(source),
        targets_(std::move(targets)),
        currentPath_(path),
        allPaths_(paths),
        predecessors_(predecessors),
        distances_(distances) {}

  const std::vector<VertexDescriptor>& getPredecessors() const {
    return predecessors_;
  }
  const std::vector<double>& getDistances() const { return distances_; }

  void edge_relaxed(EdgeDescriptor edgeDesc, const Graph& graph) {
    const Edge& edge = graph[edgeDesc];
    if (targets_.empty() || targets_.find(edge.end_) != targets_.end()) {
      rebuild_path(target(edgeDesc, graph), graph);
    }
  }

  void rebuild_path(VertexDescriptor vertex, const Graph& graph) {
    currentPath_.edges_.clear();
    for (VertexDescriptor v = vertex; v != source_; v = predecessors_[v]) {
      EdgeDescriptor e;
      bool exists;
      boost::tie(e, exists) = edge(predecessors_[v], v, graph);
      if (exists) {
        currentPath_.push_back(graph[e]);
      }
    }
    currentPath_.reverse();
    allPaths_.push_back(currentPath_);
  }
};
