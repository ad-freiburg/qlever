// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#pragma once

#include <algorithm>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/depth_first_search.hpp>
#include <boost/graph/dijkstra_shortest_paths.hpp>
#include <boost/graph/graph_selectors.hpp>
#include <boost/graph/graph_traits.hpp>
#include <exception>
#include <optional>

/**
 * @brief Represents an edge in the graph.
 */
struct Edge {
  // The starting node ID.
  uint64_t start_;

  // The ending node ID.
  uint64_t end_;

  // Properties associated with the edge.
  std::vector<Id> edgeProperties_;

  // The weight of the edge.
  double weight_ = 1;

  /**
   * @brief Converts the edge to a pair of IDs.
   * @return A pair of IDs representing the start and end of the edge.
   */
  std::pair<Id, Id> toIds() const {
    return {Id::fromBits(start_), Id::fromBits(end_)};
  }
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
  void push_back(Edge edge) { edges_.push_back(edge); }

  /**
   * @brief Reverses the order of the edges in the path.
   */
  void reverse() { std::reverse(edges_.begin(), edges_.end()); }

  /**
   * @brief Returns the ID of the first node in the path, if it exists.
   * @return The ID of the first node, or std::nullopt if the path is empty.
   */
  std::optional<uint64_t> firstNode() const {
    return !empty() ? std::optional<uint64_t>{edges_.front().start_}
                    : std::nullopt;
  }

  /**
   * @brief Returns the ID of the last node in the path, if it exists.
   * @return The ID of the last node, or std::nullopt if the path is empty.
   */
  std::optional<uint64_t> lastNode() const {
    return !empty() ? std::optional<uint64_t>{edges_.back().end_}
                    : std::nullopt;
  }

  /**
   * @brief Checks if the path ends with the given node ID.
   * @param node The node ID to check.
   * @return True if the path ends with the given node ID, false otherwise.
   */
  bool ends_with(uint64_t node) const {
    return (!empty() && node == lastNode().value());
  }
};

/**
 * @brief Boost graph types and descriptors.
 */
typedef boost::adjacency_list<boost::vecS, boost::vecS, boost::directedS,
                              boost::no_property, Edge>
    Graph;
typedef boost::graph_traits<Graph>::vertex_descriptor VertexDescriptor;
typedef boost::graph_traits<Graph>::edge_descriptor EdgeDescriptor;

using PredecessorMap = std::unordered_map<VertexDescriptor, std::vector<Edge>>;

struct StopSearchException : public std::exception {
    const char* what() const noexcept override {
        return "Stop DFS";
    }
};

/**
 * @brief Visitor for performing a depth-first search to find all paths.
 */
class AllPathsVisitor : public boost::default_dfs_visitor {
  VertexDescriptor start_;

  PredecessorMap& predecessors_;

 public:
  /**
   * @brief Constructor for AllPathsVisitor.
   * @param targets Set of target node IDs.
   * @param path Reference to the current path being explored.
   * @param paths Reference to the collection of all found paths.
   * @param indexToId Mapping from indices to IDs.
   */
  AllPathsVisitor(VertexDescriptor start, PredecessorMap& predecessors)
      : start_(start),
        predecessors_(predecessors) {}

  /**
   * @brief Examines an edge during the depth-first search.
   * @param edgeDesc The descriptor of the edge being examined.
   * @param graph The graph being searched.
   */
  void examine_edge(EdgeDescriptor edgeDesc, const Graph& graph) {
    const Edge& edge = graph[edgeDesc];

    if (!predecessors_.contains(edge.end_)) {
      predecessors_[edge.end_] = {};
    }

    auto& predEdges = predecessors_[edge.end_];
    predEdges.push_back(edge);
  }

  /**
   * @brief Called when a vertex has been finished during the depth-first
   * search.
   * @param vertex The descriptor of the vertex being finished.
   * @param graph The graph being searched.
   */
  void finish_vertex(VertexDescriptor vertex, const Graph& graph) {
    (void)graph;
    if (vertex == start_) {
      throw StopSearchException();
    }
  }
};

/**
 * @brief Visitor for performing Dijkstra's algorithm to find all shortest
 * paths.
 */
class DijkstraAllPathsVisitor : public boost::default_dijkstra_visitor {
  // The source vertex descriptor.
  VertexDescriptor source_;

  // Set of target node IDs.
  std::unordered_set<uint64_t> targets_;

  // Reference to the current path being explored.
  Path& currentPath_;

  // Reference to the collection of all found paths.
  std::vector<Path>& allPaths_;

  // Reference to the vector of predecessors.
  std::vector<VertexDescriptor>& predecessors_;

  // Reference to the vector of distances.
  std::vector<double>& distances_;

 public:
  /**
   * @brief Constructor for DijkstraAllPathsVisitor.
   * @param source The source vertex descriptor.
   * @param targets Set of target node IDs.
   * @param path Reference to the current path being explored.
   * @param paths Reference to the collection of all found paths.
   * @param predecessors Reference to the vector of predecessors.
   * @param distances Reference to the vector of distances.
   */
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

  /**
   * @brief Returns the vector of predecessors.
   * @return The vector of predecessors.
   */
  const std::vector<VertexDescriptor>& getPredecessors() const {
    return predecessors_;
  }

  /**
   * @brief Returns the vector of distances.
   * @return The vector of distances.
   */
  const std::vector<double>& getDistances() const { return distances_; }

  /**
   * @brief Called when an edge is relaxed during Dijkstra's algorithm.
   * @param edgeDesc The descriptor of the edge being relaxed.
   * @param graph The graph being searched.
   */
  void edge_relaxed(EdgeDescriptor edgeDesc, const Graph& graph) {
    const Edge& edge = graph[edgeDesc];
    if (targets_.empty() || targets_.find(edge.end_) != targets_.end()) {
      rebuild_path(target(edgeDesc, graph), graph);
    }
  }

  /**
   * @brief Rebuilds the path from the source to the given vertex.
   * @param vertex The descriptor of the vertex.
   * @param graph The graph being searched.
   */
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
