// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_GRAPHMANAGER_H
#define QLEVER_GRAPHMANAGER_H

#include "global/Id.h"
#include "index/EncodedIriManager.h"
#include "index/Index.h"
#include "util/HashSet.h"
#include "util/json.h"

// The header is quite heavy.
class QueryExecutionContext;

// Keeps track of existing graphs in an inexact way. Only the absence of graphs
// can be queried. Graphs that are determined not absent by GraphManager may or
// may not actually exist.
class GraphManager {
  // A superset of all graphs that are currently in use.
  ad_utility::Synchronized<ad_utility::HashSet<Id>> graphs_;
  LocalVocab graphLocalVocab_;

 public:
  GraphManager() = default;
  explicit GraphManager(ad_utility::HashSet<Id> graphs);

  static GraphManager fillFromIndex(const EncodedIriManager* encodedIriManager,
                                    QueryExecutionContext& qec);
  static GraphManager fromExistingGraphs(ad_utility::HashSet<Id> graphs);

  friend void to_json(nlohmann::json& j, const GraphManager& graphManager);
  friend void from_json(const nlohmann::json& j, GraphManager& graphManager);

  void addGraphs(ad_utility::HashSet<Id> graphs);
  // TODO: the name is bad
  bool graphExists(const Id& graph) const;
  auto getGraphs() const { return graphs_.rlock(); }

  friend std::ostream& operator<<(std::ostream& os,
                                  const GraphManager& graphManager);

  // Manages the allocated (but not necessarily used or existing) graphs from a
  // graph namespace (defined by having the same prefix in the IRI).
  class GraphNamespaceManager {
    // TODO: note: this seems broadly similar to the BlankNodeManager
    std::string prefix_ = std::string(QLEVER_INTERNAL_GRAPH_IRI);
    ad_utility::Synchronized<uint64_t> allocatedGraphs_ =
        ad_utility::Synchronized<uint64_t>(0ul);

   public:
    GraphNamespaceManager() = default;
    GraphNamespaceManager(std::string prefix, uint64_t allocatedGraphs);

    static GraphNamespaceManager fromGraphManager(
        std::string prefix, const GraphManager& graphManager,
        const Index::Vocab& vocab);

    ad_utility::triple_component::Iri allocateNewGraph();

    friend void to_json(nlohmann::json& j,
                        const GraphNamespaceManager& namespaceManager);
    friend void from_json(const nlohmann::json& j,
                          GraphNamespaceManager& namespaceManager);

    friend std::ostream& operator<<(
        std::ostream& os, const GraphNamespaceManager& namespaceManager);
  };

private:
  std::optional<GraphNamespaceManager> namespaceManager_;
public:
  GraphNamespaceManager& getNamespaceManager();
  void initializeNamespaceManager(std::string prefix, const GraphManager& graphManager, const Index::Vocab& vocab);
};

#endif  // QLEVER_GRAPHMANAGER_H
