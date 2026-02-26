// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_GRAPHMANAGER_H
#define QLEVER_GRAPHMANAGER_H

#include "index/EncodedIriManager.h"
#include "util/json.h"

// The header is quite heavy.
class QueryExecutionContext;

class GraphManager {
  // A superset of all graphs that are currently in use.
  std::vector<std::string> graphs_;
  uint64_t allocatedGraphs_;

 public:
  GraphManager() = default;
  explicit GraphManager(std::vector<std::string> graphs, uint64_t allocatedGraphs);

  static GraphManager fillFromIndex(const EncodedIriManager* encodedIriManager,
                                    QueryExecutionContext& qec);
  static GraphManager fromExistingGraphs(std::vector<std::string> graphs);

  friend void to_json(nlohmann::json& j, const GraphManager& graphManager) {
    j["graphs"] = graphManager.graphs_;
    j["allocatedGraphs"] = graphManager.allocatedGraphs_;
  }
  friend void from_json(const nlohmann::json& j, GraphManager& graphManager) {
    graphManager.graphs_ = static_cast<std::vector<std::string>>(j["graphs"]);
    graphManager.allocatedGraphs_ = j["allocatedGraphs"].get<uint64_t>();
  }

  void addGraphs(std::vector<std::string> graphs);
  // TODO: the name is bad
  bool graphExists(const std::string& graph) const;
  ad_utility::triple_component::Iri getNewInternalGraph();

  friend std::ostream& operator<<(std::ostream& os,
                                   const GraphManager& graphManager) {
    os << "GraphManager(graphs=[";
    for (size_t i = 0; i < graphManager.graphs_.size(); ++i) {
      if (i > 0) os << ", ";
      os << graphManager.graphs_[i];
    }
    os << "], allocatedGraphs=" << graphManager.allocatedGraphs_ << ")";
    return os;
  }
};

#endif  // QLEVER_GRAPHMANAGER_H
