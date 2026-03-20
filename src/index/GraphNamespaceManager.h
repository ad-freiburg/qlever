// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_GRAPHNAMESPACEMANAGER_H
#define QLEVER_SRC_INDEX_GRAPHNAMESPACEMANAGER_H

#include "global/Constants.h"
#include "gtest/gtest_prod.h"
#include "rdfTypes/Iri.h"
#include "util/CopyableSynchronization.h"
#include "util/Serializer/SerializeAtomic.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/Serializer.h"
#include "util/Synchronized.h"
#include "util/json.h"

// Generates new graphs with a fixed prefix that don't exist yet. Currently,
// the graphs are of the form `{prefix}/{ascending number}`.
class GraphNamespaceManager {
  std::string prefixWithoutBraces_ = std::string(QLEVER_NEW_GRAPH_PREFIX);
  // The smallest number such that the graph for this number and all after it
  // are not used. Graphs that are generated are not necessarily all used so
  // there may be "gaps" in the actually used graphs.
  ad_utility::CopyableAtomic<uint64_t> nextUnallocatedGraph_ = 0;

  FRIEND_TEST(GraphNamespaceManager, storeAndRestoreData);
  FRIEND_TEST(IndexImpl, graphNamespaceManagerIntegration);

 public:
  GraphNamespaceManager() = default;
  // `nextUnallocatedGraph` is the smallest number for which the graph does not
  // exist for it and any larger number.
  GraphNamespaceManager(std::string prefix, uint64_t nextUnallocatedGraph);

  // Return a new graph IRI that has not been allocated previously.
  ad_utility::triple_component::Iri allocateNewGraph();

  friend void to_json(nlohmann::json& j,
                      const GraphNamespaceManager& namespaceManager);
  friend void from_json(const nlohmann::json& j,
                        GraphNamespaceManager& namespaceManager);

  friend std::ostream& operator<<(
      std::ostream& os, const GraphNamespaceManager& namespaceManager) {
    os << "GraphNamespaceManager(prefix=\""
       << namespaceManager.prefixWithoutBraces_ << "\", allocatedGraphs="
       << namespaceManager.nextUnallocatedGraph_.load() << ")";
    return os;
  }

  AD_SERIALIZE_FRIEND_FUNCTION(GraphNamespaceManager) {
    serializer | arg.prefixWithoutBraces_;
    serializer | arg.nextUnallocatedGraph_;
  }
};

#endif  // QLEVER_SRC_INDEX_GRAPHNAMESPACEMANAGER_H
