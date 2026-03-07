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
#include "util/Synchronized.h"
#include "util/json.h"

// Manages the allocated (but not necessarily used or existing) graphs from a
// graph namespace (defined by having the same prefix in the IRI).
class GraphNamespaceManager {
  std::string prefixWithoutBraces_ = std::string(QLEVER_NEW_GRAPH_PREFIX);
  ad_utility::Synchronized<uint64_t> allocatedGraphs_ =
      ad_utility::Synchronized<uint64_t>(0ul);
  std::optional<std::string> fileNameForPersisting_;

  FRIEND_TEST(GraphNamespaceManager, storeAndRestoreData);

 public:
  GraphNamespaceManager() = default;
  GraphNamespaceManager(std::string prefix, uint64_t allocatedGraphs);

  ad_utility::triple_component::Iri allocateNewGraph();

  void setFilenameForPersistentUpdatesAndReadFromDisk(
      std::optional<std::string> filename);

  friend void to_json(nlohmann::json& j,
                      const GraphNamespaceManager& namespaceManager);
  friend void from_json(const nlohmann::json& j,
                        GraphNamespaceManager& namespaceManager);

  friend std::ostream& operator<<(
      std::ostream& os, const GraphNamespaceManager& namespaceManager);

 private:
  void writeToDisk(uint64_t allocatedGraphs) const;
  void readFromDisk();
};

#endif  // QLEVER_SRC_INDEX_GRAPHNAMESPACEMANAGER_H
