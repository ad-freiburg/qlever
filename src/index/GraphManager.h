// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_GRAPHMANAGER_H
#define QLEVER_SRC_INDEX_GRAPHMANAGER_H

#include "index/Index.h"
#include "util/json.h"

// Manages the allocated (but not necessarily used or existing) graphs from a
// graph namespace (defined by having the same prefix in the IRI).
class GraphNamespaceManager {
  std::string prefix_ = std::string(QLEVER_INTERNAL_GRAPH_IRI);
  std::atomic<uint64_t> allocatedGraphs_ = std::atomic(0ul);
  std::optional<std::string> fileNameForPersisting_;

  FRIEND_TEST(GraphNamespaceManager, storeAndRestoreData);

 public:
  GraphNamespaceManager() = default;
  GraphNamespaceManager(std::string prefix, uint64_t allocatedGraphs);
  // TODO: ...
  GraphNamespaceManager& operator=(GraphNamespaceManager&& other) noexcept {
    prefix_ = std::move(other.prefix_);
    allocatedGraphs_ = other.allocatedGraphs_.load();
    return *this;
  }
  GraphNamespaceManager(const GraphNamespaceManager& other)
      : GraphNamespaceManager(other.prefix_, other.allocatedGraphs_.load()) {}

  ad_utility::triple_component::Iri allocateNewGraph();

  void setFilenameForPersistentUpdatesAndReadFromDisk(
      std::optional<std::string> filename);

  friend void to_json(nlohmann::json& j,
                      const GraphNamespaceManager& namespaceManager);
  friend void from_json(const nlohmann::json& j,
                        GraphNamespaceManager& namespaceManager);

  friend std::ostream& operator<<(
      std::ostream& os, const GraphNamespaceManager& namespaceManager);

  void writeToDisk() const;

 private:
  void setPersists(std::optional<std::string> filename);
  void readFromDisk();
};

#endif  // QLEVER_GRAPHMANAGER_H
