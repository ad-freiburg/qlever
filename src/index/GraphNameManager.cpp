// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/GraphNameManager.h"

#include <filesystem>

#include "util/Serializer/FileSerializer.h"

// _____________________________________________________________________________
GraphNameManager::GraphNameManager(std::string prefixWithoutBraces,
                                   uint64_t nextUnallocatedGraph)
    : prefixWithoutBraces_(std::move(prefixWithoutBraces)),
      nextUnallocatedGraph_(nextUnallocatedGraph) {}

// _____________________________________________________________________________
ad_utility::triple_component::Iri GraphNameManager::allocateNewGraph() {
  return ad_utility::triple_component::Iri::fromIriref(
      absl::StrCat("<", prefixWithoutBraces_, nextUnallocatedGraph_++, ">"));
}

// _____________________________________________________________________________
void GraphNameManager::writeToDisk() const {
  if (!filenameForPersisting_.has_value()) {
    return;
  }
  auto path = filenameForPersisting_.value();
  auto tempPath = path;
  tempPath += ".tmp";
  ad_utility::serialization::FileWriteSerializer serializer{tempPath.c_str()};
  serializer | *this;
  std::filesystem::rename(tempPath, path);
}

// _____________________________________________________________________________
void GraphNameManager::readFromDisk() {
  if (!filenameForPersisting_.has_value()) {
    return;
  }
  if (!std::filesystem::exists(filenameForPersisting_.value())) {
    return;
  }

  ad_utility::serialization::FileReadSerializer serializer{
      filenameForPersisting_.value().c_str()};
  serializer | *this;
}

// _____________________________________________________________________________
void GraphNameManager::setFilenameForPersistingAndReadFromDisk(
    std::filesystem::path filename) {
  filenameForPersisting_ = std::move(filename);
  readFromDisk();
}

// _____________________________________________________________________________
void to_json(nlohmann::json& j, const GraphNameManager& namespaceManager) {
  j["prefix"] = namespaceManager.prefixWithoutBraces_;
  j["allocatedGraphs"] = namespaceManager.nextUnallocatedGraph_.load();
}

// _____________________________________________________________________________
void from_json(const nlohmann::json& j, GraphNameManager& namespaceManager) {
  j.at("prefix").get_to(namespaceManager.prefixWithoutBraces_);
  namespaceManager.nextUnallocatedGraph_ =
      j.at("allocatedGraphs").get<uint64_t>();
}
