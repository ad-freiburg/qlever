// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/GraphNamespaceManager.h"

#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"

// _____________________________________________________________________________
GraphNamespaceManager::GraphNamespaceManager(std::string prefixWithoutBraces,
                                             uint64_t allocatedGraphs)
    : prefixWithoutBraces_(std::move(prefixWithoutBraces)),
      allocatedGraphs_(allocatedGraphs) {}

// _____________________________________________________________________________
ad_utility::triple_component::Iri GraphNamespaceManager::allocateNewGraph() {
  auto graphId = allocatedGraphs_.withWriteLock([this](auto& allocatedGraphs) {
    auto graphId = allocatedGraphs++;
    writeToDisk();
    return graphId;
  });
  return ad_utility::triple_component::Iri::fromIriref(
      absl::StrCat("<", prefixWithoutBraces_, std::to_string(graphId), ">"));
}

// _____________________________________________________________________________
void GraphNamespaceManager::setFilenameForPersistentUpdatesAndReadFromDisk(
    std::optional<std::string> filename) {
  fileNameForPersisting_ = std::move(filename);
  readFromDisk();
}

// _____________________________________________________________________________
void GraphNamespaceManager::writeToDisk() const {
  // TODO: compress the serialization
  if (fileNameForPersisting_) {
    ad_utility::serialization::FileWriteSerializer serializer{
        fileNameForPersisting_.value()};
    serializer << prefixWithoutBraces_;
    serializer << *allocatedGraphs_.rlock();
  }
}

// _____________________________________________________________________________
void GraphNamespaceManager::readFromDisk() {
  AD_CORRECTNESS_CHECK(fileNameForPersisting_);
  if (std::filesystem::exists(fileNameForPersisting_.value())) {
    ad_utility::serialization::FileReadSerializer serializer{
        fileNameForPersisting_.value()};
    serializer >> prefixWithoutBraces_;
    uint64_t allocatedGraphs;
    serializer >> allocatedGraphs;
    *allocatedGraphs_.wlock() = allocatedGraphs;
  }
}

// _____________________________________________________________________________
void to_json(nlohmann::json& j, const GraphNamespaceManager& namespaceManager) {
  j["prefix"] = namespaceManager.prefixWithoutBraces_;
  j["allocatedGraphs"] = *namespaceManager.allocatedGraphs_.rlock();
}

// _____________________________________________________________________________
void from_json(const nlohmann::json& j,
               GraphNamespaceManager& namespaceManager) {
  j.at("prefix").get_to(namespaceManager.prefixWithoutBraces_);
  *namespaceManager.allocatedGraphs_.wlock() =
      j.at("allocatedGraphs").get<uint64_t>();
}

// _____________________________________________________________________________
std::ostream& operator<<(std::ostream& os,
                         const GraphNamespaceManager& namespaceManager) {
  os << "GraphNamespaceManager(prefix=\""
     << namespaceManager.prefixWithoutBraces_
     << "\", allocatedGraphs=" << *namespaceManager.allocatedGraphs_.rlock()
     << ")";
  return os;
}
