// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "GraphManager.h"
// _____________________________________________________________________________
GraphNamespaceManager::GraphNamespaceManager(std::string prefix,
                                             uint64_t allocatedGraphs)
    : prefix_(std::move(prefix)), allocatedGraphs_(allocatedGraphs) {}

// _____________________________________________________________________________
ad_utility::triple_component::Iri GraphNamespaceManager::allocateNewGraph() {
  auto graphId = allocatedGraphs_++;
  writeToDisk();
  return ad_utility::triple_component::Iri::fromIriref(
      prefix_ + std::to_string(graphId) + ">");
}

// _____________________________________________________________________________
void GraphNamespaceManager::setFilenameForPersistentUpdatesAndReadFromDisk(
    std::optional<std::string> filename) {
  setPersists(std::move(filename));
  readFromDisk();
}

// _____________________________________________________________________________
void GraphNamespaceManager::setPersists(std::optional<std::string> filename) {
  fileNameForPersisting_ = std::move(filename);
}

// _____________________________________________________________________________
void GraphNamespaceManager::writeToDisk() const {
  // TODO: compress the serialization
  if (fileNameForPersisting_) {
    ad_utility::serialization::FileWriteSerializer serializer{
        fileNameForPersisting_.value()};
    serializer << prefix_;
    serializer << allocatedGraphs_.load();
  }
}

void GraphNamespaceManager::readFromDisk() {
  AD_CORRECTNESS_CHECK(fileNameForPersisting_);
  if (std::filesystem::exists(fileNameForPersisting_.value())) {
    ad_utility::serialization::FileReadSerializer serializer{
        fileNameForPersisting_.value()};
    serializer >> prefix_;
    uint64_t allocatedGraphs;
    serializer >> allocatedGraphs;
    allocatedGraphs_.store(allocatedGraphs);
  }
}

// _____________________________________________________________________________
void to_json(nlohmann::json& j, const GraphNamespaceManager& namespaceManager) {
  j["prefix"] = namespaceManager.prefix_;
  j["allocatedGraphs"] = namespaceManager.allocatedGraphs_.load();
}

// _____________________________________________________________________________
void from_json(const nlohmann::json& j,
               GraphNamespaceManager& namespaceManager) {
  j.at("prefix").get_to(namespaceManager.prefix_);
  namespaceManager.allocatedGraphs_.store(
      j.at("allocatedGraphs").get<uint64_t>());
}

// _____________________________________________________________________________
std::ostream& operator<<(std::ostream& os,
                         const GraphNamespaceManager& namespaceManager) {
  os << "GraphNamespaceManager(prefix=\"" << namespaceManager.prefix_
     << "\", allocatedGraphs=" << namespaceManager.allocatedGraphs_.load()
     << ")";
  return os;
}
