// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/GraphNamespaceManager.h"

// _____________________________________________________________________________
GraphNamespaceManager::GraphNamespaceManager(std::string prefixWithoutBraces,
                                             uint64_t nextUnallocatedGraph)
    : prefixWithoutBraces_(std::move(prefixWithoutBraces)),
      nextUnallocatedGraph_(nextUnallocatedGraph) {}

// _____________________________________________________________________________
ad_utility::triple_component::Iri GraphNamespaceManager::allocateNewGraph() {
  return ad_utility::triple_component::Iri::fromIriref(absl::StrCat(
      "<", prefixWithoutBraces_, std::to_string(nextUnallocatedGraph_++), ">"));
}

// _____________________________________________________________________________
void to_json(nlohmann::json& j, const GraphNamespaceManager& namespaceManager) {
  j["prefix"] = namespaceManager.prefixWithoutBraces_;
  j["allocatedGraphs"] = namespaceManager.nextUnallocatedGraph_.load();
}

// _____________________________________________________________________________
void from_json(const nlohmann::json& j,
               GraphNamespaceManager& namespaceManager) {
  j.at("prefix").get_to(namespaceManager.prefixWithoutBraces_);
  namespaceManager.nextUnallocatedGraph_ =
      j.at("allocatedGraphs").get<uint64_t>();
}
