// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/GraphNameManager.h"

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
