// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "GraphManager.h"

#include "global/Constants.h"

using namespace ad_utility;

// ____________________________________________________________________________
triple_component::Iri GraphManager::getNewInternalGraph() {
  auto graphId =
      allocatedGraphs_.withWriteLock([](auto& counter) { return counter++; });
  return triple_component::Iri::fromIriref(QLEVER_NEW_GRAPH_PREFIX +
                                           std::to_string(graphId) + ">");
}
