// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_GRAPHMANAGER_H
#define QLEVER_SRC_ENGINE_GRAPHMANAGER_H

#include "engine/QueryExecutionContext.h"
#include "index/EncodedIriManager.h"
#include "rdfTypes/Iri.h"
#include "util/Synchronized.h"

class GraphManager {
  ad_utility::Synchronized<uint64_t> allocatedGraphs_;
  ad_utility::Synchronized<bool> isInitialized_;

 public:
  void initializeFromIndex(const EncodedIriManager* encodedIriManager,
                           QueryExecutionContext qec);
  void initializeForTesting(uint64_t);

  static ad_utility::HashMap<std::string, uint64_t> calculateGraphSizes(
      const EncodedIriManager* encodedIriManager, QueryExecutionContext qec);

  GraphManager() : allocatedGraphs_(0ul), isInitialized_(false) {}

  ad_utility::triple_component::Iri getNewInternalGraph();
};

#endif  // QLEVER_SRC_ENGINE_GRAPHMANAGER_H
