// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef GRAPHMANAGER_H
#define GRAPHMANAGER_H

#include "rdfTypes/Iri.h"
#include "util/Synchronized.h"

class GraphManager {
  ad_utility::Synchronized<uint64_t> allocatedGraphs_;

 public:
  QL_EXPLICIT(true)
  GraphManager(uint64_t largestAllocatedGraph)
      : allocatedGraphs_(largestAllocatedGraph) {}

  ad_utility::triple_component::Iri getNewInternalGraph();
};

#endif  // GRAPHMANAGER_H
