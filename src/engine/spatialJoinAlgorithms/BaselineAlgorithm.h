// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_BASELINEALGORITHM_H
#define QLEVER_SRC_ENGINE_BASELINEALGORITHM_H

#include "engine/spatialJoinAlgorithms/RtreeEntryAlgorithm.h"

// The naive nested-loop spatial join: for every row of the left table, scan
// the whole right table. Used as a slow but simple reference implementation
// and as a fallback when no other algorithm applies.
class BaselineAlgorithm : public RtreeEntryAlgorithm {
 public:
  using RtreeEntryAlgorithm::RtreeEntryAlgorithm;

  Result run() override;

 private:
  // The maximum-results constraint, only meaningful for algorithms
  // supporting a `NearestNeighbors` task (this one and `S2GeometryAlgorithm`),
  // so it's self-computed from `config_` here rather than in the base class.
  std::optional<size_t> maxResults_ = config_.getMaxResults();
};

#endif  // QLEVER_SRC_ENGINE_BASELINEALGORITHM_H
