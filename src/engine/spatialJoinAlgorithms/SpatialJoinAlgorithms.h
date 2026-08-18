// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2025        Patrick Brosi <brosi@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINALGORITHMS_H
#define QLEVER_SRC_ENGINE_SPATIALJOINALGORITHMS_H

#include "engine/Result.h"
#include "engine/SpatialJoin.h"

// Abstract base class for the algorithms computing a `SpatialJoin`. Each
// concrete algorithm (baseline, S2, bounding box/r-tree, libspatialjoin, ...)
// lives in its own `<Name>Algorithm.h`/`.cpp` and implements `run()`. This
// base class only holds the input shared by ALL of them and the couple of
// helpers every algorithm needs (writing a result row, checking for
// cancellation). Helpers shared by only a subset of the algorithms belong in
// an intermediate class instead (see `RtreeEntryAlgorithm.h`), so the other
// algorithms don't inherit state/methods they never use.
class SpatialJoinAlgorithms {
 public:
  // initialize the Algorithm with the needed parameters
  SpatialJoinAlgorithms(QueryExecutionContext* qec,
                        PreparedSpatialJoinParams params,
                        SpatialJoinConfiguration config,
                        std::optional<SpatialJoin*> spatialJoin = std::nullopt);
  virtual ~SpatialJoinAlgorithms() = default;

  // Run the algorithm and compute the join result.
  virtual Result run() = 0;

 protected:
  // Helper to add a row to the result table. Combines the selected columns
  // (given in `params_`) from the given row in each input table respectively.
  // Input tables are swapped if requested (required for the `WITHIN` join).
  void addResultTableEntry(IdTable* result, const IdTableView<0>* resultLeft,
                           const IdTableView<0>* resultRight, size_t rowLeft,
                           size_t rowRight, Id distance,
                           bool swapLeftAndRight = false) const;

  // Calls the `cancellationWrapper` which throws if the query has been
  // cancelled.
  void throwIfCancelled() const;

  QueryExecutionContext* qec_;
  PreparedSpatialJoinParams params_;
  SpatialJoinConfiguration config_;
  std::optional<SpatialJoin*> spatialJoin_;
};

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINALGORITHMS_H
