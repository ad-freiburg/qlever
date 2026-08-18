// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2025        Patrick Brosi <brosi@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/spatialJoinAlgorithms/SpatialJoinAlgorithms.h"

// ____________________________________________________________________________
SpatialJoinAlgorithms::SpatialJoinAlgorithms(
    QueryExecutionContext* qec, PreparedSpatialJoinParams params,
    SpatialJoinConfiguration config, std::optional<SpatialJoin*> spatialJoin)
    : qec_{qec},
      params_{std::move(params)},
      config_{std::move(config)},
      spatialJoin_{spatialJoin} {}

// ____________________________________________________________________________
void SpatialJoinAlgorithms::addResultTableEntry(
    IdTable* result, const IdTableView<0>* idTableLeft,
    const IdTableView<0>* idTableRight, size_t rowLeft, size_t rowRight,
    Id distance, bool swapLeftAndRight) const {
  auto resrow = result->numRows();
  result->emplace_back();
  size_t rescol = 0;

  // This helper copies values from `copyFrom` into the table `res` for all
  // columns given in `sourceColumns`.
  auto addColumns = [&resrow, &rescol, &result](
                        const IdTableView<0>* copyFrom, size_t rowIndCopy,
                        const std::vector<ColumnIndex>& sourceColumns) {
    for (size_t col : sourceColumns) {
      result->at(resrow, rescol) = copyFrom->at(rowIndCopy, col);
      ++rescol;
    }
  };

  if (swapLeftAndRight) {
    // Swap back the tables from a `SpatialJoinType::WITHIN` join.
    addColumns(idTableRight, rowRight, params_.leftSelectedCols_);
    addColumns(idTableLeft, rowLeft, params_.rightSelectedCols_);
  } else {
    addColumns(idTableLeft, rowLeft, params_.leftSelectedCols_);
    addColumns(idTableRight, rowRight, params_.rightSelectedCols_);
  }

  if (config_.distanceVariable_.has_value()) {
    result->at(resrow, rescol) = distance;
    // rescol isn't used after that in this function, but future updates,
    // which add additional columns, would need to remember to increase
    // rescol at this place otherwise. If they forget to do this, the
    // distance column will be overwritten, the variableToColumnMap will
    // not work and so on
    // rescol += 1;
  }
}

// ____________________________________________________________________________
void SpatialJoinAlgorithms::throwIfCancelled() const {
  if (spatialJoin_.has_value()) {
    spatialJoin_.value()->checkCancellationWrapperForSpatialJoinAlgorithms();
  }
}
