// Copyright 2021, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#ifndef QLEVER_SORTPERFORMANCEESTIMATOR_H
#define QLEVER_SORTPERFORMANCEESTIMATOR_H

#include <array>

#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/Timer.h"

/// Estimates the time it takes to sort an IdTable with a given number of rows
/// and columns;
class SortPerformanceEstimator {
  using Timer = ad_utility::Timer;

 public:
  // Create a SortPerformanceEstimator. This involves sorting large results to
  // get good estimates. The call might take several minutes, depending on the
  // memory available on the allocator. For this reason we have designed this
  // explicit factory function.

  // Create a random IdTable with the specified number of rows and columns. Sort
  // this table and return the time that this sorting took.
  static Timer::Duration measureSortingTime(
      size_t numRows, size_t numColumns,
      const ad_utility::AllocatorWithLimit<Id>& allocator);

  // Compute and return an Estimate for how long sorting an IdTable with the
  // specified number of rows and columns takes.
  Timer::Duration estimatedSortTime(size_t numRows,
                                    size_t numCols) const noexcept;

  // Create an uninitialized SortPerformanceEstimator, which is cheap. Before
  // using it, computeEstimatesExpensively has to be called
  SortPerformanceEstimator() = default;

  // Expensive constructor, directly calls computeEstimatesExpensively
  explicit SortPerformanceEstimator(
      const ad_utility::AllocatorWithLimit<Id>& allocator,
      size_t maxNumberOfElementsToSort);

  /// Set up the sort estimates. This will take some time. Only samples, that
  /// can be allocated by the allocator and that have less than
  /// `maxNumberOfElementsToSort` elements will actually be measured.
  void computeEstimatesExpensively(
      const ad_utility::AllocatorWithLimit<Id>& allocator,
      size_t maxNumberOfElementsToSort);

 private:
  // The number of columns for which we will sample the sorting time as a base
  // for the estimates. It is crucial that we have values for 5 and 6, because
  // at this point the IdTableImplementation changes.
  static constexpr std::array<size_t, 4> sampleValuesCols{1, 3, 5, 6};
  // The number of rows for which we will sample the sorting time as a base for
  // the estimates.
  static constexpr std::array<size_t, 4> sampleValuesRows{
      10'000, 100'000, 1'000'000, 10'000'000};
  static constexpr size_t NUM_SAMPLES_COLS = sampleValuesCols.size();
  static constexpr size_t NUM_SAMPLES_ROWS = sampleValuesRows.size();

  // The time in seconds for the samples that are sorted during initialization.
  // _samples[i][j] is the measured time it takes to sort an IdTable with
  // sampleValuesRows[i] rows and sampleValuesCols[j] columns.
  // The values are default-initialized to 0.
  std::array<std::array<Timer::Duration, NUM_SAMPLES_COLS>, NUM_SAMPLES_ROWS>
      _samples{};

  bool _estimatesWereCalculated = false;
};

#endif  // QLEVER_SORTPERFORMANCEESTIMATOR_H
