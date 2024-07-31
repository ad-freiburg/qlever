// Copyright 2021, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "engine/SortPerformanceEstimator.h"

#include <absl/strings/str_cat.h>

#include <cstdlib>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/idTable/IdTable.h"
#include "global/RuntimeParameters.h"
#include "util/CancellationHandle.h"
#include "util/Log.h"
#include "util/Random.h"
#include "util/Timer.h"

// ___________________________________________________________________
IdTable createRandomIdTable(
    size_t numRows, size_t numColumns,
    const ad_utility::AllocatorWithLimit<Id>& allocator) {
  IdTable result{allocator};
  result.setNumColumns(numColumns);
  result.reserve(numRows);

  ad_utility::FastRandomIntGenerator<uint32_t> generator;

  for (size_t i = 0; i < numRows; ++i) {
    result.emplace_back();
    for (size_t j = 0; j < numColumns; ++j) {
      result(i, j) = Id::makeFromVocabIndex(VocabIndex::make(generator()));
    }
  }
  return result;
}

template <size_t I>
constexpr bool isSorted(const std::array<size_t, I>& array) {
  return std::is_sorted(array.begin(), array.end());
}

// ____________________________________________________________________
auto SortPerformanceEstimator::measureSortingTime(
    size_t numRows, size_t numColumns,
    const ad_utility::AllocatorWithLimit<Id>& allocator) -> Timer::Duration {
  auto randomTable = createRandomIdTable(numRows, numColumns, allocator);
  ad_utility::Timer timer{ad_utility::Timer::Started};
  // Always sort on the first column for simplicity;
  CALL_FIXED_SIZE(numColumns, &Engine::sort, &randomTable, 0ull);
  return timer.value();
}

// ____________________________________________________________________________
SortPerformanceEstimator::SortPerformanceEstimator(
    const ad_utility::AllocatorWithLimit<Id>& allocator,
    size_t maxNumElementsToSort) {
  computeEstimatesExpensively(allocator, maxNumElementsToSort);
}

auto SortPerformanceEstimator::estimatedSortTime(size_t numRows,
                                                 size_t numCols) const noexcept
    -> Timer::Duration {
  if (!_estimatesWereCalculated) {
    LOG(WARN) << "The estimates of the SortPerformanceEstimator were never set "
                 "up, Sorts will thus never time out"
              << std::endl;
    return Timer::Duration::zero();
  }
  // Return the index of the element in the !sorted! `sampleVector`, which is
  // closest to 'value'
  auto getClosestIndex = [](const auto& sampleVector, size_t value) -> size_t {
    // Return the absolute distance to `value`
    auto dist = [value](const size_t& x) {
      return std::abs(static_cast<long long>(value) -
                      static_cast<long long>(x));
    };

    // Returns true iff `a` is closer to `value` than `b`
    auto cmp = [&dist](const auto& a, const auto& b) {
      return dist(a) < dist(b);
    };

    auto closestIterator =
        std::min_element(sampleVector.begin(), sampleVector.end(), cmp);
    return closestIterator - sampleVector.begin();
  };

  // Get index of the closest samples wrt. numRows and numCols
  auto rowIndex = getClosestIndex(sampleValuesRows, numRows);
  auto columnIndex = getClosestIndex(sampleValuesCols, numCols);

  // start with the closest sample
  Timer::Duration result = _samples[rowIndex][columnIndex];

  LOG(TRACE) << "Closest sample result was " << sampleValuesRows[rowIndex]
             << " rows with " << sampleValuesCols[columnIndex]
             << " columns and an estimate of " << Timer::toSeconds(result)
             << " seconds." << std::endl;

  auto numRowsInSample = static_cast<double>(sampleValuesRows[rowIndex]);
  double rowRatio = static_cast<double>(numRows) / numRowsInSample;

  auto numColumnsInSample = static_cast<double>(sampleValuesCols[columnIndex]);
  double columnRatio = static_cast<double>(numCols) / numColumnsInSample;
  // Scale linearly with the number of rows. Scale the number of columns with
  // the square root. The cast `toDuration` is necessary because the
  // multiplication with a float (`rowRation`) propagates from the integer-
  // based `Duration` to a float-based `std::chrono::duration`. The cast is
  // semantically valid as the `result` is typically much larger than the
  // timer resolution specified via the `Duration` (currently microseconds).
  result = Timer::toDuration(result * rowRatio * std::sqrt(columnRatio));

  return result;
}

void SortPerformanceEstimator::computeEstimatesExpensively(
    const ad_utility::AllocatorWithLimit<Id>& allocator,
    size_t maxNumberOfElementsToSort) {
  static_assert(isSorted(sampleValuesCols));
  static_assert(isSorted(sampleValuesRows));

  LOG(INFO) << "Sorting random result tables to estimate the sorting "
               "performance of this machine ..."
            << std::endl;

  _samples.fill({});
  for (size_t i = 0; i < NUM_SAMPLES_ROWS; ++i) {
    for (size_t j = 0; j < NUM_SAMPLES_COLS; ++j) {
      auto rows = sampleValuesRows[i];
      auto cols = sampleValuesCols[j];
      // Track if the current sample could be measured, or if we
      // have to estimate it from smaller samples.
      bool estimateSortingTime = false;
      try {
        // If the sorting volume does not exceed `maxNumberOfElementsToSort` or
        // for the smallest sample size (i = 0 and j = 0), measure the running
        // time. Otherwise, or if the measurement uses too much space, estimate
        // it from a smaller sample size.
        if (rows * cols > maxNumberOfElementsToSort && (i > 0 || j > 0)) {
          estimateSortingTime = true;
        }
#ifndef NDEBUG
        if (rows > 100000) {
          estimateSortingTime = true;
        }
#endif
        if (!estimateSortingTime) {
          _samples[i][j] = measureSortingTime(rows, cols, allocator);
        }
      } catch (const ad_utility::detail::AllocationExceedsLimitException& e) {
        estimateSortingTime = true;
      }

      if (estimateSortingTime) {
        // These estimates are not too important, since results of this size
        // cannot be sorted anyway because of the memory limit.
        LOG(TRACE) << "Creating the table failed because of a lack of memory"
                   << std::endl;
        LOG(TRACE) << "Creating an estimate from a smaller result" << std::endl;
        if (i > 0) {
          // Assume that sorting time grows linearly in the number of rows. For
          // details on the usage of `toDuration()` see its first usage above.
          float ratio = static_cast<float>(sampleValuesRows[i]) /
                        static_cast<float>(sampleValuesRows[i - 1]);
          _samples[i][j] = Timer::toDuration(_samples[i - 1][j] * ratio);
        } else if (j > 0) {
          // Assume that sorting time grows with the square root in the number
          // of columns. The square root is just a heuristic: a simple function
          // between linear and constant. For details on the usage of
          // `toDuration()` see its first usage above.
          float ratio = static_cast<float>(sampleValuesCols[j]) /
                        static_cast<float>(sampleValuesCols[j - 1]);
          _samples[i][j] =
              Timer::toDuration(_samples[i][j - 1] * std::sqrt(ratio));
        } else {
          // not even the smallest IdTable could be created, this should never
          // happen.
          LOG(WARN)
              << "Could not create any estimate for the sorting performance. "
              << "Setting all estimates to 0. This means that no sort "
              << "operations will be canceled." << std::endl;
        }
        LOG(TRACE) << "Estimated the sort time to be " << std::fixed
                   << std::setprecision(3) << Timer::toSeconds(_samples[i][j])
                   << " seconds." << std::endl;
      }
    }
  }
  LOG(DEBUG) << "Done computing sort estimates" << std::endl;
  _estimatesWereCalculated = true;
}
// ___________________________________________________________________________
void SortPerformanceEstimator::throwIfEstimateTooLong(
    size_t numRows, size_t numColumns,
    std::chrono::steady_clock::time_point deadline,
    std::string_view operationDescriptor) const {
  auto sortEstimateCancellationFactor =
      RuntimeParameters().get<"sort-estimate-cancellation-factor">();
  auto now = std::chrono::steady_clock::now();
  if (now > deadline || estimatedSortTime(numRows, numColumns) >
                            (deadline - now) * sortEstimateCancellationFactor) {
    // The estimated time for this sort is much larger than the actually
    // remaining time, cancel this operation.
    throw ad_utility::CancellationException(
        absl::StrCat(operationDescriptor,
                     " was canceled, because time estimate exceeded "
                     "remaining time by a factor of ",
                     sortEstimateCancellationFactor));
  }
}
