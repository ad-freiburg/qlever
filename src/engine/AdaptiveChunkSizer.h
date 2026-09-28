// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_ADAPTIVECHUNKSIZER_H
#define QLEVER_SRC_ENGINE_ADAPTIVECHUNKSIZER_H

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "util/Exception.h"

namespace qlever::export_streaming {

// _____________________________________________________________________________
// Configuration parameters for adaptive chunk sizing: the chunk size starts at
// `initialChunkBytes_` and is multiplied by `growthFactor_` after every chunk
// up to `maxChunkBytes_`.
struct AdaptiveChunkConfig {
  // Size of the first chunk in bytes (64 KiB).
  size_t initialChunkBytes_ = 64 * 1024;

  // Upper bound of the chunk size in bytes (4 MiB).
  size_t maxChunkBytes_ = 4 * 1024 * 1024;

  // Multiplier to scale buffer capacity on each successful chunk flush.
  double growthFactor_ = 2.0;

  // Prior heuristic estimate of byte size per formatted row/triple (120 bytes).
  double initialEstimatedRowBytes_ = 120.0;

  // Minimum allowed rows per chunk to avoid excessive chunk fragmentation.
  size_t minChunkRows_ = 1;

  // Maximum allowed rows per chunk to prevent unbounded memory spikes.
  size_t maxChunkRows_ = 1'000'000;
};

// _____________________________________________________________________________
// Immutable summary metrics and accounting state of an AdaptiveChunkSizer.
struct AdaptiveChunkStats {
  size_t chunksFlushed_{0};
  uint64_t totalBytes_{0};
  uint64_t totalRows_{0};
  size_t currentChunkBytes_{0};
  double averageRowBytes_{0.0};
  size_t targetRowsForNextChunk_{0};
};

// _____________________________________________________________________________
// Chunk sizes for a streamed query export.
//
// With a fixed chunk size, the first chunk is handed to the HTTP layer only
// after a full chunk has been formatted. `AdaptiveChunkSizer` makes the first
// chunk small and grows the chunk size geometrically:
//   1. The first chunk has `initialChunkBytes_` (64 KiB by default).
//   2. After every chunk, the size is multiplied by `growthFactor_` (2 by
//      default): 64 KiB, 128 KiB, 256 KiB, ...
//   3. The size stays at `maxChunkBytes_` once it is reached.
//   4. It keeps an estimate of the bytes per row (from the recorded chunks),
//      so that callers that cut chunks by rows can ask for a row count.
class AdaptiveChunkSizer {
 private:
  AdaptiveChunkConfig config_;
  size_t currentChunkBytesTarget_;
  uint64_t totalBytesObserved_{0};
  uint64_t totalRowsObserved_{0};
  size_t chunksFlushed_{0};
  double estimatedRowBytes_;

 public:
  // ___________________________________________________________________________
  // Default constructor: 64 KiB, doubling up to 4 MiB.
  AdaptiveChunkSizer() : AdaptiveChunkSizer(AdaptiveChunkConfig{}) {}

  // ___________________________________________________________________________
  // Construct with custom configuration.
  explicit AdaptiveChunkSizer(AdaptiveChunkConfig config)
      : config_{std::move(config)},
        currentChunkBytesTarget_{config_.initialChunkBytes_},
        estimatedRowBytes_{config_.initialEstimatedRowBytes_} {
    AD_CONTRACT_CHECK(config_.initialChunkBytes_ > 0);
    AD_CONTRACT_CHECK(config_.maxChunkBytes_ >= config_.initialChunkBytes_);
    AD_CONTRACT_CHECK(std::isfinite(config_.growthFactor_) &&
                      config_.growthFactor_ >= 1.0);
    AD_CONTRACT_CHECK(config_.initialEstimatedRowBytes_ > 0.0);
    AD_CONTRACT_CHECK(config_.minChunkRows_ >= 1);
    AD_CONTRACT_CHECK(config_.maxChunkRows_ >= config_.minChunkRows_);
  }

  // ___________________________________________________________________________
  // Convenience constructor with explicit capacity parameters.
  AdaptiveChunkSizer(size_t initialBytes, size_t maxBytes,
                     double growthFactor = 2.0,
                     double initialEstimatedRowBytes = 120.0)
      : AdaptiveChunkSizer(AdaptiveChunkConfig{
            initialBytes, maxBytes, growthFactor, initialEstimatedRowBytes}) {}

  // ___________________________________________________________________________
  // Target byte capacity for the active chunk buffer.
  [[nodiscard]] size_t currentChunkBytes() const noexcept {
    return currentChunkBytesTarget_;
  }

  // ___________________________________________________________________________
  // Average formatted bytes per row/triple observed so far.
  [[nodiscard]] double averageRowBytes() const noexcept {
    return estimatedRowBytes_;
  }

  // ___________________________________________________________________________
  // Calculate the recommended number of rows/triples to process in the next
  // batch based on current target chunk size and estimated row byte size.
  // Result is guaranteed to be clamped between [minChunkRows, maxChunkRows].
  [[nodiscard]] size_t targetRowCount() const {
    AD_CORRECTNESS_CHECK(estimatedRowBytes_ > 0.0);
    const double rawTargetRows =
        static_cast<double>(currentChunkBytesTarget_) / estimatedRowBytes_;
    const size_t rows = static_cast<size_t>(std::ceil(rawTargetRows));
    return std::clamp(rows, config_.minChunkRows_, config_.maxChunkRows_);
  }

  // ___________________________________________________________________________
  // Overload: target row count clamped by the total remaining un-exported rows.
  [[nodiscard]] size_t targetRowCount(size_t remainingRows) const {
    return std::min(targetRowCount(), remainingRows);
  }

  // ___________________________________________________________________________
  // Check if a buffer containing `bytesBuffered` has reached the current
  // target.
  [[nodiscard]] bool isChunkFull(size_t bytesBuffered) const noexcept {
    return bytesBuffered >= currentChunkBytesTarget_;
  }

  // ___________________________________________________________________________
  // Check if either the target byte capacity or target row count has been met.
  [[nodiscard]] bool isChunkFull(size_t bytesBuffered,
                                 size_t rowsBuffered) const {
    return bytesBuffered >= currentChunkBytesTarget_ ||
           (rowsBuffered > 0 && rowsBuffered >= targetRowCount());
  }

  // ___________________________________________________________________________
  // Feedback callback invoked whenever a chunk is flushed.
  // Updates running empirical row-size statistics and exponentially scales up
  // chunk capacity for the next batch up to `maxChunkBytes_`.
  void recordChunk(size_t bytesWritten, size_t rowCount) {
    if (rowCount > 0 && bytesWritten > 0) {
      totalBytesObserved_ += bytesWritten;
      totalRowsObserved_ += rowCount;
      // Exponentially weighted moving average combined with cumulative average
      // to adapt smoothly to varying row lengths while maintaining stability.
      const double chunkAvg =
          static_cast<double>(bytesWritten) / static_cast<double>(rowCount);
      const double cumulativeAvg = static_cast<double>(totalBytesObserved_) /
                                   static_cast<double>(totalRowsObserved_);
      // Weight recent chunk 30% and cumulative history 70%
      estimatedRowBytes_ = std::max(1.0, 0.7 * cumulativeAvg + 0.3 * chunkAvg);
    }

    ++chunksFlushed_;

    // Exponential ramp-up towards `maxChunkBytes_`. An empty chunk carries no
    // information and does not advance the ramp. The comparison is done in
    // `double`, so a large growth factor cannot overflow the conversion to
    // `size_t`.
    if (bytesWritten > 0 && currentChunkBytesTarget_ < config_.maxChunkBytes_) {
      const double nextBytes =
          std::ceil(static_cast<double>(currentChunkBytesTarget_) *
                    config_.growthFactor_);
      currentChunkBytesTarget_ =
          nextBytes >= static_cast<double>(config_.maxChunkBytes_)
              ? config_.maxChunkBytes_
              : static_cast<size_t>(nextBytes);
    }
  }

  // ___________________________________________________________________________
  // Convenience alias for recordChunk.
  void recordChunkFlushed(size_t bytesWritten, size_t rowCount) {
    recordChunk(bytesWritten, rowCount);
  }

  // ___________________________________________________________________________
  // Reset sizer back to initial 64 KB state (e.g. for reusing across queries).
  void reset() noexcept {
    currentChunkBytesTarget_ = config_.initialChunkBytes_;
    totalBytesObserved_ = 0;
    totalRowsObserved_ = 0;
    chunksFlushed_ = 0;
    estimatedRowBytes_ = config_.initialEstimatedRowBytes_;
  }

  // ___________________________________________________________________________
  // Accessors for diagnostic accounting and telemetry.
  [[nodiscard]] size_t chunksFlushed() const noexcept { return chunksFlushed_; }
  [[nodiscard]] uint64_t totalBytes() const noexcept {
    return totalBytesObserved_;
  }
  [[nodiscard]] uint64_t totalRows() const noexcept {
    return totalRowsObserved_;
  }
  [[nodiscard]] const AdaptiveChunkConfig& config() const noexcept {
    return config_;
  }

  // ___________________________________________________________________________
  // Snapshot of current statistics.
  [[nodiscard]] AdaptiveChunkStats stats() const {
    // Field order: chunksFlushed, totalBytes, totalRows, currentChunkBytes,
    // averageRowBytes, targetRowsForNextChunk.
    return AdaptiveChunkStats{chunksFlushed_,     totalBytesObserved_,
                              totalRowsObserved_, currentChunkBytesTarget_,
                              estimatedRowBytes_, targetRowCount()};
  }
};

}  // namespace qlever::export_streaming

namespace qlever {
using export_streaming::AdaptiveChunkConfig;
using export_streaming::AdaptiveChunkSizer;
using export_streaming::AdaptiveChunkStats;
}  // namespace qlever

#endif  // QLEVER_SRC_ENGINE_ADAPTIVECHUNKSIZER_H
