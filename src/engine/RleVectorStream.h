// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace ql::engine::rle {

// A column of `Id`s stored as runs: a `Run` is an `Id` together with the number
// of consecutive rows that hold it. Memory is proportional to the number of
// runs, not rows; `materialize` expands the runs into a flat column.
//
// Invariants: `totalRows()` is the sum of all run lengths, every run has a
// positive length, and two adjacent runs only have the same `Id` when the
// first one has the maximal length `UINT32_MAX`.
class RleVectorStream {
 public:
  struct Run {
    Id value_{Id::makeUndefined()};
    uint32_t length_ = 0;
  };

 private:
  std::vector<Run> runs_;
  size_t totalUncompressedRows_ = 0;

  static constexpr uint32_t MAX_RUN_LENGTH =
      std::numeric_limits<uint32_t>::max();

 public:
  RleVectorStream() = default;
  RleVectorStream(const RleVectorStream&) = default;
  RleVectorStream& operator=(const RleVectorStream&) = default;
  // A moved-from stream is empty, so its row count stays consistent.
  RleVectorStream(RleVectorStream&& other) noexcept
      : runs_{std::exchange(other.runs_, {})},
        totalUncompressedRows_{std::exchange(other.totalUncompressedRows_, 0)} {
  }
  RleVectorStream& operator=(RleVectorStream&& other) noexcept {
    runs_ = std::exchange(other.runs_, {});
    totalUncompressedRows_ = std::exchange(other.totalUncompressedRows_, 0);
    return *this;
  }

  // Append `length` rows with `value`. Extends the last run if it has the same
  // value; a run that would exceed `UINT32_MAX` rows is filled up to that
  // length and the rest starts a new run. Appending zero rows is a no-op.
  void append(Id value, uint32_t length) {
    if (length == 0) {
      return;
    }
    AD_CONTRACT_CHECK(
        length <= std::numeric_limits<size_t>::max() - totalUncompressedRows_,
        "Too many rows for an `RleVectorStream`");
    if (runs_.empty() || runs_.back().value_ != value) {
      runs_.push_back({value, length});
    } else {
      uint64_t merged = static_cast<uint64_t>(runs_.back().length_) + length;
      if (merged <= MAX_RUN_LENGTH) {
        runs_.back().length_ = static_cast<uint32_t>(merged);
      } else {
        // Add the new run first, so that the stream is unchanged if the
        // allocation throws.
        runs_.push_back(
            {value, static_cast<uint32_t>(merged - MAX_RUN_LENGTH)});
        runs_[runs_.size() - 2].length_ = MAX_RUN_LENGTH;
      }
    }
    totalUncompressedRows_ += length;
  }

  // Return the runs of equal consecutive `Id`s in `column`, or `std::nullopt`
  // if `column` has more than `maxNumRuns` runs. In the latter case, `column`
  // is only read up to the first run beyond the limit.
  static std::optional<RleVectorStream> fromColumn(ql::span<const Id> column,
                                                   size_t maxNumRuns) {
    RleVectorStream stream;
    auto runBegin = column.begin();
    while (runBegin != column.end()) {
      if (stream.numRuns() >= maxNumRuns) {
        return std::nullopt;
      }
      const Id value = *runBegin;
      auto runEnd = std::find_if(runBegin, column.end(),
                                 [value](Id id) { return id != value; });
      // `append` merges consecutive chunks of the same value.
      for (size_t remaining = runEnd - runBegin; remaining > 0;) {
        auto chunk =
            static_cast<uint32_t>(std::min<size_t>(remaining, MAX_RUN_LENGTH));
        stream.append(value, chunk);
        remaining -= chunk;
      }
      runBegin = runEnd;
    }
    return stream;
  }

  // The number of runs.
  size_t numRuns() const { return runs_.size(); }
  // The number of rows, i.e. the sum of all run lengths.
  size_t totalRows() const { return totalUncompressedRows_; }
  // The runs in row order. Invalidated by the next `append`.
  ql::span<const Run> runs() const { return runs_; }

  // Write the rows into the first `totalRows()` elements of `dest`, which must
  // have at least that size. Elements beyond are left unchanged.
  void materialize(ql::span<Id> dest) const {
    AD_CONTRACT_CHECK(dest.size() >= totalUncompressedRows_,
                      "Destination too small for the rows of the stream");
    auto out = dest.begin();
    for (const auto& run : runs_) {
      out = std::fill_n(out, run.length_, run.value_);
    }
  }
};

}  // namespace ql::engine::rle
