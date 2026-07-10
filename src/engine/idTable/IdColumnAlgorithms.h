// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNALGORITHMS_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNALGORITHMS_H

#include <algorithm>
#include <cstring>
#include <vector>

#include "backports/concepts.h"
#include "engine/idTable/IdColumn.h"
#include "engine/idTable/IdTable.h"
#include "global/Constants.h"
#include "global/Id.h"

// This file contains algorithms that exploit the split storage layout of the
// `Id` columns (see `IdColumn.h`): because the datatype bytes and the payload
// words are stored in separate contiguous arrays, algorithms on sorted
// columns can first cheaply partition the input into runs of equal datatypes
// and then work on the plain `uint64_t` payloads only. This makes the hot
// loops as fast as with the previous packed 64-bit ID layout (or faster,
// because the datatype handling is amortized per run instead of per element).
//
// IMPORTANT: All algorithms in this file compare IDs bitwise. They must
// therefore only be used if the involved columns contain no IDs of type
// `LocalVocabIndex` (those compare by their position in the vocabulary, not
// by their bits). All entry points below therefore detect this case and
// report it, s.t. the caller can fall back to the generic implementation.

namespace columnBasedIdTable {

// A contiguous range `[begin_, end_)` of positions of a column that all have
// the same datatype.
struct DatatypeRun {
  Datatype type_;
  size_t begin_;
  size_t end_;
};

// Compute the runs of equal datatypes of the `column`. For columns that are
// sorted in the datatype-major ID order this yields at most one run per
// datatype. This is a single cheap pass over the datatype bytes.
inline std::vector<DatatypeRun> computeDatatypeRuns(ConstIdColumnSpan column) {
  std::vector<DatatypeRun> runs;
  auto types = column.datatypeSpan();
  size_t i = 0;
  while (i < types.size()) {
    uint8_t type = types[i];
    // Find the end of the run. `memchr`-style scanning would also be
    // possible, but this loop is already memory-bound and vectorizes well.
    size_t end = i + 1;
    while (end < types.size() && types[end] == type) {
      ++end;
    }
    runs.push_back({static_cast<Datatype>(type), i, end});
    i = end;
  }
  return runs;
}

// Return true iff any of the `runs` has the `LocalVocabIndex` datatype.
inline bool anyRunIsLocalVocab(const std::vector<DatatypeRun>& runs) {
  return std::any_of(runs.begin(), runs.end(), [](const DatatypeRun& run) {
    return run.type_ == Datatype::LocalVocabIndex;
  });
}

// Try to perform a merge/zipper join of two sorted columns without UNDEF
// values. On success, call `addRow(leftIndex, rightIndex)` for each matching
// pair of positions (in the same order as the generic zipper join) and
// return `true`. Return `false` without calling `addRow` if any of the
// columns contains IDs of type `LocalVocabIndex` (see above); the caller
// then has to use the generic join.
//
// The join first merges the datatype runs of the two columns (runs of
// datatypes that only appear on one side are skipped in O(1)) and then joins
// the runs of equal datatypes on the plain payload words.
CPP_template(typename AddRow, typename CancelCallback)(
    requires ql::concepts::invocable<AddRow, size_t, size_t> CPP_and
        ql::concepts::invocable<
            CancelCallback>) bool tryZipperJoinOnDatatypeRuns(ConstIdColumnSpan
                                                                  left,
                                                              ConstIdColumnSpan
                                                                  right,
                                                              AddRow addRow,
                                                              CancelCallback
                                                                  cancelCallback) {
  auto runsLeft = computeDatatypeRuns(left);
  auto runsRight = computeDatatypeRuns(right);
  if (anyRunIsLocalVocab(runsLeft) || anyRunIsLocalVocab(runsRight)) {
    return false;
  }

  auto payloadsLeft = left.payloadSpan();
  auto payloadsRight = right.payloadSpan();

  // Join two runs of the same datatype on the plain payloads. This is a
  // classic merge join on `uint64_t` values, including the cross product for
  // duplicate values.
  size_t stepsUntilCancelCheck = 0;
  auto joinRuns = [&](const DatatypeRun& runLeft, const DatatypeRun& runRight) {
    size_t l = runLeft.begin_;
    size_t r = runRight.begin_;
    while (l < runLeft.end_ && r < runRight.end_) {
      if (++stepsUntilCancelCheck >= (1ull << 20)) {
        stepsUntilCancelCheck = 0;
        cancelCallback();
      }
      uint64_t valueLeft = payloadsLeft[l];
      uint64_t valueRight = payloadsRight[r];
      if (valueLeft < valueRight) {
        ++l;
      } else if (valueRight < valueLeft) {
        ++r;
      } else {
        // Find the ranges of equal values on both sides and add their cross
        // product.
        size_t endLeft = l + 1;
        while (endLeft < runLeft.end_ && payloadsLeft[endLeft] == valueLeft) {
          ++endLeft;
        }
        size_t endRight = r + 1;
        while (endRight < runRight.end_ &&
               payloadsRight[endRight] == valueLeft) {
          ++endRight;
        }
        for (size_t i = l; i < endLeft; ++i) {
          for (size_t j = r; j < endRight; ++j) {
            addRow(i, j);
          }
        }
        l = endLeft;
        r = endRight;
      }
    }
  };

  // Merge the datatype runs. Note: The datatype-major ID order guarantees
  // that the runs of a sorted column are sorted by their datatype.
  size_t indexLeft = 0;
  size_t indexRight = 0;
  while (indexLeft < runsLeft.size() && indexRight < runsRight.size()) {
    cancelCallback();
    const auto& runLeft = runsLeft[indexLeft];
    const auto& runRight = runsRight[indexRight];
    if (runLeft.type_ < runRight.type_) {
      ++indexLeft;
    } else if (runRight.type_ < runLeft.type_) {
      ++indexRight;
    } else {
      joinRuns(runLeft, runRight);
      ++indexLeft;
      ++indexRight;
    }
  }
  return true;
}

// Try to sort the `table` by the single column with index `keyColumn`. On
// success return `true`. Return `false` without modifying the table if the
// key column contains IDs of type `LocalVocabIndex` (see above); the caller
// then has to use the generic sort.
//
// The sort works as follows:
// 1. Partition the (payload, rowIndex) pairs of the key column by their
//    datatype using a counting sort on the datatype bytes.
// 2. Sort each datatype partition by the plain payload words (which is
//    exactly the datatype-major ID order within a partition).
// 3. Apply the resulting permutation to all columns of the table, one column
//    at a time (gather into a scratch buffer, then copy back). This avoids
//    the expensive row-wise swaps of the comparison sort, which have to
//    touch all payload and datatype buffers of the table for every swap.
inline bool trySortBySingleKeyColumn(::IdTable& table, ColumnIndex keyColumn) {
  const size_t numRows = table.numRows();
  if (numRows == 0) {
    return true;
  }
  auto key = std::as_const(table).getColumn(keyColumn);
  auto keyTypes = key.datatypeSpan();
  auto keyPayloads = key.payloadSpan();
  if (std::memchr(keyTypes.data(), static_cast<int>(Datatype::LocalVocabIndex),
                  keyTypes.size()) != nullptr) {
    return false;
  }

  // Step 1: Counting sort of the (payload, rowIndex) pairs by datatype.
  struct PayloadAndIndex {
    uint64_t payload_;
    uint64_t index_;
  };
  std::array<size_t, 256> counts{};
  for (uint8_t type : keyTypes) {
    ++counts[type];
  }
  std::array<size_t, 257> offsets{};
  for (size_t t = 0; t < 256; ++t) {
    offsets[t + 1] = offsets[t] + counts[t];
  }
  std::vector<PayloadAndIndex> permutation(numRows);
  {
    std::array<size_t, 256> positions{};
    std::copy(offsets.begin(), offsets.end() - 1, positions.begin());
    for (size_t i = 0; i < numRows; ++i) {
      permutation[positions[keyTypes[i]]++] = {keyPayloads[i], i};
    }
  }

  // Step 2: Sort each datatype partition by the payload.
  auto comparator = [](const PayloadAndIndex& a, const PayloadAndIndex& b) {
    return a.payload_ < b.payload_;
  };
  for (size_t t = 0; t < 256; ++t) {
    if (counts[t] > 1) {
      auto begin = permutation.begin() + offsets[t];
      auto end = permutation.begin() + offsets[t + 1];
      if constexpr (USE_PARALLEL_SORT) {
        ad_utility::parallel_sort(begin, end, comparator,
                                  ad_utility::parallel_tag(NUM_SORT_THREADS));
      } else {
        std::sort(begin, end, comparator);
      }
    }
  }

  // Step 3: Apply the permutation to all columns, one column at a time.
  std::vector<uint64_t> scratchPayloads(numRows);
  std::vector<uint8_t> scratchTypes(numRows);
  for (size_t c = 0; c < table.numColumns(); ++c) {
    auto column = table.getColumn(c);
    auto payloads = column.payloadSpan();
    auto types = column.datatypeSpan();
    for (size_t i = 0; i < numRows; ++i) {
      scratchPayloads[i] = payloads[permutation[i].index_];
      scratchTypes[i] = types[permutation[i].index_];
    }
    std::memcpy(payloads.data(), scratchPayloads.data(),
                numRows * sizeof(uint64_t));
    std::memcpy(types.data(), scratchTypes.data(), numRows * sizeof(uint8_t));
  }
  return true;
}

}  // namespace columnBasedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNALGORITHMS_H
