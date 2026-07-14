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
#include <limits>
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

// Try to perform a merge/zipper join of two sorted columns without UNDEF
// values. Call `addRow(leftIndex, rightIndex)` for single matching pairs and
// `addRows(leftBegin, leftEnd, rightBegin, rightEnd)` for the Cartesian
// product of groups of equal elements, in the same order as the generic
// zipper join. `notFoundAction(leftIndex)` is called for all elements of the
// left column without a matching element in the right column (pass
// `ad_utility::noop` unless this is an OPTIONAL join or a MINUS).
//
// The algorithm compares the datatype bytes and payload words of the split
// column storage directly and thus avoids materializing `Id`s in the hot
// loop. IDs of type `LocalVocabIndex` (which do not compare bitwise, but by
// their position in the vocabulary) are handled gracefully: the columns are
// processed in maximal chunks that contain no `LocalVocabIndex` IDs (these
// chunks are joined by the fast bitwise loop), and only the elements at and
// around the `LocalVocabIndex` positions are compared with the semantic `Id`
// comparison.
CPP_template(typename AddRow, typename AddRows, typename NotFoundAction,
             typename CancelCallback)(
    requires ql::concepts::invocable<AddRow, size_t, size_t> CPP_and
        ql::concepts::invocable<AddRows, size_t, size_t, size_t, size_t>
            CPP_and ql::concepts::invocable<NotFoundAction, size_t>
                CPP_and ql::concepts::invocable<
                    CancelCallback>) void zipperJoinIdColumns(ConstIdColumnSpan
                                                                  left,
                                                              ConstIdColumnSpan
                                                                  right,
                                                              AddRow addRow,
                                                              AddRows addRows,
                                                              NotFoundAction
                                                                  notFoundAction,
                                                              CancelCallback
                                                                  cancelCallback) {
  constexpr uint8_t localVocabType =
      static_cast<uint8_t>(Datatype::LocalVocabIndex);
  const auto typesLeft = left.datatypeSpan();
  const auto typesRight = right.datatypeSpan();
  const auto payloadsLeft = left.payloadSpan();
  const auto payloadsRight = right.payloadSpan();
  const size_t numLeft = left.size();
  const size_t numRight = right.size();

  auto makeId = [](uint8_t type, uint64_t payload) {
    return Id::fromBits({type, payload});
  };

  // Return the position of the next ID of type `LocalVocabIndex` at or after
  // the position `from` (or the size of the column if there is none).
  auto nextLocalVocab = [](ql::span<const uint8_t> types, size_t from) {
    const void* pointer =
        std::memchr(types.data() + from, localVocabType, types.size() - from);
    return pointer == nullptr
               ? types.size()
               : static_cast<size_t>(static_cast<const uint8_t*>(pointer) -
                                     types.data());
  };

  // Return the end of the group of elements that compare equal to the
  // element at position `group`. The group is first extended bitwise; if the
  // group then borders an ID of type `LocalVocabIndex` (or starts with one),
  // it is extended further using the semantic `Id` comparison (groups of
  // equivalent elements can contain interleaved `LocalVocabIndex` and
  // `VocabIndex`/`EncodedVal` IDs with different bit representations).
  auto groupEnd = [&makeId](ql::span<const uint8_t> types,
                            ql::span<const uint64_t> payloads, size_t numRows,
                            size_t group) {
    size_t end = group + 1;
    while (end < numRows && types[end] == types[group] &&
           payloads[end] == payloads[group]) {
      ++end;
    }
    if (end < numRows &&
        (types[end] == localVocabType || types[group] == localVocabType)) {
      Id representative = makeId(types[group], payloads[group]);
      while (end < numRows &&
             representative == makeId(types[end], payloads[end])) {
        ++end;
      }
    }
    return end;
  };

  size_t l = 0;
  size_t r = 0;
  // Cached positions of the next `LocalVocabIndex` IDs. They are recomputed
  // lazily, s.t. the total cost of all the `memchr` calls stays linear.
  size_t nextLocalVocabLeft = nextLocalVocab(typesLeft, 0);
  size_t nextLocalVocabRight = nextLocalVocab(typesRight, 0);
  size_t stepsUntilCancelCheck = 0;

  // Return the end of the run of equal datatype bytes that starts at `begin`
  // (bounded by `end`).
  auto typeRunEnd = [](ql::span<const uint8_t> types, size_t begin,
                       size_t end) {
    uint8_t type = types[begin];
    size_t position = begin + 1;
    while (position < end && types[position] == type) {
      ++position;
    }
    return position;
  };

  while (l < numLeft && r < numRight) {
    if (nextLocalVocabLeft < l) {
      nextLocalVocabLeft = nextLocalVocab(typesLeft, l);
    }
    if (nextLocalVocabRight < r) {
      nextLocalVocabRight = nextLocalVocab(typesRight, r);
    }

    // Phase 1: Bitwise merging of the chunks before the next
    // `LocalVocabIndex` IDs. The chunks are further subdivided into runs of
    // equal datatypes: runs of a datatype that only appears on one side are
    // skipped wholesale, and runs of equal datatypes are joined with a
    // payload-only merge loop (a single 64-bit comparison per step, exactly
    // as fast as with packed 64-bit IDs).
    bool groupTouchesChunkEnd = false;
    while (l < nextLocalVocabLeft && r < nextLocalVocabRight &&
           !groupTouchesChunkEnd) {
      uint8_t typeLeft = typesLeft[l];
      uint8_t typeRight = typesRight[r];
      if (typeLeft != typeRight) {
        // The datatype-major ID order guarantees that the smaller type run
        // has no matches on the other side, so it can be skipped completely.
        if (typeLeft < typeRight) {
          size_t runEnd = typeRunEnd(typesLeft, l, nextLocalVocabLeft);
          for (; l < runEnd; ++l) {
            notFoundAction(l);
          }
        } else {
          r = typeRunEnd(typesRight, r, nextLocalVocabRight);
        }
        continue;
      }
      // Both current elements have the same datatype: merge the two type
      // runs on the payloads only.
      const size_t runEndLeft = typeRunEnd(typesLeft, l, nextLocalVocabLeft);
      const size_t runEndRight = typeRunEnd(typesRight, r, nextLocalVocabRight);
      while (l < runEndLeft && r < runEndRight) {
        if (++stepsUntilCancelCheck >= (1ull << 20)) {
          stepsUntilCancelCheck = 0;
          cancelCallback();
        }
        uint64_t payloadLeft = payloadsLeft[l];
        uint64_t payloadRight = payloadsRight[r];
        if (payloadLeft < payloadRight) {
          notFoundAction(l);
          ++l;
        } else if (payloadRight < payloadLeft) {
          ++r;
        } else {
          // Find the ranges of equal payloads on both sides. Note: The
          // scans stop at the run ends, which is correct because the
          // elements there differ in their datatype byte.
          size_t endLeft = l + 1;
          while (endLeft < runEndLeft && payloadsLeft[endLeft] == payloadLeft) {
            ++endLeft;
          }
          size_t endRight = r + 1;
          while (endRight < runEndRight &&
                 payloadsRight[endRight] == payloadRight) {
            ++endRight;
          }
          // If a group extends up to a following `LocalVocabIndex` ID, the
          // group might semantically continue there, so it must be handled
          // by the semantic phase below.
          if ((endLeft == nextLocalVocabLeft && endLeft < numLeft) ||
              (endRight == nextLocalVocabRight && endRight < numRight)) {
            groupTouchesChunkEnd = true;
            break;
          }
          if (endLeft == l + 1 && endRight == r + 1) {
            addRow(l, r);
          } else {
            addRows(l, endLeft, r, endRight);
          }
          l = endLeft;
          r = endRight;
        }
      }
    }
    if (l >= numLeft || r >= numRight) {
      break;
    }

    // Phase 2: Semantic merging. Compare the current elements as `Id`s
    // (this correctly handles IDs of type `LocalVocabIndex`) and process
    // single elements or whole groups of equivalent elements. Keep going as
    // long as one of the current elements is of type `LocalVocabIndex`
    // (this processes contiguous regions of `LocalVocabIndex` IDs in one
    // tight loop).
    do {
      cancelCallback();
      Id idLeft = makeId(typesLeft[l], payloadsLeft[l]);
      Id idRight = makeId(typesRight[r], payloadsRight[r]);
      auto comparison = idLeft.compareThreeWay(idRight);
      if (comparison < 0) {
        notFoundAction(l);
        ++l;
      } else if (comparison > 0) {
        ++r;
      } else {
        size_t endLeft = groupEnd(typesLeft, payloadsLeft, numLeft, l);
        size_t endRight = groupEnd(typesRight, payloadsRight, numRight, r);
        if (endLeft == l + 1 && endRight == r + 1) {
          addRow(l, r);
        } else {
          addRows(l, endLeft, r, endRight);
        }
        l = endLeft;
        r = endRight;
      }
    } while (
        l < numLeft && r < numRight &&
        (typesLeft[l] == localVocabType || typesRight[r] == localVocabType));
  }
  // Report the unmatched trailing elements of the left column (relevant for
  // OPTIONAL joins and MINUS).
  for (; l < numLeft; ++l) {
    notFoundAction(l);
  }
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

namespace detail {

// Return `true` iff all elements of the `values` are equal.
template <typename T>
bool allElementsEqual(ql::span<T> values) {
  if (values.empty()) {
    return true;
  }
  const auto first = values.front();
  for (const auto& value : values) {
    if (value != first) {
      return false;
    }
  }
  return true;
}

// The implementation of `trySortByKeyColumnsBitwise` below for a fixed number
// of (non-constant) key columns. All preconditions (no `LocalVocabIndex` IDs
// in the key columns, at most `uint32_t` many rows) have already been checked
// by the caller.
template <size_t NumKeys, typename Table>
void sortByKeyColumnsBitwiseImpl(
    Table& table, const std::array<size_t, NumKeys>& keyColumns) {
  const size_t numRows = table.numRows();
  struct Entry {
    std::array<uint64_t, NumKeys> payloads_;
    uint32_t index_;
    std::array<uint8_t, NumKeys> types_;
  };
  std::vector<Entry> entries(numRows);
  // Fill the entries row-major in a single pass (reads from `NumKeys`
  // sequential streams, writes each entry exactly once).
  {
    std::array<const uint64_t*, NumKeys> payloads;
    std::array<const uint8_t*, NumKeys> types;
    for (size_t k = 0; k < NumKeys; ++k) {
      auto column = table.getColumn(keyColumns[k]);
      payloads[k] = column.payloadSpan().data();
      types[k] = column.datatypeSpan().data();
    }
#pragma omp parallel for
    for (size_t i = 0; i < numRows; ++i) {
      auto& entry = entries[i];
      for (size_t k = 0; k < NumKeys; ++k) {
        entry.payloads_[k] = payloads[k][i];
        entry.types_[k] = types[k][i];
      }
      entry.index_ = static_cast<uint32_t>(i);
    }
  }

  auto comparator = [](const Entry& a, const Entry& b) {
    for (size_t k = 0; k < NumKeys; ++k) {
      if (a.types_[k] != b.types_[k]) {
        return a.types_[k] < b.types_[k];
      }
      if (a.payloads_[k] != b.payloads_[k]) {
        return a.payloads_[k] < b.payloads_[k];
      }
    }
    return false;
  };
  if constexpr (USE_PARALLEL_SORT) {
    // Note: Deliberately no thread limit here; this replaces the unlimited
    // `parallel_sort` calls of the external sorter.
    ad_utility::parallel_sort(entries.begin(), entries.end(), comparator);
  } else {
    std::sort(entries.begin(), entries.end(), comparator);
  }

  // Apply the permutation to all columns. The scratch buffers are reused
  // across the columns, the gather loop is parallelized within each column.
  const auto numColumns = table.numColumns();
  std::vector<uint64_t> scratchPayloads(numRows);
  std::vector<uint8_t> scratchTypes(numRows);
  for (size_t c = 0; c < numColumns; ++c) {
    decltype(auto) column = table.getColumn(c);
    auto payloads = column.payloadSpan();
    auto types = column.datatypeSpan();
    // Constant columns are invariant under the permutation.
    if (allElementsEqual(ql::span<const uint64_t>{payloads}) &&
        allElementsEqual(ql::span<const uint8_t>{types})) {
      continue;
    }
#pragma omp parallel for
    for (size_t i = 0; i < numRows; ++i) {
      scratchPayloads[i] = payloads[entries[i].index_];
      scratchTypes[i] = types[entries[i].index_];
    }
    std::memcpy(payloads.data(), scratchPayloads.data(),
                numRows * sizeof(uint64_t));
    std::memcpy(types.data(), scratchTypes.data(), numRows * sizeof(uint8_t));
  }
}

// Call `sortByKeyColumnsBitwiseImpl` with the first `numActive` entries of
// the `activeKeys` as the (compile-time sized) array of key columns.
CPP_template(size_t N, size_t MaxKeys, typename Table)(requires(
    N >= 1 &&
    N <= MaxKeys)) void sortByKeyColumnsBitwiseDispatch(Table& table,
                                                        const std::array<
                                                            size_t, MaxKeys>&
                                                            activeKeys,
                                                        size_t numActive) {
  AD_CORRECTNESS_CHECK(numActive >= 1 && numActive <= MaxKeys);
  if (numActive == N) {
    std::array<size_t, N> keys;
    std::copy(activeKeys.begin(), activeKeys.begin() + N, keys.begin());
    sortByKeyColumnsBitwiseImpl<N>(table, keys);
    return;
  }
  if constexpr (N > 1) {
    sortByKeyColumnsBitwiseDispatch<N - 1>(table, activeKeys, numActive);
  }
}

}  // namespace detail

// Try to sort the `table` by the given `keyColumns` (compared in order,
// each datatype-major bitwise, which is exactly the ID order as long as no
// IDs of type `LocalVocabIndex` are involved). On success return `true`.
// Return `false` without modifying the table if one of the key columns
// contains an ID of type `LocalVocabIndex` (the caller then has to use a
// generic comparison sort), or if the table has more than `uint32_t` many
// rows (which in practice never happens for the blockwise sorts this
// function is used for).
//
// The sort extracts the keys and row indices into a compact contiguous
// array, sorts that array (in parallel if enabled), and then applies the
// resulting permutation to all columns of the table, one column at a time.
// This is much faster than a comparison sort on the rows of the table,
// which has to touch all payload and datatype buffers of the table for
// every swap (or, for the parallel multiway mergesort, has to materialize
// complete rows into its temporary buffers). Key columns whose value is
// constant within the `table` (e.g. typically the graph column) don't
// influence the order and are excluded, which makes the key entries smaller
// and the comparisons cheaper.
CPP_template(size_t NumKeys, typename Table)(requires(
    NumKeys >=
    1)) bool trySortByKeyColumnsBitwise(Table& table,
                                        const std::array<size_t, NumKeys>&
                                            keyColumns) {
  const size_t numRows = table.numRows();
  if (numRows <= 1) {
    return true;
  }
  if (numRows > std::numeric_limits<uint32_t>::max()) {
    return false;
  }
  for (size_t keyColumn : keyColumns) {
    auto types = table.getColumn(keyColumn).datatypeSpan();
    if (std::memchr(types.data(), static_cast<int>(Datatype::LocalVocabIndex),
                    types.size()) != nullptr) {
      return false;
    }
  }

  // Determine the key columns that are not constant.
  std::array<size_t, NumKeys> activeKeys{};
  size_t numActive = 0;
  for (size_t keyColumn : keyColumns) {
    auto column = table.getColumn(keyColumn);
    if (!(detail::allElementsEqual(
              ql::span<const uint64_t>{column.payloadSpan()}) &&
          detail::allElementsEqual(
              ql::span<const uint8_t>{column.datatypeSpan()}))) {
      activeKeys[numActive] = keyColumn;
      ++numActive;
    }
  }
  // If all key columns are constant, the table is trivially sorted.
  if (numActive == 0) {
    return true;
  }
  detail::sortByKeyColumnsBitwiseDispatch<NumKeys>(table, activeKeys,
                                                   numActive);
  return true;
}

}  // namespace columnBasedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNALGORITHMS_H
