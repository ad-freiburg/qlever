// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_BITWISEKEYSORT_H
#define QLEVER_SRC_ENGINE_IDTABLE_BITWISEKEYSORT_H

#include <algorithm>
#include <array>
#include <boost/asio/thread_pool.hpp>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <type_traits>
#include <vector>

#include "backports/algorithm.h"
#include "backports/asio.h"
#include "backports/span.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/Exception.h"
#include "util/ParallelExecutor.h"
#include "util/UninitializedAllocator.h"
#include "util/blockSort/BlockIndirectSort.h"
#include "util/blockSort/BoostSortHeaders.h"

// Sort the rows of an `IdTable` by the bits of some of its columns (see
// `sortByBitwiseKeys` below). This is the sort order of all the sorts of the
// index build (there are no local vocab IDs then, so the `Id`s compare by
// their bits), and it can be computed much faster than by comparing the rows
// of the column-based table in place: the keys of each row are gathered into a
// contiguous row-major array, which is then sorted with a cheap comparison and
// without any proxy references, and the columns are permuted accordingly.
namespace ad_utility {

// Where, and with how many threads, a `sortByBitwiseKeys` runs. By default it
// runs serially in the calling thread.
struct BitwiseKeySortParallelism {
  ql::any_io_executor executor_{};
  size_t numThreads_ = 1;

  // Whether the sort runs in parallel. This is never the case on a thread that
  // runs the `executor_` itself (see `blockIndirectSort`, whose caller blocks
  // until the tasks on the executor have run).
  bool isParallel() const {
    return numThreads_ > 1 && static_cast<bool>(executor_) &&
           !runsInThisThread();
  }

 private:
  // Whether the calling thread is one of the threads of the `executor_`. This
  // can only be determined for the executor of a `boost::asio::thread_pool`
  // (which the global executor is); for all other executors, assume that it is
  // the case, which is the safe choice (see `isParallel`).
  bool runsInThisThread() const {
    using PoolExecutor = boost::asio::thread_pool::executor_type;
    const auto* poolExecutor = executor_.target<PoolExecutor>();
    return poolExecutor == nullptr || poolExecutor->running_in_this_thread();
  }
};

// A comparator for the rows of an `IdTable` can declare that it compares the
// bits of certain columns lexicographically by defining a static member
// `bitwiseKeyColumns` of type `std::array<size_t, N>`. A sort with such a
// comparator can then be replaced by `sortByBitwiseKeys`, see e.g. the
// `BlockSorter` in `CompressedExternalIdTable.h`.
template <typename Comparator, typename = void>
constexpr bool hasBitwiseKeyColumns = false;
template <typename Comparator>
constexpr bool hasBitwiseKeyColumns<
    Comparator, std::void_t<decltype(Comparator::bitwiseKeyColumns)>> = true;

namespace bitwiseKeySort::detail {

// A row of keys, possibly followed by the index of the row in the table.
template <size_t Width>
using KeyRow = std::array<uint64_t, Width>;

// The memory for the key rows. Each thread reuses one such buffer for all its
// sorts (see `keyRowsOfThread`), because a freshly allocated buffer of the
// size of a sorter block has to be faulted in page by page, which costs more
// than the sort itself for large blocks.
using KeyBuffer = std::vector<
    uint64_t,
    ad_utility::default_init_allocator<uint64_t, std::allocator<uint64_t>>>;

// Return `numRows` key rows of `Width` in the given `buffer` (which is grown
// if necessary), as a span over the (trivially copyable, padding-free)
// `KeyRow`s.
template <size_t Width>
ql::span<KeyRow<Width>> keyRowsInBuffer(KeyBuffer& buffer, size_t numRows) {
  static_assert(sizeof(KeyRow<Width>) == Width * sizeof(uint64_t));
  const size_t numWords = numRows * Width;
  if (buffer.size() < numWords) {
    // Free the old memory before allocating the new one.
    buffer = KeyBuffer{};
    buffer.resize(numWords);
  }
  return {reinterpret_cast<KeyRow<Width>*>(buffer.data()), numRows};
}

// The key rows of `Width` for `numRows` rows in the buffer of the calling
// thread, see `KeyBuffer`.
template <size_t Width>
ql::span<KeyRow<Width>> keyRowsOfThread(size_t numRows) {
  thread_local KeyBuffer buffer;
  return keyRowsInBuffer<Width>(buffer, numRows);
}

// The same for the second buffer of the calling thread, which the parallel
// sample sort (see `sampleSortInParallel`) scatters the rows into.
template <size_t Width>
ql::span<KeyRow<Width>> scratchRowsOfThread(size_t numRows) {
  thread_local KeyBuffer buffer;
  return keyRowsInBuffer<Width>(buffer, numRows);
}

// Lexicographic comparison of the key rows. The loop over a constant number of
// keys is unrolled by the compiler. This is a function object (and not a
// function), so that the sorting algorithms can inline it.
template <size_t Width>
struct LessKeyRow {
  bool operator()(const KeyRow<Width>& a, const KeyRow<Width>& b) const {
    for (size_t i = 0; i < Width; ++i) {
      if (a[i] != b[i]) {
        return a[i] < b[i];
      }
    }
    return false;
  }
};

// The buffer for the bucket indices of the parallel sample sort (see
// `sampleSortInParallel`), one byte per row, which the thread reuses.
inline std::vector<uint8_t, ad_utility::default_init_allocator<
                                uint8_t, std::allocator<uint8_t>>>&
bucketIndicesOfThread() {
  thread_local std::vector<uint8_t, ad_utility::default_init_allocator<
                                        uint8_t, std::allocator<uint8_t>>>
      buffer;
  return buffer;
}

// Below this many rows, the parallel sort uses `blockIndirectSort`, above it
// the sample sort below.
constexpr size_t MIN_ROWS_FOR_SAMPLE_SORT = 1 << 20;

// Sort the `rows` in parallel with a sample sort: pick splitters from a random
// sample, partition the rows into buckets by the splitters (a histogram pass
// and a scatter pass, both parallel over ranges of rows), and sort each bucket
// with `pdqsort`, in parallel over the buckets. The partitioning is only two
// passes of sequential memory access, and the buckets are sorted completely
// independently of each other, which is what makes this faster than a
// parallel quicksort (whose top-level partitions are sequential) or a merge
// of sorted parts.
//
// This needs a second buffer of the size of the `rows` (the rows are scattered
// into it, sorted there, and copied back) and one byte per row for the bucket
// indices, both of which the calling thread keeps for its next sort.
template <size_t Width>
void sampleSortInParallel(ql::span<KeyRow<Width>> rows,
                          const BitwiseKeySortParallelism& parallelism) {
  using Row = KeyRow<Width>;
  const size_t numRows = rows.size();
  const size_t numThreads = parallelism.numThreads_;
  // Several buckets per thread, so that buckets of different sizes still
  // balance well. At most 256, so that a bucket index fits into a byte.
  const size_t numBuckets = std::min<size_t>(256, numThreads * 8);
  constexpr size_t oversampling = 32;

  // The splitters: a sorted random sample, of which every `oversampling`-th
  // element is taken.
  std::vector<Row> sample;
  sample.reserve(numBuckets * oversampling);
  {
    // A cheap fixed-seed generator suffices (the sort is correct for any
    // splitters, they only affect the balance of the buckets).
    uint64_t state = 0x9E3779B97F4A7C15ull;
    for (size_t i = 0; i < numBuckets * oversampling; ++i) {
      state ^= state << 13;
      state ^= state >> 7;
      state ^= state << 17;
      sample.push_back(rows[state % numRows]);
    }
  }
  boost::sort::pdqsort(sample.begin(), sample.end(), LessKeyRow<Width>{});
  std::vector<Row> splitters;
  for (size_t b = 1; b < numBuckets; ++b) {
    splitters.push_back(sample[b * oversampling]);
  }
  // The bucket of a row is the number of splitters that are not greater than
  // it (so rows equal to a splitter go to the bucket after it).
  auto bucketOf = [&splitters](const Row& row) -> uint8_t {
    return static_cast<uint8_t>(std::upper_bound(splitters.begin(),
                                                 splitters.end(), row,
                                                 LessKeyRow<Width>{}) -
                                splitters.begin());
  };

  // Pass 1: the bucket of each row, and a histogram per task.
  const size_t numTasks = numThreads * 4;
  const size_t rowsPerTask = (numRows + numTasks - 1) / numTasks;
  // NOTE: A reference to the buffer of this thread is taken here, because a
  // `thread_local` variable that is named inside the lambdas below would
  // refer to the buffer of whichever thread runs the lambda.
  auto& buckets = bucketIndicesOfThread();
  if (buckets.size() < numRows) {
    buckets.resize(numRows);
  }
  std::vector<std::vector<size_t>> histograms(
      numTasks, std::vector<size_t>(numBuckets, 0));
  auto taskRange = [&](size_t taskIdx) {
    size_t begin = std::min(taskIdx * rowsPerTask, numRows);
    size_t end = std::min(begin + rowsPerTask, numRows);
    return std::pair{begin, end};
  };
  runIndexedTasksOnExecutor(parallelism.executor_, numThreads, numTasks,
                            [&](size_t taskIdx) {
                              auto [begin, end] = taskRange(taskIdx);
                              auto& histogram = histograms[taskIdx];
                              for (size_t i = begin; i < end; ++i) {
                                uint8_t bucket = bucketOf(rows[i]);
                                buckets[i] = bucket;
                                ++histogram[bucket];
                              }
                            });

  // The offsets: bucket by bucket, task by task (so that the rows of a bucket
  // keep the order of the tasks, which doesn't matter for the sort, but makes
  // the scatter sequential per task and bucket).
  std::vector<std::vector<size_t>> offsets(numTasks,
                                           std::vector<size_t>(numBuckets, 0));
  std::vector<size_t> bucketBegin(numBuckets + 1, 0);
  {
    size_t offset = 0;
    for (size_t b = 0; b < numBuckets; ++b) {
      bucketBegin[b] = offset;
      for (size_t t = 0; t < numTasks; ++t) {
        offsets[t][b] = offset;
        offset += histograms[t][b];
      }
    }
    bucketBegin[numBuckets] = offset;
    AD_CORRECTNESS_CHECK(offset == numRows);
  }

  // Pass 2: scatter the rows into the buckets of the second buffer (which
  // the thread reuses as well, see `KeyBuffer`).
  ql::span<Row> scattered = scratchRowsOfThread<Width>(numRows);
  runIndexedTasksOnExecutor(parallelism.executor_, numThreads, numTasks,
                            [&](size_t taskIdx) {
                              auto [begin, end] = taskRange(taskIdx);
                              auto& offset = offsets[taskIdx];
                              for (size_t i = begin; i < end; ++i) {
                                scattered[offset[buckets[i]]++] = rows[i];
                              }
                            });

  // Pass 3: sort the buckets, largest first (for the balance), and copy each
  // sorted bucket back.
  std::vector<size_t> bucketOrder(numBuckets);
  std::iota(bucketOrder.begin(), bucketOrder.end(), size_t{0});
  ql::ranges::sort(bucketOrder, [&bucketBegin](size_t a, size_t b) {
    return bucketBegin[a + 1] - bucketBegin[a] >
           bucketBegin[b + 1] - bucketBegin[b];
  });
  runIndexedTasksOnExecutor(
      parallelism.executor_, numThreads, numBuckets, [&](size_t taskIdx) {
        size_t b = bucketOrder[taskIdx];
        auto begin = scattered.begin() + bucketBegin[b];
        auto end = scattered.begin() + bucketBegin[b + 1];
        boost::sort::pdqsort(begin, end, LessKeyRow<Width>{});
        std::copy(begin, end, rows.begin() + bucketBegin[b]);
      });
}

// Call `f(begin, end)` for disjoint ranges `[begin, end)` that cover
// `[0, numRows)`, in parallel if the `parallelism` allows it. Return only when
// all the calls are done.
template <typename F>
void forRowRanges(size_t numRows, const BitwiseKeySortParallelism& parallelism,
                  const F& f) {
  constexpr size_t minRowsPerTask = 1 << 14;
  if (!parallelism.isParallel() || numRows < 2 * minRowsPerTask) {
    f(0, numRows);
    return;
  }
  const size_t numTasks =
      std::min(numRows / minRowsPerTask, parallelism.numThreads_ * 4);
  const size_t rowsPerTask = (numRows + numTasks - 1) / numTasks;
  runIndexedTasksOnExecutor(parallelism.executor_, parallelism.numThreads_,
                            numTasks, [&](size_t taskIdx) {
                              size_t begin = taskIdx * rowsPerTask;
                              size_t end =
                                  std::min(begin + rowsPerTask, numRows);
                              if (begin < end) {
                                f(begin, end);
                              }
                            });
}

// The implementation of `sortByBitwiseKeys`. If `WithIndex` is `true`, the
// key rows carry the index of their row as the last element, which is needed
// to permute the columns that are not keys.
template <size_t Width, bool WithIndex, size_t NumKeys, typename Table>
void sortImpl(Table& table, const std::array<size_t, NumKeys>& keyColumns,
              const BitwiseKeySortParallelism& parallelism) {
  static_assert(Width == NumKeys + (WithIndex ? 1 : 0));
  const size_t numRows = table.numRows();
  // NOTE: Only a parallel sort (of a large block, by a dedicated thread, see
  // `BlockSorter`) uses the buffer of the thread. A serial sort typically
  // runs on one of many short-lived threads (e.g. the twin sorters of the
  // chunks of the permutation writer), which should not each hold a large
  // buffer; the allocator reuses the memory of a freed buffer for the next
  // sort anyway.
  KeyBuffer localBuffer;
  auto rows = parallelism.isParallel()
                  ? keyRowsOfThread<Width>(numRows)
                  : keyRowsInBuffer<Width>(localBuffer, numRows);
  // Gather the keys row by row (one contiguous write per row), reading from
  // all the key columns at the same time.
  std::array<const Id*, NumKeys> keyPointers;
  for (size_t k = 0; k < NumKeys; ++k) {
    keyPointers[k] = table.getColumn(keyColumns[k]).data();
  }
  forRowRanges(numRows, parallelism, [&](size_t begin, size_t end) {
    for (size_t i = begin; i < end; ++i) {
      KeyRow<Width>& row = rows[i];
      for (size_t k = 0; k < NumKeys; ++k) {
        row[k] = keyPointers[k][i].getBits();
      }
      if constexpr (WithIndex) {
        row[NumKeys] = i;
      }
    }
  });

  if (parallelism.isParallel() && numRows >= MIN_ROWS_FOR_SAMPLE_SORT) {
    sampleSortInParallel(rows, parallelism);
  } else if (parallelism.isParallel()) {
    blockSort::blockIndirectSort(rows, LessKeyRow<Width>{},
                                 static_cast<uint32_t>(parallelism.numThreads_),
                                 parallelism.executor_);
  } else {
    boost::sort::pdqsort(rows.begin(), rows.end(), LessKeyRow<Width>{});
  }

  // Permute the columns that are not keys via the index: gather the column
  // into a buffer, then copy it back. The key columns are then overwritten by
  // the keys.
  if constexpr (WithIndex) {
    std::vector<Id, ad_utility::default_init_allocator<Id, std::allocator<Id>>>
        buffer(numRows);
    for (size_t c = 0; c < table.numColumns(); ++c) {
      if (ql::ranges::find(keyColumns, c) != keyColumns.end()) {
        continue;
      }
      auto column = table.getColumn(c);
      forRowRanges(numRows, parallelism, [&](size_t begin, size_t end) {
        for (size_t i = begin; i < end; ++i) {
          buffer[i] = column[rows[i][NumKeys]];
        }
      });
      forRowRanges(numRows, parallelism, [&](size_t begin, size_t end) {
        std::copy(buffer.begin() + begin, buffer.begin() + end,
                  column.begin() + begin);
      });
    }
  }
  // Scatter the keys back row by row (one contiguous read per row), writing
  // to all the key columns at the same time.
  std::array<Id*, NumKeys> keyColumnPointers;
  for (size_t k = 0; k < NumKeys; ++k) {
    keyColumnPointers[k] = table.getColumn(keyColumns[k]).data();
  }
  forRowRanges(numRows, parallelism, [&](size_t begin, size_t end) {
    for (size_t i = begin; i < end; ++i) {
      const KeyRow<Width>& row = rows[i];
      for (size_t k = 0; k < NumKeys; ++k) {
        keyColumnPointers[k][i] = Id::fromBits(row[k]);
      }
    }
  });
}
}  // namespace bitwiseKeySort::detail

// Sort the rows of the `table` (an `IdTable` or `IdTableStatic`, not a view)
// by the bits of the `keyColumns`, compared lexicographically as unsigned
// 64-bit integers. This is the order of `Id::compareWithoutLocalVocab` and of
// `compressedRelationHelpers::pickBitsOfColumns`. The sort is stable. It runs
// serially in the calling thread, unless the `parallelism` says otherwise.
//
// The sort needs additional memory of `(keyColumns.size() + 1) * 8` bytes per
// row (8 bytes less per row if the keys are exactly all the columns), which
// the calling thread keeps for its next sort, see `keyRowsOfThread`.
template <size_t NumKeys, typename Table>
void sortByBitwiseKeys(Table& table,
                       const std::array<size_t, NumKeys>& keyColumns,
                       const BitwiseKeySortParallelism& parallelism = {}) {
  static_assert(NumKeys > 0);
  if (table.numRows() < 2) {
    return;
  }
  for (size_t c : keyColumns) {
    AD_CONTRACT_CHECK(c < table.numColumns());
  }
  // If the keys are exactly all the columns, the key rows are the complete rows
  // and no index is needed.
  auto sortedKeys = keyColumns;
  ql::ranges::sort(sortedKeys);
  bool keysAreAllColumns =
      table.numColumns() == NumKeys &&
      ql::ranges::adjacent_find(sortedKeys) == sortedKeys.end();
  if (keysAreAllColumns) {
    bitwiseKeySort::detail::sortImpl<NumKeys, false>(table, keyColumns,
                                                     parallelism);
  } else {
    bitwiseKeySort::detail::sortImpl<NumKeys + 1, true>(table, keyColumns,
                                                        parallelism);
  }
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_ENGINE_IDTABLE_BITWISEKEYSORT_H
