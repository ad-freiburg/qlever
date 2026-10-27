// Copyright 2021 - 2025 The QLever Authors, in particular:
//
// 2021 - 2024 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025        Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_COMPRESSEDRELATIONHELPERSIMPL_H_
#define QLEVER_SRC_INDEX_COMPRESSEDRELATIONHELPERSIMPL_H_

#include <algorithm>
#include <array>
#include <functional>
#include <optional>
#include <utility>

#include "backports/algorithm.h"
#include "backports/numeric.h"
#include "index/compressedRelationWriter/CompressedRelationWriter.h"
#include "util/Algorithm.h"
#include "util/ExceptionHandling.h"

namespace compressedRelationHelpers {

static constexpr size_t c0Idx = 0;
static constexpr size_t c1Idx = 1;
static constexpr size_t c2Idx = 2;

// Return the binary representation of the given `id` and make sure (using
// `AD_EXPENSIVE_CHECK`) that it can be compared bitwise, which should always be
// true for index building, because there are no `Id`s of a local vocabulary
// then. Comparing the bits is much cheaper than the general comparison of
// `Id`s, and (in contrast to the latter) it can be vectorized.
constexpr Id::T bitsOfIdWithoutLocalVocab(Id id) {
  AD_EXPENSIVE_CHECK(id.canBeComparedBitwise());
  return id.getBits();
}

// Return the binary representation (see `bitsOfIdWithoutLocalVocab` above) of
// the cells of the `row` with the given column indices `Cols`. The resulting
// `std::array`s compare lexicographically, which makes the comparison of rows
// by these columns really cheap. Note: In a micro benchmark (sorting 5M rows
// with various distributions of duplicates) materializing the arrays was as
// fast as a handwritten comparison of the bits with an early exit, and both
// were 20-35% faster than comparing the `Id`s themselves.
template <size_t... Cols>
inline constexpr auto pickBitsOfColumns =
    [](const auto& row) -> std::array<Id::T, sizeof...(Cols)> {
  return {bitsOfIdWithoutLocalVocab(row[Cols])...};
};

// Compares two rows based on the columns `c1Idx`, `c2Idx`, and
// `ADDITIONAL_COLUMN_GRAPH_ID` only (it ignores the column `c0Idx` as well as
// any payload columns). The comparison is performed on the bits of the `Id`s,
// see `pickBitsOfColumns` above.
struct ComparatorForConstCol0 {
  template <typename A, typename B>
  constexpr bool operator()(const A& a, const B& b) const {
    auto pickBits = pickBitsOfColumns<c1Idx, c2Idx, ADDITIONAL_COLUMN_GRAPH_ID>;
    return pickBits(a) < pickBits(b);
  }
};

// Helper function to make a row from `IdTable` easier to compare. This selects
// the binary representation of the cells of the given row in the columns
// `c0Idx`, `c1Idx`, and `c2Idx`, see `pickBitsOfColumns` above.
inline constexpr auto pickFirstThreeColumnsOfIdsWithoutLocalVocab =
    pickBitsOfColumns<c0Idx, c1Idx, c2Idx>;

// Collect elements of type `T` in batches of size 100'000 and apply the
// `Function` to each batch. For the last batch (which might be smaller)  the
// function is applied in the destructor.
CPP_template(typename T, typename Function)(
    requires ad_utility::InvocableWithExactReturnType<Function, void,
                                                      std::vector<T> &&>)
struct Batcher {
  Function function_;
  size_t blocksize_;
  std::vector<T> vec_;

  // ___________________________________________________________________________
  Batcher(Function function, size_t blocksize)
      : function_{std::move(function)}, blocksize_{blocksize} {
    vec_.reserve(blocksize_);
  }

  // ___________________________________________________________________________
  void operator()(T t) {
    vec_.push_back(std::move(t));
    if (vec_.size() >= blocksize_) {
      function_(std::move(vec_));
      vec_.clear();
      vec_.reserve(blocksize_);
    }
  }

  // ___________________________________________________________________________
  ~Batcher() {
    ad_utility::terminateIfThrows(
        [this]() {
          if (!vec_.empty()) {
            function_(std::move(vec_));
          }
        },
        "Batcher function threw an exception while processing the final "
        "(possibly incomplete) block.");
  }

  // No copy or move operations (neither needed nor easy to get right).
  Batcher(const Batcher&) = delete;
  Batcher& operator=(const Batcher&) = delete;
};

using MetadataCallback = CompressedRelationWriter::MetadataCallback;

// The `CompressedRelationMetadata` for a single permutation can be directly
// input blockwise to the `MetadataCallback` (collecting the blocks uses the
// `Batcher` helper from above).
using SingleMetadataWriter =
    Batcher<CompressedRelationMetadata, MetadataCallback>;

// A class that is called for all pairs of `CompressedRelationMetadata` for
// the same `col0Id` and the "twin permutations" (e.g. PSO and POS). The
// multiplicity of the last column is exchanged and then the metadata are
// passed on to the respective `MetadataCallback`.
class PairMetadataWriter {
 private:
  SingleMetadataWriter batcher1_;
  SingleMetadataWriter batcher2_;

 public:
  // ___________________________________________________________________________
  PairMetadataWriter(MetadataCallback callback1, MetadataCallback callback2,
                     size_t blocksize)
      : batcher1_{std::move(callback1), blocksize},
        batcher2_{std::move(callback2), blocksize} {}

  // ___________________________________________________________________________
  void operator()(CompressedRelationMetadata md1,
                  CompressedRelationMetadata md2) {
    md1.multiplicityCol2_ = md2.multiplicityCol1_;
    md2.multiplicityCol2_ = md1.multiplicityCol1_;
    batcher1_(md1);
    batcher2_(md2);
  }
};

// The number of distinct IDs in a sorted column of a single block, together
// with the first and the last ID of that column. The latter two are needed to
// correct the count at the boundary between two consecutive blocks, where an ID
// that ends the one block and starts the other must not be counted twice.
struct DistinctIdCountOfBlock {
  size_t count_ = 0;
  Id first_ = Id::makeUndefined();
  Id last_ = Id::makeUndefined();
};

// Return the `DistinctIdCountOfBlock` of the `column`, which has to be sorted
// and must not be empty.
inline DistinctIdCountOfBlock countDistinctIds(ql::span<const Id> column) {
  AD_CORRECTNESS_CHECK(!column.empty());
  // Note: The comparison of the bits (instead of the general comparison of
  // `Id`s) is not only much cheaper per element, it also makes this reduction
  // vectorizable, which matters because it touches every single ID.
  size_t count = ql::transform_reduce(
      column.begin() + 1, column.end(), column.begin(), size_t{1},
      std::plus<>{}, [](Id current, Id previous) {
        return static_cast<size_t>(bitsOfIdWithoutLocalVocab(current) !=
                                   bitsOfIdWithoutLocalVocab(previous));
      });
  return {count, column.front(), column.back()};
}

// Count the distinct IDs in a sorted sequence of IDs that is split into several
// consecutive blocks, which are added one after the other via `addBlock`.
class DistinctIdCounter {
  size_t count_ = 0;
  // The bits (see `bitsOfIdWithoutLocalVocab`) of the last ID of the previously
  // added block, which must not be counted again if the next block starts with
  // it.
  std::optional<Id::T> lastBitsOfPreviousBlock_;

 public:
  // Add the `column` of the next block, which has to be sorted, and whose
  // first ID must not be smaller than the last ID of the previous block. An
  // empty `column` is ignored.
  void addBlock(ql::span<const Id> column) {
    if (column.empty()) {
      return;
    }
    addCountOfBlock(countDistinctIds(column));
  }

  // Like `addBlock`, but for a block whose `DistinctIdCountOfBlock` has
  // already been computed (e.g. concurrently, see
  // `CompressedRelationWriter::addCompleteLargeRelation`).
  void addCountOfBlock(const DistinctIdCountOfBlock& countOfBlock) {
    count_ += countOfBlock.count_;
    if (lastBitsOfPreviousBlock_ ==
        bitsOfIdWithoutLocalVocab(countOfBlock.first_)) {
      --count_;
    }
    lastBitsOfPreviousBlock_ = bitsOfIdWithoutLocalVocab(countOfBlock.last_);
  }

  // Reset the counter, so that it can be reused for the next relation. Use
  // this instead of `getAndReset` if the count itself is not needed.
  void reset() {
    count_ = 0;
    lastBitsOfPreviousBlock_.reset();
  }

  // Return the number of distinct IDs in all the blocks that have been added
  // since the last reset, and reset the counter.
  size_t getAndReset() {
    auto count = count_;
    reset();
    return count;
  }
};

// Return true if a relation with `numRows` rows is small for a permutation with
// the given `blocksize`. Small relations are batched together into blocks of
// small relations (for which no metadata is stored), while large relations are
// stored in blocks of their own.
inline bool isSmallRelation(size_t numRows, size_t blocksize) {
  return static_cast<double>(numRows) <= 0.8 * static_cast<double>(blocksize);
}

// Return the index of the first element `e` in `range[begin, end)` for which
// `projection(e) != value`, or `end` if there is no such element. This is
// the implementation of `findFirstTripleChange` below.
template <typename Range, typename Value, typename Projection>
size_t findFirstDifferent(const Range& range, size_t begin, size_t end,
                          const Value& value, const Projection& projection) {
  auto it = ql::ranges::find_if(
      range.begin() + begin, range.begin() + end,
      [&value](const auto& projected) { return projected != value; },
      projection);
  return static_cast<size_t>(it - range.begin());
}

// Return the index of the first row in `rows[begin, end)` whose first three
// columns differ from the `lastTriple`, or `end` if there is no such row.
// This is the first position at which a new block for a large relation may
// be started, because equal triples (when disregarding the graph and the
// payload columns) have to stay in the same block.
//
// Note: The rows are compared row-wise (and on the bits of the `Id`s, which
// is much cheaper, see `pickFirstThreeColumnsOfIdsWithoutLocalVocab`).
// Scanning each of the three columns separately and taking the minimum of
// the resulting offsets would use the cache more efficiently, but it would
// also be slower if one of the columns is (almost) constant. This function
// is only called once a complete block of a large relation has been found,
// and it then typically finds a differing row immediately, so it is not a
// bottleneck (unless in a very degenerate case) and we keep the simpler
// implementation.
template <typename Rows, typename Triple>
size_t findFirstTripleChange(const Rows& rows, size_t begin, size_t end,
                             const Triple& lastTriple) {
  return findFirstDifferent(rows, begin, end, lastTriple,
                            pickFirstThreeColumnsOfIdsWithoutLocalVocab);
}

// Return the end of the run of rows of an input block that starts at row
// `begin` and consists of all rows that have the same `col0` ID as that row.
// Such a run is exactly the part of one relation that is contained in the
// input block. The end is found by an exponential ("galloping") search over
// the sorted `col0` column, see `ad_utility::gallopingPartitionPoint`.
//
// NOTE: A linear scan would be slow for the long run of a large relation
// (which may span the complete input block), and a binary search would be
// slow for the short runs of the many small relations. The galloping search
// finds a short run with a handful of comparisons and a long run with a
// logarithmic number of them. The comparison is performed on the bits of the
// `Id`s, which is much cheaper, see `bitsOfIdWithoutLocalVocab`.
template <typename Col0>
size_t findEndOfRun(const Col0& col0, size_t begin) {
  const auto col0Bits = bitsOfIdWithoutLocalVocab(col0[begin]);
  auto belongsToRun = [col0Bits](Id id) {
    return bitsOfIdWithoutLocalVocab(id) == col0Bits;
  };
  // Note: The row `begin` is guaranteed to belong to the run, so the search
  // starts after it.
  auto it = ad_utility::gallopingPartitionPoint(col0.begin() + begin + 1,
                                                col0.end(), belongsToRun);
  return static_cast<size_t>(it - col0.begin());
}

// Cut the `rows` of a large relation into slices of about `blocksize` rows
// each and call `sliceCallback(begin, end)` for the row range of each slice,
// in order. Rows that agree in their first three columns are never split
// across two slices, so a slice may be larger than `blocksize`. The `rows`
// must not be empty.
template <typename Rows, typename SliceCallback>
void forEachSliceOfLargeBlock(const Rows& rows, size_t blocksize,
                              SliceCallback sliceCallback) {
  const size_t numRows = rows.numRows();
  AD_CORRECTNESS_CHECK(numRows > 0 && blocksize > 0);
  size_t begin = 0;
  while (begin < numRows) {
    size_t end = std::min(begin + blocksize, numRows);
    // Never split rows whose first three columns are equal across two
    // blocks.
    while (end < numRows &&
           pickFirstThreeColumnsOfIdsWithoutLocalVocab(rows[end]) ==
               pickFirstThreeColumnsOfIdsWithoutLocalVocab(rows[end - 1])) {
      ++end;
    }
    sliceCallback(begin, end);
    begin = end;
  }
}

// A buffer that turns the sorted blocks of a large relation into blocks that
// can be written as they are, because rows that agree in their first three
// columns (the triple without the graph) never end up in two different
// blocks. Each block is buffered until the next one shows that its last triple
// does not continue; the rows at the beginning of the next block that continue
// it are moved into the buffered block. Feed the blocks in order via `push`,
// and get the last block via `finish`.
template <typename Block>
class TripleAlignedBlockBuffer {
 private:
  std::optional<Block> bufferedBlock_;

 public:
  // Add the next sorted `block`. Return the previously buffered block if it is
  // complete now, and `std::nullopt` otherwise. Empty blocks are ignored.
  std::optional<Block> push(Block block) {
    if (block.empty()) {
      return std::nullopt;
    }
    if (!bufferedBlock_.has_value()) {
      bufferedBlock_ = std::move(block);
      return std::nullopt;
    }
    const auto lastTriple =
        pickFirstThreeColumnsOfIdsWithoutLocalVocab(bufferedBlock_->back());
    const size_t numContinuingRows =
        findFirstTripleChange(block, 0, block.numRows(), lastTriple);
    if (numContinuingRows > 0) {
      bufferedBlock_->insertAtEnd(block, 0, numContinuingRows);
      block.erase(block.begin(), block.begin() + numContinuingRows);
    }
    // If the `block` consists only of rows that continue the buffered block,
    // then the next block might continue it as well, so nothing is complete
    // yet.
    if (block.empty()) {
      return std::nullopt;
    }
    return std::exchange(bufferedBlock_, std::move(block));
  }

  // Return the last buffered block, if any. This has to be called after the
  // last `push`.
  std::optional<Block> finish() { return std::exchange(bufferedBlock_, {}); }
};

}  // namespace compressedRelationHelpers

#endif  // QLEVER_SRC_INDEX_COMPRESSEDRELATIONHELPERSIMPL_H_
