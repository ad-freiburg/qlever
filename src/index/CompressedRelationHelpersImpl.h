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

#include <array>
#include <deque>
#include <future>
#include <optional>
#include <utility>

#include "index/CompressedRelationWriter.h"
#include "util/ExceptionHandling.h"
#include "util/GlobalExecutor.h"
#include "util/TaskQueueOnExecutor.h"

namespace compressedRelationHelpers {

static constexpr size_t c1Idx = 1;
static constexpr size_t c2Idx = 2;

// Return the binary representation of the given `id` and make sure (using
// `AD_EXPENSIVE_CHECK`) that it can be compared bitwise, which should always be
// true for index building, because there are no `Id`s of a local vocabulary
// then. Comparing the bits is much cheaper than the general comparison of
// `Id`s, and (in contrast to the latter) it can be vectorized.
inline Id::T bitsOfIdWithoutLocalVocab(Id id) {
  AD_EXPENSIVE_CHECK(id.canBeComparedBitwise());
  return id.getBits();
}

// Compares two rows based on the second, third and fourth column only (it
// ignores the first column as well as any payload columns). The comparison is
// performed on the bits of the `Id`s, see `bitsOfIdWithoutLocalVocab` above.
struct ComparatorForConstCol0 {
  // Pick the bits of the cells that this comparator looks at. The resulting
  // `std::array`s compare lexicographically, which is exactly the desired
  // order.
  template <typename Row>
  static std::array<Id::T, 3> pickBits(const Row& row) {
    return {bitsOfIdWithoutLocalVocab(row[c1Idx]),
            bitsOfIdWithoutLocalVocab(row[c2Idx]),
            bitsOfIdWithoutLocalVocab(row[ADDITIONAL_COLUMN_GRAPH_ID])};
  }

  template <typename A, typename B>
  bool operator()(const A& a, const B& b) const {
    return pickBits(a) < pickBits(b);
  }
};

// Helper function to make a row from `IdTable` easier to compare. This selects
// the binary representation (see `bitsOfIdWithoutLocalVocab` above) of the
// cells of the given row with the indices 0, 1 and 2. This way comparison
// becomes really cheap.
inline auto pickFirstThreeColumnsOfIdsWithoutLocalVocab = [](const auto& row) {
  return std::array{bitsOfIdWithoutLocalVocab(row[0]),
                    bitsOfIdWithoutLocalVocab(row[1]),
                    bitsOfIdWithoutLocalVocab(row[2])};
};

// Collect elements of type `T` in batches of size 100'000 and apply the
// `Function` to each batch. For the last batch (which might be smaller)  the
// function is applied in the destructor.
CPP_template(typename T, typename Function)(
    requires ad_utility::InvocableWithExactReturnType<
        Function, void, std::vector<T>&&>) struct Batcher {
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
  size_t count = 1;
  // Note: The comparison of the bits (instead of the general comparison of
  // `Id`s) is not only much cheaper per element, it also makes this loop
  // vectorizable, which matters because it touches every single ID.
  for (size_t i = 1; i < column.size(); ++i) {
    count += static_cast<size_t>(bitsOfIdWithoutLocalVocab(column[i]) !=
                                 bitsOfIdWithoutLocalVocab(column[i - 1]));
  }
  return {count, column.front(), column.back()};
}

// Read the blocks of the `BlockRange` (skipping empty blocks) and count the
// distinct IDs in one of their columns on the global thread pool, while the
// caller is still processing the preceding blocks. The blocks are yielded again
// in their original order, because the further processing of the blocks has to
// happen in order.
template <typename BlockRange>
class AsyncDistinctIdCounter {
 public:
  using Block = std::decay_t<ql::ranges::range_value_t<BlockRange>>;

 private:
  // One block that has been read, together with the (possibly still running)
  // computation of the number of distinct IDs in its column.
  struct Entry {
    Block block_;
    std::future<DistinctIdCountOfBlock> count_;
  };

  size_t columnIdx_;
  size_t maxNumBlocksInFlight_;
  ql::ranges::iterator_t<BlockRange> it_;
  ql::ranges::sentinel_t<BlockRange> end_;
  std::deque<Entry> pending_;
  // The last ID of the column of the previously yielded block, which must not
  // be counted again if the next block starts with it.
  Id lastIdOfPreviousBlock_ = Id::makeUndefined();
  bool hasPreviousBlock_ = false;

  // NOTE: This member is deliberately declared last, so that its destructor
  // (which waits for all pending tasks) runs before the `pending_` blocks,
  // into which those tasks point, are destroyed.
  ad_utility::TaskQueueOnExecutor queue_;

 public:
  // Construct from the `blocks` (which have to outlive this object), the index
  // of the column whose distinct IDs are counted, and the maximal number of
  // blocks that are read ahead.
  AsyncDistinctIdCounter(BlockRange& blocks, size_t columnIdx,
                         size_t maxNumBlocksInFlight)
      : columnIdx_{columnIdx},
        maxNumBlocksInFlight_{maxNumBlocksInFlight},
        it_{ql::ranges::begin(blocks)},
        end_{ql::ranges::end(blocks)},
        queue_{ad_utility::globalExecutor(), maxNumBlocksInFlight,
               "Counting distinct IDs in the blocks of a large relation"} {
    AD_CONTRACT_CHECK(maxNumBlocksInFlight_ > 0);
  }

  // Return the next non-empty block together with the number of distinct IDs in
  // its column, where an ID that already ended the previous block is not
  // counted again. Return `std::nullopt` once all blocks have been yielded.
  std::optional<std::pair<Block, size_t>> next() {
    readAhead();
    if (pending_.empty()) {
      return std::nullopt;
    }
    // Note: `get()` rethrows an exception that the background task has thrown.
    DistinctIdCountOfBlock countOfBlock = pending_.front().count_.get();
    Block block = std::move(pending_.front().block_);
    pending_.pop_front();
    size_t count = countOfBlock.count_;
    if (hasPreviousBlock_ &&
        bitsOfIdWithoutLocalVocab(lastIdOfPreviousBlock_) ==
            bitsOfIdWithoutLocalVocab(countOfBlock.first_)) {
      --count;
    }
    lastIdOfPreviousBlock_ = countOfBlock.last_;
    hasPreviousBlock_ = true;
    return std::pair<Block, size_t>{std::move(block), count};
  }

 private:
  // Read blocks and start the counting for them until `maxNumBlocksInFlight_`
  // blocks are pending or the input is exhausted.
  void readAhead() {
    while (pending_.size() < maxNumBlocksInFlight_ && it_ != end_) {
      Block block = std::move(*it_);
      ++it_;
      if (block.empty()) {
        continue;
      }
      pending_.push_back(Entry{std::move(block), {}});
      // Note: The `column` is a view into the memory of the block. That memory
      // is stable, because `std::deque` never moves its elements, and because
      // the entry is only popped after its task has completed (and moving the
      // block then doesn't move the memory of its columns).
      ql::span<const Id> column =
          std::as_const(pending_.back().block_).getColumn(columnIdx_);
      pending_.back().count_ =
          queue_.submit([column]() { return countDistinctIds(column); });
    }
  }
};

}  // namespace compressedRelationHelpers

#endif  // QLEVER_SRC_INDEX_COMPRESSEDRELATIONHELPERSIMPL_H_
