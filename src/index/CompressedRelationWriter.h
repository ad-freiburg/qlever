// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2025        Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2025        Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_COMPRESSEDRELATIONWRITER_H
#define QLEVER_SRC_INDEX_COMPRESSEDRELATIONWRITER_H

#include <gtest/gtest_prod.h>

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "engine/idTable/IdTable.h"
#include "engine/idTable/IdTableOrSharedIdTableView.h"
#include "global/Id.h"
#include "index/CompressedRelationMetadata.h"
#include "index/KeyOrder.h"
#include "util/AllocatorWithLimit.h"
#include "util/File.h"
#include "util/Iterators.h"
#include "util/MemorySize/MemorySize.h"
#include "util/RecyclingPool.h"
#include "util/Synchronized.h"
#include "util/TaskQueue.h"
#include "util/Timer.h"

// This type is used to buffer small relations that will be stored in the same
// block.
using SmallRelationsBuffer = IdTable;

/// Manage the compression and serialization of relations during the index
/// build.
class CompressedRelationWriter {
 private:
  ad_utility::Synchronized<ad_utility::File> outfile_;
  ad_utility::Synchronized<std::vector<CompressedBlockMetadataNoBlockIndex>>
      blockBuffer_;
  // If multiple small relations are stored in the same block, keep track of the
  // first and last `col0Id`.
  Id currentBlockFirstCol0_ = Id::makeUndefined();
  Id currentBlockLastCol0_ = Id::makeUndefined();

  // The actual number of columns that is stored by this writer. Is 2 if there
  // are no additional special payloads.
  size_t numColumns_;

  ad_utility::AllocatorWithLimit<Id> allocator_ =
      ad_utility::makeUnlimitedAllocator<Id>();
  // A buffer for small relations that will be stored in the same block.
  SmallRelationsBuffer smallRelationsBuffer_{numColumns_, allocator_};
  ad_utility::MemorySize uncompressedBlocksizePerColumn_;

  // When we store a large relation with multiple blocks then we keep track of
  // its `col0Id`, mostly for sanity checks.
  Id currentCol0Id_ = Id::makeUndefined();
  size_t currentRelationPreviousSize_ = 0;

  ad_utility::TaskQueue<false> blockWriteQueue_;
  ad_utility::timer::ThreadSafeTimer blockWriteQueueTimer_;

  // This callback is invoked for each block of small relations (which share the
  // same block), after this block has been completely handled by this writer.
  // The callback is used to efficiently pass the block from a permutation to
  // its twin permutation, which only has to re-sort and write the block.
  using SmallBlocksCallback = std::function<void(IdTable)>;
  SmallBlocksCallback smallBlocksCallback_;

  // A pool of block buffers whose blocks have already been written and whose
  // memory can therefore be reused for the following blocks (see
  // `takeBlockBuffer`). All blocks (of small as well as of large relations)
  // have a similar size, so a single pool can serve all of them. The pool is
  // shared, so that the two writers of a permutation pair (see
  // `PermutationWriter`) and the `PermutationWriter` itself can use the same
  // buffers, see `shareBlockBufferPoolWith`.
  using BlockBufferPool = ad_utility::RecyclingPool<IdTable>;
  std::shared_ptr<BlockBufferPool> blockBufferPool_ =
      std::make_shared<BlockBufferPool>();

 public:
  /// Create using a filename, to which the relation data will be written.
  /// If `numWriterThreads` is set, it determines the number of threads that
  /// compress and write blocks; otherwise the runtime parameter
  /// `permutation-writer-num-threads` is used (see `makeBlockWriteQueue`).
  explicit CompressedRelationWriter(
      size_t numColumns, ad_utility::File f,
      ad_utility::MemorySize uncompressedBlocksizePerColumn,
      std::optional<size_t> numWriterThreads = std::nullopt)
      : outfile_{std::move(f)},
        numColumns_{numColumns},
        uncompressedBlocksizePerColumn_{uncompressedBlocksizePerColumn},
        blockWriteQueue_{makeBlockWriteQueue(numWriterThreads)} {}
  // Two helper types used to make the interface of the function
  // `createPermutationPair` below safer and more explicit.
  using MetadataCallback =
      std::function<void(ql::span<const CompressedRelationMetadata>)>;

  struct WriterAndCallback {
    std::unique_ptr<CompressedRelationWriter> writer_;
    MetadataCallback callback_;
  };

  // A block of rows that is to be compressed and written by this writer. It
  // either owns its rows (as an `IdTable`, whose buffer is then given back to
  // the `blockBufferPool_`), or it is a non-owning view of rows that are owned
  // elsewhere. The latter allows writing a block of a large relation directly
  // from the input block in which its rows reside, without copying them into
  // an intermediate buffer first, see
  // `PermutationWriter::addRowsOfCurrentRelation`.
  using BlockToWrite = ad_utility::IdTableOrSharedIdTableView;

  // The `PermutationWriter` can be used to write single or pair permutations.
  // It is defined in `CompressedRelationPermutationWriterImpl.h`.
  template <bool WritePair>
  struct PermutationWriter;

  // Helper for `createPermutation` and `createPermutationPair` below. For
  // blocks from the input generators these callbacks are invoked after the
  // respective block has been written.
  using PerBlockCallbacks =
      std::vector<std::function<void(const IdTableStatic<0>&)>>;

  // Helper struct for the result of `createPermutation`.
  struct PermutationSingleResult {
    size_t numDistinctCol0_;
    std::vector<CompressedBlockMetadata> blockMetadata_;
  };

  // Write a single permutation. It is required for example for materialized
  // views. This function should not be used for regular index building (when
  // writing twin permutations, like POS and PSO, the function
  // `createPermutationPair` below is more efficient than calling this function
  // twice).
  //
  // The `writerAndCallback` is a writer for the permutation together with
  // a callback that is called for each of the created metadata.
  //
  // `sortedTriples` are the input blocks of triples (plus possibly additional
  // columns). The first three columns must be sorted according to
  // the `permutation` (which corresponds to the `writerAndCallback`).
  //
  // The `permutation` contains the column indices indicating the permutation to
  // be built (as an array, for example `[0, 1, 2]`). The `sortedTriples` must
  // be sorted by this permutation.
  //
  // With `showProgressBar` set to `false`, this writes no progress bar of its
  // own. That is for callers that write several permutations and want to
  // report the overall progress themselves.
  static PermutationSingleResult createPermutation(
      WriterAndCallback writerAndCallback,
      ad_utility::InputRangeTypeErased<IdTableStatic<0>> sortedTriples,
      qlever::KeyOrder permutation, const PerBlockCallbacks& perBlockCallbacks,
      bool showProgressBar = true);

 private:
  // Internal helper for `PermutationWriter<true>` (that is, in pair mode).
  // Defined in `CompressedRelationPermutationWriterImpl.h`.
  struct AddBlockOfSmallRelationsToSwitched;

 public:
  // Helper struct for the result of `createPermutation`.
  struct PermutationPairResult {
    size_t numDistinctCol0_;
    std::vector<CompressedBlockMetadata> blockMetadata_;
    std::vector<CompressedBlockMetadata> blockMetadataSwitched_;
  };

  // Write two permutations that only differ by the order of the col1 and
  // col2 (e.g. POS and PSO). Prefer this function over `createPermutation` when
  // both twins are needed.
  //
  // The `basename` filename/path will be used as a prefix for names of
  // temporary files for external sorting of the twin permutation.
  //
  // `writerAndCallback1`: A writer for the first permutation together with
  // a callback that is called for each of the created metadata.
  //
  // `writerAndCallback2`: The same as `writerAndCallback1`, but for the
  // other permutation.
  //
  // `sortedTriples`: The inputs as blocks of triples (plus possibly
  // additional columns). The first three columns must be sorted according to
  // the `permutation` (which corresponds to the `writerAndCallback1`).
  //
  // `permutation`: The permutation to be built (as a permutation of the
  // array `[0, 1, 2]`). The `sortedTriples` must be sorted by this permutation.
  static PermutationPairResult createPermutationPair(
      const std::string& basename, WriterAndCallback writerAndCallback1,
      WriterAndCallback writerAndCallback2,
      ad_utility::InputRangeTypeErased<IdTableStatic<0>> sortedTriples,
      qlever::KeyOrder permutation, const PerBlockCallbacks& perBlockCallbacks);

  /// Get all the CompressedBlockMetaData that were created by the calls to
  /// addRelation. This also closes the writer. The typical workflow is:
  /// add all relations and then call this method.
  std::vector<CompressedBlockMetadata> getFinishedBlocks() && {
    finish();
    auto blocks = std::move(*(blockBuffer_.wlock()));
    ql::ranges::sort(blocks, {},
                     &CompressedBlockMetadataNoBlockIndex::firstTriple_);

    std::vector<CompressedBlockMetadata> result;
    result.reserve(blocks.size());
    // Write the correct block indices
    for (size_t i : ad_utility::integerRange(blocks.size())) {
      result.push_back({std::move(blocks.at(i)), i});
    }

    AD_CORRECTNESS_CHECK(
        CompressedBlockMetadata::checkInvariantsForSortedBlocks(result));
    return result;
  }

  // Compute the multiplicity of given the number of elements and the number of
  // distinct elements. It is basically `numElements / numDistinctElements` with
  // the following addition: the result will only be exactly `1.0` if
  // `numElements == numDistinctElements`, s.t. `1.0` is equivalent to
  // `functional`. Note that this is not automatically ensured by the division
  // because of the numeric properties of `float`.
  static float computeMultiplicity(size_t numElements,
                                   size_t numDistinctElements);

  // Return the blocksize (in number of triples) of this writer. Note that the
  // actual sizes of blocks will slightly vary due to new relations starting in
  // new blocks etc.
  size_t blocksize() const {
    return std::max(
        size_t{1},
        size_t{uncompressedBlocksizePerColumn_.getBytes() / sizeof(Id)});
  }

 private:
  /// Finish writing all relations which have previously been added, but might
  /// still be in some internal buffer.
  void finish() {
    AD_CORRECTNESS_CHECK(currentRelationPreviousSize_ == 0);
    writeBufferedRelationsToSingleBlock();
    auto timer = blockWriteQueueTimer_.startMeasurement();
    blockWriteQueue_.finish();
    timer.stop();
    outfile_.wlock()->close();
  }

  // Compress the contents of `smallRelationsBuffer_` into a single
  // block and write it to outfile_. Update `currentBlockData_` with the meta
  // data of the written block. Then clear `smallRelationsBuffer_`.
  void writeBufferedRelationsToSingleBlock();

  // Compress the `column` and write it to the `outfile_`. Return the offset and
  // size of the compressed column in the `outfile_`.
  CompressedBlockMetadata::OffsetAndCompressedSize compressAndWriteColumn(
      ql::span<const Id> column);

  // Return the number of columns that is stored inside the blocks.
  size_t numColumns() const { return numColumns_; }

  // Compress the given `block` and write it to the `outfile_`. The
  // `firstCol0Id` and `lastCol0Id` are needed to set up the block's metadata
  // which is appended to the internal buffer. If `invokeCallback` is true and
  // the `smallBlocksCallback_` is not empty, then
  // `smallBlocksCallback_(std::move(block))` is called AFTER the block has
  // completely been dealt with.
  void compressAndWriteBlock(Id firstCol0Id, Id lastCol0Id, BlockToWrite block,
                             bool invokeCallback);

  // Return the number of rows that a single block of small relations may hold
  // at most.
  //
  // Note: The `blocksize()` is only a soft target which blocks may exceed in
  // two independent ways. First, a block of small relations is filled up to
  // the 1.5-fold of the `blocksize()` (which is exactly the capacity returned
  // here), and it is only completed once the rows that are to be added next
  // don't fit into it anymore, so the last relation that is added to a block
  // may push it even beyond that capacity. Second, a block of a large relation
  // grows beyond the `blocksize()` whenever equal triples (when disregarding
  // the graph and the payload columns) would otherwise be split across two
  // blocks, see `PermutationWriter::addRowsOfCurrentRelation`.
  //
  // NOTE: there are some unit tests that rely on the factor `3 / 2` below.
  size_t smallRelationBlockCapacity() const { return (3 * blocksize()) / 2; }

  // Return the number of rows that can still be added to the current block of
  // small relations without starting a new block. May be zero.
  size_t numRowsUntilSmallRelationBlockIsFull() const {
    size_t capacity = smallRelationBlockCapacity();
    size_t numBuffered = smallRelationsBuffer_.numRows();
    return numBuffered >= capacity ? 0 : capacity - numBuffered;
  }

  // Add a batch of one or more small relations that will be stored in a single
  // block, possibly together with other small relations. The rows
  // `[beginIdx, endIdx)` of `relations` have to consist of the complete rows of
  // those relations, in ascending order of their `col0` ID, where `firstCol0Id`
  // and `lastCol0Id` are the `col0` IDs of the first and of the last of them.
  // The individual relations don't have to be delimited any further, because a
  // block of small relations only stores the first and the last `col0` ID (see
  // `writeBufferedRelationsToSingleBlock`). The `relations` may be an
  // arbitrary kind of `IdTable`, in particular a view. That way a range of
  // rows of a larger table can be added directly, without materializing it in
  // an intermediate buffer first.
  //
  // Note: For all current callers the `col0` IDs are stored in column 0 of
  // `relations`, so that `firstCol0Id == relations(beginIdx, 0)` and
  // `lastCol0Id == relations(endIdx - 1, 0)` (this is checked below). They are
  // still passed explicitly, because the callers have them at hand anyway, and
  // because the function otherwise doesn't depend on the layout of the
  // arbitrary `Table`.
  //
  // Note: A new block is started if the complete batch doesn't fit into the
  // current one. The resulting blocks are therefore exactly the same as if the
  // relations of the batch were added one by one, provided that the caller has
  // limited the batch to the number of rows reported by
  // `numRowsUntilSmallRelationBlockIsFull` above (see
  // `PermutationWriter::writeCompleteSmallRelations`).
  //
  // Note: In contrast to `finishLargeRelation` this function computes no
  // `CompressedRelationMetadata`, because no metadata is persisted for small
  // relations. It is instead computed lazily at query time, see
  // `CompressedRelationReader::getMetadataForSmallRelation`.
  template <typename Table>
  void addSmallRelations(Id firstCol0Id, Id lastCol0Id, const Table& relations,
                         size_t beginIdx, size_t endIdx) {
    AD_CORRECTNESS_CHECK(beginIdx < endIdx && endIdx <= relations.numRows());
    AD_CORRECTNESS_CHECK(firstCol0Id == relations(beginIdx, 0) &&
                         lastCol0Id == relations(endIdx - 1, 0));
    size_t numRows = endIdx - beginIdx;
    // Make sure that the blocks don't become too large: If the previously
    // buffered small relations together with the new relations would exceed
    // the capacity of a block, then we start a new block for the batch.
    if (numRows + smallRelationsBuffer_.numRows() >
        smallRelationBlockCapacity()) {
      writeBufferedRelationsToSingleBlock();
    }
    // We have to keep track of the first and last `col0` of each block.
    if (smallRelationsBuffer_.numRows() == 0) {
      currentBlockFirstCol0_ = firstCol0Id;
    }
    currentBlockLastCol0_ = lastCol0Id;

    // Note: `insertAtEnd` appends the columns of the input contiguously, which
    // is much faster than appending the rows one by one, because the
    // `IdTable`s are stored column-based. Appending a whole batch of relations
    // at once is therefore much more efficient than appending each of them
    // separately.
    smallRelationsBuffer_.insertAtEnd(relations, beginIdx, endIdx);
  }

  // Add a single small relation. This is the special case of
  // `addSmallRelations` above with a batch that consists of one relation. Only
  // the rows `[beginIdx, endIdx)` of the `relation` are added; if `endIdx` is
  // not specified, all rows starting at `beginIdx` are added.
  template <typename Table>
  void addSmallRelation(Id col0Id, const Table& relation, size_t beginIdx = 0,
                        std::optional<size_t> endIdx = std::nullopt) {
    addSmallRelations(col0Id, col0Id, relation, beginIdx,
                      endIdx.value_or(relation.numRows()));
  }

  // Add a new block for a large relation that is to be stored in multiple
  // blocks. This function may only be called if one of the following holds:
  // * This is the first call to `addBlockForLargeRelation` or
  // `addSmallRelation`.
  // * The previously called function was `addSmallRelation` or
  // `finishLargeRelation`.
  // * The previously called function was `addBlockForLargeRelation` with the
  // same `col0Id`.
  void addBlockForLargeRelation(Id col0Id, BlockToWrite relation);

  // Return an empty block buffer with room for at least `2 * blocksize()`
  // rows, which is taken from the `blockBufferPool_` if possible (then its
  // memory is typically already allocated). Thread-safe.
  IdTable takeBlockBuffer();

  // Use the same `blockBufferPool_` as the `other` writer.
  void shareBlockBufferPoolWith(const CompressedRelationWriter& other) {
    blockBufferPool_ = other.blockBufferPool_;
  }

  // Return the `blockBufferPool_`, e.g. to give buffers back to it via
  // `BlockBufferPool::makeRecyclingOwner`.
  const std::shared_ptr<BlockBufferPool>& blockBufferPool() const {
    return blockBufferPool_;
  }

  // This function must be called after all blocks of a large relation have been
  // added via `addBlockForLargeRelation` before any other function may be
  // called. In particular, it has to be called after the last block of the last
  // relation was added (in case this relation is large). Otherwise, an
  // assertion inside the `finish()` function (which is also called by the
  // destructor) will fail.
  CompressedRelationMetadata finishLargeRelation(size_t numDistinctC1);

  // Add a complete large relation by calling `addBlockForLargeRelation` for
  // each block in the `sortedBlocks` and then calling `finishLargeRelation`.
  // The number of distinct col1 entries will be computed from the blocks
  // directly.
  template <typename T>
  CompressedRelationMetadata addCompleteLargeRelation(Id col0Id,
                                                      T&& sortedBlocks);

  // This is a function in `CompressedRelationsTest.cpp` that tests the
  // internals of this class and therefore needs private access.
  template <typename T>
  friend std::pair<std::vector<CompressedBlockMetadata>,
                   std::vector<CompressedRelationMetadata>>
  compressedRelationTestWriteCompressedRelations(
      T inputs, std::string filename, ad_utility::MemorySize blocksize,
      size_t inputBlockSize);

  // Create a `TaskQueue` for the compression and writing of blocks. The number
  // of threads is `numThreadsOverride` if set, and otherwise determined by the
  // runtime parameter "permutation-writer-num-threads". In both cases, a value
  // of 0 means "as many threads as the hardware has", and larger values are
  // capped at that number.
  static ad_utility::TaskQueue<false> makeBlockWriteQueue(
      std::optional<size_t> numThreadsOverride);
  FRIEND_TEST(CompressedRelationWriter,
              isInitializedWithCorrectNumberOfThreads);
};

#endif  // QLEVER_SRC_INDEX_COMPRESSEDRELATIONWRITER_H
