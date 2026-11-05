// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_COMPRESSEDRELATIONWRITER_RELATIONBLOCKCUTTER_H
#define QLEVER_SRC_INDEX_COMPRESSEDRELATIONWRITER_RELATIONBLOCKCUTTER_H

#include <algorithm>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/compressedRelationWriter/CompressedRelationHelpersImpl.h"
#include "index/compressedRelationWriter/SmallRelationsBlockBuilder.h"
#include "util/Exception.h"

// The part of writing a permutation that cuts the sorted input blocks into the
// relations (the runs of rows with the same `col0` ID) and the rows of each
// relation into the blocks of the permutation. Complete small relations are
// written in batches without being buffered, and the blocks of a large
// relation are written directly from the input block whenever possible.
// Everything else (how a block is written, what happens when a relation is
// complete, the twin permutation, the metadata, ...) is left to the `Derived`
// class (CRTP), see `CompressedRelationWriter::PermutationWriter` for an
// example.
//
// The `Derived` class has to provide the following hooks:
//
// * `void addBlockForLargeRelation()`: Write the rows in the `relation_` buffer
//   as the next block of the current (large) relation, increment the
//   `numBlocksCurrentRel_`, and continue with an empty `relation_` buffer. Do
//   nothing if the buffer is empty.
// * `void addBlockOfLargeRelationWithoutCopying(IdTableView<0> block)`: Write
//   the `block`, which is a view of rows of the `inputBlock_`, as the next
//   block of the current (large) relation, and increment the
//   `numBlocksCurrentRel_`. The `relation_` buffer is empty. The rows of the
//   `block` have to be kept alive until the block has been written (e.g. by
//   sharing the ownership of the `inputBlock_`).
// * `void finishRelation()`: The current relation (whose rows are in the
//   `relation_` buffer, unless they have already been written as blocks) is
//   complete. Write what is left of it, and reset the `relation_` buffer and
//   the `numBlocksCurrentRel_`. Do nothing if `col0IdCurrentRelation_` is
//   empty, because then the relation has already been written by
//   `writeCompleteSmallRelations`.
// * `void addCompleteSmallRelations(Id firstCol0Id, Id lastCol0Id, const
//   PermutedCols& permutedCols, size_t begin, size_t end, size_t
//   numRelations)`: Write the batch of `numRelations` complete small relations
//   in the rows `[begin, end)` of `permutedCols`, see
//   `writeCompleteSmallRelations`.
// * `const SmallRelationsBlockBuilder& smallRelationsBlockBuilder() const`:
//   The builder to which `addCompleteSmallRelations` adds the batches, which
//   determines how large a batch may be.
//
// The following hooks are optional, see their default implementations below:
// `mayWriteDirectly()`, `mayWriteCompleteSmallRelations()`, and
// `increaseTripleCounter(size_t)`.
template <typename Derived>
class RelationBlockCutter {
 protected:
  // The (soft) number of rows of a block of the permutation.
  const size_t blocksize_;

  // The `col0` ID of the relation that is currently being written. Empty
  // before the first input block, and after a batch of complete small
  // relations has been written by `writeCompleteSmallRelations`.
  std::optional<Id> col0IdCurrentRelation_;

  // The buffered rows of the current relation, which have not been written as
  // a block yet.
  IdTable relation_;
  // The number of blocks of the current relation that have already been
  // written.
  size_t numBlocksCurrentRel_ = 0;

  // The input block that is currently being processed (see `addInputBlock`).
  // It is held via a `shared_ptr`, because a block of a large relation may be
  // written directly from it, without copying its rows into the `relation_`
  // buffer first. If the writing happens in the background, then it has to
  // keep the input block alive, see `addBlockOfLargeRelationWithoutCopying`.
  std::shared_ptr<const IdTableStatic<0>> inputBlock_;

 public:
  // Construct with the given `blocksize` and the (empty) buffer for the rows
  // of the current relation.
  RelationBlockCutter(size_t blocksize, IdTable relationBuffer)
      : blocksize_{blocksize}, relation_{std::move(relationBuffer)} {
    AD_CONTRACT_CHECK(blocksize_ > 0);
  }

 protected:
  // Default implementations of the optional hooks, see the comment at the top
  // of the class.

  // Return true if a block of a large relation may currently be written
  // directly from the input block (see `addRowsOfCurrentRelation`).
  bool mayWriteDirectly() const { return true; }

  // Return true if a run of rows that forms a complete small relation may
  // currently be written via `writeCompleteSmallRelations` (instead of being
  // buffered in `relation_` like any other run).
  bool mayWriteCompleteSmallRelations() const { return true; }

  // Called whenever `numTriples` rows of the input have been handled, e.g. for
  // a progress bar.
  void increaseTripleCounter([[maybe_unused]] size_t numTriples) {}

  // Process the next sorted input `block`, which must not be empty. Its column
  // `col0Column` contains the `col0` IDs, and the `permutedColIndices` are the
  // indices of all its columns in the order in which they are stored in the
  // permutation. The `block` is stored in `inputBlock_` (which is where the
  // `Derived` class can take it from afterwards).
  void addInputBlock(std::shared_ptr<const IdTableStatic<0>> block,
                     ColumnIndex col0Column,
                     const std::vector<ColumnIndex>& permutedColIndices) {
    inputBlock_ = std::move(block);
    const auto& input = *inputBlock_;
    AD_CORRECTNESS_CHECK(!input.empty());
    const size_t numRows = input.numRows();
    auto firstCol = input.getColumn(col0Column);
    auto permutedCols = input.asColumnSubsetView(permutedColIndices);
    if (!col0IdCurrentRelation_.has_value()) {
      col0IdCurrentRelation_ = firstCol[0];
    }

    // The input is sorted by `col0`, so the block consists of consecutive
    // runs of rows that all belong to the same relation. We first determine
    // the extent of such a run by looking at `col0` only, and then handle
    // the run as a whole instead of row by row. That way the columns can be
    // copied contiguously, and we often know the fate of the complete
    // relation before touching any of its data (see
    // `isCompleteSmallRelation` and `addRowsOfCurrentRelation` below).
    size_t runBegin = 0;
    while (runBegin < numRows) {
      Id col0Id = firstCol[runBegin];
      if (col0Id != col0IdCurrentRelation_) {
        derived().finishRelation();
        col0IdCurrentRelation_ = col0Id;
      }
      size_t runEnd =
          compressedRelationHelpers::findEndOfRun(firstCol, runBegin);
      // If the complete relation is already known here, and it is small,
      // then we can write it without buffering it in `relation_` first,
      // together with as many of the directly following relations as
      // possible.
      if (isCompleteSmallRelation(runBegin, runEnd, numRows)) {
        runBegin = writeCompleteSmallRelations(permutedCols, firstCol, runBegin,
                                               runEnd, numRows);
      } else {
        addRowsOfCurrentRelation(permutedCols, runBegin, runEnd);
        runBegin = runEnd;
      }
    }
  }

  // Return true if the current relation is large, given that it is complete.
  // That is the case if blocks of it have already been written, or if the
  // buffered rows are too many for a small relation.
  bool currentRelationIsLarge() const {
    return numBlocksCurrentRel_ > 0 ||
           !compressedRelationHelpers::isSmallRelation(relation_.numRows(),
                                                       blocksize_);
  }

 private:
  // Access the `Derived` class (CRTP). The `static_assert`s can't be at the
  // class scope, because the `Derived` class is incomplete there.
  Derived& derived() {
    static_assert(std::is_base_of_v<RelationBlockCutter, Derived>);
    return static_cast<Derived&>(*this);
  }
  const Derived& derived() const {
    static_assert(std::is_base_of_v<RelationBlockCutter, Derived>);
    return static_cast<const Derived&>(*this);
  }

  // The special case of `compressedRelationHelpers::findFirstTripleChange`
  // where the rows are compared to the last triple that is currently buffered
  // in `relation_`, which therefore must not be empty.
  template <typename Rows>
  size_t findFirstTripleChangeAfterBuffer(const Rows& rows, size_t begin,
                                          size_t end) const {
    using compressedRelationHelpers::
        pickFirstThreeColumnsOfIdsWithoutLocalVocab;
    AD_CORRECTNESS_CHECK(!relation_.empty());
    return compressedRelationHelpers::findFirstTripleChange(
        rows, begin, end,
        pickFirstThreeColumnsOfIdsWithoutLocalVocab(relation_.back()));
  }

  // Append the rows `[begin, end)` of `permutedCols`, which all belong to the
  // current relation (that is, they all have the same value for column 0), to
  // the `relation_` buffer. The rows are appended in chunks that are as large
  // as possible, which is much faster than appending them one by one, because
  // the `IdTable`s are stored column-based. A new block for a large relation
  // is started whenever the buffer has reached the `blocksize_` and the first
  // three columns change (see
  // `compressedRelationHelpers::findFirstTripleChange`).
  //
  // Note: This function is always called for the rows of a large relation, but
  // also for the rows of a small relation that spans several input blocks,
  // because in that case we don't know yet that the relation will be small
  // (see `isCompleteSmallRelation` below). It therefore has to work correctly
  // for both cases.
  template <typename PermutedCols>
  void addRowsOfCurrentRelation(const PermutedCols& permutedCols, size_t begin,
                                size_t end) {
    using compressedRelationHelpers::
        pickFirstThreeColumnsOfIdsWithoutLocalVocab;
    while (begin < end) {
      // Determine the largest chunk of rows that may be appended in one go.
      size_t chunkEnd;
      if (relation_.numRows() < blocksize_) {
        // If the buffer is empty and the remaining rows suffice for a complete
        // block, then that block can be written directly from the input block,
        // without copying its rows into the buffer first (which for a large
        // relation is a substantial part of the work of the calling thread).
        // This requires that the block can be completed within the current
        // input block, which is exactly the case if a triple change is found
        // before the end of the rows; otherwise further equal triples may
        // follow in the next input block, which then have to end up in the
        // same block.
        //
        // Note: The `Derived` class may forbid this (see `mayWriteDirectly`).
        // The result of that hook may change inside this loop, because it
        // may depend on the blocks that have been written so far.
        std::optional<size_t> directBlockEnd =
            [this, begin, end, &permutedCols]() -> std::optional<size_t> {
          if (!relation_.empty() || end - begin < blocksize_ ||
              !derived().mayWriteDirectly()) {
            return std::nullopt;
          }
          size_t blockEnd = compressedRelationHelpers::findFirstTripleChange(
              permutedCols, begin + blocksize_, end,
              pickFirstThreeColumnsOfIdsWithoutLocalVocab(
                  permutedCols[begin + blocksize_ - 1]));
          if (blockEnd == end) {
            return std::nullopt;
          }
          return blockEnd;
        }();
        if (directBlockEnd.has_value()) {
          size_t blockEnd = directBlockEnd.value();
          // Note: The `Derived` class has to keep the rows of this view alive
          // (e.g. by sharing the ownership of the `inputBlock_`), see the
          // `addBlockOfLargeRelationWithoutCopying` hook.
          derived().addBlockOfLargeRelationWithoutCopying(
              permutedCols.subView(begin, blockEnd - begin));
          derived().increaseTripleCounter(blockEnd - begin);
          begin = blockEnd;
          continue;
        }
        // The buffer is not yet full, so we can simply append the rows that
        // are missing to reach the `blocksize_`.
        chunkEnd = std::min(end, begin + (blocksize_ - relation_.numRows()));
      } else {
        // The buffer is full, so we may only append rows that are equal to the
        // last buffered row with respect to the first three columns, because
        // equal triples have to stay in the same block.
        chunkEnd = findFirstTripleChangeAfterBuffer(permutedCols, begin, end);
        if (chunkEnd == begin) {
          // The very next row is already different, so the block is complete.
          // The next iteration then starts filling a fresh buffer.
          derived().addBlockForLargeRelation();
          continue;
        }
        // Otherwise the buffer deliberately grows beyond the `blocksize_`. If
        // `chunkEnd < end`, then the block is completed by the branch above in
        // the very next iteration. If `chunkEnd == end`, then we have to leave
        // the block open, because the rows of this relation may continue in
        // the next input block with further equal triples, which then have to
        // end up in the same block. Such a block is eventually written either
        // by the next call to this function or by `finishRelation`.
      }
      relation_.insertAtEnd(permutedCols, begin, chunkEnd);
      derived().increaseTripleCounter(chunkEnd - begin);
      begin = chunkEnd;
    }
  }

  // Return true if the rows `[begin, end)` of the current input block form a
  // complete relation that has to be written as a small relation. That is the
  // case if the relation neither has started in a previous input block (then
  // `relation_` would be non-empty, or blocks for it would already have been
  // written), nor may continue in the next one (then the run would extend to
  // the end of the block), and if it is small enough. The criterion for being
  // small is exactly the one that `currentRelationIsLarge` above uses. The
  // `Derived` class may forbid this (see `mayWriteCompleteSmallRelations`).
  bool isCompleteSmallRelation(size_t begin, size_t end,
                               size_t numRowsOfBlock) const {
    return relation_.empty() && numBlocksCurrentRel_ == 0 &&
           end < numRowsOfBlock &&
           compressedRelationHelpers::isSmallRelation(end - begin,
                                                      blocksize_) &&
           derived().mayWriteCompleteSmallRelations();
  }

  // Write the maximal batch of consecutive complete small relations that
  // starts with the rows `[begin, firstRunEnd)` of `permutedCols` (via the
  // `addCompleteSmallRelations` hook), and return the first row of the input
  // block that is not part of that batch. The batch consists of the relation
  // `[begin, firstRunEnd)`, which the caller has to have checked to be a
  // complete small relation (see `isCompleteSmallRelation` above), plus all
  // directly following complete small relations that still fit into the
  // block of small relations that the batch is added to (which is either the
  // current or a fresh one, see
  // `SmallRelationsBlockBuilder::capacityForBatch`).
  //
  // Writing the batch has two advantages over writing its relations one by
  // one: All its rows are copied into the buffer of the block of small
  // relations with a single `insertAtEnd`, which is much faster for relations
  // with only a handful of rows, and the `relation_` buffer is bypassed
  // completely, so that the rows are copied only once in total.
  template <typename PermutedCols, typename Col0>
  size_t writeCompleteSmallRelations(const PermutedCols& permutedCols,
                                     const Col0& col0, size_t begin,
                                     size_t firstRunEnd,
                                     size_t numRowsOfBlock) {
    AD_CORRECTNESS_CHECK(begin < firstRunEnd);
    // The number of rows that the batch may hold at most, see
    // `SmallRelationsBlockBuilder::capacityForBatch`.
    size_t capacity = derived().smallRelationsBlockBuilder().capacityForBatch(
        firstRunEnd - begin);

    // Greedily extend the batch by the following relations, as long as they
    // are complete small relations that still fit. Note that the first
    // relation is always part of the batch, even in the (currently impossible,
    // see `isCompleteSmallRelation` and
    // `SmallRelationsBlockBuilder::capacityForBatch`) case that it exceeds the
    // capacity all by itself. The loop is always left via one of the `break`s,
    // because a run that reaches the end of the input block is never a complete
    // small relation, which is exactly what the check at the beginning of the
    // loop body asserts. That check also guarantees that the indexing of `col0`
    // inside `findEndOfRun` is safe.
    size_t end = firstRunEnd;
    size_t numRelations = 1;
    Id lastCol0Id = col0[begin];
    for (;;) {
      AD_CORRECTNESS_CHECK(end < numRowsOfBlock);
      size_t nextEnd = compressedRelationHelpers::findEndOfRun(col0, end);
      if (!isCompleteSmallRelation(end, nextEnd, numRowsOfBlock) ||
          nextEnd - begin > capacity) {
        break;
      }
      lastCol0Id = col0[end];
      end = nextEnd;
      ++numRelations;
    }

    derived().addCompleteSmallRelations(col0IdCurrentRelation_.value(),
                                        lastCol0Id, permutedCols, begin, end,
                                        numRelations);
    derived().increaseTripleCounter(end - begin);
    // All relations of the batch are complete, so the next run of the input
    // starts a new relation and `finishRelation` has nothing left to do for
    // the last relation of the batch.
    col0IdCurrentRelation_.reset();
    return end;
  }
};

#endif  // QLEVER_SRC_INDEX_COMPRESSEDRELATIONWRITER_RELATIONBLOCKCUTTER_H
