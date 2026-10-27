// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_SMALLRELATIONSBLOCKBUILDER_H
#define QLEVER_SRC_INDEX_SMALLRELATIONSBLOCKBUILDER_H

#include <utility>

#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/Exception.h"

// Collect the rows of small relations into blocks of a permutation. Several
// small relations share a single block, and only the first and the last `col0`
// ID of such a block are stored (no metadata is stored for small relations).
// This class only builds the blocks; the caller decides when a block is
// complete (see `fits`) and how it is written (see `takeBlock`).
class SmallRelationsBlockBuilder {
 public:
  // A complete block of small relations, together with the `col0` IDs of its
  // first and of its last relation.
  struct Block {
    Id firstCol0_;
    Id lastCol0_;
    IdTable rows_;
  };

 private:
  // The rows of the block that is currently being built.
  IdTable buffer_;
  // The number of rows that a single block may hold at most, see `capacity`.
  size_t capacity_;
  // The `col0` IDs of the first and of the last relation in the `buffer_`.
  Id firstCol0_ = Id::makeUndefined();
  Id lastCol0_ = Id::makeUndefined();

 public:
  // Create a builder for a permutation with the given `blocksize` that builds
  // the first block in the (empty) `buffer`.
  SmallRelationsBlockBuilder(IdTable buffer, size_t blocksize)
      : buffer_{std::move(buffer)}, capacity_{(3 * blocksize) / 2} {
    AD_CONTRACT_CHECK(buffer_.empty());
  }

  // Return the number of rows that a single block of small relations may hold
  // at most.
  //
  // Note: The `blocksize` is only a soft target which blocks may exceed in
  // two independent ways. First, a block of small relations is filled up to
  // the 1.5-fold of the `blocksize` (which is exactly the capacity returned
  // here), and it is only completed once the rows that are to be added next
  // don't fit into it anymore, so the last relation that is added to a block
  // may push it even beyond that capacity. Second, a block of a large relation
  // grows beyond the `blocksize` whenever equal triples (when disregarding
  // the graph and the payload columns) would otherwise be split across two
  // blocks, see `PermutationWriter::addRowsOfCurrentRelation`.
  //
  // NOTE: there are some unit tests that rely on the factor `3 / 2` above.
  size_t capacity() const { return capacity_; }

  // Return the number of rows in the block that is currently being built.
  size_t numRows() const { return buffer_.numRows(); }
  bool empty() const { return buffer_.empty(); }

  // Return the number of rows that can still be added to the current block of
  // small relations without starting a new block. May be zero.
  size_t numRowsUntilFull() const {
    return numRows() >= capacity_ ? 0 : capacity_ - numRows();
  }

  // Return the number of rows that a batch of consecutive small relations
  // (see `add`) may have at most, if the first relation of the batch has
  // `numRowsOfFirstRelation` rows. If not even that relation fits into the
  // current block, then a new block is started for the batch in any case, so
  // the capacity of a complete fresh block is available for it. That way a
  // batch is never cut short just because the current block happens to be
  // almost full, while the resulting blocks are still exactly the same as if
  // the relations were added one by one.
  size_t capacityForBatch(size_t numRowsOfFirstRelation) const {
    size_t capacity = numRowsUntilFull();
    return numRowsOfFirstRelation > capacity ? capacity_ : capacity;
  }

  // Return true if `numRows` additional rows fit into the current block. If
  // they don't, then the current block has to be taken (via `takeBlock`)
  // before they are added.
  bool fits(size_t numRows) const { return numRows <= numRowsUntilFull(); }

  // Add a batch of one or more small relations to the current block. The rows
  // `[beginIdx, endIdx)` of `relations` have to consist of the complete rows of
  // those relations, in ascending order of their `col0` ID, where `firstCol0Id`
  // and `lastCol0Id` are the `col0` IDs of the first and of the last of them.
  // The individual relations don't have to be delimited any further, because a
  // block of small relations only stores the first and the last `col0` ID. The
  // `relations` may be an arbitrary kind of `IdTable`, in particular a view.
  // That way a range of rows of a larger table can be added directly, without
  // materializing it in an intermediate buffer first. The batch has to `fit`,
  // unless the current block is empty (then a batch that exceeds the
  // `capacity` forms a block of its own).
  //
  // Note: For all current callers the `col0` IDs are stored in column 0 of
  // `relations`, so that `firstCol0Id == relations(beginIdx, 0)` and
  // `lastCol0Id == relations(endIdx - 1, 0)` (this is checked below). They are
  // still passed explicitly, because the callers have them at hand anyway, and
  // because the function otherwise doesn't depend on the layout of the
  // arbitrary `Table`.
  template <typename Table>
  void add(Id firstCol0Id, Id lastCol0Id, const Table& relations,
           size_t beginIdx, size_t endIdx) {
    AD_CORRECTNESS_CHECK(beginIdx < endIdx && endIdx <= relations.numRows());
    AD_CORRECTNESS_CHECK(firstCol0Id == relations(beginIdx, 0) &&
                         lastCol0Id == relations(endIdx - 1, 0));
    AD_CORRECTNESS_CHECK(empty() || fits(endIdx - beginIdx));
    // We have to keep track of the first and last `col0` of each block.
    if (empty()) {
      firstCol0_ = firstCol0Id;
    }
    lastCol0_ = lastCol0Id;

    // Note: `insertAtEnd` appends the columns of the input contiguously, which
    // is much faster than appending the rows one by one, because the
    // `IdTable`s are stored column-based. Appending a whole batch of relations
    // at once is therefore much more efficient than appending each of them
    // separately.
    buffer_.insertAtEnd(relations, beginIdx, endIdx);
  }

  // Return the current block, which must not be empty, and continue with the
  // next block in the (empty) `freshBuffer`.
  Block takeBlock(IdTable freshBuffer) {
    AD_CORRECTNESS_CHECK(!empty());
    AD_CORRECTNESS_CHECK(freshBuffer.empty() &&
                         freshBuffer.numColumns() == buffer_.numColumns());
    return Block{firstCol0_, lastCol0_,
                 std::exchange(buffer_, std::move(freshBuffer))};
  }
};

#endif  // QLEVER_SRC_INDEX_SMALLRELATIONSBLOCKBUILDER_H
