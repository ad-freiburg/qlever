// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_CHUNKWRITER_H
#define QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_CHUNKWRITER_H

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "engine/idTable/CompressedExternalIdTable.h"
#include "engine/idTable/IdTable.h"
#include "index/CompressedRelationHelpersImpl.h"
#include "index/CompressedRelationWriter.h"
#include "index/chunkwisePermutationWriter/Options.h"
#include "util/NoCopyNoMove.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace chunkwisePermutationWriter {

// The sorter that sorts the twin rows of a large relation (where `col0` is
// constant, so only the remaining columns are compared).
using TwinSorter = ad_utility::CompressedExternalIdTableSorter<
    compressedRelationHelpers::ComparatorForConstCol0, 0>;

// The part of a relation that lies at the beginning or at the end of a chunk
// (or that covers a whole chunk), see the explanation at the top of
// `ChunkwisePermutationWriter.h`.
// All the rows of a part are rows of the same relation (the one with the
// `col0Id_`), and the parts of a relation are consolidated in the order of
// their chunks.
struct BoundaryPart {
  Id col0Id_;
  // The rows of the part that precede the first block which the chunk has
  // written directly. For a small part (see `isLarge`) these are all the rows
  // of the part.
  IdTable rows_;
  // The rows of the part that follow the last block which the chunk has
  // written directly. Only present if the part is large and reaches the end of
  // its chunk.
  std::optional<IdTable> tail_;
  // The total number of rows of the part, including the ones in the blocks
  // that were written directly.
  size_t numRows_ = 0;
  // The number of distinct `col1` IDs of all the rows of the part (in order),
  // together with the first and the last of them, so that the counts of the
  // parts of a relation can be folded, see `DistinctIdCounter`.
  compressedRelationHelpers::DistinctIdCountOfBlock distinctCol1_;
  // The sorter that holds the twin rows (columns 1 and 2 swapped) of *all* the
  // rows of the part, including the `rows_` and the `tail_`. Only present if
  // the part is large.
  std::unique_ptr<TwinSorter> twinSorter_;
  // The sorted runs of the `twinSorter_`, which the chunk finishes before it
  // hands the part over (see `ChunkWriter::finishChunk`), so that a part that
  // waits for the consolidation only holds the runs on disk (and the `rows_`
  // and `tail_`) in memory. Present iff the `twinSorter_` is.
  std::optional<ad_utility::CompressedIdTableRunsInput<0>> twinRuns_;

  // Construct a part from the relation it belongs to and from the rows that
  // precede the first directly written block, see `rows_`.
  BoundaryPart(Id col0Id, IdTable rows)
      : col0Id_{col0Id}, rows_{std::move(rows)} {}

  // A part is large if the chunk has decided that its relation is large,
  // because the part alone has more rows than fit into a block. A large part
  // has written blocks directly and has a twin sorter.
  bool isLarge() const { return twinSorter_ != nullptr; }
};

// Everything that a single chunk of the merge hands back to the consolidation
// once it is finished.
struct ChunkResult {
  // The part of the first relation of the chunk, which may continue the last
  // relation of the previous chunk. Absent for the first chunk (whose first
  // relation has no predecessor and is handled like any other relation) and
  // for a chunk without rows.
  std::optional<BoundaryPart> firstPart_;
  // The part of the last relation of the chunk, which may continue in the
  // next chunk. Absent for the last chunk (whose last relation is complete
  // and is handled like any other relation), for a chunk without rows, and
  // if the chunk consists of a single relation (see `spansWholeChunk_`).
  std::optional<BoundaryPart> lastPart_;
  // If `true`, then the chunk consists of a single relation, which is both
  // its first and its last relation: the `firstPart_` then also continues in
  // the next chunk, and there is no `lastPart_`.
  bool spansWholeChunk_ = false;
  // The metadata of the relations that start and end inside the chunk
  // (excluding the boundary parts), for the permutation and for its twin, in
  // the order of the relations. Small relations have no metadata.
  std::vector<std::pair<CompressedRelationMetadata, CompressedRelationMetadata>>
      middleMetadata_;
  // The number of relations that start and end inside the chunk (excluding
  // the boundary parts).
  size_t numMiddleRelations_ = 0;
  // The number of rows of the chunk (after the removal of duplicates).
  size_t numRows_ = 0;
};

class ChunkwisePermutationWriter;

// The writer of a single chunk of the merge. It is driven by a single thread
// at a time (the chunk's coroutine of the merge), but by different threads for
// different blocks, which is fine because every operation on it is sequenced
// by the merge. It only uses the thread-safe parts of the two
// `CompressedRelationWriter`s (the compression and writing of a block in the
// calling thread, and the pool of block buffers) and keeps all the other state
// (the current relation, the buffer of small relations, the twin sorter) for
// itself.
class ChunkWriter : public ad_utility::NoCopyNoMove {
 private:
  using DistinctIdCounter = compressedRelationHelpers::DistinctIdCounter;

  ChunkwisePermutationWriter& shared_;
  size_t chunkIndex_;
  // Whether this is the last chunk of the merge, whose last relation is
  // complete (see `ChunkResult::lastPart_`).
  const bool isLastChunk_;
  CompressedRelationWriter& writer1_;
  CompressedRelationWriter& writer2_;
  const size_t blocksize_;
  const size_t numColumns_;

  // The buffered rows of the current relation, see
  // `CompressedRelationWriter::PermutationWriter::relation_`.
  IdTable relation_;
  // The current relation and its statistics so far.
  std::optional<Id> col0IdCurrentRelation_;
  size_t numBlocksCurrentRel_ = 0;
  size_t numRowsCurrentRel_ = 0;
  DistinctIdCounter distinctCol1Counter_;
  std::optional<Id> firstCol1CurrentRel_;
  Id lastCol1CurrentRel_ = Id::makeUndefined();

  // Whether the current relation is the first relation of the chunk, which
  // becomes a boundary part. Its first block of rows (the `head_`) is kept in
  // memory instead of being written, because it has to be consolidated with
  // the end of the previous chunk. The first chunk has no previous chunk, so
  // this is `false` from the start for it.
  bool inFirstRelation_;
  std::optional<IdTable> head_;

  // The small relations that are batched into the current block of small
  // relations, see `CompressedRelationWriter::smallRelationsBuffer_`.
  IdTable smallRelationsBuffer_;
  Id currentSmallBlockFirstCol0_ = Id::makeUndefined();
  Id currentSmallBlockLastCol0_ = Id::makeUndefined();

  // The twin sorter of this chunk, which is created lazily by the first large
  // relation, and which moves into the `BoundaryPart` of a large boundary
  // relation (so it may be created several times, hence the counter, which
  // makes the names of their files unique).
  std::unique_ptr<TwinSorter> twinSorter_;
  size_t numTwinSortersCreated_ = 0;

  // The last row of the previous input block (if any), for the removal of
  // duplicates across the blocks of this chunk.
  std::optional<IdTable::row_type> lastRowOfPreviousBlock_;

  // The input block that is currently being processed, see
  // `addBlockOfLargeRelationWithoutCopying`.
  SharedBlock inputBlock_;

  ChunkResult result_;

 public:
  ChunkWriter(ChunkwisePermutationWriter& shared, size_t chunkIndex);
  // Give the block buffers back to the pool.
  ~ChunkWriter();

  // Process the next input block of this chunk, which has to be sorted and
  // not empty. Return the block (with duplicates removed, if configured) for
  // the callbacks, or `nullptr` if nothing is left of it.
  SharedBlock processBlock(Block block);

  // Finish the chunk after its last block and return everything that the
  // consolidation needs.
  ChunkResult finishChunk();

 private:
  // Remove consecutive duplicate rows from the `block`, also with respect to
  // the last row of the previous block.
  void removeDuplicates(Block& block);

  // The equivalents of the respective functions of
  // `CompressedRelationWriter::PermutationWriter`, see there.
  template <typename PermutedCols>
  void addRowsOfCurrentRelation(const PermutedCols& permutedCols, size_t begin,
                                size_t end);
  template <typename PermutedCols, typename Col0>
  size_t writeCompleteSmallRelations(const PermutedCols& permutedCols,
                                     const Col0& col0, size_t begin,
                                     size_t firstRunEnd, size_t numRowsOfBlock);
  bool isCompleteSmallRelation(size_t begin, size_t end,
                               size_t numRowsOfBlock) const;
  void addBlockForLargeRelation();
  void addBlockOfLargeRelationWithoutCopying(IdTableView<0> block);
  void finishRelation();

  // Account for a block of rows of the current relation that has been
  // finalized (a block written directly, the head, the tail, or the rows of a
  // small part): count the rows and the distinct `col1` IDs.
  void countRowsOfCurrentRelation(IdTableView<0> rows);

  // Write a block of the current large relation with `writer1_`, push its twin
  // rows to the twin sorter, and account for it.
  void writeBlockOfLargeRelation(
      IdTableView<0> block,
      CompressedRelationWriter::BlockToWrite::Owner owner);

  // Push the twin rows (columns 1 and 2 swapped) of the `rows` to the twin
  // sorter of this chunk, which is created if necessary.
  void pushTwinRows(IdTableView<0> rows);
  TwinSorter& twinSorter();

  // The first relation of the chunk is complete (it ends inside the chunk if
  // `reachesChunkEnd` is `false`, otherwise it is also the last relation):
  // turn it into a boundary part.
  void finishFirstRelation(bool reachesChunkEnd);

  // The current relation (which is not the first one) is still open when the
  // chunk ends: turn it into a boundary part.
  void finishLastRelation();

  // Turn the statistics of the current relation into the
  // `DistinctIdCountOfBlock` of its part, and reset everything for the next
  // relation.
  compressedRelationHelpers::DistinctIdCountOfBlock
  distinctCol1OfCurrentRelation();
  void resetCurrentRelation();

  // The equivalents of the respective functions of `CompressedRelationWriter`
  // for the chunk-local buffer of small relations.
  size_t smallRelationBlockCapacity() const { return (3 * blocksize_) / 2; }
  size_t numRowsUntilSmallRelationBlockIsFull() const;
  template <typename Table>
  void addSmallRelations(Id firstCol0Id, Id lastCol0Id, const Table& relations,
                         size_t beginIdx, size_t endIdx);
  void writeBufferedSmallRelationsToSingleBlock();

  // Write the twin of the current large relation, which is complete, from the
  // twin sorter of this chunk (which is cleared afterwards). Return the
  // metadata of the twin.
  CompressedRelationMetadata writeTwinOfCompleteLargeRelation(Id col0Id);
};

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_CHUNKWRITER_H
