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

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "engine/idTable/CompressedExternalIdTable.h"
#include "engine/idTable/IdTable.h"
#include "index/KeyOrder.h"
#include "index/compressedRelationWriter/CompressedRelationHelpersImpl.h"
#include "index/compressedRelationWriter/CompressedRelationWriter.h"
#include "index/compressedRelationWriter/RelationBlockCutter.h"
#include "index/compressedRelationWriter/SmallRelationsBlockBuilder.h"
#include "util/InputRangeUtils.h"
#include "util/MemorySize/MemorySize.h"
#include "util/NoCopyNoMove.h"

namespace chunkwisePermutationWriter {
// The blocks of sorted triples that the writer consumes, and a block that is
// shared with other consumers, which may hold on to it asynchronously.
//
// NOTE: These types are also available in the C++17 mode of the build (unlike
// the rest of this file), so that code which exists in both modes can refer
// to them.
using Block = IdTableStatic<0>;
using SharedBlock = std::shared_ptr<const Block>;
}  // namespace chunkwisePermutationWriter

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// The writer of a single chunk of a pair of twin permutations (e.g. PSO and
// POS), for the case that the sorted triples are produced by a parallel merge
// (see `parallelBlockMerge::parallelBlockMergeToSink`) that splits its output
// into consecutive chunks, which are produced concurrently. Every chunk is
// handed to a `ChunkWriter` of its own, which writes the blocks of the
// permutation (and of its twin) right away, without waiting for the previous
// chunks:
//
// * All the relations that start and end inside of the chunk are handled
//   exactly like the classical `CompressedRelationWriter::PermutationWriter`
//   handles them: small relations are batched into blocks of small relations,
//   large relations are written as blocks of their own, and the twin of a large
//   relation is sorted with a twin sorter that is private to the chunk (and
//   that does all its work in the thread of the chunk). The metadata of these
//   relations is collected in the `ChunkResult`.
// * The first and the last relation of a chunk may continue in the neighboring
//   chunks, so the chunk returns them as a `BoundaryPart` (see below) instead
//   of writing them completely. The parts of the same relation of consecutive
//   chunks then have to be consolidated in the order of the chunks, which is
//   the job of the user of this class. The two cases are symmetric: as the
//   chunks run concurrently, a chunk knows neither whether its last relation
//   continues in the next chunk, nor whether its first relation started in the
//   previous chunk (see `ChunkWriter::inFirstRelation_` for the consequences
//   for the first relation).
namespace chunkwisePermutationWriter {

// The sorter that sorts the twin rows of a large relation (where `col0` is
// constant, so only the remaining columns are compared).
using TwinSorter = ad_utility::CompressedExternalIdTableSorter<
    compressedRelationHelpers::ComparatorForConstCol0, 0>;

// The part of a relation that lies at the beginning or at the end of a chunk
// (or that covers a whole chunk), see the explanation at the top of this
// file.
//
// All the rows of a part are rows of the same relation (the one with the
// `col0Id_`), and the parts of a relation are consolidated in the order of
// their chunks. A chunk with several relations has two separate parts (its
// `ChunkResult::firstPart_` and `ChunkResult::lastPart_`), each with its own
// `col0Id_`.
//
// The rows of a part are either written by the chunk directly, as complete
// blocks of the permutation (the "written blocks" below), or kept in memory
// for the consolidation, which is only possible for the rows before and after
// the written blocks:
//
// * A small part (see `isLarge`) has no written blocks, all its rows are in
//   the `rowsBeforeWrittenBlocks_`.
// * A large first part of a chunk keeps its first block of rows (which
//   might have to be merged with the end of the previous chunk) in the
//   `rowsBeforeWrittenBlocks_`, and writes the rest. If the part reaches the
//   end of its chunk, then the rows after the last written block are kept in
//   the `rowsAfterWrittenBlocks_`.
// * A large last part of a chunk has started inside the chunk, so all its
//   blocks are written directly, the `rowsBeforeWrittenBlocks_` are empty, and
//   the remaining rows are kept in the `rowsAfterWrittenBlocks_`.
struct BoundaryPart {
  Id col0Id_;
  // The rows of the part that precede its written blocks, see above.
  IdTable rowsBeforeWrittenBlocks_;
  // The rows of the part that follow its written blocks, see above. Only
  // present if the part is large, reaches the end of its chunk, and has such
  // rows.
  std::optional<IdTable> rowsAfterWrittenBlocks_;
  // The number of rows of the relation `col0Id_` inside this chunk, including
  // the ones in the written blocks.
  size_t numRows_ = 0;
  // The number of distinct `col1` IDs of all the rows of the part (in order),
  // together with the first and the last of them, so that the counts of the
  // parts of a relation can be folded, see `DistinctIdCounter`.
  compressedRelationHelpers::DistinctIdCountOfBlock distinctCol1_;
  // The sorter that holds the twin rows (columns 1 and 2 swapped) of *all* the
  // rows of the part, including the `rowsBeforeWrittenBlocks_` and the
  // `rowsAfterWrittenBlocks_`. Only present if the part is large.
  std::unique_ptr<TwinSorter> twinSorter_;
  // The sorted runs of the `twinSorter_`, which the chunk finishes as soon as
  // the part is complete (see `ChunkWriter::finishTwinSorterOfPart`), so that
  // a part that waits for the consolidation only holds the runs on disk (and
  // the `rowsBeforeWrittenBlocks_` and `rowsAfterWrittenBlocks_`) in memory.
  // Present iff the `twinSorter_` is.
  std::optional<ad_utility::CompressedIdTableRunsInput<0>> twinRuns_;

  // Construct a part from the relation it belongs to and from the rows that
  // precede its written blocks, see `rowsBeforeWrittenBlocks_`.
  BoundaryPart(Id col0Id, IdTable rowsBeforeWrittenBlocks)
      : col0Id_{col0Id},
        rowsBeforeWrittenBlocks_{std::move(rowsBeforeWrittenBlocks)} {}

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
  // the boundary parts). Unlike the `middleMetadata_`, this also counts the
  // small relations.
  size_t numMiddleRelations_ = 0;
  // The number of rows of the chunk (after the removal of duplicates).
  size_t numRows_ = 0;
  // The first and the last row of the chunk (after the removal of
  // duplicates). Absent iff the chunk has no rows. They are used to check that
  // equal rows are never split across chunks, see
  // `BoundaryConsolidator::addChunkResult`.
  std::optional<IdTable::row_type> firstRow_;
  std::optional<IdTable::row_type> lastRow_;
};

// The state that all the `ChunkWriter`s of a single merge share. All of it is
// either constant while the chunks run or thread-safe.
struct ChunkWriterContext {
  // The writers of the permutation and of its twin. The chunks only use their
  // thread-safe parts, see `ChunkWriter`. The blocks of small relations of the
  // `writer1_` are passed on to the `writer2_` by its `smallBlocksCallback_`
  // (see `CompressedRelationWriter::AddBlockOfSmallRelationsToSwitched`),
  // which the constructor sets up.
  CompressedRelationWriter& writer1_;
  CompressedRelationWriter& writer2_;
  // The column of the input blocks that contains the `col0` IDs, and the
  // indices of all their columns in the order in which they are stored in the
  // permutation (so there is one index per column), see
  // `compressedRelationHelpers::permutedColIndices`.
  ColumnIndex col0Column_;
  std::vector<ColumnIndex> permutedColIndices_;
  // If `true`, then the chunks remove consecutive duplicate rows from their
  // input blocks (which the merge of an external sorter may yield). This is
  // correct per chunk, because a merge never splits equal rows across chunks.
  bool removeDuplicates_ = false;
  // The prefix of the names of the files of the twin sorters.
  std::string twinSorterBasename_;
  // The memory limit of a single twin sorter. Every chunk has at most one twin
  // sorter that holds rows in memory at a time (the ones of its finished
  // boundary parts only hold their runs on disk, see
  // `BoundaryPart::twinRuns_`), so this is the memory of the twin sorters per
  // running chunk.
  ad_utility::MemorySize twinSorterMemoryPerChunk_;

  // Statistics that are counted by all the chunks together: the number of
  // input rows (before the removal of duplicates), the number of rows after
  // the removal of duplicates (only if `removeDuplicates_` is set), and the
  // number of rows that have been processed so far (e.g. for a progress bar).
  std::atomic<size_t> numInputRows_{0};
  std::atomic<size_t> numUniqueRows_{0};
  std::atomic<size_t> numRowsProcessed_{0};

  // Create the context from its members (see above), and connect the
  // `writer1` and the `writer2`: the blocks of small relations of the
  // `writer1` are passed on to the `writer2`, which therefore also has to use
  // the same pool of block buffers. The two writers must have the same
  // blocksize and the same number of columns.
  ChunkWriterContext(CompressedRelationWriter& writer1,
                     CompressedRelationWriter& writer2, ColumnIndex col0Column,
                     std::vector<ColumnIndex> permutedColIndices,
                     bool removeDuplicates, std::string twinSorterBasename,
                     ad_utility::MemorySize twinSorterMemoryPerChunk);

  // The number of columns of the input blocks and of the permutations.
  size_t numColumns() const { return permutedColIndices_.size(); }

  // Create a twin sorter with the given `filename`, which does all its work
  // in the calling thread (see the implementation).
  std::unique_ptr<TwinSorter> makeTwinSorter(const std::string& filename) const;

 private:
  // Set up the `writer1` and the `writer2` for the `ChunkWriter`s, see the
  // constructor.
  static void connectTwinWriters(CompressedRelationWriter& writer1,
                                 CompressedRelationWriter& writer2);
};

// The writer of a single chunk of the merge. It is driven by a single thread
// at a time (the chunk's coroutine of the merge), but by different threads for
// different blocks, which is fine because every operation on it is sequenced
// by the merge. It only uses the thread-safe parts of the two
// `CompressedRelationWriter`s (the compression and writing of a block in the
// calling thread, and the pool of block buffers) and keeps all the other state
// (the current relation, the buffer of small relations, the twin sorter) for
// itself. The cutting of the input blocks into relations and blocks is
// inherited from the `RelationBlockCutter`, see there for the hooks that are
// implemented below.
class ChunkWriter : public RelationBlockCutter<ChunkWriter>,
                    public ad_utility::NoCopyNoMove {
 private:
  using Base = RelationBlockCutter<ChunkWriter>;
  friend Base;
  using DistinctIdCounter = compressedRelationHelpers::DistinctIdCounter;

  ChunkWriterContext& context_;
  size_t chunkIndex_;
  // Whether this is the last chunk of the merge, whose last relation is
  // complete (see `ChunkResult::lastPart_`).
  const bool isLastChunk_;

  // The statistics of the current relation so far.
  size_t numRowsCurrentRel_ = 0;
  DistinctIdCounter distinctCol1Counter_;
  std::optional<Id> firstCol1CurrentRel_;
  Id lastCol1CurrentRel_ = Id::makeUndefined();

  // Whether the current relation is the first relation of the chunk, which
  // becomes a boundary part. As the chunks run concurrently, this chunk
  // doesn't know whether the relation started in the previous chunk, which
  // has two consequences:
  //
  // * The relation is never written as a small relation, because together
  //   with the rows from the previous chunk it might be large.
  // * Its first block of rows (the `head_`) is kept in memory instead of being
  //   written, because the remaining rows of the relation at the end of the
  //   previous chunk (less than a block) have to precede it. Writing the head
  //   directly would leave those rows as a tiny block of their own in front of
  //   it, and could split rows that agree in their first three columns
  //   across blocks. Only the blocks after the head are written directly.
  //
  // The first chunk has no previous chunk, so this is `false` from the start
  // for it, and its first relation is handled like any other relation (e.g.
  // it may be written as a small relation).
  bool inFirstRelation_;
  std::optional<IdTable> head_;

  // The small relations that are batched into the current block of small
  // relations, see `CompressedRelationWriter::smallRelations_`.
  SmallRelationsBlockBuilder smallRelations_;

  // The twin sorter of this chunk, which is created lazily by the first large
  // relation, and which moves into the `BoundaryPart` of a large boundary
  // relation (so it may be created several times, hence the counter, which
  // makes the names of their files unique).
  std::unique_ptr<TwinSorter> twinSorter_;
  size_t numTwinSortersCreated_ = 0;

  // The last row of the previous input block (if any), for the removal of
  // duplicates across the blocks of this chunk.
  std::optional<IdTable::row_type> lastRowOfPreviousBlock_;

  ChunkResult result_;

 public:
  // Create the writer of the chunk with the given `chunkIndex` of a merge with
  // `numChunks` chunks.
  ChunkWriter(ChunkWriterContext& context, size_t chunkIndex, size_t numChunks);
  // Give the block buffers back to the pool.
  ~ChunkWriter();

  // Process the next input block of this chunk, which has to be sorted and
  // not empty. Return the block (with duplicates removed, if configured) for
  // further consumers (e.g. the sorter of the next permutation), or `nullptr`
  // if nothing is left of it.
  SharedBlock processBlock(Block block);

  // Finish the chunk after its last block and return everything that the
  // consolidation needs.
  ChunkResult finishChunk();

  // Helpers that are shared by the chunks and by the consolidation of their
  // results. All of them are thread-safe (they only use the thread-safe parts
  // of the `writer`).

  // Write the `rows` of the large relation with the given `col0Id` as blocks
  // of (about) the blocksize of the `writer`, such that rows which agree in
  // their first three columns are never split across blocks. If `useQueue` is
  // `true`, then the blocks are compressed and written by the queue of the
  // `writer` (which is only allowed for a thread that is not a thread of the
  // pool), otherwise in the calling thread.
  //
  // PRECONDITIONS: The `rows` are sorted, all of them belong to the relation
  // `col0Id`, and they directly follow the rows of that relation that have
  // previously been written with the `writer` (they need not be the complete
  // relation). This function only writes the blocks: counting the rows and the
  // distinct `col1` IDs, creating the metadata of the relation (see
  // `CompressedRelationWriter::makeLargeRelationMetadata`), and handling the
  // twin rows are the job of the caller.
  static void writeRowsOfLargeRelation(CompressedRelationWriter& writer,
                                       Id col0Id, IdTable rows, bool useQueue);

  // Write the sorted `blocks` of the twin of the large relation with the given
  // `col0Id` with the `writer` in the calling thread, and return the metadata
  // of the twin. This is the counterpart of
  // `CompressedRelationWriter::addCompleteLargeRelation` for the chunks, which
  // run on the pool and therefore must neither use the (blocking) block write
  // queue nor the (not thread-safe) bookkeeping of the `writer`. The `blocks`
  // must contain at least one row.
  static CompressedRelationMetadata writeSortedBlocksOfLargeRelation(
      CompressedRelationWriter& writer, Id col0Id,
      ad_utility::InputRangeTypeErased<Block> blocks);

 private:
  // If the `context_` says so, remove consecutive duplicate rows from the
  // `block`, also with respect to the last row of the previous block. Update
  // the statistics of the `context_` accordingly.
  void removeDuplicatesIfNecessary(Block& block);

  // The hooks of the `RelationBlockCutter`, see there and the respective
  // functions of `CompressedRelationWriter::PermutationWriter`.
  void addBlockForLargeRelation();
  void addBlockOfLargeRelationWithoutCopying(IdTableView<0> block);
  void finishRelation();
  template <typename PermutedCols>
  void addCompleteSmallRelations(Id firstCol0Id, Id lastCol0Id,
                                 const PermutedCols& permutedCols, size_t begin,
                                 size_t end, size_t numRelations);
  const SmallRelationsBlockBuilder& smallRelationsBlockBuilder() const {
    return smallRelations_;
  }
  // Return `true` iff the current relation is the first relation of a chunk
  // that is not the first one (see `inFirstRelation_`), and it is not yet
  // decided whether its part is large: it has fewer rows than a block so far,
  // and all of them are still in the `relation_` buffer. Once a block is
  // complete, the part is large and that block becomes its `head_`.
  bool sizeOfFirstRelationIsUndecided() const {
    return inFirstRelation_ && !head_.has_value();
  }
  // The first block of the first relation of the chunk is kept in memory (see
  // `head_`), so it must never be written directly from the input block.
  bool mayWriteDirectly() const { return !sizeOfFirstRelationIsUndecided(); }
  // The first relation of the chunk is never written as a small relation by
  // the chunk, because it may continue the last relation of the previous
  // chunk. This doesn't apply to the very first chunk, see `inFirstRelation_`.
  bool mayWriteCompleteSmallRelations() const { return !inFirstRelation_; }

  // Account for a block of rows of the current relation that has been
  // finalized (a block written directly, the head, the tail, or the rows of a
  // small part): count the rows and the distinct `col1` IDs.
  void countRowsOfCurrentRelation(const IdTableView<0>& rows);

  // Write a block of the current large relation with `writer1_`, push its twin
  // rows to the twin sorter, and account for it.
  void writeBlockOfLargeRelation(
      IdTableView<0> block,
      CompressedRelationWriter::BlockToWrite::Owner owner);

  // Push the twin rows (columns 1 and 2 swapped) of the `rows` to the twin
  // sorter of this chunk, which is created if necessary. The `rows` are taken
  // by value, because their columns are swapped in place.
  void pushTwinRows(IdTableView<0> rows);
  TwinSorter& twinSorter();

  // The first relation of the chunk is complete (it ends inside the chunk if
  // `reachesChunkEnd` is `false`, otherwise it is also the last relation):
  // turn it into a boundary part, which is then stored in
  // `result_.firstPart_`.
  void finishFirstRelation(bool reachesChunkEnd);

  // The current relation (which is not the first one) is still open when the
  // chunk ends: turn it into a boundary part, which is then stored in
  // `result_.lastPart_`.
  void finishLastRelation();

  // All the twin rows of the `part` have been pushed: if it is large, then
  // finish the input phase of its twin sorter (which sorts and writes its last
  // block), see `BoundaryPart::twinRuns_`.
  static void finishTwinSorterOfPart(BoundaryPart& part);

  // Turn the statistics of the current relation into the
  // `DistinctIdCountOfBlock` of its part, and reset everything for the next
  // relation.
  compressedRelationHelpers::DistinctIdCountOfBlock
  distinctCol1OfCurrentRelation();
  void resetCurrentRelation();

  // Append the rows `[beginIdx, endIdx)` of the `relations`, which are one or
  // more complete small relations from `firstCol0Id` to `lastCol0Id`, to the
  // current block of small relations (`smallRelations_`). If they don't fit,
  // then the current block is written first, see
  // `writeBufferedSmallRelationsToSingleBlock`.
  template <typename Table>
  void addSmallRelations(Id firstCol0Id, Id lastCol0Id, const Table& relations,
                         size_t beginIdx, size_t endIdx);

  // Compress and write the current block of small relations (if it is not
  // empty) with the `writer1` of the `context_` in the calling thread. The
  // `smallBlocksCallback_` of that writer also writes the twin of the block to
  // the `writer2`, see `ChunkWriterContext`.
  void writeBufferedSmallRelationsToSingleBlock();

  // Write the twin of the current large relation, which is complete, from the
  // twin sorter of this chunk (which is cleared afterwards). Return the
  // metadata of the twin.
  CompressedRelationMetadata writeTwinOfCompleteLargeRelation(Id col0Id);
};

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_CHUNKWRITER_H
