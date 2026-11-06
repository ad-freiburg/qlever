// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_BOUNDARYCONSOLIDATOR_H
#define QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_BOUNDARYCONSOLIDATOR_H

#include <optional>
#include <string>
#include <vector>

#include "index/chunkwisePermutationWriter/ChunkWriter.h"
#include "index/chunkwisePermutationWriter/Options.h"
#include "index/compressedRelationWriter/CompressedRelationHelpersImpl.h"
#include "index/compressedRelationWriter/CompressedRelationWriter.h"
#include "util/MemorySize/MemorySize.h"
#include "util/NoCopyNoMove.h"

namespace chunkwisePermutationWriter {

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace net = boost::asio;

// The consolidation of the results of the chunks of the chunkwise permutation
// writer (see the explanation at the top of `ChunkWriter.h`): it is fed the
// `ChunkResult`s in the order of the chunks, concatenates the `BoundaryPart`s
// of the relations that cross the boundaries of the chunks, writes them (and
// their twins), and emits the metadata of all the relations in the order of
// the relations. It runs on a single thread, which must not
// be a thread of the pool (it uses the blocking parts of the two writers and
// a parallel merge on the `executor`).
//
// NOTE on thread safety: The consolidator has no writers of its own, it uses
// the `writer1_` and `writer2_` of the `ChunkWriterContext`, which the chunks
// use concurrently. This is safe, because the two use disjoint parts of the
// writers:
// * The chunks only use the thread-safe parts, namely `takeBlockBuffer`,
//   `blockBufferPool`, and `compressAndWriteBlockInCallingThread` (which only
//   touches the synchronized file and block metadata of the writer, and the
//   thread-safe callback to the twin writer). Each chunk batches its small
//   relations in a `SmallRelationsBlockBuilder` of its own.
// * The consolidator uses the parts that are not thread-safe, namely the
//   batching of small relations of the writers themselves (`addSmallRelation`
//   and `writeBufferedRelationsToSingleBlock`), the (blocking) block write
//   queue (`compressAndWriteBlock`), `addCompleteLargeRelation`, and `finish`.
//   It is the only user of those parts, and it is used by a single thread.
class BoundaryConsolidator : public ad_utility::NoCopyNoMove {
 private:
  // The `writer1_` and `writer2_` of the `context_` (which own the files of
  // the two permutations) are used by the chunks too, see `ChunkWriter`.
  ChunkWriterContext& context_;
  // The writer of the metadata, which batches them. It is reset by `finish`,
  // which passes the last batch on to the callbacks.
  std::optional<compressedRelationHelpers::PairMetadataWriter> writeMetadata_;
  net::any_io_executor executor_;
  // The memory of the parallel merge of the twin rows of a boundary relation,
  // see `Options::boundaryTwinMergeMemory_`.
  ad_utility::MemorySize boundaryTwinMergeMemory_;

  // The parts of the boundary relation that is currently open, in the order
  // of their chunks. All of them have the same `col0Id_`.
  std::vector<BoundaryPart> openParts_;

  // The total number of rows and of distinct `col0` IDs of all the chunks that
  // have been added so far.
  size_t numRows_ = 0;
  size_t numDistinctCol0_ = 0;

  // The number of chunks that have been added so far, to check that they are
  // added in order.
  size_t numChunksAdded_ = 0;
  // The last row of the last chunk with rows that has been added so far, to
  // check that equal rows are never split across chunks.
  std::optional<IdTable::row_type> lastRowOfPreviousChunk_;

 public:
  // Create a consolidator that writes with the writers of the `context`,
  // reports the metadata of the relations of the two permutations via the
  // `metadataCallback1` and `metadataCallback2`, and merges the twin rows of
  // the boundary relations on the `executor`.
  BoundaryConsolidator(
      ChunkWriterContext& context,
      compressedRelationHelpers::MetadataCallback metadataCallback1,
      compressedRelationHelpers::MetadataCallback metadataCallback2,
      net::any_io_executor executor,
      ad_utility::MemorySize boundaryTwinMergeMemory);

  // Consolidate the `chunk`, which has to be the result of the chunk with the
  // given `chunkIndex`. The chunks have to be added in order, starting with
  // index 0, which is checked. A chunk without any rows is skipped, in
  // particular it doesn't complete the boundary relation that is currently
  // open. Also check that the last row of the previous chunk and the first row
  // of this chunk are not equal, because the merge never splits equal rows
  // across chunks (which makes the removal of duplicates per chunk correct,
  // see `Options::removeDuplicates_`).
  void addChunkResult(size_t chunkIndex, ChunkResult chunk);

  // Write the boundary relation that is still open (if any), pass the last
  // metadata on to the callbacks, finish the two writers, and return their
  // blocks. Must be called exactly once, after the
  // results of all the chunks have been added.
  PermutationPairResult finish();

  // The total number of rows of all the chunks that have been added so far.
  size_t numRows() const { return numRows_; }

 private:
  // Append the `part` to the `openParts_`. If it belongs to a different
  // relation than the open parts, then that relation is complete and is
  // written first.
  void addPart(BoundaryPart part);

  // Write the relation of the `openParts_` (if any), see
  // `writeBoundaryRelation`.
  void closeOpenParts();

  // Write the complete relation that consists of the `parts`, which are given
  // in the order of their chunks, together with its twin. Dispatch to
  // `addSmallBoundaryRelation` or `writeLargeBoundaryRelation`.
  void writeBoundaryRelation(std::vector<BoundaryPart> parts);

  // Add the small relation `col0Id` that consists of the (small) `parts` to
  // the block of small relations that `writer1()` currently builds. The twin
  // is handled by the callback of `writer1()`. The `parts` are consumed.
  void addSmallBoundaryRelation(Id col0Id, std::vector<BoundaryPart>& parts);

  // Write the large relation `col0Id` with `numRows` rows that consists of the
  // `parts`, and its twin, and emit their metadata. The `parts` are consumed.
  void writeLargeBoundaryRelation(Id col0Id, std::vector<BoundaryPart>& parts,
                                  size_t numRows);

  // Write the twin of the large relation `col0Id` that consists of the
  // `parts`, by merging the twin rows of all the parts, and return its
  // metadata.
  CompressedRelationMetadata writeTwinOfBoundaryRelation(
      Id col0Id, std::vector<BoundaryPart>& parts);

  CompressedRelationWriter& writer1() { return context_.writer1_; }
  CompressedRelationWriter& writer2() { return context_.writer2_; }
};

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_BOUNDARYCONSOLIDATOR_H
