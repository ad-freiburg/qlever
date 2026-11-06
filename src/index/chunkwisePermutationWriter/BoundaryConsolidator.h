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
// writer (see the explanation at the top of `ChunkwisePermutationWriter.h`):
// it is fed the `ChunkResult`s in the order of the chunks, concatenates the
// `BoundaryPart`s of the relations that cross the boundaries of the chunks,
// writes them (and their twins), and emits the metadata of all the relations
// in the order of the relations. It runs on a single thread, which must not
// be a thread of the pool (it uses the blocking parts of the two writers and
// a parallel merge on the `executor`).
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

  // Consolidate the `chunk`, which has to be the result of the chunk that
  // follows the chunk of the previous call (or of the first chunk). A chunk
  // without any rows is skipped, in particular it doesn't complete the
  // boundary relation that is currently open.
  void addChunkResult(ChunkResult chunk);

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
  // in the order of their chunks, together with its twin.
  void writeBoundaryRelation(std::vector<BoundaryPart> parts);
  CompressedRelationMetadata writeTwinOfBoundaryRelation(
      Id col0Id, std::vector<BoundaryPart>& parts);

  CompressedRelationWriter& writer1() { return context_.writer1_; }
  CompressedRelationWriter& writer2() { return context_.writer2_; }
};

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_BOUNDARYCONSOLIDATOR_H
