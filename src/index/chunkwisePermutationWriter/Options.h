// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_OPTIONS_H
#define QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_OPTIONS_H

#include <exception>
#include <functional>
#include <memory>

#include "engine/idTable/IdTable.h"
#include "index/compressedRelationWriter/CompressedRelationWriter.h"
#include "util/MemorySize/MemorySize.h"

// The options and the types of the interface of the chunkwise permutation
// writer, see `index/chunkwisePermutationWriter/ChunkwisePermutationWriter.h`.
namespace chunkwisePermutationWriter {

using WriterAndCallback = CompressedRelationWriter::WriterAndCallback;
using PermutationPairResult = CompressedRelationWriter::PermutationPairResult;

// The blocks of sorted triples that the writer consumes, and a block that is
// shared with the callbacks, which may hold on to it asynchronously.
using Block = IdTableStatic<0>;
using SharedBlock = std::shared_ptr<const Block>;

// A callback that is invoked for every input block, from the thread of the
// chunk that the block belongs to. It has to be thread-safe, because it is
// invoked for several blocks at the same time, and it is invoked for the
// blocks in an arbitrary order, see the IMPORTANT note at the top of
// `ChunkwisePermutationWriter.h`. The callback may finish its work
// asynchronously (for example by pushing the block to the next external sorter
// via `asyncPushBlock`), which is why it is handed the `done` callback: it has
// to be invoked exactly once, as soon as the callback is done with the `block`,
// with the exception that occurred (or `nullptr`). The `block` is kept alive
// for as long as the callback holds on to its `shared_ptr`. The number of
// blocks for which the callbacks are in flight at the same time is bounded, see
// `Options::numCallbackBlocksInFlight_`.
using DoneCallback = std::function<void(std::exception_ptr)>;
using ConcurrentBlockCallback = std::function<void(SharedBlock, DoneCallback)>;

// The tuning knobs of the writer.
struct Options {
  // If `true`, then consecutive duplicate rows (which the merge of the sorter
  // may yield) are removed before anything else is done with a block. This is
  // the equivalent of `ad_utility::uniqueBlockView`, performed by the chunk
  // threads instead of by an additional stage in front of the writer (both use
  // `ad_utility::removeDuplicatesFromBlock`). The view itself can't be used
  // here, because the chunks are handed their blocks one by one (the writer is
  // a sink of the merge, not the consumer of a range), and because they run on
  // the global thread pool, from which the view must not be consumed. Removing
  // the duplicates per chunk is correct, because the merge never splits equal
  // rows across chunks.
  bool removeDuplicates_ = false;
  // The memory limit of the twin sorter of a single chunk, which sorts the
  // twin rows of a large relation. The sorter only needs a file (and hence the
  // disk) if a single relation has more rows than fit into this memory. Up to
  // three such sorters (the working one and the ones of the two boundary
  // parts) exist per chunk that the merge keeps in flight, and the results of
  // finished chunks (which hold the sorters of their boundary parts) are
  // bounded by the same number, see `finishChunkImpl`.
  ad_utility::MemorySize twinSorterMemoryPerChunk_ =
      ad_utility::MemorySize::megabytes(256);
  // The memory limit of the parallel merge that sorts the twin rows of a
  // relation that spans several chunks (see `BoundaryPart`).
  ad_utility::MemorySize boundaryTwinMergeMemory_ =
      ad_utility::MemorySize::gigabytes(4);
  // The number of input blocks for which the `ConcurrentBlockCallback`s may be
  // in flight at the same time, see there. Each of them keeps its block in
  // memory.
  size_t numCallbackBlocksInFlight_ = 8;
};

// The blocks that the twin sorters yield for a large relation are this many
// times larger than the blocksize of the permutation; the writer cuts them
// into blocks of the permutation again, see
// `CompressedRelationWriter::PermutationWriter::twinSorterBlocksizeFactor_`.
constexpr inline size_t TWIN_SORTER_BLOCKSIZE_FACTOR = 16;

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_OPTIONS_H
