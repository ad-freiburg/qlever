// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_H
#define QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_H

#include <atomic>
#include <condition_variable>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "engine/idTable/CompressedExternalIdTable.h"
#include "engine/idTable/IdTable.h"
#include "index/CompressedRelationHelpersImpl.h"
#include "index/CompressedRelationWriter.h"
#include "index/KeyOrder.h"
#include "util/AsyncResourcePool.h"
#include "util/GlobalExecutor.h"
#include "util/MemorySize/MemorySize.h"
#include "util/NoCopyNoMove.h"
#include "util/ProgressBar.h"
#include "util/Timer.h"

// Write a pair of twin permutations (e.g. PSO and POS) directly from the
// chunks of the parallel merge of an external sorter, see
// `chunkwisePermutationWriter::createPermutationPair` below.
//
// The classical `CompressedRelationWriter::createPermutationPair` consumes the
// sorted triples as a single sequential range. For a large input, the parallel
// merge that produces this range has to compress and spill most of its output
// blocks to disk, because the chunks of the merge finish in an arbitrary order
// while the consumer reads them in order; afterwards the blocks are read back
// and decompressed, and the single consuming thread then does all the work of
// cutting them into the blocks of the permutation. The writer in this file
// avoids all of that: it is a *sink* of the parallel merge (see
// `parallelBlockMerge::SinkConcept`), so every chunk of the merge hands its
// output blocks directly to a writer of its own, which writes the blocks of the
// permutation (and of the twin permutation) right away and in parallel to all
// the other chunks. Only the relations at the boundaries of the chunks need a
// (small) consolidation step, which runs in order on the thread that drives
// the merge:
//
// * Every chunk handles all the relations that start and end inside of it
//   exactly like the classical writer does: small relations are batched into
//   blocks of small relations, large relations are written as blocks of their
//   own, and the twin of a large relation is sorted with a twin sorter that is
//   private to the chunk (and that merges serially, in the thread of the
//   chunk). The metadata of these relations is collected per chunk.
// * The first and the last relation of a chunk may continue in the neighboring
//   chunks, so the chunk returns them as a `BoundaryPart` (see below) instead
//   of writing them: for a small part all the rows are kept in memory, for a
//   large part the chunk writes the complete blocks in its middle directly and
//   keeps only the rows before the first and after the last of those blocks in
//   memory, together with its twin sorter. A relation may also span several
//   complete chunks, each of which then consists of a single such part.
// * The consolidation on the driving thread concatenates the parts of a
//   relation in chunk order: the rows that two adjacent parts keep in memory
//   are written as blocks (such that triples which are equal when disregarding
//   the graph never end up in different blocks), the counts are folded, the
//   twin rows of all the parts are merged with a single parallel merge over the
//   runs of all their twin sorters, and the metadata of the relation is emitted
//   at the right place in the sequence of all relations.
//
// IMPORTANT: The callbacks that are invoked for the input blocks (see
// `ConcurrentBlockCallback`) are invoked concurrently and in an arbitrary order
// of the blocks. This writer can therefore not be used if a callback needs the
// blocks in order (as for example the pattern creation does), the classical
// writer has to be used then.
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
// blocks in an arbitrary order, see the IMPORTANT note at the top of this
// file. The callback may finish its work asynchronously (for example by
// pushing the block to the next external sorter via `asyncPushBlock`), which
// is why it is handed the `done` callback: it has to be invoked exactly once,
// as soon as the callback is done with the `block`, with the exception that
// occurred (or `nullptr`). The `block` is kept alive for as long as the
// callback holds on to its `shared_ptr`. The number of blocks for which the
// callbacks are in flight at the same time is bounded, see
// `Options::numCallbackBlocksInFlight_`.
using DoneCallback = std::function<void(std::exception_ptr)>;
using ConcurrentBlockCallback = std::function<void(SharedBlock, DoneCallback)>;

// The tuning knobs of the writer.
struct Options {
  // If `true`, then consecutive duplicate rows (which the merge of the sorter
  // may yield) are removed before anything else is done with a block. This is
  // the equivalent of `ad_utility::uniqueBlockView`, performed by the chunk
  // threads instead of by an additional stage in front of the writer.
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

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace net = boost::asio;

// The sorter that sorts the twin rows of a large relation (where `col0` is
// constant, so only the remaining columns are compared).
using TwinSorter = ad_utility::CompressedExternalIdTableSorter<
    compressedRelationHelpers::ComparatorForConstCol0, 0>;

// The part of a relation that lies at the beginning or at the end of a chunk
// (or that covers a whole chunk), see the explanation at the top of this file.
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

// The writer of a pair of permutations that is fed directly by the chunks of
// the parallel merge. It is the sink of the merge (see
// `parallelBlockMerge::SinkConcept`), and at the same time the object through
// which the driving thread consolidates the results of the chunks (see
// `finish`). It is always owned by a `shared_ptr` (see `create`), because the
// merge holds on to its sink until the last of its coroutines is done.
class ChunkwisePermutationWriter
    : public std::enable_shared_from_this<ChunkwisePermutationWriter>,
      public ad_utility::NoCopyNoMove {
 private:
  friend class ChunkWriter;
  using BlockToWrite = CompressedRelationWriter::BlockToWrite;
  using Semaphore = ad_utility::AsyncResourcePool<void>;
  using Permit = Semaphore::Handle;

  std::string basename_;
  std::unique_ptr<CompressedRelationWriter> writer1_;
  std::unique_ptr<CompressedRelationWriter> writer2_;
  compressedRelationHelpers::PairMetadataWriter writeMetadata_;
  qlever::KeyOrder permutation_;
  std::vector<ColumnIndex> permutedColIndices_;
  const size_t blocksize_;
  const size_t numColumns_;
  Options options_;
  std::vector<ConcurrentBlockCallback> callbacks_;

  net::any_io_executor executor_;
  // Bounds the number of blocks for which the callbacks are in flight.
  Semaphore callbackPermits_;

  // The state that is shared between the chunks and the driving thread, all of
  // it protected by `mutex_` (except for the atomics).
  std::mutex mutex_;
  std::condition_variable conditionVariable_;
  size_t numChunks_ = 0;
  std::vector<std::unique_ptr<ChunkWriter>> chunkWriters_;
  // The results of the chunks that the consolidation has not taken yet. A
  // chunk releases its permit of the merge as soon as it has stored its
  // result here, which only holds a few blocks of rows in memory (the twin
  // rows of the boundary parts are on disk, see `BoundaryPart::twinRuns_`), so
  // the results may safely pile up while the consolidation is busy.
  std::vector<std::optional<ChunkResult>> chunkResults_;
  std::vector<bool> chunkIsDone_;
  std::exception_ptr exception_;
  std::atomic<bool> stopRequested_{false};
  // The number of blocks for which the callbacks are currently in flight.
  size_t numCallbackBlocksInFlight_ = 0;

  // Statistics. The `numRowsProcessed_` are counted by the chunks, and the
  // driving thread reports them via the `progressBar_` while it waits for the
  // chunks (see `takeChunkResult`), by copying them into
  // `numRowsForProgressBar_` (the progress bar needs a plain counter).
  std::atomic<size_t> numInputRows_{0};
  std::atomic<size_t> numUniqueRows_{0};
  std::atomic<size_t> numRowsProcessed_{0};
  size_t numRowsForProgressBar_ = 0;
  ad_utility::ProgressBar progressBar_{numRowsForProgressBar_,
                                       "Triples sorted: "};
  size_t numDistinctCol0_ = 0;
  size_t numRows_ = 0;
  ad_utility::timer::ThreadSafeTimer chunkTimer_;
  ad_utility::Timer consolidationWaitTimer_{ad_utility::Timer::Stopped};
  ad_utility::Timer consolidationWorkTimer_{ad_utility::Timer::Stopped};

  struct PrivateTag {};

 public:
  // The constructor is effectively private, use `create()` instead.
  ChunkwisePermutationWriter(PrivateTag, std::string basename,
                             WriterAndCallback writerAndCallback1,
                             WriterAndCallback writerAndCallback2,
                             qlever::KeyOrder permutation,
                             std::vector<ConcurrentBlockCallback> callbacks,
                             Options options);

  // Create a writer, see the constructor for the arguments. The `basename` is
  // the prefix of the names of the temporary files (the twin sorters).
  static std::shared_ptr<ChunkwisePermutationWriter> create(
      std::string basename, WriterAndCallback writerAndCallback1,
      WriterAndCallback writerAndCallback2, qlever::KeyOrder permutation,
      std::vector<ConcurrentBlockCallback> callbacks, Options options);

  // The sink factory for the merge, see
  // `parallelBlockMerge::SinkFactoryConcept`. Is called exactly once, with the
  // number of chunks of the merge.
  std::shared_ptr<ChunkwisePermutationWriter> makeSink(size_t numChunks);

  // Consolidate the results of the chunks (as they become available, in the
  // order of the chunks), wait for the merge (the `mergeCompletion` is the
  // future that `CompressedExternalIdTableSorter::mergeToSink` returned) and
  // for the callbacks, rethrow the first exception that a chunk or a callback
  // reported, finish the two writers and return their blocks. May only be
  // called once, by the thread that drives the merge (which must not be a
  // thread of the executor).
  PermutationPairResult finish(std::future<void> mergeCompletion);

  // The number of rows that were pushed to this writer before the removal of
  // duplicates (which is the number of rows that the sorter yields).
  size_t numInputRows() const { return numInputRows_.load(); }

  // The interface of the sink, see `parallelBlockMerge::SinkConcept`.
  bool stopRequested() const noexcept { return stopRequested_.load(); }

  template <typename CompletionToken>
  auto asyncPush(size_t chunkIndex, Block block,
                 CompletionToken&& completionToken) {
    return spawnOnExecutor(pushImpl(chunkIndex, std::move(block)),
                           AD_FWD(completionToken));
  }

  template <typename CompletionToken>
  auto asyncFinishChunk(size_t chunkIndex, CompletionToken&& completionToken) {
    return spawnOnExecutor(finishChunkImpl(chunkIndex),
                           AD_FWD(completionToken));
  }

  template <typename CompletionToken>
  auto asyncPushException(std::exception_ptr exception,
                          CompletionToken&& completionToken) {
    return ad_utility::runFunctionOnExecutor(
        executor_,
        [this, exception = std::move(exception)]() mutable {
          storeException(std::move(exception));
        },
        AD_FWD(completionToken));
  }

  template <typename CompletionToken>
  auto asyncStop(CompletionToken&& completionToken) {
    return ad_utility::runFunctionOnExecutor(
        executor_, [this]() { requestStop(); }, AD_FWD(completionToken));
  }

 private:
  // Run the coroutine `awaitable` on the executor and complete the
  // `completionToken` with its result, via a `post` to the executor that is
  // associated with the token (never inline, see the IMPORTANT note at
  // `parallelBlockMerge::SinkConcept`).
  template <typename CompletionToken>
  auto spawnOnExecutor(net::awaitable<bool> awaitable,
                       CompletionToken&& completionToken) {
    return net::async_initiate<CompletionToken, void(std::exception_ptr, bool)>(
        [this, awaitable = std::move(awaitable)](auto handler) mutable {
          auto executor = net::get_associated_executor(handler, executor_);
          net::co_spawn(executor_, std::move(awaitable),
                        [executor, handler = std::move(handler)](
                            std::exception_ptr exception, bool result) mutable {
                          net::post(executor, [handler = std::move(handler),
                                               exception = std::move(exception),
                                               result]() mutable {
                            std::move(handler)(std::move(exception), result);
                          });
                        });
        },
        completionToken);
  }

  // The bodies of `asyncPush` and `asyncFinishChunk`.
  net::awaitable<bool> pushImpl(size_t chunkIndex, Block block);
  net::awaitable<bool> finishChunkImpl(size_t chunkIndex);

  // Invoke all the `callbacks_` for the `block`, and give the `permit` back
  // once all of them are done.
  void invokeCallbacks(SharedBlock block, Permit permit);

  // Store the `exception` (if it is the first one) and stop the merge.
  void storeException(std::exception_ptr exception);
  void requestStop();

  // Return the writer of the chunk with the given index (created on demand).
  ChunkWriter& chunkWriter(size_t chunkIndex);

  // The consolidation, see `finish` and the explanation at the top of this
  // file. `takeChunkResult` waits for the chunk to finish, and reports the
  // progress of the chunks while it waits (see `reportProgress`).
  ChunkResult takeChunkResult(size_t chunkIndex);
  void reportProgress();
  void consolidate();
  void writeBoundaryRelation(std::vector<BoundaryPart> parts);
  CompressedRelationMetadata writeTwinOfBoundaryRelation(
      Id col0Id, std::vector<BoundaryPart>& parts);
  void waitForCallbacks();

  // Helpers that are shared by the chunks and the consolidation. All of them
  // are thread-safe (they only use the thread-safe parts of the writers).

  // Write the `rows` of the large relation with the given `col0Id` as blocks
  // of (about) the blocksize of the `writer`, such that rows which agree in
  // their first three columns are never split across blocks. If `useQueue` is
  // `true`, then the blocks are compressed and written by the queue of the
  // `writer` (which is only allowed for the driving thread), otherwise in the
  // calling thread.
  static void writeRowsOfLargeRelation(CompressedRelationWriter& writer,
                                       Id col0Id, IdTable rows, bool useQueue);

  // Write the sorted `blocks` of the twin of the large relation with the given
  // `col0Id` with the `writer` (see `writeRowsOfLargeRelation` for `useQueue`),
  // and return the metadata of the twin.
  static CompressedRelationMetadata writeSortedBlocksOfLargeRelation(
      CompressedRelationWriter& writer, Id col0Id,
      ad_utility::InputRangeTypeErased<Block> blocks, bool useQueue);

  // Create the metadata of a large relation.
  static CompressedRelationMetadata makeMetadata(Id col0Id, size_t numRows,
                                                 size_t numDistinctCol1);

  // Create a twin sorter with the given `filename`.
  std::unique_ptr<TwinSorter> makeTwinSorter(const std::string& filename) const;
};

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// Write the pair of permutations that `writerAndCallback1` and
// `writerAndCallback2` describe (see
// `CompressedRelationWriter::createPermutationPair` for the arguments that
// both functions share) directly from the chunks of the merge of the
// `sortedInput`, which has to be sorted by the `permutation`. The sorter is
// consumed (see `CompressedExternalIdTableSorter::mergeToSink`) and is released
// again when this function returns. The `callbacks` are invoked for every
// input block, concurrently and in an arbitrary order, see
// `ConcurrentBlockCallback`.
//
// NOTE: In the C++17 mode of the build there is no parallel merge and hence
// no chunkwise writer, so this function throws. A caller has to use the
// classical `CompressedRelationWriter::createPermutationPair` in that mode,
// see `IndexImpl::createPSOAndPOSChunkwise` for an example.
template <typename Sorter>
PermutationPairResult createPermutationPair(
    const std::string& basename, WriterAndCallback writerAndCallback1,
    WriterAndCallback writerAndCallback2, Sorter& sortedInput,
    qlever::KeyOrder permutation,
    std::vector<ConcurrentBlockCallback> callbacks, Options options = {}) {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  (void)basename, (void)writerAndCallback1, (void)writerAndCallback2,
      (void)sortedInput, (void)permutation, (void)callbacks, (void)options;
  throw std::runtime_error{
      "The chunkwise permutation writer is not available in the C++17 mode "
      "of the build"};
#else
  auto writer = ChunkwisePermutationWriter::create(
      basename, std::move(writerAndCallback1), std::move(writerAndCallback2),
      std::move(permutation), std::move(callbacks), options);
  const size_t numRowsInSorter = sortedInput.size();
  auto mergeCompletion = sortedInput.template mergeToSink<0>(
      [&writer](size_t numChunks) { return writer->makeSink(numChunks); });
  auto result = writer->finish(std::move(mergeCompletion));
  AD_CORRECTNESS_CHECK(writer->numInputRows() == numRowsInSorter,
                       "The number of rows that the sorter has yielded (",
                       writer->numInputRows(),
                       ") differs from the number of rows that were pushed to "
                       "it (",
                       numRowsInSorter, ")");
  return result;
#endif
}

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_H
