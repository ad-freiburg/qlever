// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_CHUNKWISEPERMUTATIONWRITER_H
#define QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_CHUNKWISEPERMUTATIONWRITER_H

#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "backports/asio.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/KeyOrder.h"
#include "index/chunkwisePermutationWriter/BoundaryConsolidator.h"
#include "index/chunkwisePermutationWriter/ChunkWriter.h"
#include "index/chunkwisePermutationWriter/ConcurrentCallbackDispatcher.h"
#include "index/chunkwisePermutationWriter/Options.h"
#include "index/chunkwisePermutationWriter/Statistics.h"
#include "index/compressedRelationWriter/CompressedRelationWriter.h"
#include "util/AsioHelpers.h"
#include "util/GlobalExecutor.h"
#include "util/NoCopyNoMove.h"
#include "util/ThreadSafeReorderBuffer.h"

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
//   chunks, so the chunk returns them as a `BoundaryPart` (see `ChunkWriter.h`)
//   instead of writing them: for a small part all the rows are kept in memory,
//   for a large part the chunk writes the complete blocks in its middle
//   directly and keeps only the rows before the first and after the last of
//   those blocks in memory, together with its twin sorter. A relation may also
//   span several complete chunks, each of which then consists of a single such
//   part.
// * The twin of a relation that crosses the boundaries of chunks needs the
//   rows of all its parts, so a chunk doesn't merge the twin sorter of such a
//   part, but only hands over its sorted runs on disk (see
//   `BoundaryPart::twinRuns_`). The consolidation then sorts the complete twin
//   with a single parallel merge over the runs of all the parts (see
//   `BoundaryConsolidator::writeTwinOfBoundaryRelation`).
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

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace net = boost::asio;

// The writer of a pair of permutations that is fed directly by the chunks of
// the parallel merge. It is the sink of the merge (see
// `parallelBlockMerge::SinkConcept`), and at the same time the object through
// which the driving thread consolidates the results of the chunks (see
// `finish`). It is always owned by a `shared_ptr`, see `create`.
//
// The work is split among the following components, which this class owns and
// drives:
// * A `ChunkWriter` per chunk writes everything that lies inside the chunk.
// * The `ThreadSafeReorderBuffer` hands the results of the chunks over to the
//   driving thread, in the order of the chunks, and holds the first exception
//   and the stop state that the chunks, the callbacks, and the driving thread
//   share.
// * The `BoundaryConsolidator` consolidates the results of the chunks on the
//   driving thread.
// * The `ConcurrentCallbackDispatcher` invokes the `ConcurrentBlockCallback`s
//   for the blocks of the chunks.
// * The `Statistics` report the progress and the timing.
class ChunkwisePermutationWriter
    : public std::enable_shared_from_this<ChunkwisePermutationWriter>,
      public ad_utility::NoCopyNoMove {
 private:
  // The writers of the two permutations, which the `context_` refers to.
  std::unique_ptr<CompressedRelationWriter> writer1_;
  std::unique_ptr<CompressedRelationWriter> writer2_;
  net::any_io_executor executor_;
  // The state that is shared by all the `ChunkWriter`s (including the
  // statistics that they count).
  ChunkWriterContext context_;
  BoundaryConsolidator consolidator_;
  Statistics statistics_;
  // The results of the chunks that the consolidation has not taken yet. A
  // chunk releases its permit of the merge as soon as it has stored its
  // result here, which only holds a few blocks of rows in memory (the twin
  // rows of the boundary parts are on disk, see `BoundaryPart::twinRuns_`), so
  // the results may safely pile up while the consolidation is busy.
  ad_utility::ThreadSafeReorderBuffer<ChunkResult> chunkResults_;
  ConcurrentCallbackDispatcher callbackDispatcher_;

  // The writers of the chunks, which are created on demand (see
  // `chunkWriter`). The vector has one slot per chunk and is never resized.
  const size_t numChunks_;
  std::vector<std::unique_ptr<ChunkWriter>> chunkWriters_;

  struct PrivateTag {};

 public:
  // Use `create` instead. The constructor has to be public, because
  // `std::make_shared` (which `create` uses) can only call a public
  // constructor, but as the `PrivateTag` is private, only `create` can call
  // it.
  ChunkwisePermutationWriter(PrivateTag, size_t numChunks,
                             const std::string& basename,
                             WriterAndCallback writerAndCallback1,
                             WriterAndCallback writerAndCallback2,
                             const qlever::KeyOrder& permutation,
                             std::vector<ConcurrentBlockCallback> callbacks,
                             const Options& options,
                             net::any_io_executor executor);

  // Create a writer, which is at the same time the sink of a parallel merge
  // with `numChunks` chunks (at least one). The arguments:
  // * `basename`: The prefix of the names of the temporary files (of the twin
  //   sorters).
  // * `writerAndCallback1`, `writerAndCallback2`: The writers of the two
  //   permutations, together with the callbacks that receive the metadata of
  //   the large relations (see `CompressedRelationWriter::WriterAndCallback`).
  //   Both writers must have the same number of columns and the same
  //   blocksize.
  // * `permutation`: The permutation that is written by `writerAndCallback1`.
  //   The input of the merge has to be sorted by it, and the graph has to be
  //   its fourth column. The second permutation is its twin, with the second
  //   and third column swapped.
  // * `callbacks`: Invoked for every input block, see
  //   `ConcurrentBlockCallback`.
  // * `options`: See `Options`.
  // * `executor`: The executor on which the chunks write their blocks, the
  //   callbacks are dispatched, and the twins of the boundary relations are
  //   merged. Typically the executor of the merge.
  //
  // The writer is always owned by a `shared_ptr`, which is why it can only be
  // created by this function: the merge holds on to its sink via a
  // `shared_ptr` until the last of its coroutines is done (possibly after the
  // driving thread has returned from `finish`), and the callbacks that are
  // still in flight keep the writer alive via `shared_from_this()` (see
  // `pushImpl`).
  static std::shared_ptr<ChunkwisePermutationWriter> create(
      size_t numChunks, const std::string& basename,
      WriterAndCallback writerAndCallback1,
      WriterAndCallback writerAndCallback2, const qlever::KeyOrder& permutation,
      std::vector<ConcurrentBlockCallback> callbacks, const Options& options,
      net::any_io_executor executor);

  // Consolidate the results of the chunks (as they become available, in the
  // order of the chunks), wait for the merge (the `mergeCompletion` is the
  // future that `CompressedExternalIdTableSorter::mergeToSink` returned) and
  // for the callbacks, rethrow the first exception that a chunk or a callback
  // reported, finish the two writers and return their blocks. May only be
  // called once, by the thread that drives the merge. That thread must not be
  // a thread of the `executor_` (or of the executor of the merge), because
  // this function blocks it until all the chunks are done, see
  // `createPermutationPair`. This also holds if an exception is thrown: the
  // remaining chunks are then stopped, and the exception only propagates once
  // all the chunks and callbacks are done.
  PermutationPairResult finish(std::future<void> mergeCompletion);

  // The number of rows that were pushed to this writer before the removal of
  // duplicates (which is the number of rows that the sorter yields).
  size_t numInputRows() const { return context_.numInputRows_.load(); }

  // The interface of the sink, see `parallelBlockMerge::SinkConcept`.
  bool stopRequested() const noexcept { return chunkResults_.stopRequested(); }

  template <typename CompletionToken>
  auto asyncPush(size_t chunkIndex, Block block,
                 CompletionToken&& completionToken) {
    // NOTE: The completion is always posted, never invoked inline, see the
    // IMPORTANT note at `parallelBlockMerge::SinkConcept`.
    return ad_utility::coSpawnWithPostedCompletion(
        executor_, pushImpl(chunkIndex, std::move(block)),
        AD_FWD(completionToken));
  }

  template <typename CompletionToken>
  auto asyncFinishChunk(size_t chunkIndex, CompletionToken&& completionToken) {
    return ad_utility::coSpawnWithPostedCompletion(
        executor_, finishChunkImpl(chunkIndex), AD_FWD(completionToken));
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
  // The bodies of `asyncPush` and `asyncFinishChunk`.
  net::awaitable<bool> pushImpl(size_t chunkIndex, Block block);
  net::awaitable<bool> finishChunkImpl(size_t chunkIndex);

  // Store the `exception` (if it is the first one) and stop the merge.
  void storeException(std::exception_ptr exception);
  // Stop the merge: wake up the chunks that wait for the callbacks and the
  // driving thread.
  void requestStop();

  // Return the writer of the chunk with the given index (created on demand).
  ChunkWriter& chunkWriter(size_t chunkIndex);

  // Feed the results of all the chunks to the `consolidator_`, in order, as
  // they become available.
  void consolidate();
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
// `ConcurrentBlockCallback`. The `executor` is the one of the writer, see
// `ChunkwisePermutationWriter::create`. It should typically be the executor
// that the merge of the `sortedInput` runs on (see
// `CompressedExternalIdTableSorter::setMergeExecutor`), which is also the
// global executor by default.
//
// IMPORTANT: This function blocks the calling thread until the merge and all
// the chunks (and callbacks) are done, so it must not be called from a thread
// of the `executor` or of the executor of the merge: if all the threads of
// such a pool were blocked like this, the chunks could never run.
//
// NOTE: In the C++17 mode of the build there is no parallel merge and hence
// no chunkwise writer, so this function throws. A caller has to use the
// classical `CompressedRelationWriter::createPermutationPair` in that mode.
template <typename Sorter>
PermutationPairResult createPermutationPair(
    const std::string& basename, WriterAndCallback writerAndCallback1,
    WriterAndCallback writerAndCallback2, Sorter& sortedInput,
    qlever::KeyOrder permutation,
    std::vector<ConcurrentBlockCallback> callbacks, Options options = {},
    ql::any_io_executor executor = ad_utility::globalExecutor()) {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  (void)basename, (void)writerAndCallback1, (void)writerAndCallback2,
      (void)sortedInput, (void)permutation, (void)callbacks, (void)options,
      (void)executor;
  throw std::runtime_error{
      "The chunkwise permutation writer is not available in the C++17 mode "
      "of the build"};
#else
  const size_t numRowsInSorter = sortedInput.size();
  // The number of chunks is only known once the merge has started, so the
  // writer is created by the sink factory, which the merge calls exactly once
  // (also for an empty input, see `parallelBlockMerge::SinkFactoryConcept`).
  std::shared_ptr<ChunkwisePermutationWriter> writer;
  auto chunkwisePermutationWriterFactory =
      [&writer, &basename, &writerAndCallback1, &writerAndCallback2,
       &permutation, &callbacks, &options, &executor](size_t numChunks) {
        AD_CORRECTNESS_CHECK(writer == nullptr);
        writer = ChunkwisePermutationWriter::create(
            numChunks, basename, std::move(writerAndCallback1),
            std::move(writerAndCallback2), permutation, std::move(callbacks),
            options, std::move(executor));
        return writer;
      };
  auto mergeCompletion =
      sortedInput.template mergeToSink<0>(chunkwisePermutationWriterFactory);
  AD_CORRECTNESS_CHECK(writer != nullptr);
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

#endif  // QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_CHUNKWISEPERMUTATIONWRITER_H
