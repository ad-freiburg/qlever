// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/chunkwisePermutationWriter/ChunkwisePermutationWriter.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <absl/cleanup/cleanup.h>

#include <chrono>

namespace chunkwisePermutationWriter {

namespace {
// Check that the `executor` is not empty and return it. This is called in the
// member initializer list of the writer, so that the check happens before the
// members that use the executor are constructed.
net::any_io_executor checkNotEmpty(net::any_io_executor executor) {
  AD_CONTRACT_CHECK(static_cast<bool>(executor),
                    "The executor of a chunkwise permutation writer must not "
                    "be empty");
  return executor;
}
}  // namespace

// _____________________________________________________________________________
ChunkwisePermutationWriter::ChunkwisePermutationWriter(
    PrivateTag, size_t numChunks, const std::string& basename,
    WriterAndCallback writerAndCallback1, WriterAndCallback writerAndCallback2,
    const qlever::KeyOrder& permutation,
    std::vector<ConcurrentBlockCallback> callbacks, const Options& options,
    net::any_io_executor executor)
    : writer1_{std::move(writerAndCallback1.writer_)},
      writer2_{std::move(writerAndCallback2.writer_)},
      executor_{checkNotEmpty(std::move(executor))},
      context_{*writer1_,
               *writer2_,
               permutation.keys().at(0),
               compressedRelationHelpers::permutedColIndices(
                   permutation, writer1_->numColumns()),
               options.removeDuplicates_,
               basename,
               options.twinSorterMemoryPerChunk_},
      consolidator_{context_, std::move(writerAndCallback1.callback_),
                    std::move(writerAndCallback2.callback_), executor_,
                    options.boundaryTwinMergeMemory_},
      statistics_{context_},
      callbackDispatcher_{executor_, std::move(callbacks),
                          options.numCallbackBlocksInFlight_,
                          [this](std::exception_ptr exception) {
                            storeException(std::move(exception));
                          }},
      numChunks_{numChunks},
      chunkWriters_(numChunks) {
  AD_CONTRACT_CHECK(numChunks > 0,
                    "A chunkwise permutation writer needs at least one chunk");
  chunkResults_.setNumSlots(numChunks);
  // This logic only works for permutations that have the graph as the fourth
  // column.
  AD_CORRECTNESS_CHECK(permutation.keys().at(3) == 3);
  AD_CORRECTNESS_CHECK(writer1_->blocksize() == writer2_->blocksize());
  AD_CORRECTNESS_CHECK(writer1_->numColumns() == writer2_->numColumns());
  AD_CORRECTNESS_CHECK(writer1_->blocksize() > 0);
}

// _____________________________________________________________________________
std::shared_ptr<ChunkwisePermutationWriter> ChunkwisePermutationWriter::create(
    size_t numChunks, const std::string& basename,
    WriterAndCallback writerAndCallback1, WriterAndCallback writerAndCallback2,
    const qlever::KeyOrder& permutation,
    std::vector<ConcurrentBlockCallback> callbacks, const Options& options,
    net::any_io_executor executor) {
  return std::make_shared<ChunkwisePermutationWriter>(
      PrivateTag{}, numChunks, basename, std::move(writerAndCallback1),
      std::move(writerAndCallback2), permutation, std::move(callbacks), options,
      std::move(executor));
}

// _____________________________________________________________________________
ChunkWriter& ChunkwisePermutationWriter::chunkWriter(size_t chunkIndex) {
  // NOTE: The vector is never resized, and the slot of a chunk is only ever
  // touched by the (single) operation of that chunk which is currently in
  // flight, so no lock is needed here.
  auto& writer = chunkWriters_.at(chunkIndex);
  if (writer == nullptr) {
    writer = std::make_unique<ChunkWriter>(context_, chunkIndex, numChunks_);
  }
  return *writer;
}

// _____________________________________________________________________________
void ChunkwisePermutationWriter::storeException(std::exception_ptr exception) {
  if (chunkResults_.storeException(std::move(exception))) {
    callbackDispatcher_.cancel();
  }
}

// _____________________________________________________________________________
void ChunkwisePermutationWriter::requestStop() {
  // Wake up the chunks that wait for the callbacks (they see the stop
  // afterwards) and the driving thread.
  callbackDispatcher_.cancel();
  chunkResults_.requestStop();
}

// _____________________________________________________________________________
net::awaitable<bool> ChunkwisePermutationWriter::pushImpl(size_t chunkIndex,
                                                          Block block) {
  if (stopRequested()) {
    co_return false;
  }
  SharedBlock sharedBlock;
  try {
    auto timer = statistics_.chunkTimer_.startMeasurement();
    sharedBlock = chunkWriter(chunkIndex).processBlock(std::move(block));
  } catch (...) {
    storeException(std::current_exception());
    co_return false;
  }
  if (sharedBlock != nullptr) {
    // NOTE: The aliasing `shared_ptr` keeps this writer (and hence the
    // dispatcher and the `storeException` that it reports to) alive until the
    // last callback for the block is done.
    std::shared_ptr<ConcurrentCallbackDispatcher> dispatcher{
        shared_from_this(), &callbackDispatcher_};
    if (!co_await ConcurrentCallbackDispatcher::asyncDispatch(
            std::move(dispatcher), std::move(sharedBlock))) {
      co_return false;
    }
  }
  co_return !stopRequested();
}

// _____________________________________________________________________________
net::awaitable<bool> ChunkwisePermutationWriter::finishChunkImpl(
    size_t chunkIndex) {
  std::optional<ChunkResult> result;
  if (!stopRequested()) {
    try {
      auto timer = statistics_.chunkTimer_.startMeasurement();
      result = chunkWriter(chunkIndex).finishChunk();
      // The chunk is done, so its writer (and the temporary file of its twin
      // sorter) can go.
      chunkWriters_.at(chunkIndex).reset();
    } catch (...) {
      storeException(std::current_exception());
    }
  }
  // NOTE: A chunk without a result doesn't deliver anything. In that case a
  // stop has been requested (by `storeException` above, or before), which
  // also wakes up the consumer in `consolidate`.
  if (result.has_value()) {
    chunkResults_.deliver(chunkIndex, std::move(result).value());
  }
  co_return !stopRequested();
}

// _____________________________________________________________________________
void ChunkwisePermutationWriter::consolidate() {
  // Wait for the result of the next chunk, and measure the time spent waiting.
  auto takeNextChunk = [this]() {
    statistics_.consolidationWaitTimer_.cont();
    absl::Cleanup stopTimer = [this]() {
      statistics_.consolidationWaitTimer_.stop();
    };
    // Wake up regularly to report the progress of the chunks.
    return chunkResults_.takeNext(std::chrono::seconds(1),
                                  [this]() { statistics_.reportProgress(); });
  };
  size_t chunkIndex = 0;
  while (std::optional<ChunkResult> chunk = takeNextChunk()) {
    statistics_.consolidationWorkTimer_.cont();
    consolidator_.addChunkResult(chunkIndex, std::move(chunk).value());
    statistics_.consolidationWorkTimer_.stop();
    ++chunkIndex;
  }
  AD_CORRECTNESS_CHECK(chunkIndex == numChunks_);
}

// _____________________________________________________________________________
PermutationPairResult ChunkwisePermutationWriter::finish(
    std::future<void> mergeCompletion) {
  AD_CONTRACT_CHECK(
      mergeCompletion.valid(),
      "`finish` needs the future of the merge, see `mergeToSink`");
  {
    // Whatever happens, wait for the chunks that are still running and for the
    // callbacks before leaving this scope, because they refer to the writers
    // and to the callbacks. If `consolidate` throws, this waiting happens
    // during the unwinding, before the exception propagates. This is safe,
    // because neither `std::future::wait` nor `waitUntilIdle` throws.
    absl::Cleanup waitForChunksAndCallbacks = [this, &mergeCompletion]() {
      mergeCompletion.wait();
      callbackDispatcher_.waitUntilIdle();
    };
    try {
      consolidate();
    } catch (...) {
      // Stop the chunks that are still running, so that the waiting above
      // terminates quickly.
      requestStop();
      throw;
    }
  }
  // Propagate an exception of the merge (the future is still valid after
  // `wait`).
  mergeCompletion.get();
  chunkResults_.rethrowIfException();
  // NOTE: All the chunks and callbacks are done now, so an exception of the
  // consolidator may propagate directly.
  statistics_.consolidationWorkTimer_.cont();
  auto result = consolidator_.finish();
  statistics_.consolidationWorkTimer_.stop();

  AD_CORRECTNESS_CHECK(consolidator_.numRows() ==
                       context_.numRowsProcessed_.load());
  statistics_.logSummary();
  return result;
}

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
