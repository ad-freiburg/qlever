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

#include <chrono>

#include "util/GlobalExecutor.h"

namespace chunkwisePermutationWriter {

// _____________________________________________________________________________
ChunkwisePermutationWriter::ChunkwisePermutationWriter(
    PrivateTag, const std::string& basename,
    WriterAndCallback writerAndCallback1, WriterAndCallback writerAndCallback2,
    const qlever::KeyOrder& permutation,
    std::vector<ConcurrentBlockCallback> callbacks, const Options& options)
    : writer1_{std::move(writerAndCallback1.writer_)},
      writer2_{std::move(writerAndCallback2.writer_)},
      executor_{ad_utility::globalExecutor()},
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
                          }} {
  // This logic only works for permutations that have the graph as the fourth
  // column.
  AD_CORRECTNESS_CHECK(permutation.keys().at(3) == 3);
  AD_CORRECTNESS_CHECK(writer1_->blocksize() == writer2_->blocksize());
  AD_CORRECTNESS_CHECK(writer1_->numColumns() == writer2_->numColumns());
  AD_CORRECTNESS_CHECK(writer1_->blocksize() > 0);
}

// _____________________________________________________________________________
std::shared_ptr<ChunkwisePermutationWriter> ChunkwisePermutationWriter::create(
    const std::string& basename, WriterAndCallback writerAndCallback1,
    WriterAndCallback writerAndCallback2, const qlever::KeyOrder& permutation,
    std::vector<ConcurrentBlockCallback> callbacks, const Options& options) {
  return std::make_shared<ChunkwisePermutationWriter>(
      PrivateTag{}, basename, std::move(writerAndCallback1),
      std::move(writerAndCallback2), permutation, std::move(callbacks),
      options);
}

// _____________________________________________________________________________
std::shared_ptr<ChunkwisePermutationWriter>
ChunkwisePermutationWriter::makeSink(size_t numChunks) {
  AD_CONTRACT_CHECK(numChunks_ == 0 && numChunks > 0,
                    "The sink of a chunkwise permutation writer can only be "
                    "created once");
  numChunks_ = numChunks;
  chunkWriters_.resize(numChunks);
  chunkResults_.setNumSlots(numChunks);
  return shared_from_this();
}

// _____________________________________________________________________________
ChunkWriter& ChunkwisePermutationWriter::chunkWriter(size_t chunkIndex) {
  // NOTE: The vector is never resized after `makeSink`, and the slot of a
  // chunk is only ever touched by the (single) operation of that chunk which
  // is currently in flight, so no lock is needed here.
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
  chunkResults_.deliver(chunkIndex, std::move(result));
  co_return !stopRequested();
}

// _____________________________________________________________________________
void ChunkwisePermutationWriter::consolidate() {
  for (size_t chunkIndex = 0; chunkIndex < numChunks_; ++chunkIndex) {
    statistics_.consolidationWaitTimer_.cont();
    // Wake up regularly to report the progress of the chunks.
    ChunkResult chunk =
        chunkResults_.take(chunkIndex, std::chrono::seconds(1),
                           [this]() { statistics_.reportProgress(); });
    statistics_.consolidationWaitTimer_.stop();
    statistics_.consolidationWorkTimer_.cont();
    consolidator_.addChunkResult(chunkIndex, std::move(chunk));
    statistics_.consolidationWorkTimer_.stop();
  }
}

// _____________________________________________________________________________
PermutationPairResult ChunkwisePermutationWriter::finish(
    std::future<void> mergeCompletion) {
  AD_CONTRACT_CHECK(numChunks_ > 0,
                    "The sink has to be created before `finish` is called");
  try {
    consolidate();
  } catch (...) {
    // The chunks that are still running are stopped, and we have to wait for
    // them (and for the callbacks) before the exception is propagated, because
    // they refer to the writers and to the callbacks.
    requestStop();
    mergeCompletion.wait();
    callbackDispatcher_.waitUntilIdle();
    throw;
  }
  mergeCompletion.get();
  callbackDispatcher_.waitUntilIdle();
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
