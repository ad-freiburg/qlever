// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_PARALLELBLOCKMERGETESTHELPERS_H
#define QLEVER_TEST_UTIL_PARALLELBLOCKMERGETESTHELPERS_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <range/v3/range/conversion.hpp>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Random.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

// The output policy below (`CollectingBlockSink`) exists only for the parallel
// merge, which is not available in the C++17 backports mode, see
// `util/parallelBlockMerge/ParallelMergeState.h`. These are the includes that
// only it needs.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <atomic>
#include <boost/asio/thread_pool.hpp>
#include <exception>
#include <future>
#include <mutex>
#include <type_traits>

#include "backports/asio.h"
#include "util/AsioHelpers.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/BlockSinkPolicy.h"
#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// Helpers for the tests of the parallel block merge, in particular the
// in-memory `VectorInput` input policy and the in-memory `CollectingBlockSink`
// output policy. They are needed by `ParallelBlockMergeTest.cpp` (which tests
// the merge itself), by `RunsInputPolicyTest.cpp` (which tests the input policy
// from `RunsInputPolicy.h`, `VectorInput` included), and by
// `MergeHelpersTest.cpp` (which tests the helpers from `MergeHelpers.h`).
namespace parallelBlockMergeTestHelpers {

// ___________________________________________________________________________
// An in-memory input policy.
// ___________________________________________________________________________

// Expose a set of runs, each of which is a `std::vector` of blocks, as an
// `ad_utility::parallelBlockMerge::InputConcept`. Every run (that is, the
// concatenation of its blocks) has to be sorted, and no block may be empty.
//
// NOTE: This lives in the test directory on purpose. In production the merge is
// only ever used on external (that is, on-disk) data, so an in-memory input is
// only ever needed by the tests.
//
// NOTE: `getBlock` returns a *copy* of the block, which is of course not
// efficient, but perfectly fine for the tests. The copy is also what makes the
// class correct for a merge with `moveElements == true`. The very same block
// may be read by two different chunks (namely by the two chunks whose boundary
// lies inside that block), so a chunk that moves the elements out of a block
// must not be able to affect the other one.
template <typename T>
class VectorInput {
 public:
  using value_type = T;
  using Element = T;
  using Block = std::vector<T>;

 private:
  std::vector<std::vector<Block>> runs_;

 public:
  // Construct from the blocks of every run.
  explicit VectorInput(std::vector<std::vector<Block>> runs)
      : runs_{std::move(runs)} {
    for (const auto& run : runs_) {
      AD_CONTRACT_CHECK(ql::ranges::none_of(
          run, [](const Block& block) { return block.empty(); }));
    }
  }

  // ________________________________________________________________________
  size_t numRuns() const { return runs_.size(); }

  // ________________________________________________________________________
  size_t numBlocks(size_t runIdx) const { return runs_.at(runIdx).size(); }

  // ________________________________________________________________________
  size_t numElementsInBlock(size_t runIdx, size_t blockIdx) const {
    return block(runIdx, blockIdx).size();
  }

  // ________________________________________________________________________
  const Element& firstElement(size_t runIdx, size_t blockIdx) const {
    return block(runIdx, blockIdx).front();
  }

  // ________________________________________________________________________
  const Element& lastElement(size_t runIdx, size_t blockIdx) const {
    return block(runIdx, blockIdx).back();
  }

  // Return a copy of the block, see the note at the top of this class.
  Block getBlock(size_t runIdx, size_t blockIdx) const {
    return block(runIdx, blockIdx);
  }

  // ________________________________________________________________________
  Block makeEmptyBlock() const { return Block{}; }

  // ________________________________________________________________________
  template <typename U>
  void appendToBlock(Block& block, U&& element) const {
    block.push_back(AD_FWD(element));
  }

  // ________________________________________________________________________
  ad_utility::MemorySize memorySizeOfElement(
      [[maybe_unused]] const value_type& element) const {
    return ad_utility::MemorySize::bytes(sizeof(value_type));
  }

 private:
  // Return the block with the given index of the run with the given index.
  const Block& block(size_t runIdx, size_t blockIdx) const {
    return runs_.at(runIdx).at(blockIdx);
  }
};

// Split each of the `runs` (each of which has to be sorted) into blocks of
// `blockSize` elements, where the last block of a run may be smaller, and
// return the corresponding `VectorInput`. This is the convenient way to obtain
// a `VectorInput` from flat vectors.
template <typename T>
VectorInput<T> makeVectorInput(const std::vector<std::vector<T>>& runs,
                               size_t blockSize) {
  AD_CONTRACT_CHECK(blockSize > 0);
  std::vector<std::vector<std::vector<T>>> blockedRuns;
  blockedRuns.reserve(runs.size());
  for (const auto& run : runs) {
    auto& blocks = blockedRuns.emplace_back();
    for (size_t begin = 0; begin < run.size(); begin += blockSize) {
      size_t end = std::min(begin + blockSize, run.size());
      blocks.emplace_back(run.begin() + begin, run.begin() + end);
    }
  }
  return VectorInput<T>{std::move(blockedRuns)};
}

// Pin down that `VectorInput` models the input policy concept that it
// documents.
static_assert(
    ad_utility::parallelBlockMerge::InputConcept<VectorInput<size_t>>);

// ___________________________________________________________________________
// An in-memory output policy. NOTE: Only the parallel merge pushes to a sink,
// so everything until the end of this section is C++20 only, see the note at
// the includes above.
// ___________________________________________________________________________

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace net = boost::asio;

// Return the executor of a process-wide thread pool with
// `defaultMergeParallelism()` threads, created lazily on the first call and
// shared by all callers. It is meant for a test that must not join the pool of
// its merge, because it waits for that merge via the sink instead (see
// `CollectingBlockSink::waitUntilAllChunksAreFinished`).
//
// NOTE: This is deliberately a test helper and not part of the merge itself.
// The merge requires an executor that its caller owns and runs, so that the
// caller stays in control of the threads and of their shutdown; a process-wide
// pool would take that control away. The threads of the pool here are idle
// (parked in a condition variable inside Boost.Asio, so they cost no CPU) for
// as long as there is no work, and the pool is stopped and joined by the
// destructor of the function-local static at the end of the process.
inline ql::any_io_executor sharedTestExecutor() {
  static net::thread_pool pool{
      ad_utility::parallelBlockMerge::defaultMergeParallelism()};
  return pool.get_executor();
}

// Collect the output blocks of a merge in memory, one `std::vector` of blocks
// per chunk, as an `ad_utility::parallelBlockMerge::SinkConcept`.
//
// NOTE: This lives in the test directory on purpose, just like `VectorInput`
// above. A production sink hands the blocks on to a consumer (and drops them
// afterwards) instead of keeping all of them.
//
// This sink is deliberately as simple as a sink can be: it buffers *every*
// block, so that a producer is never suspended and the back-pressure of the
// merge is never exercised, it guards its state with a plain mutex instead of a
// strand, and it makes no attempt to hand the blocks on in the global order.
// What it does give the tests is the complete record of what the merge pushed:
// the blocks of every chunk in the order in which they were pushed, how often
// each chunk was finished, and the first exception. Concatenating the blocks in
// the order of their chunk index (see `mergedElements` below) therefore yields
// exactly the globally sorted output.
template <typename Block>
class CollectingBlockSink : public ad_utility::NoCopyNoMove {
 public:
  // Everything that a single chunk pushed.
  struct Chunk {
    std::vector<Block> blocks_{};
    // The number of end-of-chunk sentinels that this chunk sent. The merge
    // sends exactly one per chunk that it dispatches at all, so a test can
    // detect both a missing and a superfluous sentinel.
    size_t numSentinels_ = 0;
  };

 private:
  // The executor on which the state of this sink is modified, and to which the
  // completion handlers are posted if they have no executor of their own.
  ql::any_io_executor executor_;
  // Stop the merge as soon as that many blocks were pushed in total, which is
  // how a test simulates a consumer that abandons the merge. The value `0`
  // means "never stop".
  size_t stopAfterNumBlocks_;
  mutable std::mutex mutex_;
  // The following members are all guarded by `mutex_`.
  std::vector<Chunk> chunks_;
  size_t numPushedBlocks_ = 0;
  size_t numFinishedChunks_ = 0;
  std::exception_ptr exception_;
  std::promise<void> allChunksFinished_;
  std::future<void> allChunksFinishedFuture_ = allChunksFinished_.get_future();
  // NOTE: This is only ever *written* under the `mutex_`, so that
  // `stopRequested()` can be read from anywhere without locking, just like in a
  // production sink.
  std::atomic<bool> stopRequested_{false};

 public:
  // Construct a sink for a merge with `numChunks` chunks, all operations of
  // which run on the `executor`. Pass a positive `stopAfterNumBlocks` to make
  // the sink stop the merge as soon as that many blocks were pushed.
  CollectingBlockSink(ql::any_io_executor executor, size_t numChunks,
                      size_t stopAfterNumBlocks = 0)
      : executor_{std::move(executor)},
        stopAfterNumBlocks_{stopAfterNumBlocks},
        chunks_(numChunks) {
    AD_CONTRACT_CHECK(numChunks > 0);
  }

  // ________________________________________________________________________
  bool stopRequested() const noexcept { return stopRequested_.load(); }

  // ________________________________________________________________________
  template <typename CompletionToken>
  auto asyncPush(size_t chunkIndex, Block block,
                 CompletionToken&& completionToken) {
    return runOnExecutor(
        [this, chunkIndex, block = std::move(block)]() mutable {
          return pushBlock(chunkIndex, std::move(block));
        },
        AD_FWD(completionToken));
  }

  // ________________________________________________________________________
  template <typename CompletionToken>
  auto asyncFinishChunk(size_t chunkIndex, CompletionToken&& completionToken) {
    return runOnExecutor([this, chunkIndex] { return finishChunk(chunkIndex); },
                         AD_FWD(completionToken));
  }

  // ________________________________________________________________________
  template <typename CompletionToken>
  auto asyncPushException(std::exception_ptr exception,
                          CompletionToken&& completionToken) {
    return runOnExecutor(
        [this, exception = std::move(exception)]() mutable {
          std::lock_guard<std::mutex> lock{mutex_};
          // Only the first exception is kept, and it also stops the merge.
          if (exception_ == nullptr) {
            exception_ = std::move(exception);
            stopRequested_.store(true);
          }
        },
        AD_FWD(completionToken));
  }

  // ________________________________________________________________________
  template <typename CompletionToken>
  auto asyncStop(CompletionToken&& completionToken) {
    return runOnExecutor(
        [this] {
          std::lock_guard<std::mutex> lock{mutex_};
          stopRequested_.store(true);
        },
        AD_FWD(completionToken));
  }

  // What every chunk pushed. IMPORTANT: Only call this once the merge is
  // complete, that is once every coroutine of the merge is done (either
  // because every chunk was finished, or because the thread pool of the merge
  // was joined).
  const std::vector<Chunk>& chunks() const { return chunks_; }

  // Rethrow the first exception that a chunk pushed, if there is one. The same
  // IMPORTANT note as at `chunks()` applies.
  void rethrowIfException() const {
    if (exception_ != nullptr) {
      std::rethrow_exception(exception_);
    }
  }

  // Block until every chunk has sent its end-of-chunk sentinel.
  //
  // IMPORTANT: Only call this for a merge that was not stopped. A merge that is
  // stopped (by `asyncStop`, by an exception, or by `stopAfterNumBlocks`) does
  // not dispatch its remaining chunks at all, so those chunks never send a
  // sentinel and this would wait forever. Join the thread pool of such a merge
  // instead.
  void waitUntilAllChunksAreFinished() const {
    allChunksFinishedFuture_.wait();
  }

 private:
  // Run the `function` on `executor_` and complete the token afterwards, see
  // `ad_utility::runFunctionOnExecutor`. This is what makes every completion
  // handler of this sink run via a `net::post` and never inline, which the
  // `SinkConcept` requires.
  template <typename Function, typename CompletionToken>
  auto runOnExecutor(Function function, CompletionToken&& completionToken) {
    return ad_utility::runFunctionOnExecutor(executor_, std::move(function),
                                             AD_FWD(completionToken));
  }

  // Store the `block` as the next block of the chunk with the given
  // `chunkIndex` and return whether the merge should keep going.
  bool pushBlock(size_t chunkIndex, Block block) {
    std::lock_guard<std::mutex> lock{mutex_};
    if (stopRequested_.load()) {
      return false;
    }
    chunks_.at(chunkIndex).blocks_.push_back(std::move(block));
    ++numPushedBlocks_;
    if (stopAfterNumBlocks_ != 0 && numPushedBlocks_ >= stopAfterNumBlocks_) {
      stopRequested_.store(true);
      return false;
    }
    return true;
  }

  // Record the end-of-chunk sentinel of the chunk with the given `chunkIndex`
  // and return whether the merge was not stopped.
  bool finishChunk(size_t chunkIndex) {
    std::lock_guard<std::mutex> lock{mutex_};
    Chunk& chunk = chunks_.at(chunkIndex);
    ++chunk.numSentinels_;
    if (chunk.numSentinels_ == 1) {
      ++numFinishedChunks_;
      if (numFinishedChunks_ == chunks_.size()) {
        allChunksFinished_.set_value();
      }
    }
    return !stopRequested_.load();
  }
};

// Pin down that `CollectingBlockSink` models the output policy concept that it
// documents.
static_assert(ad_utility::parallelBlockMerge::SinkConcept<
              CollectingBlockSink<std::vector<size_t>>, std::vector<size_t>>);

// Return the elements of all blocks that the `sink` collected, in the order of
// the chunks and, within a chunk, in the order in which the blocks were pushed.
// For a merge that ran to completion this is exactly the globally sorted
// output. The same IMPORTANT note as at `CollectingBlockSink::chunks()`
// applies.
template <typename Block>
std::vector<ql::ranges::range_value_t<Block>> mergedElements(
    const CollectingBlockSink<Block>& sink) {
  std::vector<ql::ranges::range_value_t<Block>> result;
  for (const auto& chunk : sink.chunks()) {
    for (const auto& block : chunk.blocks_) {
      result.insert(result.end(), block.begin(), block.end());
    }
  }
  return result;
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// ___________________________________________________________________________
// Inputs and other helpers for the individual tests.
// ___________________________________________________________________________

using SizeVec = std::vector<size_t>;
using SizeInput = VectorInput<size_t>;
using Pair = std::pair<size_t, size_t>;

// A comparator that only looks at the first component of a pair, so that ties
// are visible in the second component.
struct ComparePairs {
  bool operator()(const Pair& a, const Pair& b) const {
    return a.first < b.first;
  }
};

// Return the split points of the `boundaries`, that is the upper bounds of all
// chunks but the last one.
template <typename Element>
std::vector<Element> chunkSplitPoints(
    const std::vector<ad_utility::parallelBlockMerge::ChunkBoundary<Element>>&
        boundaries) {
  std::vector<Element> result;
  for (const auto& boundary : boundaries) {
    if (boundary.hi_.has_value()) {
      result.push_back(boundary.hi_.value());
    }
  }
  return result;
}

// Return `MergeOptions` with the given number of elements per output block.
// That number is deliberately small in the tests, such that even a single chunk
// yields several blocks.
inline ad_utility::parallelBlockMerge::MergeOptions optionsWithBlockSize(
    size_t outputBlockSize = 7) {
  ad_utility::parallelBlockMerge::MergeOptions options;
  options.outputBlockSize =
      ad_utility::parallelBlockMerge::OutputBlockSize::numElements(
          outputBlockSize);
  return options;
}

// Return `numRuns` sorted vectors of random numbers, the sizes of which are
// uniformly distributed in `[minSize, maxSize]`.
inline std::vector<SizeVec> makeRandomRuns(size_t numRuns, size_t minSize,
                                           size_t maxSize) {
  // Two generators are needed, because only the `Slow` one can be restricted to
  // a range: the `valueGenerator` yields the elements (from the whole range of
  // `uint64_t`), and the `sizeGenerator` the size of a single run.
  ad_utility::FastRandomIntGenerator<uint64_t> valueGenerator;
  ad_utility::SlowRandomIntGenerator<size_t> sizeGenerator{minSize, maxSize};
  std::vector<SizeVec> runs;
  for (size_t i = 0; i < numRuns; ++i) {
    SizeVec run(sizeGenerator());
    ql::ranges::generate(run, valueGenerator);
    ql::ranges::sort(run);
    runs.push_back(std::move(run));
  }
  return runs;
}

// Return the concatenation of all `runs`.
template <typename T>
std::vector<T> concatenation(const std::vector<std::vector<T>>& runs) {
  return ::ranges::to_vector(runs | ql::views::join);
}

// Return the sorted concatenation of all `runs`.
inline SizeVec sortedConcatenation(const std::vector<SizeVec>& runs) {
  SizeVec result = concatenation(runs);
  ql::ranges::sort(result);
  return result;
}

// Return four identical runs, each of which consists of ten copies of each of
// the elements `0`, `1`, `2` and `3`. With a block size of ten, every one of
// the four distinct elements is the last element of four different blocks.
inline std::vector<SizeVec> runsWithEqualLastElements() {
  std::vector<SizeVec> runs;
  for (size_t run = 0; run < 4; ++run) {
    SizeVec elements;
    for (size_t value = 0; value < 4; ++value) {
      elements.insert(elements.end(), 10, value);
    }
    runs.push_back(std::move(elements));
  }
  return runs;
}

// Return two runs, all elements of which are equal, so that there is no way to
// actually split the input.
inline std::vector<SizeVec> runsWithEqualElements() {
  return {SizeVec(100, 42u), SizeVec(100, 42u)};
}

// Return one run with the 10000 elements `0 ... 9999` plus 50 runs with two
// elements each, all of which lie at the very beginning of the huge run.
inline std::vector<SizeVec> oneHugeAndManyTinyRuns() {
  std::vector<SizeVec> runs;
  SizeVec huge(10000);
  ql::ranges::generate(huge, [i = size_t{0}]() mutable { return i++; });
  runs.push_back(std::move(huge));
  for (size_t i = 0; i < 50; ++i) {
    runs.push_back(SizeVec{i, i + 1});
  }
  return runs;
}

}  // namespace parallelBlockMergeTestHelpers

#endif  // QLEVER_TEST_UTIL_PARALLELBLOCKMERGETESTHELPERS_H
