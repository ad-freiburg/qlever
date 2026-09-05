// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <initializer_list>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../../util/AllocatorTestHelpers.h"
#include "../../util/GTestHelpers.h"
#include "backports/filesystem.h"
#include "engine/idTable/CompressedIdTableBlockStorage.h"

// The tests in this file that drive the storage through an `InOrderBlockSink`
// use coroutines and are therefore not available in the C++17 backports mode.
// The storage itself is coroutine-free and available in that mode, and so are
// the tests that exercise it directly.
#ifndef QLEVER_CPP_17
#include <atomic>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <stdexcept>

#include "../../util/AsyncTestHelpers.h"
#include "util/parallelBlockMerge/InOrderBlockSink.h"
#endif

// NOTE: This is the same alias that `test/util/AsyncTestHelpers.h` declares,
// but that header is only included in the coroutine mode, see above.
namespace net = boost::asio;

namespace {
// The storage under test, and the strand that all of its operations are
// confined to.
template <size_t NumCols>
using Storage = ad_utility::CompressedIdTableBlockStorage<NumCols>;
using Strand = Storage<0>::Strand;

// A row of a block, as plain integers, so that the rows of a whole merge can be
// compared conveniently.
using Row = std::vector<int64_t>;

// The row that is identified by `value` and that has `numColumns` columns. The
// value of a column depends on its index, so that a permutation of the columns
// would be noticed.
Row makeRow(int64_t value, size_t numColumns) {
  Row row;
  for (size_t columnIdx = 0; columnIdx < numColumns; ++columnIdx) {
    row.push_back(value + static_cast<int64_t>(columnIdx) * 1000);
  }
  return row;
}

// The rows that are identified by `values`, with `numColumns` columns each.
std::vector<Row> makeRows(size_t numColumns,
                          const std::vector<int64_t>& values) {
  std::vector<Row> rows;
  for (int64_t value : values) {
    rows.push_back(makeRow(value, numColumns));
  }
  return rows;
}

// Create a block with `numColumns` columns that holds the rows which are
// identified by `values`, see `makeRow`.
template <size_t NumCols>
IdTableStatic<NumCols> makeBlock(size_t numColumns,
                                 const std::vector<int64_t>& values) {
  IdTableStatic<NumCols> block{numColumns,
                               ad_utility::testing::makeAllocator()};
  for (int64_t value : values) {
    block.emplace_back();
    Row row = makeRow(value, numColumns);
    for (size_t columnIdx = 0; columnIdx < numColumns; ++columnIdx) {
      block(block.numRows() - 1, columnIdx) = Id::makeFromInt(row[columnIdx]);
    }
  }
  return block;
}

// The rows of `block`, see `Row`.
template <size_t NumCols>
std::vector<Row> blockRows(const IdTableStatic<NumCols>& block) {
  std::vector<Row> rows;
  for (size_t rowIdx = 0; rowIdx < block.numRows(); ++rowIdx) {
    Row row;
    for (size_t columnIdx = 0; columnIdx < block.numColumns(); ++columnIdx) {
      row.push_back(block(rowIdx, columnIdx).getInt());
    }
    rows.push_back(std::move(row));
  }
  return rows;
}

// The values that a producer stores: one block per element of `blocks`, each of
// which lists the values that identify the rows of that block, followed by the
// end-of-run sentinel if `withSentinel` is true.
template <size_t NumCols>
std::vector<std::optional<IdTableStatic<NumCols>>> makeValues(
    size_t numColumns, const std::vector<std::vector<int64_t>>& blocks,
    bool withSentinel) {
  std::vector<std::optional<IdTableStatic<NumCols>>> values;
  for (const std::vector<int64_t>& block : blocks) {
    values.push_back(makeBlock<NumCols>(numColumns, block));
  }
  if (withSentinel) {
    values.push_back(std::nullopt);
  }
  return values;
}

// The number of rows of the blocks of a chunk. They differ from each other and
// include empty blocks, because an empty block is stored (and spilled) just
// like any other one.
const std::vector<size_t>& blockSizes() {
  static const std::vector<size_t> sizes{2, 0, 3, 1, 0, 4};
  return sizes;
}

// The plan of a whole round trip: the blocks of each chunk, and the rows that
// the consumer has to see in exactly that order.
template <size_t NumCols>
struct MergePlan {
  std::vector<std::vector<IdTableStatic<NumCols>>> chunks_;
  std::vector<Row> expectedRows_;
};

// Create a plan with `numChunks` chunks of `numBlocksPerChunk` blocks each, all
// with `numColumns` columns and with the row counts of `blockSizes`. The values
// that identify the rows are consecutive in the global order, so that a wrong
// order is immediately visible.
template <size_t NumCols>
MergePlan<NumCols> makePlan(size_t numChunks, size_t numBlocksPerChunk,
                            size_t numColumns) {
  MergePlan<NumCols> plan;
  int64_t nextValue = 0;
  for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    std::vector<IdTableStatic<NumCols>> blocks;
    for (size_t blockIndex = 0; blockIndex < numBlocksPerChunk; ++blockIndex) {
      size_t numRows =
          blockSizes()[(chunkIndex + blockIndex) % blockSizes().size()];
      std::vector<int64_t> values;
      for (size_t rowIdx = 0; rowIdx < numRows; ++rowIdx) {
        values.push_back(nextValue);
        plan.expectedRows_.push_back(makeRow(nextValue, numColumns));
        ++nextValue;
      }
      blocks.push_back(makeBlock<NumCols>(numColumns, values));
    }
    plan.chunks_.push_back(std::move(blocks));
  }
  return plan;
}

// Create a storage that spills to the file with the given `filename`, that runs
// its compression and its I/O on `ioContext`, that keeps
// `maxBufferedBlocksPerRun` blocks per run in memory, and that stores the
// spilled blocks with the given `compression`.
template <size_t NumCols>
Storage<NumCols> makeStorage(const Strand& strand, net::io_context& ioContext,
                             std::string filename,
                             size_t maxBufferedBlocksPerRun,
                             ad_utility::CompressedBlockFile::Compression
                                 compression = ad_utility::ZSTD_DEFAULT_LEVEL) {
  return Storage<NumCols>{strand,
                          ioContext.get_executor(),
                          std::move(filename),
                          ad_utility::testing::makeAllocator(),
                          maxBufferedBlocksPerRun,
                          compression};
}

// The compressions that the round trips below are run with: the default ZSTD
// level, and no compression at all. A spilled block has to arrive unchanged
// either way, see `MERGE_PHASE_SPILL_COMPRESSION` for which of the two the
// merge phase uses.
const std::vector<ad_utility::CompressedBlockFile::Compression>&
compressions() {
  static const std::vector<ad_utility::CompressedBlockFile::Compression> result{
      ad_utility::ZSTD_DEFAULT_LEVEL, ad_utility::NO_BLOCK_COMPRESSION};
  return result;
}

// Run every handler that is ready to run in any of the `contexts`, and keep
// doing that until none of them has anything left to run. Afterwards all the
// operations that were initiated before have either completed or are suspended,
// which is what makes the direct tests below deterministic.
//
// NOTE: In contrast to the harness of `test/BlockStorageTest.cpp`, a single
// `poll()` does not suffice here, because a single operation of this storage is
// a whole chain of posted handlers that alternates between the strand and the
// `ioExecutor` (on which the compression and the I/O run), and a test may
// deliberately use a *separate* `io_context` for each of the two. `poll()`
// returns the number of handlers that it ran, so the loop stops as soon as
// everything is quiescent.
//
// NOTE: This deliberately uses `poll` and not `run`, because `run` would never
// return while an operation of the storage is still suspended (a suspended
// operation counts as outstanding work). The `restart` is required because an
// `io_context` stops itself as soon as it runs out of work, after which `poll`
// would do nothing at all.
void pollUntilQuiescent(std::initializer_list<net::io_context*> contexts) {
  bool didRunSomething = true;
  while (didRunSomething) {
    didRunSomething = false;
    for (net::io_context* context : contexts) {
      if (context->stopped()) {
        context->restart();
      }
      didRunSomething = context->poll() > 0 || didRunSomething;
    }
  }
}

// Run the `function` on the `strand` and then run everything that becomes ready
// because of it, see `pollUntilQuiescent`.
template <typename Function>
void runOnStrand(std::initializer_list<net::io_context*> contexts,
                 const Strand& strand, Function function) {
  net::post(strand, std::move(function));
  pollUntilQuiescent(contexts);
}

// The outcome of the `storeBlock` operations of a single producer, which is
// recorded instead of being asserted right away, so that a test can also check
// that an operation has *not* completed yet.
struct StoreOutcomes {
  std::vector<bool> wasStored_;
};

// The outcome of the `getBlock` operations of a single run.
struct GetOutcomes {
  std::vector<std::vector<Row>> blocks_;
  bool sawSentinel_ = false;
  bool wasCancelled_ = false;
};

// Retrieve a single value of the run with the given `runIndex` and record it in
// `outcomes`. If `keepGoing` is true, immediately retrieve the next value as
// well, until the end-of-run sentinel or a cancellation arrives.
template <size_t NumCols>
void get(Storage<NumCols>& storage, size_t runIndex, GetOutcomes& outcomes,
         bool keepGoing) {
  using GetResult = typename Storage<NumCols>::GetResult;
  storage.getBlock(
      runIndex, [&storage, runIndex, &outcomes, keepGoing](
                    std::exception_ptr exception, GetResult result) {
        ASSERT_EQ(exception, nullptr);
        if (!result.has_value()) {
          outcomes.wasCancelled_ = true;
          return;
        }
        if (!result.value().has_value()) {
          outcomes.sawSentinel_ = true;
          return;
        }
        outcomes.blocks_.push_back(blockRows<NumCols>(result.value().value()));
        if (keepGoing) {
          get(storage, runIndex, outcomes, keepGoing);
        }
      });
}

// A producer of a single run, which stores its values one after the other in a
// handler chain, exactly like the producer of a chunk of the parallel merge
// does. At most one `storeBlock` of a run is therefore ever in flight, as the
// PRECONDITIONS of `BlockStorage` require.
//
// NOTE: An object of this class must neither be copied nor moved, because its
// handlers capture `this`.
template <size_t NumCols>
class Producer {
 public:
  using OptionalBlock = typename Storage<NumCols>::OptionalBlock;
  StoreOutcomes outcomes_;

 private:
  Storage<NumCols>& storage_;
  size_t runIndex_;
  std::vector<OptionalBlock> values_;
  size_t nextIndex_ = 0;

 public:
  // Construct from the `storage`, the index of the run to produce, and the
  // values (blocks and possibly the end-of-run sentinel) to store, see
  // `makeValues`.
  Producer(Storage<NumCols>& storage, size_t runIndex,
           std::vector<OptionalBlock> values)
      : storage_{storage}, runIndex_{runIndex}, values_{std::move(values)} {}

  // Store the next value, and then the one after that, and so on. Stop as soon
  // as a value was not stored, which is what a real producer does as well.
  void storeAll() {
    if (nextIndex_ >= values_.size()) {
      return;
    }
    OptionalBlock value = std::move(values_[nextIndex_]);
    ++nextIndex_;
    storage_.storeBlock(runIndex_, std::move(value),
                        [this](std::exception_ptr exception, bool wasStored) {
                          ASSERT_EQ(exception, nullptr);
                          outcomes_.wasStored_.push_back(wasStored);
                          if (wasStored) {
                            storeAll();
                          }
                        });
  }
};
}  // namespace

// _____________________________________________________________________________
TEST(CompressedIdTableBlockStorage, directRoundTripWithoutAnyBuffering) {
  for (size_t i = 0; i < compressions().size(); ++i) {
    net::io_context ioContext;
    Strand strand = net::make_strand(ioContext.get_executor());
    // Buffer nothing, such that every single block is spilled.
    Storage<0> storage = makeStorage<0>(
        strand, ioContext, gtestCurrentTestName() + "." + std::to_string(i), 0,
        compressions().at(i));
    Producer<0> producer{storage, 0, makeValues<0>(2, {{0, 1}, {}, {2}}, true)};
    GetOutcomes gets;
    runOnStrand({&ioContext}, strand, [&] {
      producer.storeAll();
      get(storage, 0, gets, true);
    });
    EXPECT_THAT(producer.outcomes_.wasStored_,
                ::testing::ElementsAre(true, true, true, true));
    EXPECT_THAT(gets.blocks_,
                ::testing::ElementsAre(makeRows(2, {0, 1}), makeRows(2, {}),
                                       makeRows(2, {2})));
    EXPECT_TRUE(gets.sawSentinel_);
    EXPECT_FALSE(gets.wasCancelled_);
  }
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlockStorage, directRoundTripWithAStaticNumberOfColumns) {
  net::io_context ioContext;
  Strand strand = net::make_strand(ioContext.get_executor());
  Storage<3> storage =
      makeStorage<3>(strand, ioContext, gtestCurrentTestName(), 1);
  Producer<3> producer{storage, 0, makeValues<3>(3, {{0}, {1, 2}, {}}, true)};
  GetOutcomes gets;
  runOnStrand({&ioContext}, strand, [&] {
    producer.storeAll();
    get(storage, 0, gets, true);
  });
  EXPECT_THAT(producer.outcomes_.wasStored_,
              ::testing::ElementsAre(true, true, true, true));
  EXPECT_THAT(gets.blocks_,
              ::testing::ElementsAre(makeRows(3, {0}), makeRows(3, {1, 2}),
                                     makeRows(3, {})));
  EXPECT_TRUE(gets.sawSentinel_);
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlockStorage, directRunsAreIndependent) {
  net::io_context ioContext;
  Strand strand = net::make_strand(ioContext.get_executor());
  Storage<0> storage =
      makeStorage<0>(strand, ioContext, gtestCurrentTestName(), 1);
  Producer<0> firstProducer{storage, 0, makeValues<0>(1, {{0}, {1}}, true)};
  Producer<0> secondProducer{storage, 1, makeValues<0>(1, {{10}, {11}}, true)};
  GetOutcomes getsOfRunOne;
  GetOutcomes getsOfRunZero;
  runOnStrand({&ioContext}, strand, [&] {
    secondProducer.storeAll();
    firstProducer.storeAll();
    // The run with the higher index may be drained first.
    get(storage, 1, getsOfRunOne, true);
    get(storage, 0, getsOfRunZero, true);
  });
  EXPECT_THAT(getsOfRunOne.blocks_,
              ::testing::ElementsAre(makeRows(1, {10}), makeRows(1, {11})));
  EXPECT_THAT(getsOfRunZero.blocks_,
              ::testing::ElementsAre(makeRows(1, {0}), makeRows(1, {1})));
  EXPECT_TRUE(getsOfRunOne.sawSentinel_);
  EXPECT_TRUE(getsOfRunZero.sawSentinel_);
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlockStorage, aConsumerWaitsForItsProducer) {
  net::io_context ioContext;
  Strand strand = net::make_strand(ioContext.get_executor());
  Storage<0> storage =
      makeStorage<0>(strand, ioContext, gtestCurrentTestName(), 0);
  GetOutcomes gets;
  // Ask for a block of a run that does not exist yet, which has to suspend.
  runOnStrand({&ioContext}, strand, [&] { get(storage, 0, gets, false); });
  EXPECT_THAT(gets.blocks_, ::testing::IsEmpty());
  Producer<0> producer{storage, 0, makeValues<0>(1, {{42}}, false)};
  // The waiting consumer is served as soon as the block has been spilled, which
  // is the case once the `ioExecutor` and the strand are quiescent again.
  runOnStrand({&ioContext}, strand, [&] { producer.storeAll(); });
  EXPECT_THAT(producer.outcomes_.wasStored_, ::testing::ElementsAre(true));
  EXPECT_THAT(gets.blocks_, ::testing::ElementsAre(makeRows(1, {42})));
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlockStorage, laterBlocksMayLandInMemory) {
  // This is the interesting invariant of this storage: `numBlocksInMemory_`
  // drops again as soon as a block is consumed, so a *later* block may well
  // land in memory while an *earlier*, spilled one is still queued ahead of it.
  // The FIFO order has to survive that.
  net::io_context ioContext;
  Strand strand = net::make_strand(ioContext.get_executor());
  Storage<0> storage =
      makeStorage<0>(strand, ioContext, gtestCurrentTestName(), 1);
  // The first block stays in memory, the second one is spilled, because the
  // single in-memory slot of the run is taken.
  Producer<0> firstProducer{storage, 0, makeValues<0>(1, {{0}, {1}}, false)};
  runOnStrand({&ioContext}, strand, [&] { firstProducer.storeAll(); });
  EXPECT_THAT(firstProducer.outcomes_.wasStored_,
              ::testing::ElementsAre(true, true));
  auto sizeAfterTheSpill = ql::filesystem::file_size(storage.spillFilename(0));
  EXPECT_GT(sizeAfterTheSpill, 0u);
  // Consuming the first block frees the single in-memory slot again.
  GetOutcomes gets;
  runOnStrand({&ioContext}, strand, [&] { get(storage, 0, gets, false); });
  EXPECT_THAT(gets.blocks_, ::testing::ElementsAre(makeRows(1, {0})));
  // So the third block lands in memory, which is why the file does not grow
  // anymore, even though the spilled second block is still queued ahead of it.
  Producer<0> secondProducer{storage, 0, makeValues<0>(1, {{2}}, true)};
  runOnStrand({&ioContext}, strand, [&] { secondProducer.storeAll(); });
  EXPECT_THAT(secondProducer.outcomes_.wasStored_,
              ::testing::ElementsAre(true, true));
  EXPECT_EQ(ql::filesystem::file_size(storage.spillFilename(0)),
            sizeAfterTheSpill);
  // The order across that boundary is the one in which the blocks were stored.
  runOnStrand({&ioContext}, strand, [&] { get(storage, 0, gets, true); });
  EXPECT_THAT(gets.blocks_,
              ::testing::ElementsAre(makeRows(1, {0}), makeRows(1, {1}),
                                     makeRows(1, {2})));
  EXPECT_TRUE(gets.sawSentinel_);
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlockStorage, theProducerIsNotBlockedByALaggingConsumer) {
  // The whole point of this storage: a producer runs ahead no matter how far
  // the consumer lags behind, in contrast to the `InMemoryBlockStorage`, whose
  // `backPressure` test in `test/BlockStorageTest.cpp` asserts the opposite.
  constexpr size_t numBlocks = 10;
  net::io_context ioContext;
  Strand strand = net::make_strand(ioContext.get_executor());
  Storage<0> storage =
      makeStorage<0>(strand, ioContext, gtestCurrentTestName(), 2);
  std::vector<std::vector<int64_t>> blocks;
  std::vector<int64_t> allValues;
  for (size_t blockIndex = 0; blockIndex < numBlocks; ++blockIndex) {
    blocks.push_back({static_cast<int64_t>(blockIndex)});
    allValues.push_back(static_cast<int64_t>(blockIndex));
  }
  Producer<0> producer{storage, 0, makeValues<0>(2, blocks, true)};
  // Push everything before the consumer reads a single block. All the pushes
  // have to complete, otherwise the producer would be stuck.
  runOnStrand({&ioContext}, strand, [&] { producer.storeAll(); });
  EXPECT_THAT(
      producer.outcomes_.wasStored_,
      ::testing::ElementsAreArray(std::vector<bool>(numBlocks + 1, true)));
  // Only two of the blocks fit in memory, so the rest really was spilled.
  EXPECT_GT(ql::filesystem::file_size(storage.spillFilename(0)), 0u);
  GetOutcomes gets;
  runOnStrand({&ioContext}, strand, [&] { get(storage, 0, gets, true); });
  std::vector<std::vector<Row>> expected;
  for (int64_t value : allValues) {
    expected.push_back(makeRows(2, {value}));
  }
  EXPECT_THAT(gets.blocks_, ::testing::ElementsAreArray(expected));
  EXPECT_TRUE(gets.sawSentinel_);
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlockStorage, eraseRunDropsTheQueue) {
  net::io_context ioContext;
  Strand strand = net::make_strand(ioContext.get_executor());
  Storage<0> storage =
      makeStorage<0>(strand, ioContext, gtestCurrentTestName(), 0);
  Producer<0> firstProducer{storage, 0, makeValues<0>(1, {{0}, {1}}, false)};
  runOnStrand({&ioContext}, strand, [&] { firstProducer.storeAll(); });
  EXPECT_THAT(firstProducer.outcomes_.wasStored_,
              ::testing::ElementsAre(true, true));
  Producer<0> secondProducer{storage, 0, makeValues<0>(1, {{2}}, true)};
  GetOutcomes gets;
  runOnStrand({&ioContext}, strand, [&] {
    storage.eraseRun(0);
    // Erasing a run that does not exist is a no-op.
    storage.eraseRun(17);
    // The queue of the run starts from scratch, so the two blocks that were
    // stored before are gone.
    secondProducer.storeAll();
    get(storage, 0, gets, true);
  });
  EXPECT_THAT(secondProducer.outcomes_.wasStored_,
              ::testing::ElementsAre(true, true));
  EXPECT_THAT(gets.blocks_, ::testing::ElementsAre(makeRows(1, {2})));
  EXPECT_TRUE(gets.sawSentinel_);
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlockStorage, aSpillThatOutlivesItsRunIsDropped) {
  // The two `io_context`s are deliberately kept apart, such that this test
  // controls exactly when the spill of a block makes progress: `strandContext`
  // runs the strand of the storage, whereas `ioExecutorContext` plays the role
  // of the `ioExecutor` on which the compression and the I/O run.
  net::io_context strandContext;
  net::io_context ioExecutorContext;
  Strand strand = net::make_strand(strandContext.get_executor());
  Storage<0> storage =
      makeStorage<0>(strand, ioExecutorContext, gtestCurrentTestName(), 0);
  Producer<0> producer{storage, 0, makeValues<0>(1, {{0}}, false)};
  // Initiate the spill, but do not let the `ioExecutor` run, so that the block
  // is still on its way to the file.
  runOnStrand({&strandContext}, strand, [&] { producer.storeAll(); });
  EXPECT_THAT(producer.outcomes_.wasStored_, ::testing::IsEmpty());
  // Erase the run while the block is being written, so that nobody is left who
  // could care about that block once the write completes.
  runOnStrand({&strandContext}, strand, [&] { storage.eraseRun(0); });
  pollUntilQuiescent({&strandContext, &ioExecutorContext});
  EXPECT_THAT(producer.outcomes_.wasStored_, ::testing::ElementsAre(false));
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlockStorage, cancelAllWakesUpAWaitingConsumer) {
  net::io_context ioContext;
  Strand strand = net::make_strand(ioContext.get_executor());
  Storage<0> storage =
      makeStorage<0>(strand, ioContext, gtestCurrentTestName(), 1);
  GetOutcomes gets;
  runOnStrand({&ioContext}, strand, [&] { get(storage, 0, gets, false); });
  EXPECT_FALSE(gets.wasCancelled_);
  runOnStrand({&ioContext}, strand, [&] { storage.cancelAll(); });
  EXPECT_TRUE(gets.wasCancelled_);
  EXPECT_THAT(gets.blocks_, ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlockStorage, cancelAllDoesNotAbortAnInFlightSpill) {
  // A producer of this storage never waits for the consumer, so the only thing
  // that `cancelAll` could interrupt is a spill that is currently in flight. It
  // deliberately does not do that, see the NOTE at `cancelAll`.
  net::io_context strandContext;
  net::io_context ioExecutorContext;
  Strand strand = net::make_strand(strandContext.get_executor());
  Storage<0> storage =
      makeStorage<0>(strand, ioExecutorContext, gtestCurrentTestName(), 0);
  Producer<0> producer{storage, 0, makeValues<0>(1, {{0}}, false)};
  runOnStrand({&strandContext}, strand, [&] { producer.storeAll(); });
  EXPECT_THAT(producer.outcomes_.wasStored_, ::testing::IsEmpty());
  runOnStrand({&strandContext}, strand, [&] { storage.cancelAll(); });
  pollUntilQuiescent({&strandContext, &ioExecutorContext});
  // NOTE: The storage reports that the block *was* stored, although it was
  // cancelled while that store was in flight, which deviates from the wording
  // of `BlockStorage::storeBlock`. That is benign, because the
  // `InOrderBlockSink` reports `false` to its producer whenever the merge was
  // stopped, no matter what the storage says. The point of this test is that
  // the producer is completed at all and therefore never hangs.
  EXPECT_THAT(producer.outcomes_.wasStored_, ::testing::ElementsAre(true));
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlockStorage, theSpillFileIsCreatedAndDeleted) {
  std::string prefix = gtestCurrentTestName();
  net::io_context ioContext;
  Strand strand = net::make_strand(ioContext.get_executor());
  {
    // NOTE: No `absl::Cleanup` that deletes the files is required here, because
    // deleting them is exactly the behavior of the storage that this test
    // checks.
    Storage<0> storage = makeStorage<0>(strand, ioContext, prefix, 0);
    EXPECT_EQ(storage.filenamePrefix(), prefix);
    std::string fileOfRunZero = storage.spillFilename(0);
    EXPECT_THAT(fileOfRunZero, ::testing::StartsWith(prefix));
    // The file is created with the first block that is spilled and not before.
    EXPECT_FALSE(ql::filesystem::exists(fileOfRunZero));
    Producer<0> producer{storage, 0, makeValues<0>(1, {{0}, {1}}, true)};
    runOnStrand({&ioContext}, strand, [&] { producer.storeAll(); });
    EXPECT_GT(ql::filesystem::file_size(fileOfRunZero), 0u);
    // A run that never spills never creates a file at all.
    Producer<0> emptyProducer{storage, 1, makeValues<0>(1, {}, true)};
    runOnStrand({&ioContext}, strand, [&] { emptyProducer.storeAll(); });
    EXPECT_FALSE(ql::filesystem::exists(storage.spillFilename(1)));
  }
  EXPECT_FALSE(ql::filesystem::exists(prefix + ".0"));
}

// _____________________________________________________________________________
// Every run spills to a file of its own, and `eraseRun` deletes it. The disk
// space of this storage is therefore proportional to the runs that are in
// flight and not to their total number, exactly like its memory.
TEST(CompressedIdTableBlockStorage, eraseRunReclaimsTheSpillFile) {
  std::string prefix = gtestCurrentTestName();
  net::io_context ioContext;
  Strand strand = net::make_strand(ioContext.get_executor());
  // Buffer nothing, such that every block of every run is spilled.
  Storage<0> storage = makeStorage<0>(strand, ioContext, prefix, 0);
  absl::Cleanup cleanup = [&storage] {
    for (size_t runIndex = 0; runIndex < 3; ++runIndex) {
      ad_utility::deleteFile(storage.spillFilename(runIndex), false);
    }
  };
  // Fill three runs, each of which then has a file of its own.
  for (size_t runIndex = 0; runIndex < 3; ++runIndex) {
    Producer<0> producer{storage, runIndex, makeValues<0>(1, {{0}, {1}}, true)};
    runOnStrand({&ioContext}, strand, [&] { producer.storeAll(); });
    EXPECT_GT(ql::filesystem::file_size(storage.spillFilename(runIndex)), 0u);
  }
  // Erasing a run deletes its file, and only its file.
  runOnStrand({&ioContext}, strand, [&] { storage.eraseRun(1); });
  EXPECT_TRUE(ql::filesystem::exists(storage.spillFilename(0)));
  EXPECT_FALSE(ql::filesystem::exists(storage.spillFilename(1)));
  EXPECT_TRUE(ql::filesystem::exists(storage.spillFilename(2)));
  runOnStrand({&ioContext}, strand, [&] {
    storage.eraseRun(0);
    storage.eraseRun(2);
  });
  EXPECT_FALSE(ql::filesystem::exists(storage.spillFilename(0)));
  EXPECT_FALSE(ql::filesystem::exists(storage.spillFilename(2)));
}

// _____________________________________________________________________________
// A storage that was created with `NO_BLOCK_COMPRESSION` writes the spilled
// blocks exactly as they are, so its file holds precisely the bytes of the
// `Id`s of those blocks. That is also the price of not compressing, see
// `MERGE_PHASE_SPILL_COMPRESSION`.
TEST(CompressedIdTableBlockStorage, theUncompressedSpillFileHasTheExactSize) {
  std::string prefix = gtestCurrentTestName();
  net::io_context ioContext;
  Strand strand = net::make_strand(ioContext.get_executor());
  static constexpr size_t numColumns = 2;
  // Buffer nothing, such that every single block is spilled.
  Storage<0> storage = makeStorage<0>(strand, ioContext, prefix, 0,
                                      ad_utility::NO_BLOCK_COMPRESSION);
  std::string filename = storage.spillFilename(0);
  Producer<0> producer{storage, 0,
                       makeValues<0>(numColumns, {{0, 1, 2}, {}, {3}}, true)};
  GetOutcomes gets;
  runOnStrand({&ioContext}, strand, [&] {
    producer.storeAll();
    get(storage, 0, gets, true);
  });
  EXPECT_THAT(gets.blocks_,
              ::testing::ElementsAre(makeRows(numColumns, {0, 1, 2}),
                                     makeRows(numColumns, {}),
                                     makeRows(numColumns, {3})));
  EXPECT_TRUE(gets.sawSentinel_);
  // Four rows of two columns were spilled, and the empty block added nothing.
  EXPECT_EQ(ql::filesystem::file_size(filename), 4 * numColumns * sizeof(Id));
}

#ifndef QLEVER_CPP_17
namespace {
// The sink that wraps the storage, which is its only production user.
template <size_t NumCols>
using Sink =
    ad_utility::parallelBlockMerge::InOrderBlockSink<IdTableStatic<NumCols>>;

// A counted latch via which a producer coroutine signals that it is done, see
// `test/InOrderBlockSinkTest.cpp`.
using Latch =
    net::experimental::concurrent_channel<void(boost::system::error_code)>;

// Create a sink whose storage spills to the file with the given `filename`,
// keeps `maxBufferedBlocksPerChunk` blocks per chunk in memory, and stores the
// spilled blocks with the given `compression`.
template <size_t NumCols>
Sink<NumCols> makeSink(net::io_context& ioContext, size_t numChunks,
                       std::string filename, size_t maxBufferedBlocksPerChunk,
                       ad_utility::CompressedBlockFile::Compression
                           compression = ad_utility::ZSTD_DEFAULT_LEVEL) {
  return Sink<NumCols>{ioContext.get_executor(), numChunks,
                       Storage<NumCols>::makeStorageFactory(
                           ioContext.get_executor(), std::move(filename),
                           ad_utility::testing::makeAllocator(),
                           maxBufferedBlocksPerChunk, compression)};
}

// Push all `blocks` to the `sink` as the chunk with the given `chunkIndex`,
// then finish that chunk and open the `latch`. Stop early if the merge was
// stopped, and count the blocks that were actually pushed in `numPushed`.
template <size_t NumCols>
net::awaitable<void> pushBlocks(Sink<NumCols>& sink, size_t chunkIndex,
                                std::vector<IdTableStatic<NumCols>> blocks,
                                Latch& latch,
                                std::atomic<size_t>* numPushed = nullptr) {
  for (IdTableStatic<NumCols>& block : blocks) {
    if (!co_await sink.asyncPush(chunkIndex, std::move(block),
                                 net::use_awaitable)) {
      break;
    }
    if (numPushed != nullptr) {
      ++(*numPushed);
    }
  }
  co_await sink.asyncFinishChunk(chunkIndex, net::use_awaitable);
  latch.try_send(boost::system::error_code{});
}

// Consume all the blocks of the `sink` and return their rows, flattened, which
// is exactly the global order that the sink promises.
template <size_t NumCols>
net::awaitable<std::vector<Row>> collectRows(Sink<NumCols>& sink) {
  std::vector<Row> rows;
  while (auto block = co_await sink.asyncGetNextBlock(net::use_awaitable)) {
    for (Row& row : blockRows<NumCols>(block.value())) {
      rows.push_back(std::move(row));
    }
  }
  co_return rows;
}

// Wait until the `latch` was opened `numTimes` times, so that the producers are
// guaranteed to not touch the sink anymore.
net::awaitable<void> waitForLatch(Latch& latch, size_t numTimes = 1) {
  for (size_t i = 0; i < numTimes; ++i) {
    co_await latch.async_receive(net::as_tuple(net::use_awaitable));
  }
}

// Abort the `sink`, but only after yielding a few times, such that the consumer
// that was started before this coroutine gets a chance to suspend inside the
// storage. Open the `latch` afterwards.
net::awaitable<void> abortAfterYielding(Sink<0>& sink,
                                        net::io_context& ioContext,
                                        Latch& latch) {
  for (size_t i = 0; i < 3; ++i) {
    co_await net::post(ioContext, net::use_awaitable);
  }
  co_await sink.asyncAbort(net::use_awaitable);
  latch.try_send(boost::system::error_code{});
}

// Run a full round trip of `numChunks` chunks with `numBlocksPerChunk` blocks
// each through a sink whose storage buffers `maxBufferedBlocksPerChunk` blocks
// per chunk and spills the rest with `compression`, and check that the consumer
// sees exactly the rows of the plan, in exactly that order.
template <size_t NumCols>
net::awaitable<void> checkRoundTrip(
    net::io_context& ioContext, size_t numColumns, size_t numChunks,
    size_t numBlocksPerChunk, size_t maxBufferedBlocksPerChunk,
    std::string filename,
    ad_utility::CompressedBlockFile::Compression compression =
        ad_utility::ZSTD_DEFAULT_LEVEL) {
  MergePlan<NumCols> plan =
      makePlan<NumCols>(numChunks, numBlocksPerChunk, numColumns);
  Sink<NumCols> sink =
      makeSink<NumCols>(ioContext, numChunks, std::move(filename),
                        maxBufferedBlocksPerChunk, compression);
  Latch latch{ioContext.get_executor(), numChunks};
  // Spawn the producers in reverse order, such that the blocks of the later
  // chunks tend to be produced first.
  for (size_t chunkIndex = numChunks; chunkIndex > 0; --chunkIndex) {
    net::co_spawn(
        ioContext,
        pushBlocks<NumCols>(sink, chunkIndex - 1,
                            std::move(plan.chunks_[chunkIndex - 1]), latch),
        net::detached);
  }
  std::vector<Row> rows = co_await collectRows<NumCols>(sink);
  EXPECT_THAT(rows, ::testing::ElementsAreArray(plan.expectedRows_))
      << "maxBufferedBlocksPerChunk = " << maxBufferedBlocksPerChunk
      << ", compression = "
      << (compression.has_value() ? std::to_string(compression.value())
                                  : std::string{"none"});
  co_await waitForLatch(latch, numChunks);
}

// The values of `maxBufferedBlocksPerChunk` that the round-trip tests below
// cover: `0` spills every single block, `1` and `2` spill only some of them,
// and a value that exceeds the number of blocks of a chunk spills nothing at
// all.
const std::vector<size_t>& bufferSizes() {
  static const std::vector<size_t> sizes{0, 1, 2, 100};
  return sizes;
}
}  // namespace

// _____________________________________________________________________________
ASYNC_TEST(CompressedIdTableBlockStorage, roundTripWithDynamicNumberOfColumns) {
  for (size_t i = 0; i < compressions().size(); ++i) {
    for (size_t maxBufferedBlocksPerChunk : bufferSizes()) {
      co_await checkRoundTrip<0>(ioContext, 2, 3, 6, maxBufferedBlocksPerChunk,
                                 gtestCurrentTestName() + "." +
                                     std::to_string(i) + "." +
                                     std::to_string(maxBufferedBlocksPerChunk),
                                 compressions().at(i));
    }
  }
}

// _____________________________________________________________________________
ASYNC_TEST(CompressedIdTableBlockStorage, roundTripWithStaticNumberOfColumns) {
  for (size_t i = 0; i < compressions().size(); ++i) {
    for (size_t maxBufferedBlocksPerChunk : bufferSizes()) {
      co_await checkRoundTrip<3>(ioContext, 3, 3, 6, maxBufferedBlocksPerChunk,
                                 gtestCurrentTestName() + "." +
                                     std::to_string(i) + "." +
                                     std::to_string(maxBufferedBlocksPerChunk),
                                 compressions().at(i));
    }
  }
}

// _____________________________________________________________________________
ASYNC_TEST(CompressedIdTableBlockStorage, chunksWithoutAnyBlock) {
  co_await checkRoundTrip<0>(ioContext, 1, 4, 0, 1, gtestCurrentTestName());
}

// _____________________________________________________________________________
ASYNC_TEST(CompressedIdTableBlockStorage, theProducerRunsAheadOfTheConsumer) {
  // The same invariant as `theProducerIsNotBlockedByALaggingConsumer` above,
  // but through the sink: the producer pushes far more blocks than the storage
  // buffers, and it does so *before* the consumer reads anything at all, so
  // this test would hang if the storage applied any back-pressure.
  constexpr size_t numBlocks = 20;
  MergePlan<0> plan = makePlan<0>(1, numBlocks, 2);
  Sink<0> sink = makeSink<0>(ioContext, 1, gtestCurrentTestName(), 2);
  Latch latch{ioContext.get_executor(), 1};
  std::atomic<size_t> numPushed{0};
  net::co_spawn(
      ioContext,
      pushBlocks<0>(sink, 0, std::move(plan.chunks_[0]), latch, &numPushed),
      net::detached);
  // Wait until the producer is completely done, without having consumed a
  // single block.
  co_await waitForLatch(latch);
  EXPECT_EQ(numPushed.load(), numBlocks);
  std::vector<Row> rows = co_await collectRows<0>(sink);
  EXPECT_THAT(rows, ::testing::ElementsAreArray(plan.expectedRows_));
}

// _____________________________________________________________________________
ASYNC_TEST(CompressedIdTableBlockStorage, pushExceptionSurfaces) {
  // Buffer nothing, such that the blocks that the producer pushed before the
  // exception are all sitting in the file.
  MergePlan<0> plan = makePlan<0>(1, 3, 2);
  Sink<0> sink = makeSink<0>(ioContext, 2, gtestCurrentTestName(), 0);
  Latch latch{ioContext.get_executor(), 1};
  net::co_spawn(ioContext,
                pushBlocks<0>(sink, 0, std::move(plan.chunks_[0]), latch),
                net::detached);
  co_await waitForLatch(latch);
  co_await sink.asyncPushException(
      std::make_exception_ptr(std::runtime_error{"kaboom"}),
      net::use_awaitable);
  EXPECT_TRUE(sink.stopRequested());
  bool didThrow = false;
  try {
    co_await sink.asyncGetNextBlock(net::use_awaitable);
  } catch (const std::runtime_error& exception) {
    didThrow = true;
    EXPECT_STREQ(exception.what(), "kaboom");
  }
  EXPECT_TRUE(didThrow);
}

// _____________________________________________________________________________
ASYNC_TEST(CompressedIdTableBlockStorage, abortWhileTheConsumerWaits) {
  // Abort while the consumer waits for a block that no producer will ever push.
  // The consumer has to report the end of the range instead of hanging.
  //
  // NOTE: Whether the consumer really is suspended inside the storage when the
  // abort arrives, or whether the abort wins the race and the consumer never
  // touches the storage at all, is not fully deterministic here; both paths
  // have to end up with an empty result. The deterministic version of the first
  // path is `cancelAllWakesUpAWaitingConsumer` above.
  Sink<0> sink = makeSink<0>(ioContext, 2, gtestCurrentTestName(), 1);
  Latch latch{ioContext.get_executor(), 1};
  net::co_spawn(ioContext, abortAfterYielding(sink, ioContext, latch),
                net::detached);
  std::optional<IdTableStatic<0>> block =
      co_await sink.asyncGetNextBlock(net::use_awaitable);
  EXPECT_FALSE(block.has_value());
  EXPECT_TRUE(sink.stopRequested());
  co_await waitForLatch(latch);
}

// _____________________________________________________________________________
ASYNC_TEST_N(CompressedIdTableBlockStorage, abortWhileProducersRun, 4) {
  // Abort in the middle of everything: many producers on several threads, all
  // of which spill most of their blocks, and a consumer that has only just
  // started. Every single producer has to arrive at its `finishChunk`, no
  // matter whether it is currently compressing a block, writing one, or about
  // to initiate the next push.
  constexpr size_t numChunks = 8;
  constexpr size_t numBlocksPerChunk = 10;
  MergePlan<0> plan = makePlan<0>(numChunks, numBlocksPerChunk, 2);
  Sink<0> sink = makeSink<0>(ioContext, numChunks, gtestCurrentTestName(), 1);
  Latch latch{ioContext.get_executor(), numChunks};
  for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    net::co_spawn(ioContext,
                  pushBlocks<0>(sink, chunkIndex,
                                std::move(plan.chunks_[chunkIndex]), latch),
                  net::detached);
  }
  // Consume a little, such that the producers really are in flight, and then
  // abort in the middle of everything.
  for (size_t i = 0; i < 3; ++i) {
    co_await sink.asyncGetNextBlock(net::use_awaitable);
  }
  co_await sink.asyncAbort(net::use_awaitable);
  EXPECT_TRUE(sink.stopRequested());
  // This hangs if a single producer was left suspended.
  co_await waitForLatch(latch, numChunks);
  // The producers are told to stop: a push that is initiated now is dropped.
  EXPECT_FALSE(
      co_await sink.asyncPush(0, makeBlock<0>(2, {0}), net::use_awaitable));
  std::optional<IdTableStatic<0>> block =
      co_await sink.asyncGetNextBlock(net::use_awaitable);
  EXPECT_FALSE(block.has_value());
}

// _____________________________________________________________________________
ASYNC_TEST_N(CompressedIdTableBlockStorage, multiThreaded, 4) {
  // Many chunks and many blocks on four threads, so that the producers, the
  // consumer, and the compression and the I/O of the storage really run
  // concurrently. Only a single block per chunk stays in memory, so most of the
  // blocks make the whole round trip through the file.
  co_await checkRoundTrip<0>(ioContext, 3, 8, 12, 1, gtestCurrentTestName());
}

// _____________________________________________________________________________
ASYNC_TEST_N(CompressedIdTableBlockStorage, multiThreadedWithoutBuffering, 4) {
  // The same, but with a static number of columns and with every single block
  // spilled.
  co_await checkRoundTrip<3>(ioContext, 3, 8, 12, 0, gtestCurrentTestName());
}
#endif  // QLEVER_CPP_17
