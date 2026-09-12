// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

// The storage is only ever used by the coroutine-based `InOrderBlockSink`, so
// it does not exist in the C++17 backports mode, see
// `util/parallelBlockMerge/BlockStorage.h`.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <cstddef>
#include <exception>
#include <optional>
#include <utility>
#include <vector>

#include "./InMemoryBlockStorage.h"

using namespace ad_utility::parallelBlockMerge;

namespace {
using Block = std::vector<int>;
using Storage = InMemoryBlockStorage<Block>;
static_assert(BlockStorageConcept<Storage, Block>);

// The outcome of a single `storeBlock`, which is recorded instead of being
// asserted right away, so that a test can also check that an operation has
// *not* completed yet.
struct StoreOutcomes {
  std::vector<bool> wasStored_;
};

// The outcome of the `getBlock` operations of a single chunk.
struct GetOutcomes {
  std::vector<Block> blocks_;
  bool sawSentinel_ = false;
  bool wasCancelled_ = false;
};

// Store the `block` (or the end-of-chunk sentinel) in the chunk with the given
// `chunkIndex` and record whether it was stored in `outcomes`.
void store(Storage& storage, size_t chunkIndex, Storage::OptionalBlock block,
           StoreOutcomes& outcomes) {
  storage.storeBlock(chunkIndex, std::move(block),
                     [&outcomes](std::exception_ptr exception, bool wasStored) {
                       ASSERT_EQ(exception, nullptr);
                       outcomes.wasStored_.push_back(wasStored);
                     });
}

// Retrieve a single value of the chunk with the given `chunkIndex` and record
// it in `outcomes`. If `keepGoing` is true, immediately retrieve the next value
// as well, until the end-of-chunk sentinel or a cancellation arrives.
void get(Storage& storage, size_t chunkIndex, GetOutcomes& outcomes,
         bool keepGoing) {
  storage.getBlock(
      chunkIndex, [&storage, chunkIndex, &outcomes, keepGoing](
                      std::exception_ptr exception, Storage::GetResult result) {
        ASSERT_EQ(exception, nullptr);
        if (result.wasCancelled()) {
          outcomes.wasCancelled_ = true;
          return;
        }
        if (result.isEndOfChunk()) {
          outcomes.sawSentinel_ = true;
          return;
        }
        outcomes.blocks_.push_back(std::move(result).get());
        if (keepGoing) {
          get(storage, chunkIndex, outcomes, keepGoing);
        }
      });
}

// Run the `function` on the `strand` of the `ioContext` and then run every
// handler of the `ioContext` that becomes ready because of it. Afterwards, all
// the operations that the `function` initiated have either completed or are
// suspended, which is what makes the tests below deterministic.
//
// NOTE: This deliberately uses `poll` and not `run`, because `run` would never
// return while an operation of the storage is still suspended (a suspended
// operation counts as outstanding work). The `restart` is required because an
// `io_context` stops itself as soon as it runs out of work, after which `poll`
// would do nothing at all.
template <typename Function>
void runOnStrand(net::io_context& ioContext, const Strand& strand,
                 Function function) {
  if (ioContext.stopped()) {
    ioContext.restart();
  }
  net::post(strand, std::move(function));
  ioContext.poll();
}
}  // namespace

// _____________________________________________________________________________
TEST(GetResult, theThreeStates) {
  using Result = GetResult<Block>;
  // A default-constructed result means that the storage was cancelled, so it
  // holds neither a block nor the end-of-chunk sentinel.
  Result cancelled;
  EXPECT_TRUE(cancelled.wasCancelled());
  EXPECT_FALSE(cancelled.isEndOfChunk());
  EXPECT_FALSE(cancelled.hasValue());
  EXPECT_ANY_THROW(std::move(cancelled).get());

  Result endOfChunk = Result::endOfChunk();
  EXPECT_FALSE(endOfChunk.wasCancelled());
  EXPECT_TRUE(endOfChunk.isEndOfChunk());
  EXPECT_FALSE(endOfChunk.hasValue());
  EXPECT_ANY_THROW(std::move(endOfChunk).get());

  Result withBlock = Result::fromBlock(Block{1, 2, 3});
  EXPECT_FALSE(withBlock.wasCancelled());
  EXPECT_FALSE(withBlock.isEndOfChunk());
  EXPECT_TRUE(withBlock.hasValue());
  EXPECT_THAT(std::move(withBlock).get(), ::testing::ElementsAre(1, 2, 3));
}

// _____________________________________________________________________________
TEST(InMemoryBlockStorage, storeAndRetrieveInOrder) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  Storage storage{strand, 3};
  StoreOutcomes stores;
  GetOutcomes gets;
  runOnStrand(ioContext, strand, [&] {
    store(storage, 0, Storage::OptionalBlock{Block{1, 2}}, stores);
    store(storage, 0, Storage::OptionalBlock{Block{3}}, stores);
    store(storage, 0, Storage::OptionalBlock{std::nullopt}, stores);
    get(storage, 0, gets, true);
  });
  EXPECT_THAT(stores.wasStored_, ::testing::ElementsAre(true, true, true));
  EXPECT_THAT(gets.blocks_, ::testing::ElementsAre(Block{1, 2}, Block{3}));
  EXPECT_TRUE(gets.sawSentinel_);
  EXPECT_FALSE(gets.wasCancelled_);
}

// _____________________________________________________________________________
TEST(InMemoryBlockStorage, chunksAreIndependent) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  Storage storage{strand, 2};
  StoreOutcomes stores;
  GetOutcomes getsOfChunkOne;
  GetOutcomes getsOfChunkZero;
  runOnStrand(ioContext, strand, [&] {
    store(storage, 1, Storage::OptionalBlock{Block{7}}, stores);
    store(storage, 0, Storage::OptionalBlock{Block{1}}, stores);
    store(storage, 1, Storage::OptionalBlock{std::nullopt}, stores);
    store(storage, 0, Storage::OptionalBlock{std::nullopt}, stores);
    // The chunk with the higher index may be drained first.
    get(storage, 1, getsOfChunkOne, true);
    get(storage, 0, getsOfChunkZero, true);
  });
  EXPECT_THAT(getsOfChunkOne.blocks_, ::testing::ElementsAre(Block{7}));
  EXPECT_THAT(getsOfChunkZero.blocks_, ::testing::ElementsAre(Block{1}));
  EXPECT_TRUE(getsOfChunkOne.sawSentinel_);
  EXPECT_TRUE(getsOfChunkZero.sawSentinel_);
}

// _____________________________________________________________________________
TEST(InMemoryBlockStorage, aConsumerWaitsForItsProducer) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  Storage storage{strand, 2};
  StoreOutcomes stores;
  GetOutcomes gets;
  // Ask for a block of a chunk that does not exist yet, which has to suspend.
  runOnStrand(ioContext, strand, [&] { get(storage, 0, gets, false); });
  EXPECT_THAT(gets.blocks_, ::testing::IsEmpty());
  runOnStrand(ioContext, strand, [&] {
    store(storage, 0, Storage::OptionalBlock{Block{42}}, stores);
  });
  EXPECT_THAT(stores.wasStored_, ::testing::ElementsAre(true));
  EXPECT_THAT(gets.blocks_, ::testing::ElementsAre(Block{42}));
}

// _____________________________________________________________________________
TEST(InMemoryBlockStorage, backPressure) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  Storage storage{strand, 1};
  StoreOutcomes stores;
  GetOutcomes gets;
  // Only a single block fits, so the second `storeBlock` has to suspend.
  runOnStrand(ioContext, strand, [&] {
    store(storage, 0, Storage::OptionalBlock{Block{1}}, stores);
    store(storage, 0, Storage::OptionalBlock{Block{2}}, stores);
  });
  EXPECT_THAT(stores.wasStored_, ::testing::ElementsAre(true));
  // Retrieving the first block makes room for the second one.
  runOnStrand(ioContext, strand, [&] { get(storage, 0, gets, false); });
  EXPECT_THAT(gets.blocks_, ::testing::ElementsAre(Block{1}));
  EXPECT_THAT(stores.wasStored_, ::testing::ElementsAre(true, true));
  runOnStrand(ioContext, strand, [&] { get(storage, 0, gets, false); });
  EXPECT_THAT(gets.blocks_, ::testing::ElementsAre(Block{1}, Block{2}));
}

// _____________________________________________________________________________
TEST(InMemoryBlockStorage, cancelAllWakesUpAWaitingConsumer) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  Storage storage{strand, 2};
  GetOutcomes gets;
  runOnStrand(ioContext, strand, [&] { get(storage, 0, gets, false); });
  EXPECT_FALSE(gets.wasCancelled_);
  runOnStrand(ioContext, strand, [&] { storage.cancelAll(); });
  EXPECT_TRUE(gets.wasCancelled_);
  EXPECT_THAT(gets.blocks_, ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(InMemoryBlockStorage, cancelAllWakesUpASuspendedProducer) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  Storage storage{strand, 1};
  StoreOutcomes stores;
  runOnStrand(ioContext, strand, [&] {
    store(storage, 0, Storage::OptionalBlock{Block{1}}, stores);
    store(storage, 0, Storage::OptionalBlock{Block{2}}, stores);
  });
  EXPECT_THAT(stores.wasStored_, ::testing::ElementsAre(true));
  runOnStrand(ioContext, strand, [&] { storage.cancelAll(); });
  // The suspended producer is woken up, and its block was not stored.
  EXPECT_THAT(stores.wasStored_, ::testing::ElementsAre(true, false));
}

// _____________________________________________________________________________
TEST(InMemoryBlockStorage, theSentinelDropsTheChunk) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  Storage storage{strand, 2};
  StoreOutcomes stores;
  GetOutcomes gets;
  runOnStrand(ioContext, strand, [&] {
    store(storage, 0, Storage::OptionalBlock{Block{1}}, stores);
    store(storage, 0, Storage::OptionalBlock{std::nullopt}, stores);
    get(storage, 0, gets, false);
  });
  // The block was retrieved, but the end-of-chunk sentinel was not, so the
  // chunk is still alive.
  EXPECT_THAT(gets.blocks_, ::testing::ElementsAre(Block{1}));
  EXPECT_FALSE(gets.sawSentinel_);
  EXPECT_EQ(storage.numLiveChunksForTesting(), 1u);
  // Retrieving the sentinel drops the chunk.
  runOnStrand(ioContext, strand, [&] { get(storage, 0, gets, false); });
  EXPECT_TRUE(gets.sawSentinel_);
  EXPECT_EQ(storage.numLiveChunksForTesting(), 0u);
}

// _____________________________________________________________________________
TEST(InMemoryBlockStorage, capacityHasToBePositive) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  EXPECT_ANY_THROW(Storage(strand, 0));
}
#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
