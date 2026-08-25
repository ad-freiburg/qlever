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

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <cstddef>
#include <exception>
#include <optional>
#include <utility>
#include <vector>

#include "util/parallelBlockMerge/BlockStorage.h"

using namespace ad_utility::parallelBlockMerge;

namespace {
using Block = std::vector<int>;
using Storage = BlockStorage<Block>;
using OptionalBlock = Storage::OptionalBlock;
using GetResult = Storage::GetResult;
using Strand = Storage::Strand;

// The outcome of a single `storeBlock`, which is recorded instead of being
// asserted right away, so that a test can also check that an operation has
// *not* completed yet.
struct StoreOutcomes {
  std::vector<bool> wasStored_;
};

// The outcome of the `getBlock` operations of a single run.
struct GetOutcomes {
  std::vector<Block> blocks_;
  bool sawSentinel_ = false;
  bool wasCancelled_ = false;
};

// Store the `block` (or the end-of-run sentinel) in the run with the given
// `runIndex` and record whether it was stored in `outcomes`.
void store(Storage& storage, size_t runIndex, OptionalBlock block,
           StoreOutcomes& outcomes) {
  storage.storeBlock(runIndex, std::move(block),
                     [&outcomes](std::exception_ptr exception, bool wasStored) {
                       ASSERT_EQ(exception, nullptr);
                       outcomes.wasStored_.push_back(wasStored);
                     });
}

// Retrieve a single value of the run with the given `runIndex` and record it in
// `outcomes`. If `keepGoing` is true, immediately retrieve the next value as
// well, until the end-of-run sentinel or a cancellation arrives.
void get(Storage& storage, size_t runIndex, GetOutcomes& outcomes,
         bool keepGoing) {
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
        outcomes.blocks_.push_back(std::move(result).value().value());
        if (keepGoing) {
          get(storage, runIndex, outcomes, keepGoing);
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
TEST(InMemoryBlockStorage, storeAndRetrieveInOrder) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  InMemoryBlockStorage<Block> storage{strand, 3};
  StoreOutcomes stores;
  GetOutcomes gets;
  runOnStrand(ioContext, strand, [&] {
    store(storage, 0, OptionalBlock{Block{1, 2}}, stores);
    store(storage, 0, OptionalBlock{Block{3}}, stores);
    store(storage, 0, OptionalBlock{std::nullopt}, stores);
    get(storage, 0, gets, true);
  });
  EXPECT_THAT(stores.wasStored_, ::testing::ElementsAre(true, true, true));
  EXPECT_THAT(gets.blocks_, ::testing::ElementsAre(Block{1, 2}, Block{3}));
  EXPECT_TRUE(gets.sawSentinel_);
  EXPECT_FALSE(gets.wasCancelled_);
}

// _____________________________________________________________________________
TEST(InMemoryBlockStorage, runsAreIndependent) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  InMemoryBlockStorage<Block> storage{strand, 2};
  StoreOutcomes stores;
  GetOutcomes getsOfRunOne;
  GetOutcomes getsOfRunZero;
  runOnStrand(ioContext, strand, [&] {
    store(storage, 1, OptionalBlock{Block{7}}, stores);
    store(storage, 0, OptionalBlock{Block{1}}, stores);
    store(storage, 1, OptionalBlock{std::nullopt}, stores);
    store(storage, 0, OptionalBlock{std::nullopt}, stores);
    // The run with the higher index may be drained first.
    get(storage, 1, getsOfRunOne, true);
    get(storage, 0, getsOfRunZero, true);
  });
  EXPECT_THAT(getsOfRunOne.blocks_, ::testing::ElementsAre(Block{7}));
  EXPECT_THAT(getsOfRunZero.blocks_, ::testing::ElementsAre(Block{1}));
  EXPECT_TRUE(getsOfRunOne.sawSentinel_);
  EXPECT_TRUE(getsOfRunZero.sawSentinel_);
}

// _____________________________________________________________________________
TEST(InMemoryBlockStorage, aConsumerWaitsForItsProducer) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  InMemoryBlockStorage<Block> storage{strand, 2};
  StoreOutcomes stores;
  GetOutcomes gets;
  // Ask for a block of a run that does not exist yet, which has to suspend.
  runOnStrand(ioContext, strand, [&] { get(storage, 0, gets, false); });
  EXPECT_THAT(gets.blocks_, ::testing::IsEmpty());
  runOnStrand(ioContext, strand,
              [&] { store(storage, 0, OptionalBlock{Block{42}}, stores); });
  EXPECT_THAT(stores.wasStored_, ::testing::ElementsAre(true));
  EXPECT_THAT(gets.blocks_, ::testing::ElementsAre(Block{42}));
}

// _____________________________________________________________________________
TEST(InMemoryBlockStorage, backPressure) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  InMemoryBlockStorage<Block> storage{strand, 1};
  StoreOutcomes stores;
  GetOutcomes gets;
  // Only a single block fits, so the second `storeBlock` has to suspend.
  runOnStrand(ioContext, strand, [&] {
    store(storage, 0, OptionalBlock{Block{1}}, stores);
    store(storage, 0, OptionalBlock{Block{2}}, stores);
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
  InMemoryBlockStorage<Block> storage{strand, 2};
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
  InMemoryBlockStorage<Block> storage{strand, 1};
  StoreOutcomes stores;
  runOnStrand(ioContext, strand, [&] {
    store(storage, 0, OptionalBlock{Block{1}}, stores);
    store(storage, 0, OptionalBlock{Block{2}}, stores);
  });
  EXPECT_THAT(stores.wasStored_, ::testing::ElementsAre(true));
  runOnStrand(ioContext, strand, [&] { storage.cancelAll(); });
  // The suspended producer is woken up, and its block was not stored.
  EXPECT_THAT(stores.wasStored_, ::testing::ElementsAre(true, false));
}

// _____________________________________________________________________________
TEST(InMemoryBlockStorage, eraseRunDropsTheBufferedBlocks) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  InMemoryBlockStorage<Block> storage{strand, 1};
  StoreOutcomes stores;
  GetOutcomes gets;
  runOnStrand(ioContext, strand, [&] {
    store(storage, 0, OptionalBlock{Block{1}}, stores);
    storage.eraseRun(0);
    // The buffer of the run is empty again, so this does not suspend, and the
    // block that was buffered before is gone.
    store(storage, 0, OptionalBlock{Block{2}}, stores);
    get(storage, 0, gets, false);
  });
  EXPECT_THAT(stores.wasStored_, ::testing::ElementsAre(true, true));
  EXPECT_THAT(gets.blocks_, ::testing::ElementsAre(Block{2}));
}

// _____________________________________________________________________________
TEST(InMemoryBlockStorage, capacityHasToBePositive) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  EXPECT_ANY_THROW(InMemoryBlockStorage<Block>(strand, 0));
}
