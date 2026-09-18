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

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>

#include "../../util/GTestHelpers.h"
#include "engine/idTable/ExternalIdTableSorterMergeConfig.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <absl/cleanup/cleanup.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <vector>

#include "../../util/AllocatorTestHelpers.h"
#include "backports/filesystem.h"
#endif

using namespace ad_utility::compressedExternalIdTable;
using namespace ad_utility::memory_literals;

namespace {

// The base configuration of all the tests below. The numbers are chosen such
// that the memory arithmetic can be verified by hand: a row has
// `2 * sizeof(Id) = 16` bytes, and the input blocks of a single chunk occupy
// `numRuns_ * numColumns_ * inputBlockSize_ = 200` bytes.
MergePhaseConfig baseConfig() {
  MergePhaseConfig config;
  config.numRuns_ = 1;
  config.numColumns_ = 2;
  config.memoryLimit_ = ad_utility::MemorySize::bytes(20'000'400);
  config.inputBlockSize_ = ad_utility::MemorySize::bytes(100);
  config.numBufferedOutputBlocks_ = 4;
  config.maxOutputBlockSize_ = 1_GB;
  config.parallelism_ = 2;
  return config;
}
}  // namespace

// _____________________________________________________________________________
// With plenty of memory, all the chunks that the parallelism offers are in
// flight, and the output blocks get whatever is left: two chunks occupy 400
// bytes of input blocks, which leaves `20'000'000` bytes for the
// `4 + 3 * 2 = 10` output blocks, hence `2'000'000 / 16 = 125'000` rows each.
TEST(ExternalIdTableSorterMergeConfig, fullParallelism) {
  auto parameters = computeMergePhaseParameters(baseConfig());
  EXPECT_EQ(parameters.numChunksInFlight_, 2u);
  EXPECT_EQ(parameters.outputBlockSize_, 125'000u);
}

// _____________________________________________________________________________
// The number of concurrent chunks is reduced rather than letting the output
// blocks fall below `MIN_MERGE_PHASE_OUTPUT_BLOCK_SIZE`: with a limit of 16 MB,
// two chunks would leave only `1'599'960 / 16 = 99'997` rows per block, which
// is just below that minimum, so a single chunk is used, which then gets the
// whole limit minus its 200 bytes of input blocks, divided by `4 + 3 = 7`.
TEST(ExternalIdTableSorterMergeConfig, parallelismIsReducedForLargeBlocks) {
  auto config = baseConfig();
  config.memoryLimit_ = ad_utility::MemorySize::bytes(16'000'000);
  auto parameters = computeMergePhaseParameters(config);
  EXPECT_EQ(parameters.numChunksInFlight_, 1u);
  EXPECT_EQ(parameters.outputBlockSize_, 142'855u);
  EXPECT_GT(parameters.outputBlockSize_, MIN_MERGE_PHASE_OUTPUT_BLOCK_SIZE);
}

// _____________________________________________________________________________
// A parallelism of one is the serial merge, which is a single chunk that gets
// all the memory.
TEST(ExternalIdTableSorterMergeConfig, serialMerge) {
  auto config = baseConfig();
  config.parallelism_ = 1;
  auto parameters = computeMergePhaseParameters(config);
  EXPECT_EQ(parameters.numChunksInFlight_, 1u);
  EXPECT_EQ(parameters.outputBlockSize_, 178'573u);
}

// _____________________________________________________________________________
// A single output block never exceeds `maxOutputBlockSize_`, which caps the
// `125'000` rows of `fullParallelism` at `1'600'000 / 16 = 100'000`.
TEST(ExternalIdTableSorterMergeConfig, outputBlockSizeIsCapped) {
  auto config = baseConfig();
  config.maxOutputBlockSize_ = ad_utility::MemorySize::bytes(1'600'000);
  auto parameters = computeMergePhaseParameters(config);
  EXPECT_EQ(parameters.numChunksInFlight_, 2u);
  EXPECT_EQ(parameters.outputBlockSize_, 100'000u);
}

// _____________________________________________________________________________
// If not even a single chunk leaves room for a usable output block, then the
// memory limit is reported as insufficient. This holds both if the input blocks
// alone already exceed the limit, and if what is left over is too small.
TEST(ExternalIdTableSorterMergeConfig, insufficientMemory) {
  auto config = baseConfig();
  config.memoryLimit_ = ad_utility::MemorySize::bytes(500'000);
  AD_EXPECT_THROW_WITH_MESSAGE(
      computeMergePhaseParameters(config),
      ::testing::HasSubstr("Insufficient memory for merging 1 blocks"));

  config.memoryLimit_ = ad_utility::MemorySize::bytes(150);
  AD_EXPECT_THROW_WITH_MESSAGE(
      computeMergePhaseParameters(config),
      ::testing::HasSubstr("Insufficient memory for merging"));
}

// _____________________________________________________________________________
// A caller that pins the size of the output blocks gets exactly that size, and
// only the number of concurrent chunks is derived from the memory limit.
TEST(ExternalIdTableSorterMergeConfig, pinnedOutputBlockSize) {
  auto config = baseConfig();
  config.outputBlockSizeOverride_ = 1'000;
  auto parameters = computeMergePhaseParameters(config);
  EXPECT_EQ(parameters.outputBlockSize_, 1'000u);
  EXPECT_EQ(parameters.numChunksInFlight_, 2u);

  // A pinned block size is never overridden, not even by a memory limit that
  // does not suffice for a single chunk. The merge then falls back to a single
  // chunk (which is exactly the serial merge) instead of throwing.
  config.memoryLimit_ = ad_utility::MemorySize::bytes(150);
  parameters = computeMergePhaseParameters(config);
  EXPECT_EQ(parameters.outputBlockSize_, 1'000u);
  EXPECT_EQ(parameters.numChunksInFlight_, 1u);
}

// _____________________________________________________________________________
// The tiny memory limits of the unit tests would always collapse the merge to a
// single chunk, so those tests disable the memory limit entirely, which yields
// small output blocks and full parallelism.
TEST(ExternalIdTableSorterMergeConfig, ignoredMemoryLimit) {
  auto config = baseConfig();
  config.memoryLimit_ = 1_B;
  config.parallelism_ = 7;
  config.ignoreMemoryLimit_ = true;
  auto parameters = computeMergePhaseParameters(config);
  EXPECT_EQ(parameters.outputBlockSize_, 5u);
  EXPECT_EQ(parameters.numChunksInFlight_, 7u);

  // A pinned block size still wins.
  config.outputBlockSizeOverride_ = 17;
  parameters = computeMergePhaseParameters(config);
  EXPECT_EQ(parameters.outputBlockSize_, 17u);
  EXPECT_EQ(parameters.numChunksInFlight_, 7u);
}

// _____________________________________________________________________________
// A merge phase without any parallelism at all would never make progress.
TEST(ExternalIdTableSorterMergeConfig, parallelismMustBePositive) {
  auto config = baseConfig();
  config.parallelism_ = 0;
  EXPECT_ANY_THROW(computeMergePhaseParameters(config));
}

// _____________________________________________________________________________
// The number of rows is the only criterion for finishing an output block, and
// the two parallelism knobs are passed on unchanged.
TEST(ExternalIdTableSorterMergeConfig, mergeOptions) {
  auto config = baseConfig();
  auto parameters = computeMergePhaseParameters(config);
  auto options = makeMergeOptions(config, parameters);
  EXPECT_EQ(options.outputBlockSize.maxNumElements(),
            parameters.outputBlockSize_);
  EXPECT_EQ(options.outputBlockSize.maxMemory(), ad_utility::MemorySize::max());
  EXPECT_EQ(options.parallelismHint, config.parallelism_);
  EXPECT_EQ(options.parallelism(), config.parallelism_);
  EXPECT_EQ(options.maxNumChunksInFlight, parameters.numChunksInFlight_);
  EXPECT_EQ(options.numChunksInFlight(100), parameters.numChunksInFlight_);
  EXPECT_EQ(options.firstChunkSizes, mergePhaseFirstChunkSizes());
  // All the buffered output blocks but two are read ahead, see
  // `MergePhaseConfig::numBufferedOutputBlocks_`.
  EXPECT_EQ(options.numPrefetchedOutputBlocks,
            config.numBufferedOutputBlocks_ - 2);
}

// _____________________________________________________________________________
// The read-ahead is never zero, no matter how small the number of buffered
// output blocks is, because a merge without any read-ahead at all would never
// make progress, see `MergeOptions::numPrefetchedOutputBlocks`.
TEST(ExternalIdTableSorterMergeConfig, mergeOptionsWithFewBufferedBlocks) {
  auto config = baseConfig();
  for (size_t numBufferedOutputBlocks : {size_t{1}, size_t{2}, size_t{3}}) {
    config.numBufferedOutputBlocks_ = numBufferedOutputBlocks;
    auto options =
        makeMergeOptions(config, computeMergePhaseParameters(config));
    EXPECT_EQ(options.numPrefetchedOutputBlocks, 1u);
  }
}

// _____________________________________________________________________________
// The leading chunks of the merge phase start at 1M elements and double from
// there, so that the first sorted rows become available early.
TEST(ExternalIdTableSorterMergeConfig, firstChunkSizes) {
  EXPECT_THAT(mergePhaseFirstChunkSizes(),
              ::testing::ElementsAre(1'000'000u, 2'000'000u, 4'000'000u,
                                     8'000'000u, 16'000'000u));
}

// _____________________________________________________________________________
// The spill files of a merge phase are named after the file of their sorter,
// and two merge phases of the same sorter never share a prefix.
TEST(ExternalIdTableSorterMergeConfig, spillFilename) {
  EXPECT_EQ(makeSpillFilename("sorter.dat", 0), "sorter.dat.merge-spill.0");
  EXPECT_NE(makeSpillFilename("sorter.dat", 0),
            makeSpillFilename("sorter.dat", 1));
  EXPECT_NE(makeSpillFilename("sorter.dat", 0),
            makeSpillFilename("otherSorter.dat", 0));
}

// _____________________________________________________________________________
// The default executor of the merge phase is a lazily created process-wide
// thread pool, so it is valid and always the same one.
TEST(ExternalIdTableSorterMergeConfig, defaultExecutor) {
  auto executor = defaultSorterMergeExecutor();
  EXPECT_TRUE(static_cast<bool>(executor));
  EXPECT_EQ(executor, defaultSorterMergeExecutor());
}

// The storage that the factory creates is coroutine-based and hence does not
// exist in the C++17 backports mode, see `makeMergePhaseBlockStorageFactory`.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
namespace {
namespace net = boost::asio;

// A block with a single row that holds the given `value` in both of its
// columns.
IdTableStatic<0> makeBlock(int64_t value) {
  IdTableStatic<0> block{2, ad_utility::testing::makeAllocator()};
  block.emplace_back();
  block(0, 0) = Id::makeFromInt(value);
  block(0, 1) = Id::makeFromInt(value);
  return block;
}

// Run everything that is ready to run on the `ioContext` until nothing is left,
// see `test/engine/idTable/CompressedIdTableBlockStorageTest.cpp`.
void pollUntilQuiescent(net::io_context& ioContext) {
  while (true) {
    if (ioContext.stopped()) {
      ioContext.restart();
    }
    if (ioContext.poll() == 0) {
      return;
    }
  }
}
}  // namespace

// _____________________________________________________________________________
// The factory wires the spill file, the allocator, the number of buffered
// blocks and the compression into the storage. The observable consequence of
// `MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK` is that the file of a chunk
// appears as soon as that chunk holds one block more than that.
TEST(ExternalIdTableSorterMergeConfig, blockStorageFactory) {
  net::io_context ioContext;
  auto strand = net::make_strand(ioContext.get_executor());
  std::string prefix = gtestCurrentTestName() + ".spill";
  auto factory = makeMergePhaseBlockStorageFactory<0>(
      ioContext.get_executor(), prefix, ad_utility::testing::makeAllocator());
  // The storage deletes the files that it creates itself, so this is only the
  // safety net for a test that fails before that happens. It is declared
  // *before* the storage, such that it runs after the storage was destroyed.
  absl::Cleanup cleanup = [&prefix] {
    ad_utility::deleteFile(prefix + ".0", false);
  };
  auto storage = factory(strand);
  EXPECT_EQ(storage.filenamePrefix(), prefix);
  EXPECT_FALSE(ql::filesystem::exists(storage.spillFilename(0)));

  // Store one block more than the storage may keep in memory, so that the last
  // one has to be spilled.
  size_t numBlocks = MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK + 1;
  std::vector<bool> wasStored;
  for (size_t i = 0; i < numBlocks; ++i) {
    net::post(strand, [&storage, &wasStored, i]() {
      storage.storeBlock(0, makeBlock(static_cast<int64_t>(i)),
                         [&wasStored](std::exception_ptr exception, bool ok) {
                           EXPECT_EQ(exception, nullptr);
                           wasStored.push_back(ok);
                         });
    });
    pollUntilQuiescent(ioContext);
  }
  EXPECT_THAT(wasStored, ::testing::Each(true));
  EXPECT_TRUE(ql::filesystem::exists(storage.spillFilename(0)));

  // No matter whether a block was spilled or not, it comes back unchanged and
  // in the order in which it was stored.
  std::vector<int64_t> values;
  for (size_t i = 0; i < numBlocks; ++i) {
    net::post(strand, [&storage, &values]() {
      using GetResult = decltype(storage)::GetResult;
      storage.getBlock(
          0, [&values](std::exception_ptr exception, GetResult result) {
            EXPECT_EQ(exception, nullptr);
            ASSERT_TRUE(result.hasValue());
            values.push_back(std::move(result).get()(0, 0).getInt());
          });
    });
    pollUntilQuiescent(ioContext);
  }
  EXPECT_THAT(values, ::testing::ElementsAre(0, 1));
}
#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
