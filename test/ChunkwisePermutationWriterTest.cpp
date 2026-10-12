// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/post.hpp>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <vector>

#include "./util/GTestHelpers.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Constants.h"
#include "index/ChunkwisePermutationWriter.h"
#include "index/CompressedRelationReader.h"
#include "index/CompressedRelationWriter.h"
#include "index/ExternalSortFunctors.h"
#include "index/LocatedTriples.h"
#include "util/GlobalExecutor.h"

// NOTE: The chunkwise writer needs the parallel merge, which only exists in the
// C++20 mode of the build.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace {
using namespace chunkwisePermutationWriter;
using ad_utility::source_location;

// Return an `Id` of type `VocabIndex` from `index`.
Id V(int64_t index) {
  AD_CONTRACT_CHECK(index >= 0);
  return Id::makeFromVocabIndex(VocabIndex::make(index));
}

// A row of the test input, as integers which are converted via `V`.
using TestRow = std::vector<int64_t>;
using TestRows = std::vector<TestRow>;

// Compare two rows of `Id`s by all of their columns, on the bits of the `Id`s.
struct CompareAllColumns {
  template <typename A, typename B>
  bool operator()(const A& a, const B& b) const {
    return ql::ranges::lexicographical_compare(a, b, {}, &Id::getBits,
                                               &Id::getBits);
  }
};

// Set a global flag for the duration of a test.
struct ScopedIgnoreMemoryLimit {
  bool previous_ =
      ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING
          .exchange(true);
  ~ScopedIgnoreMemoryLimit() {
    ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING =
        previous_;
  }
};

// The result of writing a pair of permutations with the chunkwise writer,
// together with everything that the callbacks have collected.
struct WriteResult {
  PermutationPairResult result_;
  std::vector<CompressedRelationMetadata> metadata1_;
  std::vector<CompressedRelationMetadata> metadata2_;
  size_t numRowsSeenByCallbacks_ = 0;
  size_t numBlocksSeenByCallbacks_ = 0;
};

// The configuration of a single test run.
struct Config {
  size_t numColumns_ = 4;
  // The rows per block of the permutations, so relations with more than
  // `0.8 * blocksize_` rows are large.
  size_t blocksize_ = 100;
  // The memory of the sorter of the input, which determines the size of its
  // sorted runs and hence (together with `parallelism_`) the number of chunks.
  ad_utility::MemorySize sorterMemory_ = ad_utility::MemorySize::kilobytes(500);
  size_t parallelism_ = 8;
  qlever::KeyOrder permutation_{0, 1, 2, 3};
  Options options_{
      .removeDuplicates_ = true,
      .twinSorterMemoryPerChunk_ = ad_utility::MemorySize::kilobytes(200),
      .boundaryTwinMergeMemory_ = ad_utility::MemorySize::megabytes(10),
      .numCallbackBlocksInFlight_ = 3};
  // If set, the callbacks fail for the block with this index (in the order in
  // which the callbacks are invoked).
  std::optional<size_t> failingCallbackBlock_ = std::nullopt;
  // Whether the failing callback throws directly instead of reporting the
  // exception via `done`.
  bool failingCallbackThrows_ = false;
};

// Write the `rows` (in an arbitrary order, possibly with duplicates) with the
// chunkwise writer according to the `config`, and return everything that the
// callers need for their checks. The permutation files are `basename +
// ".perm1"` and `basename + ".perm2"`, and have to be deleted by the caller.
template <typename Comparator = CompareAllColumns>
WriteResult writeRows(const TestRows& rows, const std::string& basename,
                      const Config& config) {
  using Sorter = ad_utility::CompressedExternalIdTableSorter<Comparator, 0>;
  auto allocator = ad_utility::makeUnlimitedAllocator<Id>();
  Sorter sorter{basename + ".sorter", config.numColumns_, config.sorterMemory_,
                allocator};
  sorter.setMergeExecutor(ad_utility::globalExecutor(), config.parallelism_);
  {
    IdTable input{config.numColumns_, allocator};
    input.resize(rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
      AD_CONTRACT_CHECK(rows[i].size() == config.numColumns_);
      for (size_t col = 0; col < config.numColumns_; ++col) {
        input(i, col) = V(rows[i][col]);
      }
    }
    sorter.pushBlock(input);
  }

  WriteResult result;
  auto makeWriter = [&](const std::string& filename,
                        std::vector<CompressedRelationMetadata>& metadata) {
    return WriterAndCallback{
        std::make_unique<CompressedRelationWriter>(
            config.numColumns_, ad_utility::File{filename, "w"},
            config.blocksize_),
        [&metadata](ql::span<const CompressedRelationMetadata> md) {
          metadata.insert(metadata.end(), md.begin(), md.end());
        }};
  };
  std::atomic<size_t> numRowsSeen = 0;
  std::atomic<size_t> numBlocksSeen = 0;
  std::vector<ConcurrentBlockCallback> callbacks;
  // A synchronous callback.
  callbacks.push_back([&numRowsSeen](SharedBlock block, DoneCallback done) {
    numRowsSeen.fetch_add(block->numRows());
    done(nullptr);
  });
  // An asynchronous callback (its `done` is invoked from another task), which
  // optionally fails for one block.
  callbacks.push_back([&numBlocksSeen, &config](SharedBlock block,
                                                DoneCallback done) {
    size_t blockIndex = numBlocksSeen.fetch_add(1);
    bool fail = config.failingCallbackBlock_ == blockIndex;
    if (fail && config.failingCallbackThrows_) {
      throw std::runtime_error{"callback failed by throwing"};
    }
    boost::asio::post(ad_utility::globalExecutor(), [block,
                                                     done = std::move(done),
                                                     fail]() {
      // Touch the block to make sure that it is alive.
      AD_CORRECTNESS_CHECK(!block->empty());
      done(fail ? std::make_exception_ptr(std::runtime_error{"callback failed"})
                : nullptr);
    });
  });
  result.result_ = createPermutationPair(
      basename, makeWriter(basename + ".perm1", result.metadata1_),
      makeWriter(basename + ".perm2", result.metadata2_), sorter,
      config.permutation_, std::move(callbacks), config.options_);
  result.numRowsSeenByCallbacks_ = numRowsSeen.load();
  result.numBlocksSeenByCallbacks_ = numBlocksSeen.load();
  return result;
}

// Scan the complete relation with the given `col0Id` from the permutation that
// consists of the given `blocks` and lives at `filename`. The result has the
// columns `col1`, `col2`, and all the additional columns (the graph and the
// payload columns, if any).
IdTable scanRelation(const std::string& filename,
                     const std::vector<CompressedBlockMetadata>& blocks,
                     Id col0Id, size_t numColumns) {
  CompressedRelationReader reader{ad_utility::makeUnlimitedAllocator<Id>(),
                                  ad_utility::File{filename, "r"}};
  BlockMetadataSpan blockSpan{blocks};
  BlockMetadataRanges blockRanges{{blockSpan.begin(), blockSpan.end()}};
  ScanSpecification scanSpec{col0Id, std::nullopt, std::nullopt};
  std::vector<ColumnIndex> additionalColumns;
  for (size_t col = ADDITIONAL_COLUMN_GRAPH_ID; col < numColumns; ++col) {
    additionalColumns.push_back(col);
  }
  static const LocatedTriplesPerBlock emptyLocatedTriples{};
  return reader.scan(
      CompressedRelationReader::ScanSpecAndBlocks{scanSpec, blockRanges},
      additionalColumns, std::make_shared<ad_utility::CancellationHandle<>>(),
      emptyLocatedTriples);
}

// Return the `rows` permuted by the `permutation` (the first three columns
// are permuted, all further columns stay), sorted, and without duplicates.
TestRows expectedRows(TestRows rows, const qlever::KeyOrder& permutation) {
  for (auto& row : rows) {
    auto keys = permutation.keys();
    TestRow permuted{row[keys[0]], row[keys[1]], row[keys[2]]};
    for (size_t col = 3; col < row.size(); ++col) {
      permuted.push_back(row[col]);
    }
    row = std::move(permuted);
  }
  ql::ranges::sort(rows);
  rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
  return rows;
}

// Return the rows of the twin permutation of the `rows` (which have to be
// the output of `expectedRows`): columns 1 and 2 are swapped, and the rows are
// sorted by `col0`, the new `col1`, the new `col2`, and the graph.
TestRows twinRows(TestRows rows) {
  for (auto& row : rows) {
    std::swap(row[1], row[2]);
  }
  ql::ranges::sort(rows);
  return rows;
}

// Check that the permutation at `filename` (with the given `blocks`) contains
// exactly the `expected` rows (sorted, unique, with `col0` first), relation by
// relation.
void checkPermutation(const std::string& filename,
                      const std::vector<CompressedBlockMetadata>& blocks,
                      const TestRows& expected, size_t numColumns,
                      source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  size_t begin = 0;
  while (begin < expected.size()) {
    size_t end = begin;
    while (end < expected.size() && expected[end][0] == expected[begin][0]) {
      ++end;
    }
    auto col0 = expected[begin][0];
    auto scanned = scanRelation(filename, blocks, V(col0), numColumns);
    if (scanned.numRows() != end - begin) {
      // Diagnostics: report the rows that are missing or superfluous.
      std::set<std::vector<uint64_t>> scannedRows;
      for (size_t r = 0; r < scanned.numRows(); ++r) {
        std::vector<uint64_t> row;
        for (size_t col = 0; col < scanned.numColumns(); ++col) {
          row.push_back(scanned(r, col).getBits());
        }
        scannedRows.insert(std::move(row));
      }
      std::set<std::vector<uint64_t>> expectedRowsSet;
      for (size_t i = begin; i < end; ++i) {
        std::vector<uint64_t> row;
        for (size_t col = 1; col < numColumns; ++col) {
          row.push_back(V(expected[i][col]).getBits());
        }
        expectedRowsSet.insert(std::move(row));
      }
      size_t numReported = 0;
      for (const auto& row : expectedRowsSet) {
        if (!scannedRows.contains(row) && numReported++ < 5) {
          std::cerr << "missing row:";
          for (auto bits : row) {
            std::cerr << ' ' << Id::fromBits(bits);
          }
          std::cerr << std::endl;
        }
      }
      numReported = 0;
      for (const auto& row : scannedRows) {
        if (!expectedRowsSet.contains(row) && numReported++ < 5) {
          std::cerr << "unexpected row:";
          for (auto bits : row) {
            std::cerr << ' ' << Id::fromBits(bits);
          }
          std::cerr << std::endl;
        }
      }
      std::cerr << "expected " << expectedRowsSet.size()
                << " distinct rows, scanned " << scannedRows.size()
                << " distinct rows, scanned total " << scanned.numRows()
                << std::endl;
    }
    ASSERT_EQ(scanned.numRows(), end - begin) << "relation " << col0;
    ASSERT_EQ(scanned.numColumns(), numColumns - 1);
    for (size_t i = begin; i < end; ++i) {
      for (size_t col = 1; col < numColumns; ++col) {
        ASSERT_EQ(scanned(i - begin, col - 1), V(expected[i][col]))
            << "relation " << col0 << ", row " << i - begin << ", column "
            << col;
      }
    }
    begin = end;
  }
  // No block contains more rows than it should (the `1.5 * blocksize` of a
  // block of small relations, plus the rows of the last relation that is
  // added to it, see `CompressedRelationWriter::smallRelationBlockCapacity`).
  // The invariants on the order of the blocks have already been checked by
  // `CompressedRelationWriter::getFinishedBlocks`.
  size_t numRowsInBlocks = 0;
  for (const auto& block : blocks) {
    numRowsInBlocks += block.numRows_;
  }
  EXPECT_EQ(numRowsInBlocks, expected.size());
}

// Check the metadata of the relations: exactly the relations with more than
// `0.8 * blocksize` rows have metadata, in ascending order of `col0`, with the
// correct number of rows and multiplicities.
void checkMetadata(const std::vector<CompressedRelationMetadata>& metadata,
                   const TestRows& expected, size_t blocksize,
                   source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  std::vector<CompressedRelationMetadata> expectedMetadata;
  size_t begin = 0;
  while (begin < expected.size()) {
    size_t end = begin;
    std::set<int64_t> distinctCol1;
    std::set<int64_t> distinctCol2;
    while (end < expected.size() && expected[end][0] == expected[begin][0]) {
      distinctCol1.insert(expected[end][1]);
      distinctCol2.insert(expected[end][2]);
      ++end;
    }
    size_t numRows = end - begin;
    if (static_cast<double>(numRows) > 0.8 * static_cast<double>(blocksize)) {
      expectedMetadata.push_back(CompressedRelationMetadata{
          V(expected[begin][0]), numRows,
          CompressedRelationWriter::computeMultiplicity(numRows,
                                                        distinctCol1.size()),
          CompressedRelationWriter::computeMultiplicity(numRows,
                                                        distinctCol2.size()),
          std::numeric_limits<uint64_t>::max()});
    }
    begin = end;
  }
  ASSERT_EQ(metadata.size(), expectedMetadata.size());
  for (size_t i = 0; i < metadata.size(); ++i) {
    EXPECT_EQ(metadata[i], expectedMetadata[i]) << "metadata " << i;
  }
}

// Return the number of distinct values in the first column of the `rows`.
size_t numDistinctCol0(const TestRows& rows) {
  std::set<int64_t> col0s;
  for (const auto& row : rows) {
    col0s.insert(row[0]);
  }
  return col0s.size();
}

// Run the complete check for the `rows` and the `config`: write them, and
// compare both permutations and their metadata with the expectation.
void writeAndCheck(const TestRows& rows, const Config& config,
                   source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  ScopedIgnoreMemoryLimit ignoreMemoryLimit;
  std::string basename = gtestCurrentTestName();
  std::string filename1 = basename + ".perm1";
  std::string filename2 = basename + ".perm2";
  absl::Cleanup cleanup{[&filename1, &filename2]() {
    ad_utility::deleteFile(filename1);
    ad_utility::deleteFile(filename2);
  }};
  auto result = writeRows(rows, basename, config);
  auto expected = expectedRows(rows, config.permutation_);
  auto expectedTwin = twinRows(expected);
  EXPECT_EQ(result.result_.numDistinctCol0_, numDistinctCol0(expected));
  EXPECT_EQ(result.numRowsSeenByCallbacks_, expected.size());
  checkPermutation(filename1, result.result_.blockMetadata_, expected,
                   config.numColumns_);
  checkPermutation(filename2, result.result_.blockMetadataSwitched_,
                   expectedTwin, config.numColumns_);
  checkMetadata(result.metadata1_, expected, config.blocksize_);
  checkMetadata(result.metadata2_, expectedTwin, config.blocksize_);
}

// Return random rows with `numRows` rows: the `col0` is drawn from a skewed
// distribution (a few huge relations, some medium ones, and many tiny ones),
// and the other columns from small domains, so that there are many triples
// that are equal except for the graph, and many exact duplicates.
TestRows randomRows(size_t numRows, size_t numColumns, unsigned seed) {
  std::mt19937_64 engine{seed};
  std::uniform_real_distribution<double> uniform{0.0, 1.0};
  auto randomInt = [&engine](int64_t upper) {
    return std::uniform_int_distribution<int64_t>{0, upper - 1}(engine);
  };
  TestRows rows;
  rows.reserve(numRows);
  for (size_t i = 0; i < numRows; ++i) {
    double kind = uniform(engine);
    int64_t col0 = kind < 0.4   ? 10'000 + randomInt(3)
                   : kind < 0.7 ? 20'000 + randomInt(50)
                                : randomInt(5'000);
    TestRow row{col0, randomInt(200), randomInt(200), randomInt(3)};
    // The payload columns (if any) are functions of the other columns, such
    // that the order of the twin permutation stays unambiguous.
    for (size_t col = 4; col < numColumns; ++col) {
      row.push_back(row[1] + row[2] + static_cast<int64_t>(col));
    }
    rows.push_back(std::move(row));
  }
  return rows;
}

}  // namespace

// _____________________________________________________________________________
TEST(ChunkwisePermutationWriter, emptyInput) { writeAndCheck({}, Config{}); }

// _____________________________________________________________________________
TEST(ChunkwisePermutationWriter, singleSmallRelation) {
  writeAndCheck({{1, 2, 3, 0}, {1, 2, 3, 0}, {1, 2, 4, 0}, {1, 2, 4, 1}},
                Config{});
}

// _____________________________________________________________________________
TEST(ChunkwisePermutationWriter, fewSmallRelationsInSingleBlock) {
  TestRows rows;
  for (int64_t i = 0; i < 30; ++i) {
    rows.push_back({i / 3, i % 7, i % 5, i % 2});
  }
  writeAndCheck(rows, Config{});
}

// _____________________________________________________________________________
TEST(ChunkwisePermutationWriter, singleLargeRelationInSingleChunk) {
  TestRows rows;
  for (int64_t i = 0; i < 1'000; ++i) {
    rows.push_back({7, i % 37, i % 11, i % 3});
  }
  writeAndCheck(rows, Config{});
}

// _____________________________________________________________________________
TEST(ChunkwisePermutationWriter, randomRowsManyChunks) {
  writeAndCheck(randomRows(300'000, 4, 42), Config{});
}

// _____________________________________________________________________________
TEST(ChunkwisePermutationWriter, randomRowsWithoutDuplicateRemoval) {
  auto rows = randomRows(100'000, 4, 43);
  ql::ranges::sort(rows);
  rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
  Config config;
  config.options_.removeDuplicates_ = false;
  writeAndCheck(rows, config);
}

// _____________________________________________________________________________
TEST(ChunkwisePermutationWriter, randomRowsWithPayloadColumns) {
  Config config;
  config.numColumns_ = 6;
  config.permutation_ = qlever::KeyOrder{0, 1, 2, 3};
  writeAndCheck(randomRows(100'000, 6, 44), config);
}

// _____________________________________________________________________________
TEST(ChunkwisePermutationWriter, randomRowsWithNonIdentityPermutation) {
  Config config;
  config.permutation_ = qlever::KeyOrder{1, 0, 2, 3};
  // The sorter has to sort by the permutation, which the comparator
  // `SortTriple<1, 0, 2>` does (it also compares the graph column).
  ScopedIgnoreMemoryLimit ignoreMemoryLimit;
  std::string basename = gtestCurrentTestName();
  std::string filename1 = basename + ".perm1";
  std::string filename2 = basename + ".perm2";
  absl::Cleanup cleanup{[&filename1, &filename2]() {
    ad_utility::deleteFile(filename1);
    ad_utility::deleteFile(filename2);
  }};
  auto rows = randomRows(100'000, 4, 45);
  auto result = writeRows<SortTriple<1, 0, 2>>(rows, basename, config);
  auto expected = expectedRows(rows, config.permutation_);
  auto expectedTwin = twinRows(expected);
  EXPECT_EQ(result.result_.numDistinctCol0_, numDistinctCol0(expected));
  checkPermutation(filename1, result.result_.blockMetadata_, expected, 4);
  checkPermutation(filename2, result.result_.blockMetadataSwitched_,
                   expectedTwin, 4);
  checkMetadata(result.metadata1_, expected, config.blocksize_);
  checkMetadata(result.metadata2_, expectedTwin, config.blocksize_);
}

// _____________________________________________________________________________
TEST(ChunkwisePermutationWriter, relationSpanningManyChunks) {
  // A few small relations, then one relation that is much larger than a
  // chunk, then a few small relations again.
  TestRows rows;
  for (int64_t i = 0; i < 20; ++i) {
    rows.push_back({i, i, i, 0});
  }
  for (int64_t i = 0; i < 400'000; ++i) {
    rows.push_back({100, i % 1'000, i % 777, i % 3});
  }
  for (int64_t i = 0; i < 20; ++i) {
    rows.push_back({200 + i, i, i, 0});
  }
  writeAndCheck(rows, Config{});
}

// _____________________________________________________________________________
TEST(ChunkwisePermutationWriter, twoSpanningRelationsAndTinyRelationsBetween) {
  // Two relations that span several chunks each, separated by a single tiny
  // relation, so that the boundary between the two large ones falls into a
  // chunk that contains the end of the first, the tiny one, and the beginning
  // of the second.
  TestRows rows;
  for (int64_t i = 0; i < 150'000; ++i) {
    rows.push_back({1, i % 500, i % 333, i % 2});
  }
  rows.push_back({2, 1, 1, 0});
  for (int64_t i = 0; i < 150'000; ++i) {
    rows.push_back({3, i % 500, i % 333, i % 2});
  }
  writeAndCheck(rows, Config{});
}

// _____________________________________________________________________________
TEST(ChunkwisePermutationWriter, mediumRelationsAroundTheLargeThreshold) {
  // Relations whose size is around the `0.8 * blocksize` threshold and around
  // the blocksize, so that the parts at the chunk boundaries are small for
  // some of them and large for others.
  TestRows rows;
  std::mt19937_64 engine{46};
  for (int64_t col0 = 0; col0 < 3'000; ++col0) {
    auto numRows = std::uniform_int_distribution<int64_t>{60, 140}(engine);
    for (int64_t i = 0; i < numRows; ++i) {
      rows.push_back({col0, i % 50, i, i % 2});
    }
  }
  writeAndCheck(rows, Config{});
}

// _____________________________________________________________________________
TEST(ChunkwisePermutationWriter, serialMergeParallelismOne) {
  Config config;
  config.parallelism_ = 1;
  writeAndCheck(randomRows(50'000, 4, 47), config);
}

// _____________________________________________________________________________
TEST(ChunkwisePermutationWriter, failingCallbackPropagates) {
  for (bool throws : {false, true}) {
    Config config;
    config.failingCallbackBlock_ = 2;
    config.failingCallbackThrows_ = throws;
    ScopedIgnoreMemoryLimit ignoreMemoryLimit;
    std::string basename = absl::StrCat(gtestCurrentTestName(), throws);
    std::string filename1 = basename + ".perm1";
    std::string filename2 = basename + ".perm2";
    absl::Cleanup cleanup{[&filename1, &filename2]() {
      ad_utility::deleteFile(filename1);
      ad_utility::deleteFile(filename2);
    }};
    auto rows = randomRows(100'000, 4, 48);
    AD_EXPECT_THROW_WITH_MESSAGE(writeRows(rows, basename, config),
                                 ::testing::HasSubstr("callback failed"));
  }
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
