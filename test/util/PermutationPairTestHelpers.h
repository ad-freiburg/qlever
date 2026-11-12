// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_PERMUTATIONPAIRTESTHELPERS_H
#define QLEVER_TEST_UTIL_PERMUTATIONPAIRTESTHELPERS_H

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/post.hpp>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "./GTestHelpers.h"
#include "./IndexTestHelpers.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Constants.h"
#include "index/CompressedRelationReader.h"
#include "index/KeyOrder.h"
#include "index/LocatedTriples.h"
#include "index/chunkwisePermutationWriter/ChunkwisePermutationWriter.h"
#include "index/compressedRelationWriter/CompressedRelationWriter.h"
#include "util/File.h"
#include "util/views/UniqueBlockView.h"

// Helpers for the tests that write a pair of twin permutations (for example
// PSO and POS) and check the result, both with the classical
// `CompressedRelationWriter::createPermutationPair` and with the chunkwise
// writer (see `chunkwisePermutationWriter::createPermutationPair`), as well as
// for the tests of their building blocks.
namespace permutationPairTestHelpers {
using ad_utility::source_location;

// Return an `Id` of type `VocabIndex` from `index`. Assert that `index` is
// `>= 0`.
inline Id V(int64_t index) {
  AD_CONTRACT_CHECK(index >= 0);
  return Id::makeFromVocabIndex(VocabIndex::make(index));
}

// A row of the test input, as integers which are converted via `V`.
using TestRow = std::vector<int64_t>;
using TestRows = std::vector<TestRow>;

// Scan the complete relation with the given `col0Id` from the permutation that
// consists of the given `blocks` and lives at `filename`. The result has the
// columns `col1`, `col2`, and all the additional columns of a permutation with
// `numColumns` columns (the graph and the payload columns, if any).
inline IdTable scanRelation(const std::string& filename,
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
      emptyLocatedTriples, ad_utility::testing::multiThreadExecutor());
}

// Return the `rows` permuted by the `permutation` (the first three columns
// are permuted, all further columns stay), sorted, and without duplicates.
inline TestRows expectedRows(TestRows rows,
                             const qlever::KeyOrder& permutation) {
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

// Return the rows of the twin permutation of the `rows` (with `col0` first):
// columns 1 and 2 are swapped, and the rows are sorted.
inline TestRows twinRows(TestRows rows) {
  for (auto& row : rows) {
    std::swap(row[1], row[2]);
  }
  ql::ranges::sort(rows);
  return rows;
}

// Return the number of distinct values in the first column of the `rows`.
inline size_t numDistinctCol0(const TestRows& rows) {
  std::set<int64_t> col0s;
  for (const auto& row : rows) {
    col0s.insert(row[0]);
  }
  return col0s.size();
}

namespace detail {
// Print up to five of the rows of the `rows` that are not contained in
// `others` to `std::cerr`, each prefixed by the `label`.
inline void reportRowsNotIn(const std::set<std::vector<uint64_t>>& rows,
                            const std::set<std::vector<uint64_t>>& others,
                            std::string_view label) {
  size_t numReported = 0;
  for (const auto& row : rows) {
    if (!others.contains(row) && numReported++ < 5) {
      std::cerr << label << ':';
      for (auto bits : row) {
        std::cerr << ' ' << Id::fromBits(bits);
      }
      std::cerr << std::endl;
    }
  }
}
}  // namespace detail

// Check that the permutation at `filename` (with the given `blocks`) contains
// exactly the `expected` rows (sorted, unique, with `col0` first), relation by
// relation.
inline void checkPermutation(const std::string& filename,
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
      detail::reportRowsNotIn(expectedRowsSet, scannedRows, "missing row");
      detail::reportRowsNotIn(scannedRows, expectedRowsSet, "unexpected row");
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
  // The blocks contain no additional rows. The invariants on the order of the
  // blocks have already been checked by
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
inline void checkMetadata(
    const std::vector<CompressedRelationMetadata>& metadata,
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

// Return random rows with `numRows` rows: the `col0` is drawn from a skewed
// distribution (a few huge relations, some medium ones, and many tiny ones),
// and the other columns from small domains, so that there are many triples
// that are equal except for the graph, and many exact duplicates.
inline TestRows randomRows(size_t numRows, size_t numColumns, unsigned seed) {
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

// NOTE: The chunkwise writer needs the parallel merge, which only exists in the
// C++20 mode of the build.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// The two writers of a pair of permutations that the helpers below can test.
enum class Mode {
  // `CompressedRelationWriter::createPermutationPair`, which consumes the
  // sorted input as a single range.
  Classical,
  // `chunkwisePermutationWriter::createPermutationPair`, which is the sink of
  // the parallel merge of the sorter of the input.
  Chunkwise
};

// Return the name of the `mode`, e.g. for the names of parameterized tests.
inline std::string toString(Mode mode) {
  return mode == Mode::Classical ? "Classical" : "Chunkwise";
}

// Compare two rows of `Id`s by all of their columns, on the bits of the `Id`s.
struct CompareAllColumns {
  template <typename A, typename B>
  bool operator()(const A& a, const B& b) const {
    return ql::ranges::lexicographical_compare(a, b, {}, &Id::getBits,
                                               &Id::getBits);
  }
};

// Ignore the memory limit of the external sorters for the lifetime of this
// object, so that the tests can use small sorters with many runs.
struct ScopedIgnoreMemoryLimit {
  bool previous_ =
      ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING
          .exchange(true);
  ~ScopedIgnoreMemoryLimit() {
    ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING =
        previous_;
  }
};

// The result of writing a pair of permutations, together with everything that
// the callbacks have collected.
struct WriteResult {
  CompressedRelationWriter::PermutationPairResult result_;
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
  // The options of the chunkwise writer. The `removeDuplicates_` also applies
  // to the classical writer, whose input is then passed through a
  // `uniqueBlockView` (as in the index build).
  chunkwisePermutationWriter::Options options_{
      .removeDuplicates_ = true,
      .twinSorterMemoryPerChunk_ = ad_utility::MemorySize::kilobytes(200),
      .boundaryTwinMergeMemory_ = ad_utility::MemorySize::megabytes(10),
      .numCallbackBlocksInFlight_ = 3};
  // If set, the callbacks fail for the block with this index (in the order in
  // which the callbacks are invoked). Only supported by the chunkwise writer.
  std::optional<size_t> failingCallbackBlock_ = std::nullopt;
  // Whether the failing callback throws directly instead of reporting the
  // exception via `done`.
  bool failingCallbackThrows_ = false;
};

// Write the `rows` (in an arbitrary order, possibly with duplicates) with the
// writer of the given `mode` according to the `config`, and return everything
// that the callers need for their checks. The input is sorted with an external
// sorter that uses the `Comparator`, which has to sort by the
// `config.permutation_`. The permutation files are `basename + ".perm1"` and
// `basename + ".perm2"`, and have to be deleted by the caller.
template <typename Comparator = CompareAllColumns>
WriteResult writeRows(const TestRows& rows, const std::string& basename,
                      const Config& config, Mode mode) {
  using namespace chunkwisePermutationWriter;
  using Sorter = ad_utility::CompressedExternalIdTableSorter<Comparator, 0>;
  auto allocator = ad_utility::makeUnlimitedAllocator<Id>();
  Sorter sorter{basename + ".sorter", config.numColumns_, config.sorterMemory_,
                allocator};
  sorter.setMergeExecutor(ad_utility::testing::multiThreadExecutor(),
                          config.parallelism_);
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
    return CompressedRelationWriter::WriterAndCallback{
        std::make_unique<CompressedRelationWriter>(
            config.numColumns_, ad_utility::File{filename, "w"},
            config.blocksize_, ad_utility::testing::multiThreadExecutor()),
        [&metadata](ql::span<const CompressedRelationMetadata> md) {
          metadata.insert(metadata.end(), md.begin(), md.end());
        }};
  };
  std::atomic<size_t> numRowsSeen = 0;
  std::atomic<size_t> numBlocksSeen = 0;

  if (mode == Mode::Classical) {
    AD_CONTRACT_CHECK(!config.failingCallbackBlock_.has_value(),
                      "Failing callbacks are only supported by the chunkwise "
                      "writer");
    CompressedRelationWriter::PerBlockCallbacks callbacks;
    callbacks.push_back(
        [&numRowsSeen, &numBlocksSeen](const IdTableStatic<0>& block) {
          numRowsSeen += block.numRows();
          ++numBlocksSeen;
        });
    auto sortedBlocks = sorter.template getSortedBlocks<0>();
    auto input = config.options_.removeDuplicates_
                     ? ad_utility::uniqueBlockView(std::move(sortedBlocks))
                     : std::move(sortedBlocks);
    result.result_ = CompressedRelationWriter::createPermutationPair(
        basename, makeWriter(basename + ".perm1", result.metadata1_),
        makeWriter(basename + ".perm2", result.metadata2_), std::move(input),
        config.permutation_, callbacks);
  } else {
    std::vector<ConcurrentBlockCallback> callbacks;
    // A synchronous callback.
    callbacks.push_back([&numRowsSeen](SharedBlock block, DoneCallback done) {
      numRowsSeen.fetch_add(block->numRows());
      std::move(done)(nullptr);
    });
    // An asynchronous callback (its `done` is invoked from another task),
    // which optionally fails for one block.
    callbacks.push_back([&numBlocksSeen, &config](SharedBlock block,
                                                  DoneCallback done) {
      size_t blockIndex = numBlocksSeen.fetch_add(1);
      bool fail = config.failingCallbackBlock_ == blockIndex;
      if (fail && config.failingCallbackThrows_) {
        throw std::runtime_error{"callback failed by throwing"};
      }
      boost::asio::post(
          ad_utility::testing::multiThreadExecutor(),
          [block, done = std::move(done), fail]() mutable {
            // Touch the block to make sure that it is alive.
            AD_CORRECTNESS_CHECK(!block->empty());
            std::move(done)(fail ? std::make_exception_ptr(
                                       std::runtime_error{"callback failed"})
                                 : nullptr);
          });
    });
    result.result_ = chunkwisePermutationWriter::createPermutationPair(
        basename, makeWriter(basename + ".perm1", result.metadata1_),
        makeWriter(basename + ".perm2", result.metadata2_), sorter,
        config.permutation_, std::move(callbacks), config.options_,
        ad_utility::testing::multiThreadExecutor());
  }
  result.numRowsSeenByCallbacks_ = numRowsSeen.load();
  result.numBlocksSeenByCallbacks_ = numBlocksSeen.load();
  return result;
}

// Return the basename of the files of a single run of a test, and an
// `absl::Cleanup` that deletes the two permutation files.
inline auto permutationFilesWithCleanup(std::string basename) {
  auto cleanup = absl::Cleanup{[basename]() {
    ad_utility::deleteFile(basename + ".perm1");
    ad_utility::deleteFile(basename + ".perm2");
  }};
  return std::pair{std::move(basename), std::move(cleanup)};
}

// Run the complete check for the `rows`, the `config`, and the `mode`: write
// them, and compare both permutations and their metadata with the
// expectation. See `writeRows` for the `Comparator`.
template <typename Comparator = CompareAllColumns>
void writeAndCheck(const TestRows& rows, const Config& config, Mode mode,
                   source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  ScopedIgnoreMemoryLimit ignoreMemoryLimit;
  auto [basename, cleanup] = permutationFilesWithCleanup(
      absl::StrCat(gtestCurrentTestName(), ".", toString(mode)));
  auto result = writeRows<Comparator>(rows, basename, config, mode);
  auto expected = expectedRows(rows, config.permutation_);
  auto expectedTwin = twinRows(expected);
  EXPECT_EQ(result.result_.numDistinctCol0_, numDistinctCol0(expected));
  EXPECT_EQ(result.numRowsSeenByCallbacks_, expected.size());
  checkPermutation(basename + ".perm1", result.result_.blockMetadata_, expected,
                   config.numColumns_);
  checkPermutation(basename + ".perm2", result.result_.blockMetadataSwitched_,
                   expectedTwin, config.numColumns_);
  checkMetadata(result.metadata1_, expected, config.blocksize_);
  checkMetadata(result.metadata2_, expectedTwin, config.blocksize_);
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

}  // namespace permutationPairTestHelpers

#endif  // QLEVER_TEST_UTIL_PERMUTATIONPAIRTESTHELPERS_H
