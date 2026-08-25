// Copyright 2023 - 2025 The QLever Authors, in particular:
//
// 2023 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>

#include "../../util/AllocatorTestHelpers.h"
#include "../../util/GTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "backports/filesystem.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/ExternalSortFunctors.h"
#include "util/ConstexprUtils.h"
#include "util/jthread.h"

namespace net = boost::asio;

using ad_utility::source_location;
using namespace ad_utility::memory_literals;

namespace {

static constexpr size_t NUM_COLS = NumColumnsIndexBuilding;

// From a `generator` that yields  `IdTable`s, create a single `IdTable` that is
// the concatenation of all the yielded tables.
auto idTableFromBlockGenerator = [](auto& generator) -> CopyableIdTable<0> {
  CopyableIdTable<0> result(ad_utility::testing::makeAllocator());
  for (const auto& blockStatic : generator) {
    auto block = blockStatic.clone().toDynamic();
    if (result.empty()) {
      result.setNumColumns(block.numColumns());
    } else {
      AD_CORRECTNESS_CHECK(result.numColumns() == block.numColumns());
    }
    size_t numColumns = result.numColumns();
    size_t size = result.size();
    result.resize(result.size() + block.size());
    for (auto i : ql::views::iota(0U, numColumns)) {
      decltype(auto) blockCol = block.getColumn(i);
      decltype(auto) resultCol = result.getColumn(i);
      ql::ranges::copy(blockCol, resultCol.begin() + size);
    }
  }
  return result;
};

// From a generator that generates rows of an `IdTable`, create an `IdTable`.
// The number of static and dynamic columns has to be specified (see `IdTable.h`
// for details).
template <size_t NumStaticColumns>
auto idTableFromRowGenerator = [](auto&& generator, size_t numColumns) {
  CopyableIdTable<NumStaticColumns> result(
      numColumns, ad_utility::testing::makeAllocator());
  for (const auto& row : generator) {
    result.push_back(row);
  }
  return result;
};
}  // namespace

TEST(CompressedExternalIdTable, compressedExternalIdTableWriter) {
  using namespace ad_utility::memory_literals;

  auto runTestForBlockSize = [](ad_utility::MemorySize memoryToUse,
                                ad_utility::source_location l =
                                    AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    std::string filename = "idTableCompressedWriter.compressedWriterTest.dat";
    ad_utility::CompressedExternalIdTableWriter writer{
        filename, 3, ad_utility::testing::makeAllocator(), memoryToUse};
    std::vector<CopyableIdTable<0>> tables;
    tables.push_back(makeIdTableFromVector({{2, 4, 7}, {3, 6, 8}, {4, 3, 2}}));
    tables.push_back(
        makeIdTableFromVector({{2, 3, 7}, {3, 6, 8}, {4, 2, 123}}));
    tables.push_back(makeIdTableFromVector({{0, 4, 7}}));

    for (const auto& table : tables) {
      writer.writeIdTable(table);
    }

    auto generators = writer.getAllGenerators();
    ASSERT_EQ(generators.size(), tables.size());

    using namespace ::testing;
    std::vector<CopyableIdTable<0>> result;
    auto tr = ql::ranges::transform_view(generators, idTableFromBlockGenerator);
    ql::ranges::copy(tr, std::back_inserter(result));
    EXPECT_THAT(result, ElementsAreArray(tables));
  };
  // With 10 bytes per block, the first and second IdTable are split up into
  // multiple blocks.
  runTestForBlockSize(10_B);
  // With 48 bytes, each IdTable is stored in a single block.
  runTestForBlockSize(48_B);
}

template <size_t NumStaticColumns>
void testExternalSorterImpl(
    size_t numDynamicColumns, size_t numRows,
    ad_utility::MemorySize memoryToUse, bool mergeMultipleTimes,
    std::optional<size_t> mergeParallelism = std::nullopt,
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto tr = generateLocationTrace(l);
  // NOTE: The filename has to be derived from the name of the currently running
  // test, because `ctest` runs the individual tests as concurrent processes in
  // the same directory. A hardcoded name would make the tests that use this
  // helper overwrite each other's sorter file.
  std::string filename = gtestCurrentTestName() + ".testExternalSorter.dat";
  absl::Cleanup cleanup = [&filename] { ad_utility::deleteFile(filename); };
  using namespace ad_utility::memory_literals;

  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  ad_utility::CompressedExternalIdTableSorter<SortByOSP, NumStaticColumns>
      writer{filename, numDynamicColumns, memoryToUse,
             ad_utility::testing::makeAllocator(), 5_kB};
  // NOTE: The pool is only created if it is really needed, because a
  // `parallelism` of one never touches the executor at all.
  std::optional<net::thread_pool> pool;
  if (mergeParallelism.has_value()) {
    pool.emplace(mergeParallelism.value());
    writer.setMergeExecutor(pool->get_executor(), mergeParallelism.value());
  }

  for (size_t i = 0; i < 2; ++i) {
    CopyableIdTable<NumStaticColumns> randomTable =
        createRandomlyFilledIdTable(numRows, numDynamicColumns)
            .toStatic<NumStaticColumns>();

    for (const auto& row : randomTable) {
      writer.push(row);
    }

    ql::ranges::sort(randomTable, SortByOSP{});

    if (mergeMultipleTimes) {
      writer.moveResultOnMerge() = false;
    }

    auto testMultipleTimesImpl = [&](auto k) {
      // Also test the case that the blocksize does not exactly divides the
      // number of inputs.
      auto blocksize = k == 1 ? 1 : 17;
      using namespace ::testing;
      auto generator = [&]() {
        if constexpr (k == 0) {
          // Also check that we don't accidentally get empty blocks yielded,
          // which would be unexpected.
          auto checkNonEmpty = [](auto&& idTable) -> decltype(auto) {
            EXPECT_FALSE(idTable.empty());
            return AD_FWD(idTable);
          };
          return ql::views::join(writer.getSortedBlocks(blocksize) |
                                 ql::views::transform(checkNonEmpty));
        } else {
          return writer.sortedView();
        }
      };
      if (mergeMultipleTimes || k == 0) {
        auto result = idTableFromRowGenerator<NumStaticColumns>(
            generator(), numDynamicColumns);
        ASSERT_THAT(result, ::testing::ElementsAreArray(randomTable))
            << "k = " << k;
      } else {
        EXPECT_ANY_THROW((idTableFromRowGenerator<NumStaticColumns>(
            generator(), numDynamicColumns)));
      }
      // We cannot access or change this value after the first merge.
      EXPECT_ANY_THROW(writer.moveResultOnMerge());
    };
    ad_utility::ConstexprForLoopVi(std::make_index_sequence<5>(),
                                   testMultipleTimesImpl);
    writer.clear();
  }
};

template <size_t NumStaticColumns>
void testExternalSorter(size_t numDynamicColumns, size_t numRows,
                        ad_utility::MemorySize memoryToUse,
                        std::optional<size_t> mergeParallelism = std::nullopt,
                        source_location l = AD_CURRENT_SOURCE_LOC()) {
  testExternalSorterImpl<NumStaticColumns>(
      numDynamicColumns, numRows, memoryToUse, true, mergeParallelism, l);
  testExternalSorterImpl<NumStaticColumns>(
      numDynamicColumns, numRows, memoryToUse, false, mergeParallelism, l);
}

// Test for static (`<NUM_COLS>) and dynamic (`<0>`) tables. The second
// argument to `testExternalSorter` is the number of rows, the third argument
// is the memory limit for the sorter.
TEST(CompressedExternalIdTable, sorterRandomInputs) {
  using namespace ad_utility::memory_literals;
  testExternalSorter<NUM_COLS>(NUM_COLS, 10'000, 10_kB);
  testExternalSorter<NUM_COLS>(NUM_COLS, 1000, 1_MB);
  testExternalSorter<NUM_COLS>(NUM_COLS, 0, 1_MB);

  testExternalSorter<0>(NUM_COLS, 10'000, 10_kB);
  testExternalSorter<0>(NUM_COLS, 1000, 1_MB);
  testExternalSorter<0>(NUM_COLS, 0, 1_MB);
}

// Test that destroying the sorter while an async block-sorting task is still
// running does not cause a use-after-free (caught by ASAN). This used to be a
// bug, which was fixed by calling `waitForFuture()` in the destructor of
// `CompressedExternalIdTableSorter`.
TEST(CompressedExternalIdTable, stillSortingOnDestruction) {
  struct SlowDummySorter : SortByOSP {
    std::vector<size_t> data_ = {1, 2, 3};
    std::shared_ptr<std::atomic<bool>> sleptOnce_ =
        std::make_shared<std::atomic<bool>>(false);
    SlowDummySorter() = default;
    SlowDummySorter(const SlowDummySorter& other)
        : SortByOSP(other), data_(other.data_), sleptOnce_(other.sleptOnce_) {
      if (!sleptOnce_->exchange(true)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
      [[maybe_unused]] volatile auto x = other.data_[0];
    }
  };
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  ad_utility::CompressedExternalIdTableSorter<SlowDummySorter, 0> sorter{
      "stillSortingOnDestruction.dat", NUM_COLS, 10_kB,
      ad_utility::testing::makeAllocator()};
  // With 10 kB memory and NUM_COLS (4) columns, blocksize = 10000 / (4*8*2)
  // = 156 rows. Push enough to trigger exactly one `pushBlock`.
  auto table = createRandomlyFilledIdTable(200, NUM_COLS);
  for (const auto& row : table) {
    sorter.push(row);
  }
}

TEST(CompressedExternalIdTable, sorterMemoryLimit) {
  std::string filename = "idTableCompressedSorter.memoryLimit.dat";

  // only 100 bytes of memory, not sufficient for merging
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = false;
  ad_utility::CompressedExternalIdTableSorter<SortByOSP, 0> writer{
      filename, NUM_COLS, 100_B, ad_utility::testing::makeAllocator()};

  CopyableIdTable<0> randomTable = createRandomlyFilledIdTable(100, NUM_COLS);

  // Pushing always works
  for (const auto& row : randomTable) {
    writer.push(row);
  }

  auto generator = [&writer]() { return writer.sortedView(); };
  AD_EXPECT_THROW_WITH_MESSAGE(
      (idTableFromRowGenerator<0>(generator(), NUM_COLS)),
      ::testing::ContainsRegex("Insufficient memory"));
}

// Test corner case: pushing exactly blockSize rows leaves currentBlock_ empty.
TEST(CompressedExternalIdTable, cornerCasesEmptyBlocks) {
  // Create `CompressedExternalIdTable` with a block size of exactly 10 rows.
  size_t blockSize = 10;
  std::string filename = "idTableCompressedSorter.cornerCases.dat";
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  size_t blockMemory = blockSize * NUM_COLS * sizeof(Id) * 2;
  ad_utility::CompressedExternalIdTable<0> writer{
      filename, NUM_COLS, ad_utility::MemorySize::bytes(blockMemory),
      ad_utility::testing::makeAllocator()};

  // Push exactly 10 rows. After the 10th row, one full block is written and
  // `currentBlock_` becomes empty. When `getRows()` is later called, it will
  // call `pushBlock()` with this empty block, which must be handled correctly
  // (the empty block is detected and skipped, and a dummy future is created to
  // maintain invariants). This corner case failed in an earlier version.
  CopyableIdTable<0> randomTable =
      createRandomlyFilledIdTable(blockSize, NUM_COLS);
  for (const auto& row : randomTable) {
    writer.push(row);
  }

  // Check that no failure occurs and the result is as expected.
  auto generator = writer.getRows();
  auto result = idTableFromRowGenerator<0>(generator, NUM_COLS);
  EXPECT_EQ(result.size(), randomTable.size());
}

template <size_t NumStaticColumns>
void testExternalCompressor(size_t numDynamicColumns, size_t numRows,
                            ad_utility::MemorySize memoryToUse) {
  std::string filename = "idTableCompressedSorter.testExternalCompressor.dat";
  using namespace ad_utility::memory_literals;

  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  ad_utility::CompressedExternalIdTable<NumStaticColumns> writer{
      filename, numDynamicColumns, memoryToUse,
      ad_utility::testing::makeAllocator(), 5_kB};

  for (size_t i = 0; i < 2; ++i) {
    CopyableIdTable<NumStaticColumns> randomTable =
        createRandomlyFilledIdTable(numRows, numDynamicColumns)
            .toStatic<NumStaticColumns>();

    for (const auto& row : randomTable) {
      writer.push(row);
    }

    auto generator = writer.getRows();

    using namespace ::testing;
    auto result =
        idTableFromRowGenerator<NumStaticColumns>(generator, numDynamicColumns);
    ASSERT_THAT(result, Eq(randomTable));
    writer.clear();
  }
}

TEST(CompressedExternalIdTable, compressorRandomInput) {
  using namespace ad_utility::memory_literals;
  // Test for dynamic (<0>) and static(<3>) tables.
  // Test the case that there are multiple blocks to merge (many rows but a low
  // memory limit), but also the case that there is only a single block (few
  // rows with a sufficiently large memory limit).
  testExternalCompressor<0>(3, 10'000, 10_kB);
  testExternalCompressor<0>(3, 1000, 1_MB);
  testExternalCompressor<3>(3, 10'000, 10_kB);
  testExternalCompressor<3>(3, 1000, 1_MB);
}

TEST(CompressedExternalIdTable, exceptionsWhenWritingWhileIterating) {
  std::string filename = "idTableCompressor.exceptionsWhenWritingTest.dat";
  using namespace ad_utility::memory_literals;

  ad_utility::CompressedExternalIdTable<3> writer{
      filename, 3, 10_B, ad_utility::testing::makeAllocator()};

  CopyableIdTable<3> randomTable =
      createRandomlyFilledIdTable(1000, 3).toStatic<3>();

  auto pushAll = [&randomTable, &writer] {
    for (const auto& row : randomTable) {
      writer.push(row);
    }
  };
  ASSERT_NO_THROW(pushAll());

  auto generator = writer.getRows();
  // We have obtained a generator, but have not yet started it, but pushing is
  // already disabled to make the two-phase interface more consistent.

  AD_EXPECT_THROW_WITH_MESSAGE(
      pushAll(), ::testing::ContainsRegex("currently being iterated"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      writer.clear(), ::testing::ContainsRegex("currently being iterated"));

  auto it = generator.begin();
  AD_EXPECT_THROW_WITH_MESSAGE(
      pushAll(), ::testing::ContainsRegex("currently being iterated"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      writer.clear(), ::testing::ContainsRegex("currently being iterated"));

  for (; it != generator.end(); ++it) {
  }

  // All generators have ended, we should be able to push and clear.
  ASSERT_NO_THROW(pushAll());
  ASSERT_NO_THROW(writer.clear());
}

TEST(CompressedExternalIdTable, WrongNumberOfColsWhenPushing) {
  std::string filename = "idTableCompressor.wrongNumCols.dat";
  using namespace ad_utility::memory_literals;
  auto alloc = ad_utility::testing::makeAllocator();

  ad_utility::CompressedExternalIdTableSorter<SortByOSP, NUM_COLS> writer{
      filename, NUM_COLS, 10_B, alloc};
  ad_utility::CompressedExternalIdTableSorterTypeErased& erased = writer;
  IdTableStatic<0> t1{NUM_COLS, alloc};
  EXPECT_NO_THROW(erased.pushBlock(t1));
  EXPECT_NO_THROW(t1.setNumColumns(NUM_COLS + 1));
  EXPECT_ANY_THROW(erased.pushBlock(t1));

  // Also test `pushBlock` with an `IdTableView<0>`.
  IdTableStatic<0> t2{NUM_COLS, alloc};
  IdTableView<0> v2 = t2.asStaticView<0>();
  EXPECT_NO_THROW(erased.pushBlock(v2));
  IdTableStatic<0> t3{NUM_COLS + 1, alloc};
  IdTableView<0> v3 = t3.asStaticView<0>();
  EXPECT_ANY_THROW(erased.pushBlock(v3));
}

// Test that data pushed via both `pushBlock` overloads appears correctly in the
// sorted output.
TEST(CompressedExternalIdTable, pushBlockProducesCorrectSortedOutput) {
  std::string filename = gtestCurrentTestName();
  using namespace ad_utility::memory_literals;
  auto alloc = ad_utility::testing::makeAllocator();

  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  ad_utility::CompressedExternalIdTableSorter<SortByOSP, NUM_COLS> writer{
      filename, NUM_COLS, 1_MB, alloc};
  ad_utility::CompressedExternalIdTableSorterTypeErased& erased = writer;

  // Create two blocks of random data.
  IdTable block1 = createRandomlyFilledIdTable(30, NUM_COLS);
  IdTable block2 = createRandomlyFilledIdTable(30, NUM_COLS);

  // Build the expected result: all rows from both blocks sorted by OSP.
  CopyableIdTable<0> expected(NUM_COLS, alloc);
  for (const auto& row : block1) {
    expected.push_back(row);
  }
  for (const auto& row : block2) {
    expected.push_back(row);
  }
  ql::ranges::sort(expected, SortByOSP{});

  // Push via `IdTableStatic<0>` (first overload) and `IdTableView<0>` (second).
  erased.pushBlock(block1);
  IdTableView<0> view2 = block2.asStaticView<0>();
  erased.pushBlock(view2);

  // Collect the sorted output and verify it matches the expectation.
  auto generator = erased.getSortedOutput();
  auto result = idTableFromBlockGenerator(generator);

  using namespace ::testing;
  EXPECT_THAT(result, ElementsAreArray(expected));
}

namespace {
// The number of rows per block that results from the given uncompressed block
// size. The blocks are formed per column, hence the size of a single `Id`.
size_t rowsPerBlockFor(ad_utility::MemorySize blockSize) {
  return blockSize.getBytes() / sizeof(Id);
}

// Write all the `tables` to the `writer` and then flush it, such that the
// written blocks can be read again.
void writeAndFlush(ad_utility::CompressedExternalIdTableWriter& writer,
                   const std::vector<CopyableIdTable<0>>& tables) {
  for (const auto& table : tables) {
    writer.writeIdTable(table);
  }
  writer.flush();
}

// Check that the block boundary metadata of the `writer` exactly matches the
// `tables` from which it was built.
void checkBlockMetadata(
    const ad_utility::CompressedExternalIdTableWriter& writer,
    const std::vector<CopyableIdTable<0>>& tables, size_t rowsPerBlock,
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  ASSERT_EQ(writer.numIdTables(), tables.size());
  for (size_t i = 0; i < tables.size(); ++i) {
    const auto& table = tables.at(i);
    size_t expectedNumBlocks =
        (table.numRows() + rowsPerBlock - 1) / rowsPerBlock;
    ASSERT_EQ(writer.numBlocksOfIdTable(i), expectedNumBlocks);
    for (size_t b = 0; b < expectedNumBlocks; ++b) {
      size_t lower = b * rowsPerBlock;
      size_t upper = std::min(lower + rowsPerBlock, table.numRows());
      EXPECT_EQ(writer.numRowsInBlock(i, b), upper - lower);
      EXPECT_EQ(writer.firstRowOfBlock(i, b), table.at(lower));
      EXPECT_EQ(writer.lastRowOfBlock(i, b), table.at(upper - 1));
    }
  }
}

// Check that `readBlockOfIdTable` returns exactly the rows of the `tables` from
// which the `writer` was built.
void checkBlockContents(
    const ad_utility::CompressedExternalIdTableWriter& writer,
    const std::vector<CopyableIdTable<0>>& tables, size_t rowsPerBlock,
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  ASSERT_EQ(writer.numIdTables(), tables.size());
  for (size_t i = 0; i < tables.size(); ++i) {
    const auto& table = tables.at(i);
    for (size_t b = 0; b < writer.numBlocksOfIdTable(i); ++b) {
      auto block = writer.readBlockOfIdTable(i, b);
      size_t lower = b * rowsPerBlock;
      size_t upper = std::min(lower + rowsPerBlock, table.numRows());
      ASSERT_EQ(block.numRows(), upper - lower);
      for (size_t row = 0; row < block.numRows(); ++row) {
        EXPECT_EQ(block.at(row), table.at(lower + row));
      }
    }
  }
}

// Three `IdTable`s with 3 columns each. The number of rows (6, 5, 1) is chosen
// such that it is both divisible and not divisible by the block sizes used in
// the tests below.
std::vector<CopyableIdTable<0>> testTables() {
  std::vector<CopyableIdTable<0>> tables;
  tables.push_back(makeIdTableFromVector(
      {{2, 4, 7}, {3, 6, 8}, {4, 3, 2}, {5, 1, 9}, {7, 0, 3}, {8, 8, 8}}));
  tables.push_back(makeIdTableFromVector(
      {{2, 3, 7}, {3, 6, 8}, {4, 2, 123}, {9, 9, 9}, {11, 0, 1}}));
  tables.push_back(makeIdTableFromVector({{0, 4, 7}}));
  return tables;
}
}  // namespace

// _____________________________________________________________________________
TEST(CompressedExternalIdTable, blockBoundaryMetadata) {
  auto tables = testTables();
  // With 16 bytes per block we get 2 rows per block (divides the 6 rows of the
  // first table, but not the 5 rows of the second one). With 24 bytes we get 3
  // rows per block (divides the 6 rows, not the 5 rows). With 800 bytes each
  // table fits into a single block.
  for (auto blockSize : {16_B, 24_B, 800_B}) {
    std::string filename =
        gtestCurrentTestName() + std::to_string(blockSize.getBytes()) + ".dat";
    absl::Cleanup cleanup = [&filename] {
      ad_utility::deleteFile(filename, false);
    };
    ad_utility::CompressedExternalIdTableWriter writer{
        filename, 3, ad_utility::testing::makeAllocator(), blockSize};
    writeAndFlush(writer, tables);
    checkBlockMetadata(writer, tables, rowsPerBlockFor(blockSize));
  }
}

// _____________________________________________________________________________
TEST(CompressedExternalIdTable, readBlockOfIdTableMatchesSource) {
  auto tables = testTables();
  for (auto blockSize : {16_B, 24_B, 800_B}) {
    std::string filename =
        gtestCurrentTestName() + std::to_string(blockSize.getBytes()) + ".dat";
    absl::Cleanup cleanup = [&filename] {
      ad_utility::deleteFile(filename, false);
    };
    ad_utility::CompressedExternalIdTableWriter writer{
        filename, 3, ad_utility::testing::makeAllocator(), blockSize};
    writeAndFlush(writer, tables);
    checkBlockContents(writer, tables, rowsPerBlockFor(blockSize));
  }
}

// _____________________________________________________________________________
TEST(CompressedExternalIdTable, concurrentBlockReads) {
  auto tables = testTables();
  auto blockSize = 16_B;
  std::string filename = gtestCurrentTestName() + ".dat";
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename, false);
  };
  ad_utility::CompressedExternalIdTableWriter writer{
      filename, 3, ad_utility::testing::makeAllocator(), blockSize};
  writeAndFlush(writer, tables);

  // Read all blocks concurrently from 8 threads. This only works if
  // `readBlockOfIdTable` takes a shared lock on the underlying file.
  writer.registerActiveReader();
  std::vector<ad_utility::JThread> threads;
  for ([[maybe_unused]] size_t i : ql::views::iota(0, 8)) {
    threads.emplace_back([&writer, &tables, blockSize]() {
      checkBlockContents(writer, tables, rowsPerBlockFor(blockSize));
    });
  }
  threads.clear();
  writer.unregisterActiveReader();

  // After all readers are gone, the writer can be written to again.
  EXPECT_NO_THROW(writer.writeIdTable(tables.at(0)));
}

// _____________________________________________________________________________
TEST(CompressedExternalIdTable, clearResetsBoundaryMetadata) {
  auto tables = testTables();
  auto blockSize = 16_B;
  std::string filename = gtestCurrentTestName() + ".dat";
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename, false);
  };
  ad_utility::CompressedExternalIdTableWriter writer{
      filename, 3, ad_utility::testing::makeAllocator(), blockSize};
  writeAndFlush(writer, tables);
  writer.clear();

  // After clearing, only the second batch is visible.
  std::vector<CopyableIdTable<0>> secondBatch;
  secondBatch.push_back(
      makeIdTableFromVector({{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}));
  writeAndFlush(writer, secondBatch);
  checkBlockMetadata(writer, secondBatch, rowsPerBlockFor(blockSize));
  checkBlockContents(writer, secondBatch, rowsPerBlockFor(blockSize));
}

namespace {
// The number of rows and the memory limit that are used by the tests of the
// parallel merge below. With 4 columns and a memory limit of 1 MB, a single
// presorted run holds `1'000'000 / (4 * 8 * 2) = 15'625` rows, so that 200'000
// rows yield 13 runs. The number of rows is also well above
// `DEFAULT_PARALLEL_MERGE_SERIAL_ELEMENT_THRESHOLD`, such that the genuinely
// parallel code path of the merge is taken.
constexpr size_t NUM_ROWS_PARALLEL_MERGE = 200'000;
constexpr size_t EXPECTED_NUM_RUNS_PARALLEL_MERGE = 13;
constexpr size_t BLOCKSIZE_OUTPUT_PARALLEL_MERGE = 10'000;

// The result of `sortWithParallelism` below: the sorted table together with the
// number of Boost.Asio handlers that the threads of the merge executor have
// executed, and the number of those threads that ran at least one handler. The
// latter two are the observable trace of the merge on that executor, and hence
// the way to assert that the genuinely parallel code path (which is the only
// one that touches the executor at all) was really taken.
struct SortResultWithExecutorStatistics {
  CopyableIdTable<0> table_;
  size_t numHandlers_;
  size_t numBusyThreads_;
};

// Sort the `input` with a `CompressedExternalIdTableSorter` whose merge phase
// runs on an `io_context` with `numThreads` threads, and return the sorted
// result together with the statistics of that `io_context`.
SortResultWithExecutorStatistics sortWithParallelism(
    const IdTable& input, size_t numThreads, const std::string& filename) {
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  net::io_context ioContext;
  // NOTE: The `work_guard` keeps the threads alive while the sorter is still
  // being filled, i.e. while the `io_context` has no work at all yet.
  auto workGuard = net::make_work_guard(ioContext);
  std::atomic<size_t> numHandlers{0};
  std::atomic<size_t> numBusyThreads{0};
  std::vector<ad_utility::JThread> workers;
  for (size_t i = 0; i < numThreads; ++i) {
    workers.emplace_back([&ioContext, &numHandlers, &numBusyThreads] {
      // NOTE: `io_context::run` returns the number of handlers that this thread
      // has executed.
      size_t numHandlersOfThisThread = ioContext.run();
      numHandlers += numHandlersOfThisThread;
      if (numHandlersOfThisThread > 0) {
        ++numBusyThreads;
      }
    });
  }

  CopyableIdTable<0> table{NUM_COLS, ad_utility::testing::makeAllocator()};
  {
    ad_utility::CompressedExternalIdTableSorter<SortByOSP, 0> sorter{
        filename, NUM_COLS, 1_MB, ad_utility::testing::makeAllocator(), 5_kB};
    sorter.setMergeExecutor(ioContext.get_executor(), numThreads);
    for (const auto& row : input) {
      sorter.push(row);
    }
    auto blocks = sorter.getSortedBlocks<0>(BLOCKSIZE_OUTPUT_PARALLEL_MERGE);
    table = idTableFromBlockGenerator(blocks);
  }
  workGuard.reset();
  workers.clear();
  return {std::move(table), numHandlers.load(), numBusyThreads.load()};
}
}  // namespace

// _____________________________________________________________________________
// The same assertions as in `sorterRandomInputs`, but with a merge parallelism
// of one, which gives a deterministic single-threaded reference implementation
// of the merge.
TEST(CompressedExternalIdTable, sorterWithSerialMerge) {
  testExternalSorter<NUM_COLS>(NUM_COLS, 10'000, 10_kB, 1);
  testExternalSorter<NUM_COLS>(NUM_COLS, 1000, 1_MB, 1);
  testExternalSorter<NUM_COLS>(NUM_COLS, 0, 1_MB, 1);

  testExternalSorter<0>(NUM_COLS, 10'000, 10_kB, 1);
  testExternalSorter<0>(NUM_COLS, 1000, 1_MB, 1);
  testExternalSorter<0>(NUM_COLS, 0, 1_MB, 1);
}

// _____________________________________________________________________________
// The parallel merge has to produce exactly the same output as the serial one.
// This holds exactly (and not only up to the order of equal elements), because
// `SortByOSP` compares all four columns and is therefore a total order.
TEST(CompressedExternalIdTable, sorterParallelMatchesSerial) {
  std::string serialFilename = gtestCurrentTestName() + ".serial.dat";
  std::string parallelFilename = gtestCurrentTestName() + ".parallel.dat";
  absl::Cleanup cleanup = [&serialFilename, &parallelFilename] {
    ad_utility::deleteFile(serialFilename, false);
    ad_utility::deleteFile(parallelFilename, false);
  };
  IdTable input =
      createRandomlyFilledIdTable(NUM_ROWS_PARALLEL_MERGE, NUM_COLS);

  auto serial = sortWithParallelism(input, 1, serialFilename);
  auto parallel = sortWithParallelism(input, 8, parallelFilename);

  // A merge parallelism of one never touches the executor at all, while the
  // parallel merge schedules a lot of work on it.
  EXPECT_EQ(serial.numHandlers_, 0u);
  EXPECT_GT(parallel.numHandlers_, 0u);
  ASSERT_EQ(serial.table_.numRows(), input.numRows());
  EXPECT_THAT(parallel.table_, ::testing::ElementsAreArray(serial.table_));
}

// _____________________________________________________________________________
// Merge an input with many presorted runs via the genuinely parallel code path
// and check that the result is exactly the sorted input.
TEST(CompressedExternalIdTable, sorterManyRunsParallel) {
  std::string filename = gtestCurrentTestName() + ".dat";
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename, false);
  };
  IdTable input =
      createRandomlyFilledIdTable(NUM_ROWS_PARALLEL_MERGE, NUM_COLS);
  CopyableIdTable<0> expected{NUM_COLS, ad_utility::testing::makeAllocator()};
  for (const auto& row : input) {
    expected.push_back(row);
  }
  ql::ranges::sort(expected, SortByOSP{});

  auto result = sortWithParallelism(input, 8, filename);

  // The input is large enough to be split into many presorted runs, so the
  // merge really has to merge more than a handful of runs.
  ASSERT_GE(EXPECTED_NUM_RUNS_PARALLEL_MERGE, 8u);
  // Only the parallel code path of the merge schedules anything on the
  // executor, and it really does so on more than one of its threads.
  EXPECT_GT(result.numHandlers_, 1u);
  EXPECT_GT(result.numBusyThreads_, 1u);
  ASSERT_EQ(result.table_.numRows(), input.numRows());
  EXPECT_TRUE(ql::ranges::is_sorted(result.table_, SortByOSP{}));
  EXPECT_THAT(result.table_, ::testing::ElementsAreArray(expected));
}

// _____________________________________________________________________________
// The merge phase spills its output blocks to a temporary file of its own, so
// that a chunk that has run ahead of the consumer can be merged to completion
// instead of suspending its producer, see
// `CompressedExternalIdTableSorter::makeBlockStorageFactory`. Check that this
// file is really written to and that it is deleted again afterwards.
TEST(CompressedExternalIdTable, sorterSpillsOutputBlocksToDisk) {
  std::string filename = gtestCurrentTestName() + ".dat";
  // The name of the file of the first merge phase, see
  // `CompressedExternalIdTableSorter::makeSpillFilename`.
  std::string spillFilename = filename + ".merge-spill.0";
  absl::Cleanup cleanup = [&filename, &spillFilename] {
    ad_utility::deleteFile(filename, false);
    ad_utility::deleteFile(spillFilename, false);
  };
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  IdTable input =
      createRandomlyFilledIdTable(NUM_ROWS_PARALLEL_MERGE, NUM_COLS);

  net::io_context ioContext;
  // NOTE: The `work_guard` keeps the threads alive while the sorter is still
  // being filled, i.e. while the `io_context` has no work at all yet.
  auto workGuard = net::make_work_guard(ioContext);
  std::vector<ad_utility::JThread> workers;
  for (size_t i = 0; i < 8; ++i) {
    workers.emplace_back([&ioContext] { ioContext.run(); });
  }

  CopyableIdTable<0> table{NUM_COLS, ad_utility::testing::makeAllocator()};
  {
    ad_utility::CompressedExternalIdTableSorter<SortByOSP, 0> sorter{
        filename, NUM_COLS, 1_MB, ad_utility::testing::makeAllocator(), 5_kB};
    sorter.setMergeExecutor(ioContext.get_executor(), 8);
    for (const auto& row : input) {
      sorter.push(row);
    }
    // Deliberately small output blocks, such that a single chunk produces
    // several of them and therefore has to spill, because only
    // `MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK` of them stay in memory.
    auto blocks = sorter.getSortedBlocks<0>(1000);
    // The storage creates its file while the merge is set up, which happens
    // before the consumer has pulled a single block.
    EXPECT_TRUE(ql::filesystem::exists(spillFilename));
    // The chunks that this thread does not consume yet run ahead and spill, so
    // the file grows although nothing is consumed here. Poll for that, because
    // it happens on the threads of the merge executor.
    bool wasWrittenTo = false;
    for (size_t i = 0; i < 1000 && !wasWrittenTo; ++i) {
      wasWrittenTo = ql::filesystem::file_size(spillFilename) > 0;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(wasWrittenTo);
    table = idTableFromBlockGenerator(blocks);
  }
  workGuard.reset();
  workers.clear();
  // The storage deletes its file when the merge is destroyed.
  EXPECT_FALSE(ql::filesystem::exists(spillFilename));
  ASSERT_EQ(table.numRows(), input.numRows());
  EXPECT_TRUE(ql::ranges::is_sorted(table, SortByOSP{}));
}

namespace {
// The sorted result of a single merge, together with the largest size that the
// spill file of the merge phase was observed to have while the merge ran. That
// file is append-only, so that size is its peak, see
// `CompressedIdTableBlockStorage`.
struct SortResultWithSpillFileSize {
  CopyableIdTable<0> table_;
  size_t spillFileSize_;
};

// Sort the `input` with a merge phase that runs on eight threads and that
// stores the output blocks it spills with the given `compression`, see
// `CompressedExternalIdTableSorter::setMergeSpillCompression`.
SortResultWithSpillFileSize sortWithSpillCompression(
    const IdTable& input,
    ad_utility::CompressedBlockFile::Compression compression,
    const std::string& filename, const std::string& spillFilename) {
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  net::io_context ioContext;
  // NOTE: The `work_guard` keeps the threads alive while the sorter is still
  // being filled, i.e. while the `io_context` has no work at all yet.
  auto workGuard = net::make_work_guard(ioContext);
  std::vector<ad_utility::JThread> workers;
  for (size_t i = 0; i < 8; ++i) {
    workers.emplace_back([&ioContext] { ioContext.run(); });
  }

  // The current size of the spill file, or zero if it doesn't exist. It is
  // created when the merge starts and deleted as soon as the merge is done, so
  // it has to be sampled while the merge runs.
  auto currentSpillFileSize = [&spillFilename]() {
    ql::error_code errorCode;
    auto size = ql::filesystem::file_size(spillFilename, errorCode);
    return errorCode ? size_t{0} : static_cast<size_t>(size);
  };

  CopyableIdTable<0> table{NUM_COLS, ad_utility::testing::makeAllocator()};
  size_t spillFileSize = 0;
  {
    ad_utility::CompressedExternalIdTableSorter<SortByOSP, 0> sorter{
        filename, NUM_COLS, 1_MB, ad_utility::testing::makeAllocator(), 5_kB};
    sorter.setMergeExecutor(ioContext.get_executor(), 8);
    sorter.setMergeSpillCompression(compression);
    for (const auto& row : input) {
      sorter.push(row);
    }
    // Deliberately small output blocks, such that a single chunk produces
    // several of them and therefore has to spill, see
    // `sorterSpillsOutputBlocksToDisk`.
    for (const auto& block : sorter.getSortedBlocks<0>(1000)) {
      for (const auto& row : block) {
        table.push_back(row);
      }
      spillFileSize = std::max(spillFileSize, currentSpillFileSize());
    }
  }
  workGuard.reset();
  workers.clear();
  return {std::move(table), spillFileSize};
}
}  // namespace

// _____________________________________________________________________________
// The compression with which the merge phase stores the output blocks that it
// spills is a pure trade-off between CPU and bytes on disk, so it must not
// change the result in any way. Check that, and that it really is applied.
TEST(CompressedExternalIdTable, sorterMergeSpillCompression) {
  std::string filename = gtestCurrentTestName() + ".dat";
  // Each of the three sorters below is a fresh one, so each of them spills its
  // first (and only) merge phase to this file, see
  // `CompressedExternalIdTableSorter::makeSpillFilename`.
  std::string spillFilename = filename + ".merge-spill.0";
  absl::Cleanup cleanup = [&filename, &spillFilename] {
    ad_utility::deleteFile(filename, false);
    ad_utility::deleteFile(spillFilename, false);
  };
  // The columns hold few distinct values, such that the spilled blocks are
  // highly compressible and the file sizes below differ clearly.
  std::vector<JoinColumnAndBounds> bounds;
  for (size_t columnIdx = 0; columnIdx < NUM_COLS; ++columnIdx) {
    bounds.push_back(JoinColumnAndBounds{columnIdx, 0, 20});
  }
  IdTable input =
      createRandomlyFilledIdTable(NUM_ROWS_PARALLEL_MERGE, NUM_COLS, bounds);

  auto uncompressed = sortWithSpillCompression(
      input, ad_utility::NO_BLOCK_COMPRESSION, filename, spillFilename);
  auto compressed = sortWithSpillCompression(
      input, ad_utility::ZSTD_DEFAULT_LEVEL, filename, spillFilename);
  auto fast = sortWithSpillCompression(input, 1, filename, spillFilename);

  // Whatever the compression, the merge really did spill, and the result is
  // exactly the sorted input.
  ASSERT_EQ(uncompressed.table_.numRows(), input.numRows());
  EXPECT_TRUE(ql::ranges::is_sorted(uncompressed.table_, SortByOSP{}));
  EXPECT_THAT(compressed.table_,
              ::testing::ElementsAreArray(uncompressed.table_));
  EXPECT_THAT(fast.table_, ::testing::ElementsAreArray(uncompressed.table_));
  EXPECT_GT(compressed.spillFileSize_, 0u);
  EXPECT_GT(fast.spillFileSize_, 0u);

  // The uncompressed spill file is what pays for the CPU that is saved.
  EXPECT_GT(uncompressed.spillFileSize_, compressed.spillFileSize_);
  EXPECT_GT(uncompressed.spillFileSize_, fast.spillFileSize_);
}

// _____________________________________________________________________________
// If the memory limit only permits a single in-flight chunk, then the merge
// still works, but a warning is logged.
TEST(CompressedExternalIdTable, sorterReducedParallelismWarning) {
  std::string filename = gtestCurrentTestName() + ".dat";
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename, false);
  };
  // The following values are chosen such that (with 4 columns) exactly two
  // presorted runs are created, and such that
  // `computeMergePhaseParameters` ends up with a single chunk in flight without
  // throwing: the input blocks of a single chunk cost
  // `2 * 4 * 250'000 = 2 MB`, so two concurrent chunks leave
  // `(6 - 4) MB / (4 + 3 * 2)` per output block, which is far below
  // `MIN_MERGE_PHASE_OUTPUT_BLOCK_SIZE`, whereas a single chunk still leaves
  // `(6 - 2) MB / (4 + 3) = 571 kB`, which is above the hard floor of
  // `MIN_USABLE_MERGE_PHASE_OUTPUT_BLOCK_SIZE` rows.
  const auto memory = ad_utility::MemorySize::bytes(6'000'000);
  const auto blocksizeCompression = ad_utility::MemorySize::bytes(250'000);
  // One run holds `6'000'000 / (4 * 8 * 2) = 93'750` rows, so the following
  // number of rows yields two runs.
  constexpr size_t numRows = 125'000;

  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = false;
  ad_utility::CompressedExternalIdTableSorter<SortByOSP, 0> sorter{
      filename, NUM_COLS, memory, ad_utility::testing::makeAllocator(),
      blocksizeCompression};
  net::thread_pool pool{8};
  sorter.setMergeExecutor(pool.get_executor(), 8);
  IdTable input = createRandomlyFilledIdTable(numRows, NUM_COLS);
  for (const auto& row : input) {
    sorter.push(row);
  }

  CopyableIdTable<0> result{NUM_COLS, ad_utility::testing::makeAllocator()};
  std::string logOutput;
  {
    auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
    auto blocks = sorter.getSortedBlocks<0>();
    result = idTableFromBlockGenerator(blocks);
    logOutput = logStream.str();
  }
  EXPECT_THAT(logOutput,
              ::testing::ContainsRegex(
                  "merge phase of the external sorter can only merge 1 chunks "
                  "concurrently instead of the 8 chunks"));
  EXPECT_EQ(result.numRows(), numRows);
  EXPECT_TRUE(ql::ranges::is_sorted(result, SortByOSP{}));
  pool.join();
}
