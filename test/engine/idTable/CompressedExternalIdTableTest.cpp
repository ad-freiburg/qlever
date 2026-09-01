// Copyright 2023 - 2025 The QLever Authors, in particular:
//
// 2023 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
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

// The spill files of a merge phase that are currently on disk: their number and
// their total size. Every chunk spills to a file of its own whose name starts
// with the given `prefix`, and that file is deleted as soon as the chunk has
// been fully consumed, see `CompressedIdTableBlockStorage::spillFilename`.
//
// NOTE: This tolerates a file that vanishes between being listed and being
// measured, because the merge deletes those files while this runs.
struct SpillFiles {
  size_t numFiles_ = 0;
  size_t totalSize_ = 0;
};
SpillFiles currentSpillFiles(const std::string& prefix) {
  SpillFiles result;
  ql::filesystem::path prefixAsPath{prefix};
  auto directory = prefixAsPath.parent_path();
  std::string base = prefixAsPath.filename().string();
  ql::error_code errorCode;
  for (const auto& entry : ql::filesystem::directory_iterator{
           directory.empty() ? ql::filesystem::path{"."} : directory,
           errorCode}) {
    if (entry.path().filename().string().rfind(base, 0) != 0) {
      continue;
    }
    auto size = ql::filesystem::file_size(entry.path(), errorCode);
    if (!errorCode) {
      ++result.numFiles_;
      result.totalSize_ += static_cast<size_t>(size);
    }
  }
  return result;
}

// Delete every spill file that starts with the given `prefix`, for the case
// that a test failed before the merge could clean up after itself.
void deleteSpillFiles(const std::string& prefix) {
  ql::filesystem::path prefixAsPath{prefix};
  auto directory = prefixAsPath.parent_path();
  std::string base = prefixAsPath.filename().string();
  ql::error_code errorCode;
  std::vector<ql::filesystem::path> paths;
  for (const auto& entry : ql::filesystem::directory_iterator{
           directory.empty() ? ql::filesystem::path{"."} : directory,
           errorCode}) {
    if (entry.path().filename().string().rfind(base, 0) == 0) {
      paths.push_back(entry.path());
    }
  }
  for (const auto& path : paths) {
    ql::filesystem::remove(path, errorCode);
  }
}

// _____________________________________________________________________________
// The merge phase spills its output blocks to a temporary file of its own, so
// that a chunk that has run ahead of the consumer can be merged to completion
// instead of suspending its producer, see
// `CompressedExternalIdTableSorter::makeBlockStorageFactory`. Check that this
// file is really written to and that it is deleted again afterwards.
TEST(CompressedExternalIdTable, sorterSpillsOutputBlocksToDisk) {
  std::string filename = gtestCurrentTestName() + ".dat";
  // The common prefix of the spill files of the first merge phase, see
  // `CompressedExternalIdTableSorter::makeSpillFilename`.
  std::string spillPrefix = filename + ".merge-spill.0";
  absl::Cleanup cleanup = [&filename, &spillPrefix] {
    ad_utility::deleteFile(filename, false);
    deleteSpillFiles(spillPrefix);
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
    // A spill file is created with the first block that its chunk spills, so
    // there is none before the merge has produced anything. The chunks that
    // this thread does not consume yet run ahead and spill, so files appear
    // although nothing is consumed here. Poll for that, because it happens on
    // the threads of the merge executor.
    SpillFiles spilled;
    for (size_t i = 0; i < 1000 && spilled.totalSize_ == 0; ++i) {
      spilled = currentSpillFiles(spillPrefix);
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_GT(spilled.numFiles_, 0u);
    EXPECT_GT(spilled.totalSize_, 0u);
    table = idTableFromBlockGenerator(blocks);
    // Every chunk that was fully consumed had its file deleted, so nothing is
    // left over even though neither the merge nor the sorter is destroyed yet.
    EXPECT_EQ(currentSpillFiles(spillPrefix).numFiles_, 0u);
  }
  workGuard.reset();
  workers.clear();
  EXPECT_EQ(currentSpillFiles(spillPrefix).numFiles_, 0u);
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
    const std::string& filename, const std::string& spillPrefix) {
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  net::io_context ioContext;
  // NOTE: The `work_guard` keeps the threads alive while the sorter is still
  // being filled, i.e. while the `io_context` has no work at all yet.
  auto workGuard = net::make_work_guard(ioContext);
  std::vector<ad_utility::JThread> workers;
  for (size_t i = 0; i < 8; ++i) {
    workers.emplace_back([&ioContext] { ioContext.run(); });
  }

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
      // The spill files exist only while their chunk is in flight, so their
      // total size has to be sampled while the merge runs.
      spillFileSize =
          std::max(spillFileSize, currentSpillFiles(spillPrefix).totalSize_);
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
  // first (and only) merge phase to files with this prefix, see
  // `CompressedExternalIdTableSorter::makeSpillFilename`.
  std::string spillPrefix = filename + ".merge-spill.0";
  absl::Cleanup cleanup = [&filename, &spillPrefix] {
    ad_utility::deleteFile(filename, false);
    deleteSpillFiles(spillPrefix);
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
      input, ad_utility::NO_BLOCK_COMPRESSION, filename, spillPrefix);
  auto compressed = sortWithSpillCompression(
      input, ad_utility::ZSTD_DEFAULT_LEVEL, filename, spillPrefix);
  auto fast = sortWithSpillCompression(input, 1, filename, spillPrefix);

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

namespace {

// The number of rows per block that a `CompressedExternalIdTableBase` uses for
// the given memory limit and number of columns. This mirrors the formula
// `blocksize_ = memory / (numColumns * sizeof(Id) * 2)` from
// `CompressedExternalIdTable.h`.
size_t blocksizeForMemory(ad_utility::MemorySize memory, size_t numColumns) {
  return memory.getBytes() / (numColumns * sizeof(Id) * 2);
}

// The inverse of `blocksizeForMemory`: the memory limit for which a
// `CompressedExternalIdTableBase` with `numColumns` columns uses exactly
// `blocksize` rows per block.
ad_utility::MemorySize memoryForBlocksize(size_t blocksize, size_t numColumns) {
  return ad_utility::MemorySize::bytes(blocksize * numColumns * sizeof(Id) * 2);
}

// Collect the complete sorted output of the `sorter` into a single `IdTable`.
CopyableIdTable<0> sortedOutput(
    ad_utility::CompressedExternalIdTableSorterTypeErased& sorter) {
  auto blocks = sorter.getSortedOutput();
  return idTableFromBlockGenerator(blocks);
}

// Return a copy of the `table` that is sorted by `SortByOSP`.
CopyableIdTable<0> sortedCopy(const IdTable& table) {
  CopyableIdTable<0> result{table.clone()};
  ql::ranges::sort(result, SortByOSP{});
  return result;
}

// Push the rows of the `table` row by row via `push` into one sorter, and in a
// single call to `pushBlock` into a second, identically configured sorter.
// Then check that both sorters report the same `size()` and yield exactly the
// same sorted output.
template <size_t NumStaticCols>
void testPushBlockEqualsRowWisePush(
    const IdTable& table, ad_utility::MemorySize memoryToUse,
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  auto alloc = ad_utility::testing::makeAllocator();
  using Sorter =
      ad_utility::CompressedExternalIdTableSorter<SortByOSP, NumStaticCols>;

  std::string rowWiseFile =
      absl::StrCat(gtestCurrentTestName(), ".rowWise.dat");
  std::string blockWiseFile =
      absl::StrCat(gtestCurrentTestName(), ".blockWise.dat");
  // Note: The files are already deleted by the destructor of the underlying
  // `CompressedExternalIdTableWriter`, so we don't warn if the deletion fails.
  absl::Cleanup cleanup = [&rowWiseFile, &blockWiseFile] {
    ad_utility::deleteFile(rowWiseFile, false);
    ad_utility::deleteFile(blockWiseFile, false);
  };

  Sorter rowWise{rowWiseFile, table.numColumns(), memoryToUse, alloc};
  Sorter blockWise{blockWiseFile, table.numColumns(), memoryToUse, alloc};

  for (const auto& row : table) {
    rowWise.push(row);
  }
  blockWise.pushBlock(table);

  EXPECT_EQ(rowWise.size(), table.numRows());
  EXPECT_EQ(blockWise.size(), rowWise.size());

  auto rowWiseResult = sortedOutput(rowWise);
  auto blockWiseResult = sortedOutput(blockWise);
  EXPECT_EQ(blockWiseResult.numRows(), rowWiseResult.numRows());
  EXPECT_THAT(blockWiseResult, ::testing::ElementsAreArray(rowWiseResult));
  EXPECT_THAT(blockWiseResult, ::testing::ElementsAreArray(sortedCopy(table)));
}

// Push several blocks into a (non-sorting) `CompressedExternalIdTable` via
// `pushBlock` and check that `getRows` yields the rows in exactly the order in
// which they were pushed.
template <size_t NumStaticCols>
void testCompressedExternalIdTablePushBlock(
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto alloc = ad_utility::testing::makeAllocator();
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  // Choose the memory limit such that exactly 6 rows fit into a single block.
  constexpr size_t blocksize = 6;
  auto memory = memoryForBlocksize(blocksize, NUM_COLS);
  ASSERT_EQ(blocksizeForMemory(memory, NUM_COLS), blocksize);

  std::string filename =
      absl::StrCat(gtestCurrentTestName(), ".", NumStaticCols, ".dat");
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename, false);
  };
  ad_utility::CompressedExternalIdTable<NumStaticCols> writer{
      filename, NUM_COLS, memory, alloc};

  // The concatenation of all pushed blocks, in the order in which they were
  // pushed. The block sizes cover the corner cases of an empty block, blocks
  // that are smaller and larger than the `blocksize`, and a block that exactly
  // fills a single block.
  CopyableIdTable<NumStaticCols> expected{NUM_COLS, alloc};
  for (size_t numRows : {4UL, blocksize, 13UL, 1UL, 0UL, 20UL}) {
    IdTable block = createRandomlyFilledIdTable(numRows, NUM_COLS);
    writer.pushBlock(block);
    expected.insertAtEnd(block);
  }
  EXPECT_EQ(writer.size(), expected.numRows());

  auto generator = writer.getRows();
  auto result = idTableFromRowGenerator<NumStaticCols>(generator, NUM_COLS);
  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}
}  // namespace

// _____________________________________________________________________________
TEST(CompressedExternalIdTable, pushBlockEqualsRowWisePush) {
  // Test several memory limits that lead to small blocksizes, including the
  // corner case of a single row per block.
  for (const auto& [blocksize, numRows] :
       std::vector<std::pair<size_t, size_t>>{
           {1, 17}, {2, 17}, {3, 100}, {10, 100}, {64, 500}}) {
    SCOPED_TRACE(
        absl::StrCat("blocksize = ", blocksize, ", numRows = ", numRows));
    auto memory = memoryForBlocksize(blocksize, NUM_COLS);
    ASSERT_EQ(blocksizeForMemory(memory, NUM_COLS), blocksize);
    IdTable table = createRandomlyFilledIdTable(numRows, NUM_COLS);
    // Test the static as well as the dynamic instantiation of the sorter.
    testPushBlockEqualsRowWisePush<NUM_COLS>(table, memory);
    testPushBlockEqualsRowWisePush<0>(table, memory);
  }
}

// _____________________________________________________________________________
TEST(CompressedExternalIdTable, pushBlockBlockBoundaries) {
  auto alloc = ad_utility::testing::makeAllocator();
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  // Choose the memory limit such that exactly 8 rows fit into a single block.
  constexpr size_t blocksize = 8;
  auto memory = memoryForBlocksize(blocksize, NUM_COLS);
  ASSERT_EQ(blocksizeForMemory(memory, NUM_COLS), blocksize);

  std::string filename = absl::StrCat(gtestCurrentTestName(), ".dat");
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename, false);
  };

  auto runTestForNumRows = [&](size_t numRows,
                               source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    SCOPED_TRACE(absl::StrCat("numRows = ", numRows));
    ad_utility::CompressedExternalIdTableSorter<SortByOSP, NUM_COLS> sorter{
        filename, NUM_COLS, memory, alloc};
    IdTable table = createRandomlyFilledIdTable(numRows, NUM_COLS);
    sorter.pushBlock(table);
    EXPECT_EQ(sorter.size(), numRows);
    auto result = sortedOutput(sorter);
    EXPECT_EQ(result.numRows(), numRows);
    EXPECT_THAT(result, ::testing::ElementsAreArray(sortedCopy(table)));
  };

  runTestForNumRows(0);
  runTestForNumRows(1);
  runTestForNumRows(blocksize - 1);
  runTestForNumRows(blocksize);
  runTestForNumRows(blocksize + 1);
  runTestForNumRows(2 * blocksize);
  runTestForNumRows(2 * blocksize + 1);
  runTestForNumRows(5 * blocksize);
  runTestForNumRows(7 * blocksize + 3);
}

// _____________________________________________________________________________
TEST(CompressedExternalIdTable, pushBlockMixedWithSingleRowPushes) {
  auto alloc = ad_utility::testing::makeAllocator();
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  // Choose the memory limit such that exactly 5 rows fit into a single block.
  constexpr size_t blocksize = 5;
  auto memory = memoryForBlocksize(blocksize, NUM_COLS);
  ASSERT_EQ(blocksizeForMemory(memory, NUM_COLS), blocksize);

  std::string filename = absl::StrCat(gtestCurrentTestName(), ".dat");
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename, false);
  };
  ad_utility::CompressedExternalIdTableSorter<SortByOSP, NUM_COLS> sorter{
      filename, NUM_COLS, memory, alloc};

  // All rows that have been pushed so far, in the order in which they were
  // pushed.
  CopyableIdTable<0> allRows{NUM_COLS, alloc};

  // Push `numRows` random rows one by one via `push`.
  auto pushRows = [&](size_t numRows) {
    IdTable table = createRandomlyFilledIdTable(numRows, NUM_COLS);
    for (const auto& row : table) {
      sorter.push(row);
    }
    allRows.insertAtEnd(table);
  };
  // Push a random table with `numRows` rows in a single call to `pushBlock`.
  auto pushTable = [&](size_t numRows) {
    IdTable table = createRandomlyFilledIdTable(numRows, NUM_COLS);
    sorter.pushBlock(table);
    allRows.insertAtEnd(table);
  };

  pushRows(3);
  pushTable(7);
  pushRows(1);
  pushTable(0);
  pushTable(13);
  pushRows(2);
  pushTable(blocksize);
  pushRows(blocksize);
  pushTable(1);

  EXPECT_EQ(sorter.size(), allRows.numRows());
  ql::ranges::sort(allRows, SortByOSP{});
  auto result = sortedOutput(sorter);
  EXPECT_THAT(result, ::testing::ElementsAreArray(allRows));
}

// _____________________________________________________________________________
TEST(CompressedExternalIdTable, pushBlockPreservesOrderInCompressor) {
  // Test the static as well as the dynamic instantiation.
  testCompressedExternalIdTablePushBlock<NUM_COLS>();
  testCompressedExternalIdTablePushBlock<0>();
}

// _____________________________________________________________________________
TEST(CompressedExternalIdTable, pushEmptyBlockIsNoOp) {
  auto alloc = ad_utility::testing::makeAllocator();
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  // Choose the memory limit such that exactly 4 rows fit into a single block.
  constexpr size_t blocksize = 4;
  auto memory = memoryForBlocksize(blocksize, NUM_COLS);
  ASSERT_EQ(blocksizeForMemory(memory, NUM_COLS), blocksize);

  std::string filename = absl::StrCat(gtestCurrentTestName(), ".dat");
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename, false);
  };
  ad_utility::CompressedExternalIdTableSorter<SortByOSP, NUM_COLS> sorter{
      filename, NUM_COLS, memory, alloc};

  // Pushing an empty table doesn't change the state of the sorter at all.
  IdTable emptyTable{NUM_COLS, alloc};
  sorter.pushBlock(emptyTable);
  sorter.pushBlock(emptyTable);
  EXPECT_EQ(sorter.size(), 0U);

  // Pushing after the empty pushes still works. Note that in total fewer than
  // `blocksize` rows are pushed, so no complete block is ever written to disk
  // and the sorter takes its "everything fits into a single block" shortcut. If
  // the empty pushes had created a spurious block, then an internal correctness
  // check in `transformAndPushLastBlock` would fail here.
  IdTable table = createRandomlyFilledIdTable(blocksize - 1, NUM_COLS);
  sorter.pushBlock(table);
  sorter.pushBlock(emptyTable);
  EXPECT_EQ(sorter.size(), blocksize - 1);

  auto result = sortedOutput(sorter);
  EXPECT_EQ(result.numRows(), blocksize - 1);
  EXPECT_THAT(result, ::testing::ElementsAreArray(sortedCopy(table)));
}

// _____________________________________________________________________________
// The block boundaries that are used internally are not directly observable,
// but the error message of the memory check in the merging phase contains the
// number of blocks that have to be merged. Use this to verify that `pushBlock`
// splits its input into exactly the same blocks as repeated calls to `push`.
TEST(CompressedExternalIdTable, pushBlockCreatesSameBlocksAsRowWisePush) {
  auto alloc = ad_utility::testing::makeAllocator();
  // The memory limits below are deliberately too small for the merging phase,
  // s.t. an exception that contains the number of blocks is thrown.
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = false;
  absl::Cleanup restoreFlag = [] {
    ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  };

  std::string filename = absl::StrCat(gtestCurrentTestName(), ".dat");
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename, false);
  };

  using Sorter =
      ad_utility::CompressedExternalIdTableSorter<SortByOSP, NUM_COLS>;
  auto consume = [](Sorter& sorter) {
    auto blocks = sorter.getSortedOutput(std::nullopt);
    return idTableFromBlockGenerator(blocks);
  };

  // Note: `numRows` has to be at least `blocksize`, because otherwise no block
  // at all is written to disk and the merging phase (and with it the memory
  // check) is skipped completely.
  auto runTestForBlocksize = [&](size_t blocksize, size_t numRows,
                                 source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    SCOPED_TRACE(
        absl::StrCat("blocksize = ", blocksize, ", numRows = ", numRows));
    auto memory = memoryForBlocksize(blocksize, NUM_COLS);
    ASSERT_EQ(blocksizeForMemory(memory, NUM_COLS), blocksize);
    // Each time `blocksize` rows have accumulated, a block is written to disk.
    // A possible remainder becomes one additional block.
    size_t expectedNumBlocks = (numRows + blocksize - 1) / blocksize;
    auto matcher = ::testing::ContainsRegex(
        absl::StrCat("merging ", expectedNumBlocks, " blocks"));

    IdTable table = createRandomlyFilledIdTable(numRows, NUM_COLS);
    {
      Sorter rowWise{filename, NUM_COLS, memory, alloc};
      for (const auto& row : table) {
        rowWise.push(row);
      }
      AD_EXPECT_THROW_WITH_MESSAGE(consume(rowWise), matcher);
    }
    {
      Sorter blockWise{filename, NUM_COLS, memory, alloc};
      blockWise.pushBlock(table);
      AD_EXPECT_THROW_WITH_MESSAGE(consume(blockWise), matcher);
    }
  };

  runTestForBlocksize(4, 4);
  runTestForBlocksize(4, 8);
  runTestForBlocksize(4, 9);
  runTestForBlocksize(4, 40);
  runTestForBlocksize(4, 43);
  runTestForBlocksize(1, 20);
  runTestForBlocksize(10, 100);
  runTestForBlocksize(10, 101);
}
