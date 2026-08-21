// Copyright 2023 - 2025 The QLever Authors, in particular:
//
// 2023 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../../util/AllocatorTestHelpers.h"
#include "../../util/GTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/ExternalSortFunctors.h"
#include "util/ConstexprUtils.h"
#include "util/jthread.h"

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
    ad_utility::parallelBlockMerge::SharedMergeScheduler scheduler = nullptr,
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
  if (scheduler != nullptr) {
    writer.setMergeScheduler(std::move(scheduler));
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
void testExternalSorter(
    size_t numDynamicColumns, size_t numRows,
    ad_utility::MemorySize memoryToUse,
    ad_utility::parallelBlockMerge::SharedMergeScheduler scheduler = nullptr,
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  testExternalSorterImpl<NumStaticColumns>(numDynamicColumns, numRows,
                                           memoryToUse, true, scheduler, l);
  testExternalSorterImpl<NumStaticColumns>(
      numDynamicColumns, numRows, memoryToUse, false, std::move(scheduler), l);
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

// A `MergeScheduler` that counts how many tasks were scheduled and forwards
// them to an owned `TaskQueueMergeScheduler`. It allows to assert that the
// genuinely parallel code path of the merge (which is the only one that
// schedules anything at all) was really taken.
class CountingMergeScheduler
    : public ad_utility::parallelBlockMerge::MergeScheduler {
 private:
  ad_utility::parallelBlockMerge::TaskQueueMergeScheduler scheduler_;
  std::atomic<size_t> numScheduledTasks_{0};

 public:
  explicit CountingMergeScheduler(size_t numThreads, std::string name)
      : scheduler_{numThreads, std::move(name)} {}

  // ___________________________________________________________________________
  void schedule(absl::AnyInvocable<void()> task) override {
    ++numScheduledTasks_;
    scheduler_.schedule(std::move(task));
  }

  // ___________________________________________________________________________
  size_t maxParallelism() const override { return scheduler_.maxParallelism(); }

  // The number of tasks (= chunks of the merge) that were scheduled so far.
  size_t numScheduledTasks() const { return numScheduledTasks_.load(); }
};

// Sort the `input` with a `CompressedExternalIdTableSorter` that uses the given
// `scheduler` for its merge phase, and return the sorted result.
CopyableIdTable<0> sortWithScheduler(
    const IdTable& input,
    ad_utility::parallelBlockMerge::SharedMergeScheduler scheduler,
    const std::string& filename) {
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  ad_utility::CompressedExternalIdTableSorter<SortByOSP, 0> sorter{
      filename, NUM_COLS, 1_MB, ad_utility::testing::makeAllocator(), 5_kB};
  sorter.setMergeScheduler(std::move(scheduler));
  for (const auto& row : input) {
    sorter.push(row);
  }
  auto blocks = sorter.getSortedBlocks<0>(BLOCKSIZE_OUTPUT_PARALLEL_MERGE);
  return idTableFromBlockGenerator(blocks);
}
}  // namespace

// _____________________________________________________________________________
// The same assertions as in `sorterRandomInputs`, but with an
// `InlineMergeScheduler`, which gives a deterministic single-threaded
// reference implementation of the merge.
TEST(CompressedExternalIdTable, sorterWithInlineScheduler) {
  auto makeScheduler = []() {
    return std::make_shared<
        ad_utility::parallelBlockMerge::InlineMergeScheduler>();
  };
  testExternalSorter<NUM_COLS>(NUM_COLS, 10'000, 10_kB, makeScheduler());
  testExternalSorter<NUM_COLS>(NUM_COLS, 1000, 1_MB, makeScheduler());
  testExternalSorter<NUM_COLS>(NUM_COLS, 0, 1_MB, makeScheduler());

  testExternalSorter<0>(NUM_COLS, 10'000, 10_kB, makeScheduler());
  testExternalSorter<0>(NUM_COLS, 1000, 1_MB, makeScheduler());
  testExternalSorter<0>(NUM_COLS, 0, 1_MB, makeScheduler());
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

  auto serial = sortWithScheduler(
      input,
      std::make_shared<ad_utility::parallelBlockMerge::InlineMergeScheduler>(),
      serialFilename);
  auto parallel = sortWithScheduler(
      input,
      std::make_shared<ad_utility::parallelBlockMerge::TaskQueueMergeScheduler>(
          8, "sorterParallelMatchesSerial"),
      parallelFilename);

  ASSERT_EQ(serial.numRows(), input.numRows());
  EXPECT_THAT(parallel, ::testing::ElementsAreArray(serial));
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

  auto scheduler =
      std::make_shared<CountingMergeScheduler>(8, "sorterManyRunsParallel");
  auto result = sortWithScheduler(input, scheduler, filename);

  // The input is large enough to be split into many presorted runs, so the
  // merge really has to merge more than a handful of runs.
  ASSERT_GE(EXPECTED_NUM_RUNS_PARALLEL_MERGE, 8u);
  // Only the parallel code path of the merge schedules tasks at all, and it
  // schedules exactly one task per chunk.
  EXPECT_GT(scheduler->numScheduledTasks(), 1u);
  ASSERT_EQ(result.numRows(), input.numRows());
  EXPECT_TRUE(ql::ranges::is_sorted(result, SortByOSP{}));
  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
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
  // presorted runs are created, that `computeBlockSizeForMergePhase` does not
  // throw, and that a single in-flight chunk (2 MB of input blocks plus 0.5 MB
  // of output block) already occupies more than half of the memory limit.
  const auto memory = ad_utility::MemorySize::bytes(4'000'000);
  const auto blocksizeCompression = ad_utility::MemorySize::bytes(250'000);
  // One run holds `4'000'000 / (4 * 8 * 2) = 62'500` rows, so the following
  // number of rows yields exactly two complete runs and an empty last block.
  constexpr size_t numRows = 125'000;

  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = false;
  ad_utility::CompressedExternalIdTableSorter<SortByOSP, 0> sorter{
      filename, NUM_COLS, memory, ad_utility::testing::makeAllocator(),
      blocksizeCompression};
  sorter.setMergeScheduler(
      std::make_shared<ad_utility::parallelBlockMerge::TaskQueueMergeScheduler>(
          8, "sorterReducedParallelismWarning"));
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
}
