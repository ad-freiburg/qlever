// Copyright 2023 - 2025 The QLever Authors, in particular:
//
// 2023 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../../util/AllocatorTestHelpers.h"
#include "../../util/GTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/ExternalSortFunctors.h"
#include "util/ConstexprUtils.h"

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
void testExternalSorterImpl(size_t numDynamicColumns, size_t numRows,
                            ad_utility::MemorySize memoryToUse,
                            bool mergeMultipleTimes,
                            source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto tr = generateLocationTrace(l);
  std::string filename = "idTableCompressedSorter.testExternalSorter.dat";
  using namespace ad_utility::memory_literals;

  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  ad_utility::CompressedExternalIdTableSorter<SortByOSP, NumStaticColumns>
      writer{filename, numDynamicColumns, memoryToUse,
             ad_utility::testing::makeAllocator(), 5_kB};

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
                        source_location l = AD_CURRENT_SOURCE_LOC()) {
  testExternalSorterImpl<NumStaticColumns>(numDynamicColumns, numRows,
                                           memoryToUse, true, l);
  testExternalSorterImpl<NumStaticColumns>(numDynamicColumns, numRows,
                                           memoryToUse, false, l);
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
  ad_utility::CompressedExternalIdTable<0> writer{
      filename, NUM_COLS, ad_utility::memoryForBlocksize(blockSize, NUM_COLS),
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

// `memoryForBlocksize` and `blocksizeForMemory` are inverses of each other, so
// the tests below can specify the number of rows per block instead of a memory
// limit.
// _____________________________________________________________________________
TEST(CompressedExternalIdTable, blocksizeAndMemoryAreInverses) {
  for (size_t numColumns : {1u, 2u, 3u, 7u}) {
    for (size_t blocksize : {1u, 2u, 6u, 1000u}) {
      auto memory = ad_utility::memoryForBlocksize(blocksize, numColumns);
      EXPECT_EQ(ad_utility::blocksizeForMemory(memory, numColumns), blocksize);
    }
  }
}

namespace {

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
  auto memory = ad_utility::memoryForBlocksize(blocksize, NUM_COLS);

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
    auto memory = ad_utility::memoryForBlocksize(blocksize, NUM_COLS);
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
  auto memory = ad_utility::memoryForBlocksize(blocksize, NUM_COLS);

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
  auto memory = ad_utility::memoryForBlocksize(blocksize, NUM_COLS);

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
  auto memory = ad_utility::memoryForBlocksize(blocksize, NUM_COLS);

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
    auto memory = ad_utility::memoryForBlocksize(blocksize, NUM_COLS);
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
