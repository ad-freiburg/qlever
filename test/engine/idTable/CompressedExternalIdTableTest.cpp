//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../../util/AllocatorTestHelpers.h"
#include "../../util/GTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/StxxlSortFunctors.h"

using ad_utility::source_location;
using namespace ad_utility::memory_literals;

namespace {

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
    for (auto i : std::views::iota(0U, numColumns)) {
      decltype(auto) blockCol = block.getColumn(i);
      decltype(auto) resultCol = result.getColumn(i);
      std::ranges::copy(blockCol, resultCol.begin() + size);
    }
  }
  return result;
};

// From a generator that generates rows of an `IdTable`, create an `IdTable`.
// The number of static and dynamic columns has to be specified (see `IdTable.h`
// for details).
template <size_t NumStaticColumns>
auto idTableFromRowGenerator = [](auto& generator, size_t numColumns) {
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
  std::string filename = "idTableCompressedWriter.compressedWriterTest.dat";
  ad_utility::CompressedExternalIdTableWriter writer{
      filename, 3, ad_utility::testing::makeAllocator(), 48_B};
  std::vector<CopyableIdTable<0>> tables;
  tables.push_back(makeIdTableFromVector({{2, 4, 7}, {3, 6, 8}, {4, 3, 2}}));
  tables.push_back(makeIdTableFromVector({{2, 3, 7}, {3, 6, 8}, {4, 2, 123}}));
  tables.push_back(makeIdTableFromVector({{0, 4, 7}}));

  for (const auto& table : tables) {
    writer.writeIdTable(table);
  }

  auto generators = writer.getAllGenerators();
  ASSERT_EQ(generators.size(), tables.size());

  using namespace ::testing;
  std::vector<CopyableIdTable<0>> result;
  auto tr = std::ranges::transform_view(generators, idTableFromBlockGenerator);
  std::ranges::copy(tr, std::back_inserter(result));
  ASSERT_THAT(result, ElementsAreArray(tables));
}

template <size_t NumStaticColumns>
void testExternalSorterImpl(size_t numDynamicColumns, size_t numRows,
                            ad_utility::MemorySize memoryToUse,
                            bool mergeMultipleTimes,
                            source_location l = source_location::current()) {
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

    std::ranges::sort(randomTable, SortByOSP{});

    if (mergeMultipleTimes) {
      writer.moveResultOnMerge() = false;
    }

    for (size_t k = 0; k < 5; ++k) {
      auto generator = writer.sortedView();
      using namespace ::testing;
      if (mergeMultipleTimes || k == 0) {
        auto result = idTableFromRowGenerator<NumStaticColumns>(
            generator, numDynamicColumns);
        ASSERT_THAT(result, Eq(randomTable)) << "k = " << k;
      } else {
        EXPECT_ANY_THROW((idTableFromRowGenerator<NumStaticColumns>(
            generator, numDynamicColumns)));
      }
      // We cannot access or change this value after the first merge.
      EXPECT_ANY_THROW(writer.moveResultOnMerge());
    }
    writer.clear();
  }
}

template <size_t NumStaticColumns>
void testExternalSorter(size_t numDynamicColumns, size_t numRows,
                        ad_utility::MemorySize memoryToUse,
                        source_location l = source_location::current()) {
  testExternalSorterImpl<NumStaticColumns>(numDynamicColumns, numRows,
                                           memoryToUse, true, l);
  testExternalSorterImpl<NumStaticColumns>(numDynamicColumns, numRows,
                                           memoryToUse, false, l);
}

TEST(CompressedExternalIdTable, sorterRandomInputs) {
  using namespace ad_utility::memory_literals;
  // Test for dynamic (<0>) and static(<3>) tables.
  // Test the case that there are multiple blocks to merge (many rows but a low
  // memory limit), but also the case that there is a
  testExternalSorter<0>(3, 10'000, 10_kB);
  testExternalSorter<0>(3, 1000, 1_MB);
  testExternalSorter<3>(3, 10'000, 10_kB);
  testExternalSorter<3>(3, 1000, 1_MB);
}

TEST(CompressedExternalIdTable, sorterMemoryLimit) {
  std::string filename = "idTableCompressedSorter.memoryLimit.dat";

  // only 100 bytes of memory, not sufficient for merging
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = false;
  ad_utility::CompressedExternalIdTableSorter<SortByOSP, 0> writer{
      filename, 3, 100_B, ad_utility::testing::makeAllocator()};

  CopyableIdTable<0> randomTable = createRandomlyFilledIdTable(100, 3);

  // Pushing always works
  for (const auto& row : randomTable) {
    writer.push(row);
  }

  auto generator = writer.sortedView();
  AD_EXPECT_THROW_WITH_MESSAGE((idTableFromRowGenerator<0>(generator, 3)),
                               ::testing::ContainsRegex("Insufficient memory"));
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

  // Only creating and then destroying a generator again does not prevent
  // pushing.
  { [[maybe_unused]] auto generator = writer.getRows(); }
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

  ad_utility::CompressedExternalIdTableSorter<SortByOSP, 3> writer{filename, 3,
                                                                   10_B, alloc};
  ad_utility::CompressedExternalIdTableSorterTypeErased& erased = writer;
  IdTableStatic<0> t1{3, alloc};
  EXPECT_NO_THROW(erased.pushBlock(t1));
  EXPECT_NO_THROW(t1.setNumColumns(4));
  EXPECT_ANY_THROW(erased.pushBlock(t1));
}
