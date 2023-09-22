//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../../util/AllocatorTestHelpers.h"
#include "../../util/GTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "engine/idTable/IdTableCompressedWriter.h"
#include "index/StxxlSortFunctors.h"

// Implementation of a class that inherits from `IdTable` but is copyable
// (convenient for testing).
template <size_t N = 0>
using TableImpl = std::conditional_t<N == 0, IdTable, IdTableStatic<N>>;
template <size_t N = 0>
class CopyableIdTable : public TableImpl<N> {
 public:
  using Base = TableImpl<N>;
  using Base::Base;
  CopyableIdTable(const CopyableIdTable& rhs) : Base{rhs.clone()} {}
  CopyableIdTable& operator=(const CopyableIdTable& rhs) {
    static_cast<Base&>(*this) = rhs.clone();
    return *this;
  }
};

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

TEST(IdTableCompressedWriter, compressedWriterTest) {
  using namespace ad_utility::memory_literals;
  std::string filename = "idTableCompressedWriter.compressedWriterTest.dat";
  ad_utility::IdTableCompressedWriter writer{
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
void testExternalSorter(size_t numDynamicColumns, size_t numRows,
                        ad_utility::MemorySize memoryToUse) {
  std::string filename = "idTableCompressedSorter.testExternalSorter.dat";
  using namespace ad_utility::memory_literals;

  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  ad_utility::ExternalIdTableSorter<SortByOPS, NumStaticColumns> writer{
      filename, numDynamicColumns, memoryToUse.getBytes(),
      ad_utility::testing::makeAllocator(), 5_kB};

  for (size_t i = 0; i < 2; ++i) {
    CopyableIdTable<NumStaticColumns> randomTable =
        createRandomlyFilledIdTable(numRows, numDynamicColumns)
            .toStatic<NumStaticColumns>();

    for (const auto& row : randomTable) {
      writer.push(row);
    }

    std::ranges::sort(randomTable, SortByOPS{});

    auto generator = writer.sortedView();

    using namespace ::testing;
    auto result =
        idTableFromRowGenerator<NumStaticColumns>(generator, numDynamicColumns);
    ASSERT_THAT(result, Eq(randomTable));
    writer.clear();
  }
}

TEST(IdTableCompressedSorter, testRandomInput) {
  using namespace ad_utility::memory_literals;
  // Test for dynamic (<0>) and static(<3>) tables.
  // Test the case that there are multiple blocks to merge (many rows but a low
  // memory limit), but also the case that there is a
  testExternalSorter<0>(3, 10'000, 10_kB);
  testExternalSorter<0>(3, 1000, 1_MB);
  testExternalSorter<3>(3, 10'000, 10_kB);
  testExternalSorter<3>(3, 1000, 1_MB);
}

TEST(IdTableCompressedSorter, memoryLimit) {
  std::string filename = "idTableCompressedSorter.memoryLimit.dat";

  // only 100 bytes of memory, not sufficient for merging
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = false;
  ad_utility::ExternalIdTableSorter<SortByOPS, 0> writer{
      filename, 3, 100, ad_utility::testing::makeAllocator()};

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
      filename, numDynamicColumns, memoryToUse.getBytes(),
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

TEST(ExternalIdTableCompressor, testRandomInput) {
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
