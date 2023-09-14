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

// From a `generator` that yields  IdTable`s, create a single `IdTable` that is
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

TEST(IdTableCompressedWriter, firstTest) {
  using namespace ad_utility::memory_literals;
  std::string filename = "idTableCompressedWriterFirstTest.dat";
  IdTableCompressedWriter writer{filename, 3,
                                 ad_utility::testing::makeAllocator()};
  writer.blockSizeCompression() = 48_B;
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
void testExternalSorter(size_t numDynamicColumns) {
  std::string filename = "idTableCompressedSorter.firstTest.dat";

  EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  // TODO<joka921> also test the static case.
  ExternalIdTableSorter<SortByOPS, NumStaticColumns> writer{
      filename, numDynamicColumns, 100'000,
      ad_utility::testing::makeAllocator()};

  CopyableIdTable<NumStaticColumns> randomTable =
      createRandomlyFilledIdTable(100'000, numDynamicColumns)
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
}

TEST(IdTableCompressedSorter, testRandomInput) {
  testExternalSorter<0>(3);
  testExternalSorter<3>(3);
}

TEST(IdTableCompressedSorter, memoryLimit) {
  std::string filename = "idTableCompressedSorter.firstTest.dat";

  EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  // only 100 bytes of memory, not sufficient for merging
  EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = false;
  ExternalIdTableSorter<SortByOPS, 0> writer{
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
