//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../../util/AllocatorTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "engine/idTable/IdTableCompressedWriter.h"

class CopyableIdTable : public IdTable {
 public:
  using IdTable::IdTable;
  CopyableIdTable(const CopyableIdTable& rhs) : IdTable{rhs.clone()} {}
  CopyableIdTable& operator=(const CopyableIdTable& rhs) {
    static_cast<IdTable&>(*this) = rhs.clone();
    return *this;
  }
};

auto idTableFromGenerator =
    [](auto& generator) ->CopyableIdTable {
      CopyableIdTable result(ad_utility::testing::makeAllocator());
      for (const auto& block : generator) {
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

TEST(IdTableCompressedWriter, firstTest) {
  std::string filename = "idTableCompressedWriterFirstTest.dat";
  IdTableCompressedWriter writer{filename, 3,
                                 ad_utility::testing::makeAllocator()};
  std::vector<CopyableIdTable> tables;
  tables.push_back(makeIdTableFromVector({{2, 4, 7}, {3, 6, 8}, {4, 3, 2}}));
  tables.push_back(makeIdTableFromVector({{2, 3, 7}, {3, 6, 8}, {4, 2, 123}}));
  tables.push_back(makeIdTableFromVector({{0, 4, 7}}));

  for (const auto& table : tables) {
    writer.writeIdTable(table);
  }

  auto generators = writer.getAllGenerators();
  ASSERT_EQ(generators.size(), tables.size());

  using namespace ::testing;
  std::vector<CopyableIdTable> result;
  auto tr = std::ranges::transform_view(generators, idTableFromGenerator);
  std::ranges::copy(tr, std::back_inserter(result));
  ASSERT_THAT(result, ElementsAreArray( tables));

}
TEST(IdTableCompressedSorter, firstTest) {
    std::string filename = "idTableCompressedSorter.firstTest.dat";
    [[maybe_unused]] auto firstCol = [](const auto& a, const auto& b) {
      return a[0] < b[0];
    };
    ExternalIdTableSorter<decltype(firstCol)> writer{filename, 3,
                                   ad_utility::testing::makeAllocator()};
    std::vector<CopyableIdTable> tables;
    tables.push_back(makeIdTableFromVector({{2, 4, 7}, {3, 6, 8}, {4, 3, 2}}));
    tables.push_back(makeIdTableFromVector({{2, 3, 7}, {3, 6, 8}, {4, 2, 123}}));
    tables.push_back(makeIdTableFromVector({{0, 4, 7}}));

    for (const auto& table : tables) {
        writer.pushBlock(table.clone());
    }

    auto generator = writer.sortedView();
    // TODO<joka921> First make it compile, then make it correct.

    using namespace ::testing;
    auto result = idTableFromGenerator(generator);
    ASSERT_THAT(result, Eq(tables.at(0)));
}
