// Copyright 2023 - 2024, University of Freiburg
//  Chair of Algorithms and Data Structures.
//  Authors:
//    2023 Hannah Bast <bast@cs.uni-freiburg.de>
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./util/AllocatorTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "index/CompressedRelation.h"
#include "index/LocatedTriples.h"
#include "index/Permutation.h"

namespace {
auto V = ad_utility::testing::VocabId;
// A default graph used in this test.
int g = 123948;

void addGraphColumn(IdTable& block) {
  block.addEmptyColumn();
  ql::ranges::fill(block.getColumn(block.numColumns() - 1), V(g));
}

auto IT = [](const auto& c1, const auto& c2, const auto& c3, int graph = g) {
  return IdTriple{std::array<Id, 4>{V(c1), V(c2), V(c3), V(graph)}};
};
auto PT = [](const auto& c1, const auto& c2, const auto& c3, int graph = g) {
  return CompressedBlockMetadata::PermutedTriple{V(c1), V(c2), V(c3), V(graph)};
};
auto CBM = [](const auto firstTriple, const auto lastTriple,
              CompressedBlockMetadata::GraphInfo graphs = std::nullopt) {
  size_t dummyBlockIndex = 0;
  return CompressedBlockMetadata{
      {{}, 0, firstTriple, lastTriple, std::move(graphs), false},
      dummyBlockIndex};
};

auto numBlocks =
    [](size_t numBlocks) -> testing::Matcher<const LocatedTriplesPerBlock&> {
  return AD_PROPERTY(LocatedTriplesPerBlock, LocatedTriplesPerBlock::numBlocks,
                     testing::Eq(numBlocks));
};

auto numTriplesTotal =
    [](size_t numTriples) -> testing::Matcher<const LocatedTriplesPerBlock&> {
  return AD_PROPERTY(LocatedTriplesPerBlock, LocatedTriplesPerBlock::numTriples,
                     testing::Eq(numTriples));
};

auto numTriplesInBlock = [](size_t blockIndex,
                            NumAddedAndDeleted insertsAndDeletes)
    -> testing::Matcher<const LocatedTriplesPerBlock&> {
  return testing::ResultOf(
      absl::StrCat(".numTriplesTotal(", std::to_string(blockIndex), ")"),
      [blockIndex](const LocatedTriplesPerBlock& ltpb) {
        return ltpb.numTriples(blockIndex);
      },
      testing::Eq(insertsAndDeletes));
};

auto numTriplesBlockwise =
    [](const ad_utility::HashMap<size_t, NumAddedAndDeleted>&
           numTriplesBlockwise)
    -> testing::Matcher<const LocatedTriplesPerBlock&> {
  auto blockMatchers = ad_utility::transform(
      numTriplesBlockwise,
      [](auto p) -> testing::Matcher<const LocatedTriplesPerBlock&> {
        auto [blockIndex, insertsAndDeletes] = p;
        return numTriplesInBlock(blockIndex, insertsAndDeletes);
      });
  return testing::AllOfArray(blockMatchers);
};

const qlever::KeyOrder keyOrder{0, 1, 2, 3};
}  // namespace

// Fixture with helper functions.
class LocatedTriplesTest : public ::testing::Test {
 protected:
  // Make `LocatedTriplesPerBlock` from a list of `LocatedTriple` objects (the
  // order in which the objects are given does not matter).
  LocatedTriplesPerBlock makeLocatedTriplesPerBlock(
      const std::vector<LocatedTriple>& locatedTriples) {
    LocatedTriplesPerBlock result;
    result.add(locatedTriples);
    return result;
  }
};

// Test the method that counts the number of `LocatedTriple's in a block.
TEST_F(LocatedTriplesTest, numTriplesInBlock) {
  auto locatedTriplesInBlock = [](const size_t blockIndex,
                                  const LocatedTriples& expectedLTs)
      -> testing::Matcher<const LocatedTriplesPerBlock&> {
    return testing::ResultOf(
        absl::StrCat(".map_.at(", std::to_string(blockIndex), ")"),
        [blockIndex](const LocatedTriplesPerBlock& ltpb) {
          return ltpb.map_.at(blockIndex);
        },
        testing::Eq(expectedLTs));
  };

  auto locatedTriplesAre =
      [&locatedTriplesInBlock](
          const ad_utility::HashMap<size_t, LocatedTriples>&
              locatedTriplesBlockwise) {
        auto blockMatchers = ad_utility::transform(
            locatedTriplesBlockwise,
            [&locatedTriplesInBlock](
                auto p) -> testing::Matcher<const LocatedTriplesPerBlock&> {
              auto [blockIndex, expectedLTs] = p;
              return locatedTriplesInBlock(blockIndex, expectedLTs);
            });
        // The macro does not work with templated types.
        using HashMapType = ad_utility::HashMap<size_t, LocatedTriples>;
        return testing::AllOf(
            AD_FIELD(LocatedTriplesPerBlock, map_,
                     AD_PROPERTY(HashMapType, size,
                                 testing::Eq(locatedTriplesBlockwise.size()))),
            testing::AllOfArray(blockMatchers));
      };
  using LT = LocatedTriple;

  std::vector<CompressedBlockMetadata> metadata{
      CBM(PT(5, 1, 1), PT(15, 1, 1)), CBM(PT(15, 1, 2), PT(25, 1, 1)),
      CBM(PT(25, 1, 2), PT(30, 1, 1)), CBM(PT(30, 1, 2), PT(35, 1, 1))};
  // Set up lists of located triples for three blocks.
  auto LT1 = LT{1, IT(10, 1, 0), false};
  auto LT2 = LT{1, IT(10, 2, 1), false};
  auto LT3 = LT{1, IT(11, 3, 0), true};
  auto LT4 = LT{2, IT(20, 4, 0), true};
  auto LT5 = LT{2, IT(21, 5, 0), true};
  auto LT6 = LT{4, IT(30, 6, 0), true};
  auto LT7 = LT{4, IT(32, 7, 0), false};
  auto LT8 = LT{3, IT(25, 5, 0), true};
  auto LT9 = LT{4, IT(31, 6, 1), false};
  auto locatedTriplesPerBlock =
      makeLocatedTriplesPerBlock({LT1, LT2, LT3, LT4, LT5, LT6, LT7});
  locatedTriplesPerBlock.setOriginalMetadata(metadata);

  EXPECT_THAT(locatedTriplesPerBlock, numBlocks(3));
  EXPECT_THAT(locatedTriplesPerBlock, numTriplesTotal(7));
  EXPECT_THAT(locatedTriplesPerBlock,
              numTriplesBlockwise(
                  {{1, {1, 2}}, {2, {2, 0}}, {3, {0, 0}}, {4, {1, 1}}}));
  EXPECT_THAT(locatedTriplesPerBlock,
              locatedTriplesAre(
                  {{1, {LT1, LT2, LT3}}, {2, {LT4, LT5}}, {4, {LT6, LT7}}}));

  auto handles = locatedTriplesPerBlock.add(std::vector{LT8, LT9});

  EXPECT_THAT(locatedTriplesPerBlock, numBlocks(4));
  EXPECT_THAT(locatedTriplesPerBlock, numTriplesTotal(9));
  EXPECT_THAT(locatedTriplesPerBlock,
              numTriplesBlockwise(
                  {{1, {1, 2}}, {2, {2, 0}}, {3, {1, 0}}, {4, {1, 2}}}));
  EXPECT_THAT(locatedTriplesPerBlock,
              locatedTriplesAre({{1, {LT1, LT2, LT3}},
                                 {2, {LT4, LT5}},
                                 {3, {LT8}},
                                 {4, {LT6, LT7, LT9}}}));

  locatedTriplesPerBlock.erase(3, handles[0]);
  locatedTriplesPerBlock.updateAugmentedMetadata();

  EXPECT_THAT(locatedTriplesPerBlock, numBlocks(3));
  EXPECT_THAT(locatedTriplesPerBlock, numTriplesTotal(8));
  EXPECT_THAT(locatedTriplesPerBlock,
              numTriplesBlockwise(
                  {{1, {1, 2}}, {2, {2, 0}}, {3, {0, 0}}, {4, {1, 2}}}));
  EXPECT_THAT(
      locatedTriplesPerBlock,
      locatedTriplesAre(
          {{1, {LT1, LT2, LT3}}, {2, {LT4, LT5}}, {4, {LT6, LT7, LT9}}}));

  // Erasing in a block that does not exist, raises an exception.
  EXPECT_THROW(locatedTriplesPerBlock.erase(100, handles[1]),
               ad_utility::Exception);
  locatedTriplesPerBlock.updateAugmentedMetadata();

  // Nothing changed.
  EXPECT_THAT(locatedTriplesPerBlock, numBlocks(3));
  EXPECT_THAT(locatedTriplesPerBlock, numTriplesTotal(8));
  EXPECT_THAT(locatedTriplesPerBlock,
              numTriplesBlockwise(
                  {{1, {1, 2}}, {2, {2, 0}}, {3, {0, 0}}, {4, {1, 2}}}));
  EXPECT_THAT(
      locatedTriplesPerBlock,
      locatedTriplesAre(
          {{1, {LT1, LT2, LT3}}, {2, {LT4, LT5}}, {4, {LT6, LT7, LT9}}}));

  locatedTriplesPerBlock.erase(4, handles[1]);
  locatedTriplesPerBlock.updateAugmentedMetadata();

  EXPECT_THAT(locatedTriplesPerBlock, numBlocks(3));
  EXPECT_THAT(locatedTriplesPerBlock, numTriplesTotal(7));
  EXPECT_THAT(locatedTriplesPerBlock,
              numTriplesBlockwise(
                  {{1, {1, 2}}, {2, {2, 0}}, {3, {0, 0}}, {4, {1, 1}}}));
  EXPECT_THAT(locatedTriplesPerBlock,
              locatedTriplesAre(
                  {{1, {LT1, LT2, LT3}}, {2, {LT4, LT5}}, {4, {LT6, LT7}}}));

  locatedTriplesPerBlock.clear();

  EXPECT_THAT(locatedTriplesPerBlock, numBlocks(0));
  EXPECT_THAT(locatedTriplesPerBlock, numTriplesTotal(0));
  EXPECT_THAT(locatedTriplesPerBlock,
              numTriplesBlockwise(
                  {{1, {0, 0}}, {2, {0, 0}}, {3, {0, 0}}, {4, {0, 0}}}));
  EXPECT_THAT(locatedTriplesPerBlock, locatedTriplesAre({}));
}

// Test the method that merges the matching `LocatedTriple`s from a block into
// an `IdTable`.
TEST_F(LocatedTriplesTest, mergeTriples) {
  using LT = LocatedTriple;
  std::vector<CompressedBlockMetadata> emptyMetadata;

  // Merge the `LocatesTriples` into a block with 3 index columns.
  {
    IdTable block = makeIdTableFromVector({
        {1, 10, 10},  // Row 0
        {2, 15, 20},  // Row 1
        {2, 15, 30},  // Row 2
        {2, 20, 10},  // Row 3
        {2, 30, 20},  // Row 4
        {3, 30, 30}   // Row 5
    });
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock({
        LT{1, IT(1, 5, 10), true},    // Insert before row 0
        LT{1, IT(1, 10, 10), false},  // Delete row 0
        LT{1, IT(1, 10, 11), true},   // Insert before row 1
        LT{1, IT(2, 11, 10), true},   // Insert before row 1
        LT{1, IT(2, 30, 10), true},   // Insert before row 4
        LT{1, IT(2, 30, 20), false},  // Delete row 4
        LT{1, IT(3, 30, 25), false},  // Delete non-existent row
        LT{1, IT(3, 30, 30), false},  // Delete row 5
        LT{1, IT(4, 10, 10), true},   // Insert after row 5
    });
    IdTable resultExpected = makeIdTableFromVector({
        {1, 5, 10},   // LT 1
        {1, 10, 11},  // LT 2
        {2, 11, 10},  // LT 3
        {2, 15, 20},  // orig. Row 1
        {2, 15, 30},  // orig. Row 2
        {2, 20, 10},  // orig. Row 3
        {2, 30, 10},  // LT 4
        {4, 10, 10}   // LT 7
    });

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 3, false);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));

    // Run the same test with a constant graph column that is part of the
    // result.
    addGraphColumn(block);
    addGraphColumn(resultExpected);
    merged = locatedTriplesPerBlock.mergeTriples(1, block, 3, true);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }

  // Merge the `LocatesTriples` into a block with 2 index columns. This may
  // happen if all triples in a block have the same value for the first column.
  {
    IdTable block = makeIdTableFromVector({
        {10, 10},  // Row 0
        {15, 20},  // Row 1
        {15, 30},  // Row 2
        {20, 10},  // Row 3
        {30, 20},  // Row 4
        {30, 30}   // Row 5
    });
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock({
        LT{1, IT(1, 10, 10), false},  // Delete row 0
        LT{1, IT(1, 10, 11), true},   // Insert before row 1
        LT{1, IT(1, 11, 10), true},   // Insert before row 1
        LT{1, IT(1, 21, 11), true},   // Insert before row 4
        LT{1, IT(1, 30, 10), true},   // Insert before row 4
        LT{1, IT(1, 30, 20), false},  // Delete row 4
        LT{1, IT(1, 30, 25), false},  // Delete non-existent row
        LT{1, IT(1, 30, 30), false}   // Delete row 5
    });
    IdTable resultExpected = makeIdTableFromVector({
        {10, 11},  // LT 2
        {11, 10},  // LT 3
        {15, 20},  // orig. Row 1
        {15, 30},  // orig. Row 2
        {20, 10},  // orig. Row 3
        {21, 11},  // LT 4
        {30, 10}   // LT 5
    });

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 2, false);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));

    // Run the same test with a constant graph column that is part of the
    // result.
    addGraphColumn(block);
    addGraphColumn(resultExpected);
    merged = locatedTriplesPerBlock.mergeTriples(1, block, 2, true);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }

  // Merge the `LocatesTriples` into a block with 1 index column.
  {
    IdTable block = makeIdTableFromVector({
        {10},  // Row 0
        {11},  // Row 1
        {12},  // Row 2
        {20},  // Row 3
        {23},  // Row 4
        {30}   // Row 5
    });
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock({
        LT{1, IT(1, 10, 12), false},  // Delete row 2
        LT{1, IT(1, 10, 13), true},   // Insert before row 3
    });
    IdTable resultExpected = makeIdTableFromVector({
        {10},  // orig. Row 0
        {11},  // orig. Row 1
        {13},  // LT 2
        {20},  // orig. Row 3
        {23},  // orig. Row 4
        {30}   // orig. Row 5
    });

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 1, false);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));

    // Run the same test with a constant graph column that is part of the
    // result.
    addGraphColumn(block);
    addGraphColumn(resultExpected);
    merged = locatedTriplesPerBlock.mergeTriples(1, block, 1, true);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }

  // Inserting a Triple that already exists should have no effect.
  {
    IdTable block = makeIdTableFromVector({{1, 2, 3}, {1, 3, 5}, {1, 7, 9}});
    auto locatedTriplesPerBlock =
        makeLocatedTriplesPerBlock({LT{1, IT(1, 3, 5), true}});
    IdTable resultExpected = block.clone();

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 3, false);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));

    // Run the same test with a constant graph column that is part of the
    // result.
    addGraphColumn(block);
    addGraphColumn(resultExpected);
    merged = locatedTriplesPerBlock.mergeTriples(1, block, 3, true);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }

  // Inserting a Triple that already exists should have no effect.
  {
    IdTable block = makeIdTableFromVector({{1, 2, 3}, {1, 3, 5}, {1, 7, 9}});
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock(
        {LT{1, IT(1, 2, 4), false}, LT{1, IT(1, 2, 5), false},
         LT{1, IT(1, 3, 5), false}});
    IdTable resultExpected = makeIdTableFromVector({{1, 2, 3}, {1, 7, 9}});

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 3, false);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));

    // Run the same test with a constant graph column that is part of the
    // result.
    addGraphColumn(block);
    addGraphColumn(resultExpected);
    merged = locatedTriplesPerBlock.mergeTriples(1, block, 3, true);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }

  // Merging if the block has additional columns.
  {
    auto IntId = ad_utility::testing::IntId;
    auto UndefId = ad_utility::testing::UndefId;

    IdTable block = makeIdTableFromVector({{1, 2, 3, IntId(10), IntId(11)},
                                           {1, 3, 5, IntId(12), IntId(11)},
                                           {1, 7, 9, IntId(13), IntId(14)}});
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock(
        {LT{1, IT(1, 3, 5), false}, LT{1, IT(1, 3, 6), true}});
    IdTable resultExpected =
        makeIdTableFromVector({{1, 2, 3, IntId(10), IntId(11)},
                               {1, 3, 6, UndefId(), UndefId()},
                               {1, 7, 9, IntId(13), IntId(14)}});

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 3, false);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));

    // Run the same test with a constant graph column that is part of the
    // result.
    addGraphColumn(block);
    addGraphColumn(resultExpected);
    block.setColumnSubset(std::array<ColumnIndex, 6>{0u, 1u, 2u, 5u, 3u, 4u});
    resultExpected.setColumnSubset(
        std::array<ColumnIndex, 6>{0u, 1u, 2u, 5u, 3u, 4u});
    merged = locatedTriplesPerBlock.mergeTriples(1, block, 3, true);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }

  // Merging for a block that has no located triples returns an error.
  {
    IdTable block = makeIdTableFromVector({
        {4, 10, 10},  // Row 0
        {5, 15, 20},  // Row 1
        {5, 15, 30},  // Row 2
        {5, 20, 10},  // Row 3
        {5, 30, 20},  // Row 4
        {6, 30, 30}   // Row 5
    });
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock({
        LT{1, IT(1, 5, 10), true},    // Insert before row 0
        LT{1, IT(1, 10, 10), false},  // Delete row 0
        LT{1, IT(1, 10, 11), true},   // Insert before row 1
        LT{1, IT(2, 11, 10), true},   // Insert before row 1
        LT{1, IT(2, 30, 10), true},   // Insert before row 4
        LT{1, IT(2, 30, 20), false},  // Delete row 4
        LT{1, IT(3, 30, 30), false},  // Delete row 5
        LT{1, IT(4, 10, 10), true},   // Insert after row 5
    });

    EXPECT_THROW(locatedTriplesPerBlock.mergeTriples(2, block, 3, false),
                 ad_utility::Exception);
  }

  // There cannot be fewer index columns in the block than there are columns in
  // total.
  {
    IdTable block = makeIdTableFromVector({
        {10, 10},  // Row 0
        {15, 20},  // Row 1
        {15, 30},  // Row 2
        {20, 10},  // Row 3
        {30, 20},  // Row 4
        {30, 30}   // Row 5
    });
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock({
        LT{1, IT(1, 5, 10), true},    // Insert before row 0
        LT{1, IT(1, 10, 10), false},  // Delete row 0
        LT{1, IT(1, 10, 11), true},   // Insert before row 1
        LT{1, IT(2, 11, 10), true},   // Insert before row 1
        LT{1, IT(2, 30, 10), true},   // Insert before row 4
        LT{1, IT(2, 30, 20), false},  // Delete row 4
        LT{1, IT(3, 30, 25), false},  // Delete non-existent row
        LT{1, IT(3, 30, 30), false},  // Delete row 5
        LT{1, IT(4, 10, 10), true},   // Insert after row 5
    });
    EXPECT_THROW(locatedTriplesPerBlock.mergeTriples(1, block, 3, false),
                 ad_utility::Exception);
  }

  // There can be at most 3 index columns.
  {
    IdTable block = makeIdTableFromVector({
        {1, 10, 10},  // Row 0
        {2, 15, 20},  // Row 1
        {2, 15, 30},  // Row 2
        {2, 20, 10},  // Row 3
        {2, 30, 20},  // Row 4
        {3, 30, 30}   // Row 5
    });
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock({
        LT{1, IT(1, 5, 10), true},    // Insert before row 0
        LT{1, IT(1, 10, 10), false},  // Delete row 0
        LT{1, IT(1, 10, 11), true},   // Insert before row 1
        LT{1, IT(2, 11, 10), true},   // Insert before row 1
        LT{1, IT(2, 30, 10), true},   // Insert before row 4
        LT{1, IT(2, 30, 20), false},  // Delete row 4
        LT{1, IT(3, 30, 25), false},  // Delete non-existent row
        LT{1, IT(3, 30, 30), false},  // Delete row 5
        LT{1, IT(4, 10, 10), true},   // Insert after row 5
    });
    EXPECT_THROW(locatedTriplesPerBlock.mergeTriples(1, block, 4, false),
                 ad_utility::Exception);
  }

  // There has to be at least one index row.
  {
    IdTable block = makeIdTableFromVector({
        {},  // Row 0
        {},  // Row 1
        {},  // Row 2
        {},  // Row 3
        {},  // Row 4
        {}   // Row 5
    });
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock({
        LT{1, IT(1, 5, 10), true},   // Insert before row 0
        LT{1, IT(2, 11, 10), true},  // Insert before row 1
    });
    EXPECT_THROW(locatedTriplesPerBlock.mergeTriples(1, block, 0, false),
                 ad_utility::Exception);
  }
}

// Test the `mergeTriples` functions with inputs that contain different graphs.
TEST_F(LocatedTriplesTest, mergeTriplesWithGraph) {
  using LT = LocatedTriple;
  std::vector<CompressedBlockMetadata> emptyMetadata;

  // Merge the `LocatesTriples` into a block with 3 index columns.
  {
    IdTable block = makeIdTableFromVector({
        {1, 10, 10, 0},  // Row 0
        {2, 15, 20, 0},  // Row 1
        {2, 15, 20, 1},  // Row 2
        {2, 15, 20, 2},  // Row 3
        {2, 15, 30, 1},  // Row 4
        {2, 15, 30, 3},  // Row 5
        {3, 30, 30, 0}   // Row 6
    });
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock({
        LT{1, IT(1, 5, 10, 3), true},    // Insert before row 0
        LT{1, IT(2, 15, 20, 1), false},  // Delete row 2
        LT{1, IT(2, 15, 20, 3), false},  // Delete non-existent row
        LT{1, IT(2, 15, 30, 2), true},   //  Insert between 4 and 5

    });
    IdTable resultExpected = makeIdTableFromVector({
        {1, 5, 10, 3},  // LT 0
        {1, 10, 10, 0},
        {2, 15, 20, 0},  // Row 1
        {2, 15, 20, 2},  // Row 3
        {2, 15, 30, 1},  // Row 4
        {2, 15, 30, 2},  // LT 3
        {2, 15, 30, 3},  // Row 5
        {3, 30, 30, 0}   // Row 6
    });

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 3, true);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }

  // Merge the `LocatesTriples` into a block with 2 index columns. This may
  // happen if all triples in a block have the same value for the first column.
  {
    IdTable block = makeIdTableFromVector({
        {10, 10, 1},  // Row 0
        {15, 20, 2},  // Row 1
        {15, 30, 1},  // Row 2
        {20, 10, 2},  // Row 3
    });
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock({
        LT{1, IT(1, 10, 10, 1), false},  // Delete row 0
        LT{1, IT(1, 13, 20, 3), true},   // Insert before row 1
        LT{1, IT(1, 15, 20, 1), true},   // Insert before row 1
        LT{1, IT(1, 15, 20, 2), true},   // Insert already existing row
        LT{1, IT(1, 20, 10, 1), false},  // Delete non-existent row
    });

    IdTable resultExpected = makeIdTableFromVector({{13, 20, 3},    // LT 1
                                                    {15, 20, 1},    // LT 2
                                                    {15, 20, 2},    // Row 1
                                                    {15, 30, 1},    // Row 2
                                                    {20, 10, 2}});  // Row 3

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 2, true);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }

  // Merge the `LocatesTriples` into a block with 1 index column.
  {
    IdTable block = makeIdTableFromVector({
        {10, 0},  // Row 0
        {10, 1},  // Row 1
        {12, 1},  // Row 2
        {20, 0},  // Row 3
    });
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock({
        LT{1, IT(1, 1, 10, 1), false},  // Delete row 1
        LT{1, IT(1, 1, 12, 0), true},   // Insert before row 2
    });
    IdTable resultExpected = makeIdTableFromVector({
        {10, 0},  // Row 0
        {12, 0},  // LT 1
        {12, 1},  // Row 2
        {20, 0}   // Row 3
    });

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 1, true);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }

  // Merging if the block has additional columns.
  {
    auto IntId = ad_utility::testing::IntId;
    auto UndefId = ad_utility::testing::UndefId;

    IdTable block = makeIdTableFromVector({{1, 2, 3, 0, IntId(10), IntId(11)},
                                           {1, 2, 3, 2, IntId(12), IntId(11)},
                                           {1, 7, 9, 1, IntId(13), IntId(14)}});
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock(
        {LT{1, IT(1, 2, 3, 1), true}, LT{1, IT(1, 7, 9, 1), false}});
    IdTable resultExpected =
        makeIdTableFromVector({{1, 2, 3, 0, IntId(10), IntId(11)},
                               {1, 2, 3, 1, UndefId(), UndefId()},
                               {1, 2, 3, 2, IntId(12), IntId(11)}});

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 3, true);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }
}

// Test the locating of triples in a permutation using `locatedTriple`.
TEST_F(LocatedTriplesTest, locatedTriple) {
  using LT = LocatedTriple;
  // Create a vector that is automatically converted to a span. Spans can only
  // be created from initializer lists beginning with C++26.
  using Span = std::vector<CompressedBlockMetadata>;

  // Locate the following triples, some of which exist in the relation and
  // some of which do not, and which cover a variety of positions, including
  // triples that are larger than all existing triples.
  auto T1 = IT(1, 5, 10);   // Before PT1
  auto T2 = IT(1, 15, 10);  // Before PT2
  auto T3 = IT(2, 10, 10);  // Equals PT2
  auto T4 = IT(2, 14, 20);  // Before PT3
  auto T5 = IT(2, 20, 10);  // Equals PT5
  auto T6 = IT(2, 30, 30);  // Equals PT7
  auto T7 = IT(2, 30, 31);  // Before PT8
  auto T8 = IT(9, 30, 32);  // Larger than all
  std::vector<IdTriple<0>> triplesToLocate{T1, T2, T3, T4, T5, T6, T7, T8};
  std::vector<IdTriple<0>> triplesToLocateReverse{T8, T7, T6, T5,
                                                  T4, T3, T2, T1};

  auto PT1 = PT(1, 10, 10);
  auto PT2 = PT(2, 10, 10);
  auto PT3 = PT(2, 15, 20);
  auto PT4 = PT(2, 15, 30);
  auto PT5 = PT(2, 20, 10);
  auto PT6 = PT(2, 30, 20);
  auto PT7 = PT(2, 30, 30);
  auto PT8 = PT(3, 10, 10);

  ad_utility::SharedCancellationHandle handle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  {
    // Each PTn defines a block with only a single triple.
    auto locatedTriples = LocatedTriple::locateTriplesInPermutation(
        triplesToLocate,
        Span{CBM(PT1, PT1), CBM(PT2, PT2), CBM(PT3, PT3), CBM(PT4, PT4),
             CBM(PT5, PT5), CBM(PT6, PT6), CBM(PT7, PT7), CBM(PT8, PT8)},
        keyOrder, false, handle);
    EXPECT_THAT(locatedTriples,
                testing::ElementsAreArray(
                    {LT(0, T1, false), LT(1, T2, false), LT(1, T3, false),
                     LT(2, T4, false), LT(4, T5, false), LT(6, T6, false),
                     LT(7, T7, false), LT(8, T8, false)}));
  }

  {
    // Block 1: PT1
    // Block 2: PT2 - PT3
    // Block 3: PT4 - PT5
    // Block 4: PT6 - PT7
    // Block 8: PT8
    auto locatedTriples = LocatedTriple::locateTriplesInPermutation(
        triplesToLocate,
        Span{CBM(PT1, PT1), CBM(PT2, PT3), CBM(PT4, PT5), CBM(PT6, PT7),
             CBM(PT8, PT8)},
        keyOrder, true, handle);
    EXPECT_THAT(locatedTriples,
                testing::ElementsAreArray({LT(0, T1, true), LT(1, T2, true),
                                           LT(1, T3, true), LT(1, T4, true),
                                           LT(2, T5, true), LT(3, T6, true),
                                           LT(4, T7, true), LT(5, T8, true)}));
  }

  {
    // The relations (identical first column) are in a block each.
    auto locatedTriples = LocatedTriple::locateTriplesInPermutation(
        triplesToLocate, Span{CBM(PT1, PT1), CBM(PT2, PT7), CBM(PT8, PT8)},
        keyOrder, false, handle);
    EXPECT_THAT(locatedTriples,
                testing::ElementsAreArray(
                    {LT(0, T1, false), LT(1, T2, false), LT(1, T3, false),
                     LT(1, T4, false), LT(1, T5, false), LT(1, T6, false),
                     LT(2, T7, false), LT(3, T8, false)}));
  }

  {
    // Blocks are identical to previous test, but the triples to locate are not
    // sorted. We will probably require a sorted input later, but for now this
    // is supported.
    auto locatedTriples = LocatedTriple::locateTriplesInPermutation(
        triplesToLocateReverse,
        Span{CBM(PT1, PT1), CBM(PT2, PT7), CBM(PT8, PT8)}, keyOrder, false,
        handle);
    EXPECT_THAT(locatedTriples,
                testing::ElementsAreArray(
                    {LT(3, T8, false), LT(2, T7, false), LT(1, T6, false),
                     LT(1, T5, false), LT(1, T4, false), LT(1, T3, false),
                     LT(1, T2, false), LT(0, T1, false)}));
  }

  {
    // All triples are in one block.
    auto locatedTriples = LocatedTriple::locateTriplesInPermutation(
        triplesToLocate, Span{CBM(PT1, PT8)}, keyOrder, false, handle);
    EXPECT_THAT(locatedTriples,
                testing::ElementsAreArray(
                    {LT(0, T1, false), LT(0, T2, false), LT(0, T3, false),
                     LT(0, T4, false), LT(0, T5, false), LT(0, T6, false),
                     LT(0, T7, false), LT(1, T8, false)}));
  }
}

TEST_F(LocatedTriplesTest, augmentedMetadata) {
  // Create a vector that is automatically converted to a span.
  using Span = std::vector<IdTriple<0>>;

  {
    auto PT1 = PT(1, 10, 10);
    auto PT2 = PT(2, 10, 10);
    auto PT3 = PT(2, 15, 20);
    auto PT4 = PT(2, 15, 30);
    auto PT5 = PT(2, 20, 10);
    auto PT6 = PT(2, 30, 20);
    auto PT7 = PT(2, 30, 30);
    auto PT8 = PT(3, 10, 10);
    const std::vector<CompressedBlockMetadata> metadata = {
        CBM(PT1, PT1), CBM(PT2, PT3), CBM(PT4, PT5), CBM(PT6, PT7),
        CBM(PT8, PT8)};
    std::vector<CompressedBlockMetadata> expectedAugmentedMetadata{metadata};

    auto T1 = IT(1, 5, 10);   // Before block 0
    auto T2 = IT(2, 12, 10);  // Inside block 1
    auto T3 = IT(2, 15, 30);  // Equal PT4, beginning of block 2
    auto T4 = IT(2, 40, 30);  // Before block 4

    ad_utility::SharedCancellationHandle handle =
        std::make_shared<ad_utility::CancellationHandle<>>();

    // At the beginning the augmented metadata is equal to the block metadata.
    LocatedTriplesPerBlock locatedTriplesPerBlock;
    locatedTriplesPerBlock.setOriginalMetadata(metadata);

    EXPECT_THAT(locatedTriplesPerBlock.getAugmentedMetadata(),
                testing::ElementsAreArray(expectedAugmentedMetadata));

    // Adding no triples does no changed the augmented metadata.
    locatedTriplesPerBlock.add(LocatedTriple::locateTriplesInPermutation(
        Span{}, metadata, keyOrder, true, handle));

    EXPECT_THAT(locatedTriplesPerBlock.getAugmentedMetadata(),
                testing::ElementsAreArray(expectedAugmentedMetadata));

    // T1 is before block 0. The beginning of block 0 changes.
    locatedTriplesPerBlock.add(LocatedTriple::locateTriplesInPermutation(
        Span{T1}, metadata, keyOrder, false, handle));

    expectedAugmentedMetadata[0] = CBM(T1.toPermutedTriple(), PT1);
    expectedAugmentedMetadata[0].containsDuplicatesWithDifferentGraphs_ = true;
    EXPECT_THAT(locatedTriplesPerBlock.getAugmentedMetadata(),
                testing::ElementsAreArray(expectedAugmentedMetadata));

    // T2 is inside block 1. Borders don't change.
    expectedAugmentedMetadata[1].containsDuplicatesWithDifferentGraphs_ = true;
    locatedTriplesPerBlock.add(LocatedTriple::locateTriplesInPermutation(
        Span{T2}, metadata, keyOrder, true, handle));

    EXPECT_THAT(locatedTriplesPerBlock.getAugmentedMetadata(),
                testing::ElementsAreArray(expectedAugmentedMetadata));

    // T3 is equal to PT4, the beginning of block 2. All update (update and
    // delete) add to the block borders. Borders don't change.
    expectedAugmentedMetadata[2].containsDuplicatesWithDifferentGraphs_ = true;
    locatedTriplesPerBlock.add(LocatedTriple::locateTriplesInPermutation(
        Span{T3}, metadata, keyOrder, false, handle));

    EXPECT_THAT(locatedTriplesPerBlock.getAugmentedMetadata(),
                testing::ElementsAreArray(expectedAugmentedMetadata));

    // T4 is before block 4. The beginning of block 4 changes.
    auto handles =
        locatedTriplesPerBlock.add(LocatedTriple::locateTriplesInPermutation(
            Span{T4}, metadata, keyOrder, true, handle));

    expectedAugmentedMetadata[4] = CBM(T4.toPermutedTriple(), PT8);
    expectedAugmentedMetadata[4].containsDuplicatesWithDifferentGraphs_ = true;
    EXPECT_THAT(locatedTriplesPerBlock.getAugmentedMetadata(),
                testing::ElementsAreArray(expectedAugmentedMetadata));

    // Erasing the update of T4 restores the beginning of block 4.
    locatedTriplesPerBlock.erase(4, handles[0]);
    locatedTriplesPerBlock.updateAugmentedMetadata();

    expectedAugmentedMetadata[4] = CBM(PT8, PT8);
    // The block 4 has no more updates, so we restore the info about the block
    // having no duplicates from the original metadata.
    expectedAugmentedMetadata[4].containsDuplicatesWithDifferentGraphs_ = false;
    EXPECT_THAT(locatedTriplesPerBlock.getAugmentedMetadata(),
                testing::ElementsAreArray(expectedAugmentedMetadata));

    // Clearing the updates restores the original block borders.
    locatedTriplesPerBlock.clear();

    EXPECT_THAT(locatedTriplesPerBlock.getAugmentedMetadata(),
                testing::ElementsAreArray(metadata));
  }

  {
    LocatedTriplesPerBlock ltpb;
    EXPECT_THROW(ltpb.getAugmentedMetadata(), ad_utility::Exception);
  }
}

// _____________________________________________________________________________
TEST_F(LocatedTriplesTest, augmentedMetadataGraphInfo) {
  // Create a vector that is automatically converted to a span.
  using Span = std::vector<IdTriple<0>>;

  auto PT1 = PT(1, 10, 10);
  auto PT2 = PT(2, 10, 10);
  auto PT3 = PT(2, 15, 20);
  // Two blocks, one without graph info, and one with graph info.
  const std::vector<CompressedBlockMetadata> metadata = {
      CBM(PT1, PT1), CBM(PT2, PT3, std::vector<Id>{V(13)})};
  std::vector<CompressedBlockMetadata> expectedAugmentedMetadata{metadata};

  auto T1 = IT(
      1, 10, 10,
      12);  // Before block 0 (because `12` is smaller than the default graph)
  auto T2 = IT(1, 10, 10,
               99999999);  // Becomes the lower bound of block 1, although it
                           // only differs in the graph info.
  auto T3 = IT(2, 12, 10, 17);  // Inside block 1, add graph 17.
  auto T4 = IT(2, 12, 10, 18);  // Inside block 1, add graph 18.

  auto T5 = IT(20, 30, 40, 19);  // After the last block.

  ad_utility::SharedCancellationHandle handle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  {
    LocatedTriplesPerBlock locatedTriplesPerBlock;
    locatedTriplesPerBlock.setOriginalMetadata(metadata);

    // Delete the located triples {T1 ... T4}
    locatedTriplesPerBlock.add(LocatedTriple::locateTriplesInPermutation(
        Span{T1, T2, T3, T4}, metadata, keyOrder, false, handle));

    // All the blocks have updates, so their value of `containsDuplicates..` is
    // set to `true`.
    expectedAugmentedMetadata[0] = CBM(T1.toPermutedTriple(), PT1);
    expectedAugmentedMetadata[1].firstTriple_ = T2.toPermutedTriple();
    expectedAugmentedMetadata[0].containsDuplicatesWithDifferentGraphs_ = true;
    expectedAugmentedMetadata[1].containsDuplicatesWithDifferentGraphs_ = true;

    // Note: the GraphInfo hasn't changed, because the new triples all were
    // deleted.
    EXPECT_THAT(locatedTriplesPerBlock.getAugmentedMetadata(),
                testing::ElementsAreArray(expectedAugmentedMetadata));
  }
  {
    expectedAugmentedMetadata = metadata;
    LocatedTriplesPerBlock locatedTriplesPerBlock;
    locatedTriplesPerBlock.setOriginalMetadata(metadata);

    // Add the located triples {T1 ... T5}
    locatedTriplesPerBlock.add(LocatedTriple::locateTriplesInPermutation(
        Span{T1, T2, T3, T4, T5}, metadata, keyOrder, true, handle));

    expectedAugmentedMetadata[0] = CBM(T1.toPermutedTriple(), PT1);
    expectedAugmentedMetadata[1].firstTriple_ = T2.toPermutedTriple();
    expectedAugmentedMetadata[1].graphInfo_.value() =
        std::vector{V(13), V(17), V(18), V(99999999)};

    // We have added a triple `T5` after the last block, so there now is an
    // additional block, which also stores the correct graph info.
    expectedAugmentedMetadata.push_back(
        CBM(T5.toPermutedTriple(), T5.toPermutedTriple(), std::vector{V(19)}));

    // The automatically added metadata for the last block also has the correct
    // block index and number of columns, so we have to properly initialize it.
    expectedAugmentedMetadata.back().blockIndex_ = 2;
    expectedAugmentedMetadata.back().offsetsAndCompressedSize_.resize(4,
                                                                      {0, 0});

    // All the blocks have updates, so their value of `containsDuplicates..` is
    // set to `true`.
    expectedAugmentedMetadata[0].containsDuplicatesWithDifferentGraphs_ = true;
    expectedAugmentedMetadata[1].containsDuplicatesWithDifferentGraphs_ = true;
    expectedAugmentedMetadata[2].containsDuplicatesWithDifferentGraphs_ = true;

    // Note: the GraphInfo hasn't changed, because the new triples all were
    // deleted.
    auto actualMetadata = locatedTriplesPerBlock.getAugmentedMetadata();
    EXPECT_THAT(actualMetadata,
                testing::ElementsAreArray(expectedAugmentedMetadata));

    // Test the case that a block loses its graph info if the added located
    // triples have too many distinct graphs.
    ASSERT_TRUE(actualMetadata[1].graphInfo_.has_value());
    std::vector<IdTriple<0>> triples;
    // Note: The `30` is an offset to guarantee that the added graphs are not
    // contained in the located triples before.
    for (size_t i = 30; i < 30 + 2 * MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA;
         ++i) {
      auto tr = T3;
      tr.ids().at(ADDITIONAL_COLUMN_GRAPH_ID) = V(i);
      triples.push_back(tr);
    }

    size_t numGraphsBefore = actualMetadata[1].graphInfo_.value().size();
    size_t numGraphsToMax =
        MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA - numGraphsBefore;

    // Add the exact amount of graphs such that we are at the maximum number of
    // stored graphs.
    locatedTriplesPerBlock.add(LocatedTriple::locateTriplesInPermutation(
        ql::span{triples}.subspan(0, numGraphsToMax), metadata, keyOrder, true,
        handle));
    actualMetadata = locatedTriplesPerBlock.getAugmentedMetadata();
    ASSERT_TRUE(actualMetadata[1].graphInfo_.has_value());

    // Adding one more graph will exceed the maximum.
    locatedTriplesPerBlock.add(LocatedTriple::locateTriplesInPermutation(
        ql::span{triples}.subspan(numGraphsToMax, numGraphsToMax + 1), metadata,
        keyOrder, true, handle));
    actualMetadata = locatedTriplesPerBlock.getAugmentedMetadata();
    ASSERT_FALSE(actualMetadata[1].graphInfo_.has_value());
  }
}

TEST_F(LocatedTriplesTest, debugPrints) {
  using LT = LocatedTriple;

  {
    LocatedTriples lts;
    EXPECT_THAT(lts, InsertIntoStream(testing::StrEq("{ }")));
    lts.insert(LT(0, IT(1, 1, 1, 28), true));
    EXPECT_THAT(lts, InsertIntoStream(testing::StrEq(
                         "{ LT(0 IdTriple(V:1, V:1, V:1, V:28, ) 1) }")));
  }

  {
    LocatedTriplesPerBlock ltpb;
    ltpb.setOriginalMetadata(std::vector{CBM(PT(1, 1, 1), PT(1, 10, 15))});
    EXPECT_THAT(ltpb, InsertIntoStream(testing::StrEq("")));
    ltpb.add(std::vector{LT(0, IT(1, 1, 1), true)});
    EXPECT_THAT(ltpb, InsertIntoStream(testing::StrEq(
                          "LTs in Block #0: { LT(0 IdTriple(V:1, "
                          "V:1, V:1, V:123948, ) 1) }\n")));
  }

  {
    std::vector<IdTriple<0>> idts{IT(0, 0, 0), IT(1, 2, 3)};
    EXPECT_THAT(idts, InsertIntoStream(testing::StrEq(
                          "IdTriple(V:0, V:0, V:0, V:123948, ), IdTriple(V:1, "
                          "V:2, V:3, V:123948, ), ")));
  }
}
