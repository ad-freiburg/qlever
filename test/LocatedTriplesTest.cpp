// Copyright 2023 - 2024, University of Freiburg
//  Chair of Algorithms and Data Structures.
//  Authors:
//    2023 Hannah Bast <bast@cs.uni-freiburg.de>
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./util/AllocatorTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "index/CompressedRelation.h"
#include "index/IndexImpl.h"
#include "index/LocatedTriples.h"
#include "index/Permutation.h"

namespace {
auto V = ad_utility::testing::VocabId;

auto IT = [](const auto& c1, const auto& c2, const auto& c3) {
  return IdTriple{V(c1), V(c2), V(c3)};
};

}  // namespace

// TODO<qup42>: move to LocatedTriplesTestHelpers.h if more matchers collect,
// otherwise move it to the anonymous namespace above
namespace Matchers {
inline auto numBlocks =
    [](size_t numBlocks) -> testing::Matcher<const LocatedTriplesPerBlock&> {
  return AD_PROPERTY(LocatedTriplesPerBlock, LocatedTriplesPerBlock::numBlocks,
                     testing::Eq(numBlocks));
};

inline auto numTriplesTotal =
    [](size_t numTriples) -> testing::Matcher<const LocatedTriplesPerBlock&> {
  return AD_PROPERTY(LocatedTriplesPerBlock, LocatedTriplesPerBlock::numTriples,
                     testing::Eq(numTriples));
};

inline auto numTriplesBlockwise = [](size_t blockIndex,
                                     NumAddedAndDeleted insertsAndDeletes)
    -> testing::Matcher<const LocatedTriplesPerBlock&> {
  return testing::ResultOf(
      absl::StrCat(".numTriplesTotal(", std::to_string(blockIndex), ")"),
      [blockIndex](const LocatedTriplesPerBlock& ltpb) {
        return ltpb.numTriples(blockIndex);
      },
      testing::Eq(insertsAndDeletes));
};

// A Matcher to check `numBlocks`, `numTriplesTotal` and
// `numTriplesTotal(blockIndex_)` for all blocks.
auto allNums = [](size_t blocks, size_t triples,
                  const ad_utility::HashMap<size_t, NumAddedAndDeleted>&
                      numTriplesPerBlock)
    -> testing::Matcher<const LocatedTriplesPerBlock&> {
  auto blockMatchers = ad_utility::transform(
      numTriplesPerBlock,
      [](auto p) -> testing::Matcher<const LocatedTriplesPerBlock&> {
        auto [blockIndex, insertsAndDeletes] = p;
        return numTriplesBlockwise(blockIndex, insertsAndDeletes);
      });
  return testing::AllOf(numBlocks(blocks), numTriplesTotal(triples),
                        testing::AllOfArray(blockMatchers));
};
}  // namespace Matchers
namespace m = Matchers;

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
  using LT = LocatedTriple;
  // Set up lists of located triples for three blocks.
  auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock(
      {LT{1, IT(10, 1, 0), false}, LT{1, IT(10, 2, 1), false},
       LT{1, IT(11, 3, 0), true}, LT{2, IT(20, 4, 0), true},
       LT{2, IT(21, 5, 0), true}, LT{4, IT(30, 6, 0), true},
       LT{4, IT(32, 7, 0), false}});

  EXPECT_THAT(
      locatedTriplesPerBlock,
      m::allNums(3, 7, {{1, {1, 2}}, {2, {2, 0}}, {3, {0, 0}}, {4, {1, 1}}}));

  auto handles = locatedTriplesPerBlock.add(
      {LT{3, IT(25, 5, 0), true}, LT{4, IT(31, 6, 1), false}});

  EXPECT_THAT(
      locatedTriplesPerBlock,
      m::allNums(4, 9, {{1, {1, 2}}, {2, {2, 0}}, {3, {1, 0}}, {4, {1, 2}}}));

  locatedTriplesPerBlock.erase(3, handles[0]);

  EXPECT_THAT(
      locatedTriplesPerBlock,
      m::allNums(3, 8, {{1, {1, 2}}, {2, {2, 0}}, {3, {0, 0}}, {4, {1, 2}}}));

  locatedTriplesPerBlock.erase(4, handles[1]);

  EXPECT_THAT(
      locatedTriplesPerBlock,
      m::allNums(3, 7, {{1, {1, 2}}, {2, {2, 0}}, {3, {0, 0}}, {4, {1, 1}}}));

  locatedTriplesPerBlock.clear();

  EXPECT_THAT(
      locatedTriplesPerBlock,
      m::allNums(0, 0, {{1, {0, 0}}, {2, {0, 0}}, {3, {0, 0}}, {4, {0, 0}}}));
}

TEST_F(LocatedTriplesTest, erase) {
  using LT = LocatedTriple;
  // Set up lists of located triples for three blocks.
  auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock(
      {LT{1, IT(10, 1, 0), false}, LT{1, IT(10, 2, 1), false},
       LT{1, IT(11, 3, 0), true}, LT{2, IT(20, 4, 0), true},
       LT{2, IT(21, 5, 0), true}, LT{4, IT(30, 6, 0), true},
       LT{4, IT(32, 7, 0), false}});

  auto handles = locatedTriplesPerBlock.add({LT{1, IT(15, 15, 15), false}});
  EXPECT_THROW(locatedTriplesPerBlock.erase(5, handles[0]),
               ad_utility::Exception);
}

// Test the method that merges the matching `LocatedTriple`s from a block into
// an `IdTable`.
TEST_F(LocatedTriplesTest, mergeTriples) {
  using LT = LocatedTriple;

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

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 3);
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

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 2);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }

  // Merge the `LocatesTriples` into a block with 1 index columns.
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

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 1);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }

  // Inserting a Triple that already exists should have no effect.
  {
    IdTable block = makeIdTableFromVector({{1, 2, 3}, {1, 3, 5}, {1, 7, 9}});
    auto locatedTriplesPerBlock =
        makeLocatedTriplesPerBlock({LT{1, IT(1, 3, 5), true}});
    IdTable resultExpected = block.clone();

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 3);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }
  // Inserting a Triple that already exists should have no effect.
  {
    IdTable block = makeIdTableFromVector({{1, 2, 3}, {1, 3, 5}, {1, 7, 9}});
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock(
        {LT{1, IT(1, 2, 4), false}, LT{1, IT(1, 2, 5), false},
         LT{1, IT(1, 3, 5), false}});
    IdTable resultExpected = makeIdTableFromVector({{1, 2, 3}, {1, 7, 9}});

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 3);
    EXPECT_THAT(merged, testing::ElementsAreArray(resultExpected));
  }

  // Merging if the block has additional columns.
  {
    IdTable block =
        makeIdTableFromVector({{1, 2, 3, ad_utility::testing::IntId(10),
                                ad_utility::testing::IntId(11)},
                               {1, 3, 5, ad_utility::testing::IntId(12),
                                ad_utility::testing::IntId(11)},
                               {1, 7, 9, ad_utility::testing::IntId(13),
                                ad_utility::testing::IntId(14)}});
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock(
        {LT{1, IT(1, 3, 5), false}, LT{1, IT(1, 3, 6), true}});
    IdTable resultExpected =
        makeIdTableFromVector({{1, 2, 3, ad_utility::testing::IntId(10),
                                ad_utility::testing::IntId(11)},
                               {1, 3, 6, ad_utility::testing::UndefId(),
                                ad_utility::testing::UndefId()},
                               {1, 7, 9, ad_utility::testing::IntId(13),
                                ad_utility::testing::IntId(14)}});

    auto merged = locatedTriplesPerBlock.mergeTriples(1, block, 3);
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

    EXPECT_THROW(locatedTriplesPerBlock.mergeTriples(2, block, 3),
                 ad_utility::Exception);
  }

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
    EXPECT_THROW(locatedTriplesPerBlock.mergeTriples(1, block, 4),
                 ad_utility::Exception);
  }

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
    EXPECT_THROW(locatedTriplesPerBlock.mergeTriples(1, block, 3),
                 ad_utility::Exception);
  }

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
    EXPECT_THROW(locatedTriplesPerBlock.mergeTriples(1, block, 0),
                 ad_utility::Exception);
  }
}

// Test the locating of triples in a permutation using `locatedTriple`.
TEST_F(LocatedTriplesTest, locatedTriple) {
  using LT = LocatedTriple;

  auto checkRelationsForPermutation =
      [](const Permutation& permutation,
         const std::vector<Id>& expectedRelations) {
        auto cancellationHandle =
            std::make_shared<ad_utility::CancellationHandle<>>();
        // TODO<qup42,cpp23>: use zip
        auto relationsAndCounts =
            permutation.getDistinctCol0IdsAndCounts(cancellationHandle);
        ASSERT_EQ(relationsAndCounts.numRows(), expectedRelations.size())
            << "Expected and real number of relations differ";
        for (size_t i = 0; i < expectedRelations.size(); i++) {
          auto relationId = expectedRelations[i];
          IdTable relationEntries =
              permutation.scan(CompressedRelationReader::ScanSpecification(
                                   relationId, std::nullopt, std::nullopt),
                               {}, cancellationHandle);
          // std::cout << "Relation: " << relationId << " -> " <<
          // relationEntries << std::endl;
          ASSERT_EQ(relationsAndCounts.at(i, 0), relationId);
          // ASSERT_EQ(relationsAndCounts.at(i, 1), ??);
          ASSERT_FALSE(relationEntries.empty())
              << "Relation " << relationId << " is empty in Permutation "
              << permutation.readableName();
        }
      };
  // TODO<qup42> use or delete; keep for now
  (void)checkRelationsForPermutation;
  auto displayBlocks =
      [](const vector<CompressedBlockMetadata>& blockMetadata) {
        for (size_t i = 0; i < blockMetadata.size(); i++) {
          const auto& block = blockMetadata[i];
          std::cout << "Block #" << i << "(n=" << block.numRows_
                    << "): " << block.firstTriple_ << " -> "
                    << block.lastTriple_ << std::endl;
        }
      };
  auto checkTripleLocationForPermutation =
      [](const Permutation& permutation,
         const std::vector<IdTriple>& triplesToLocate,
         const ad_utility::HashMap<size_t, std::vector<LT>>&
             expectedLocatedTriplesPerBlock) {
        LocatedTriplesPerBlock locatedTriplesPerBlock;
        // TODO<qup42> test for adding/deleting (and not only deletion)
        locatedTriplesPerBlock.add(LT::locateTriplesInPermutation(
            triplesToLocate, permutation, false));

        std::cout << locatedTriplesPerBlock;

        // Extract the sorted keys (block indices) from the map
        std::vector<size_t> blockIndices;
        for (const auto& [blockIndex, expectedLocatedTriples] :
             expectedLocatedTriplesPerBlock) {
          blockIndices.push_back(blockIndex);
        }
        std::sort(blockIndices.begin(), blockIndices.end());
        // Check that all expected blocks are present and contain the expected
        // Triples
        for (auto blockIndex : blockIndices) {
          ASSERT_TRUE(locatedTriplesPerBlock.map_.contains(blockIndex))
              << "blockIndex = " << blockIndex << " not found";
          auto locatedTriplesSet = locatedTriplesPerBlock.map_.at(blockIndex);
          std::vector<LT> computedLocatedTriples(locatedTriplesSet.begin(),
                                                 locatedTriplesSet.end());
          std::vector<LT> expectedLocatedTriples =
              expectedLocatedTriplesPerBlock.at(blockIndex);
          std::vector<LT> expectedPermutedLocatedTriples =
              ad_utility::transform(
                  std::move(expectedLocatedTriples), [&permutation](LT&& lt) {
                    return LT{lt.blockIndex_,
                              permute(lt.triple_, permutation.keyOrder()),
                              lt.shouldTripleExist_};
                  });
          ASSERT_EQ(computedLocatedTriples, expectedPermutedLocatedTriples)
              << "blockIndex = " << blockIndex
              << " doesn't have the expected LocatedTriples";
        }
        ASSERT_EQ(locatedTriplesPerBlock.numBlocks(),
                  expectedLocatedTriplesPerBlock.size());
      };
  auto createIndexFromIdTable = [](const IdTable& triplesInIndex,
                                   ad_utility::AllocatorWithLimit<Id> allocator,
                                   const std::string& indexBasename,
                                   const ad_utility::MemorySize& blockSize) {
    IndexImpl indexBuilder(allocator);
    indexBuilder.setOnDiskBase(indexBasename);
    indexBuilder.blocksizePermutationPerColumn() = blockSize;

    // This is still not a complete index. `Index::createFromOnDiskIndex`
    // cannot be used, because the vocabulary is not generated.
    // `IndexImpl::readIndexBuilderSettingsFromFile` also has to be called
    // before building the index in this case.
    std::function<bool(const ValueId&)> isInternalId = [](const ValueId&) {
      return false;
    };
    auto firstSorter = indexBuilder.makeSorter<FirstPermutation>("first");
    ad_utility::CompressedExternalIdTableSorterTypeErased&
        typeErasedFirstSorter = firstSorter;
    firstSorter.pushBlock(triplesInIndex.clone());
    auto firstSorterWithUnique =
        ad_utility::uniqueBlockView(typeErasedFirstSorter.getSortedOutput());
    auto secondSorter = indexBuilder.makeSorter<SecondPermutation>("second");
    indexBuilder.createSPOAndSOP(3, isInternalId,
                                 std::move(firstSorterWithUnique),
                                 std::move(secondSorter));
    typeErasedFirstSorter.clearUnderlying();
    auto thirdSorter = indexBuilder.makeSorter<ThirdPermutation>("third");
    indexBuilder.createOSPAndOPS(3, isInternalId,
                                 secondSorter.getSortedBlocks<0>(),
                                 std::move(thirdSorter));
    secondSorter.clear();
    indexBuilder.createPSOAndPOS(3, isInternalId,
                                 thirdSorter.getSortedBlocks<0>());
    thirdSorter.clear();
  };
  auto deletePermutation = [](const std::string& indexBasename,
                              const Permutation& permutation) {
    std::string name = indexBasename + ".index" + permutation.fileSuffix();
    ad_utility::deleteFile(name);
    ad_utility::deleteFile(name + MMAP_FILE_SUFFIX);
  };
  // The actual test, for a given block size.
  auto testWithGivenBlockSizeAll =
      [&displayBlocks, &checkTripleLocationForPermutation,
       &createIndexFromIdTable, &deletePermutation](
          const IdTable& triplesInIndex,
          const std::vector<IdTriple>& triplesToLocate,
          const ad_utility::MemorySize& blockSize,
          const ad_utility::HashMap<
              Permutation::Enum, ad_utility::HashMap<size_t, std::vector<LT>>>&
              expectedLocatedTriplesPerBlock) {
        std::string testIndexBasename = "LocatedTriplesTest.locatedTriple";

        const auto& testAllocator = ad_utility::testing::makeAllocator();
        createIndexFromIdTable(triplesInIndex, testAllocator, testIndexBasename,
                               blockSize);

        using enum Permutation::Enum;
        for (auto& perm : {SPO, SOP, OSP, OPS, PSO, POS}) {
          Permutation permutation{perm, {}, testAllocator};
          permutation.loadFromDisk(testIndexBasename);

          if (expectedLocatedTriplesPerBlock.contains(perm)) {
            displayBlocks(permutation.metaData().blockData());

            checkTripleLocationForPermutation(
                permutation, triplesToLocate,
                expectedLocatedTriplesPerBlock.at(perm));
          } else {
            std::cout << "Skipping permutation " << Permutation::toString(perm)
                      << std::endl;
          }

          deletePermutation(testIndexBasename, permutation);
        }
      };

  {
    // Triples in the index.
    IdTable triplesInIndex = makeIdTableFromVector({{1, 10, 10},    // Row 0
                                                    {2, 10, 10},    // Row 1
                                                    {2, 15, 20},    // Row 2
                                                    {2, 15, 30},    // Row 3
                                                    {2, 20, 10},    // Row 4
                                                    {2, 30, 20},    // Row 5
                                                    {2, 30, 30},    // Row 6
                                                    {3, 10, 10}});  // Row 7

    // Locate the following triples, some of which exist in the relation and
    // some of which do not, and which cover a variety of positions, including
    // triples that are larger than all existing triples.
    std::vector<IdTriple> triplesToLocate{IT(1, 5, 10),    // Before Row 0
                                          IT(1, 15, 10),   // Before Row 1
                                          IT(2, 10, 10),   // Equals Row 1
                                          IT(2, 14, 20),   // Before Row 2
                                          IT(2, 20, 10),   // Equals Row 4
                                          IT(2, 30, 30),   // Equals Row 6
                                          IT(2, 30, 31),   // Before Row 7
                                          IT(9, 30, 32)};  // Larger than all.

    // Now test for multiple block sizes (8 bytes is the minimum; number
    // determined experimentally).
    std::cout << "Index triples: " << triplesInIndex << std::endl;
    std::cout << "Delta triples: " << triplesToLocate << std::endl;

    // With block size 8, we have each triple in its own block.
    testWithGivenBlockSizeAll(
        triplesInIndex, triplesToLocate, 8_B,
        {{Permutation::SPO,
          {{0, {LT(0, IT(1, 5, 10), false)}},
           {1, {LT(1, IT(1, 15, 10), false), LT(1, IT(2, 10, 10), false)}},
           {2, {LT(2, IT(2, 14, 20), false)}},
           {4, {LT(4, IT(2, 20, 10), false)}},
           {6, {LT(6, IT(2, 30, 30), false)}},
           {7, {LT(7, IT(2, 30, 31), false)}},
           {8, {LT(8, IT(9, 30, 32), false)}}}}});

    // With block size 16, we have five blocks (Block 0 = Row 0, Block 1 = Row
    // 1+2, Block 2 = Row 3+4, Block 3 = Row 5+6, Block 4 = Row 7).
    testWithGivenBlockSizeAll(
        triplesInIndex, triplesToLocate, 16_B,
        {{Permutation::SPO,
          {{0, {LT(0, IT(1, 5, 10), false)}},
           {1,
            {LT(1, IT(1, 15, 10), false), LT(1, IT(2, 10, 10), false),
             LT(1, IT(2, 14, 20), false)}},
           {2, {LT(2, IT(2, 20, 10), false)}},
           {3, {LT(3, IT(2, 30, 30), false)}},
           {4, {LT(4, IT(2, 30, 31), false)}},
           {5, {LT(5, IT(9, 30, 32), false)}}}}});

    // With block size 32, we have four blocks (Block 0 = Row 0, Block 1 = Row
    // 1+2+3+4, Block 2 = Row 5+6, Block 3 = Row 7). Note that a
    // relation that spans multiple blocks has these blocks on its own.
    testWithGivenBlockSizeAll(
        triplesInIndex, triplesToLocate, 32_B,
        {{Permutation::SPO,
          {{0, {LT(0, IT(1, 5, 10), false)}},
           {1,
            {LT(1, IT(1, 15, 10), false), LT(1, IT(2, 10, 10), false),
             LT(1, IT(2, 14, 20), false), LT(1, IT(2, 20, 10), false)}},
           {2, {LT(2, IT(2, 30, 30), false)}},
           {3, {LT(3, IT(2, 30, 31), false)}},
           {4, {LT(4, IT(9, 30, 32), false)}}}}});

    // With block size 48, we have three blocks (Block 0 = Row 0, Block 1 = Row
    // 1+2+3+4+5+6, Block 2 = Row 7).
    testWithGivenBlockSizeAll(
        triplesInIndex, triplesToLocate, 48_B,
        {{Permutation::SPO,
          {{0, {LT(0, IT(1, 5, 10), false)}},
           {1,
            {LT(1, IT(1, 15, 10), false), LT(1, IT(2, 10, 10), false),
             LT(1, IT(2, 14, 20), false), LT(1, IT(2, 20, 10), false),
             LT(1, IT(2, 30, 30), false)}},
           {2, {LT(2, IT(2, 30, 31), false)}},
           {3, {LT(3, IT(9, 30, 32), false)}}}}});

    // With block size 100'000, we have one block.
    testWithGivenBlockSizeAll(
        triplesInIndex, triplesToLocate, 100'000_B,
        {{Permutation::SPO,
          {{0,
            {LT(0, IT(1, 5, 10), false), LT(0, IT(1, 15, 10), false),
             LT(0, IT(2, 10, 10), false), LT(0, IT(2, 14, 20), false),
             LT(0, IT(2, 20, 10), false), LT(0, IT(2, 30, 30), false),
             LT(0, IT(2, 30, 31), false)}},
           {1, {LT(1, IT(9, 30, 32), false)}}}}});
  }

  {
    // Test more thoroughly in an index that consists of a single block.
    IdTable triplesInIndex = makeIdTableFromVector({{1, 10, 10},    // Row 0
                                                    {3, 10, 10},    // Row 1
                                                    {3, 15, 20},    // Row 2
                                                    {3, 15, 30},    // Row 3
                                                    {3, 20, 10},    // Row 4
                                                    {3, 30, 20},    // Row 5
                                                    {3, 30, 30},    // Row 6
                                                    {5, 10, 10},    // Row 7
                                                    {7, 10, 10},    // Row 8
                                                    {7, 15, 20},    // Row 9
                                                    {7, 15, 30},    // Row 10
                                                    {7, 20, 10},    // Row 11
                                                    {7, 30, 20},    // Row 12
                                                    {7, 30, 30}});  // Row 13

    std::vector<IdTriple> triplesToLocate{
        IT(1, 5, 20),    // Before Row 0
        IT(1, 10, 10),   // Equal Row 0 (a small relation)
        IT(2, 20, 10),   // Before Row 1
        IT(3, 15, 30),   // Equal Row 3
        IT(3, 20, 15),   // Before Row 5
        IT(4, 30, 30),   // Before Row 7
        IT(5, 5, 10),    // Before Row 7
        IT(5, 10, 10),   // Equal Row 7
        IT(6, 10, 10),   // Before Row 8
        IT(7, 20, 5),    // Before Row 11
        IT(7, 30, 20),   // Equal Row 12
        IT(7, 30, 30),   // Equal Row 13
        IT(9, 30, 32)};  // Larger than all.

    testWithGivenBlockSizeAll(
        triplesInIndex, triplesToLocate, 100'000_B,
        {{Permutation::SPO,
          {{0,
            {LT(0, IT(1, 5, 20), false), LT(0, IT(1, 10, 10), false),
             LT(0, IT(2, 20, 10), false), LT(0, IT(3, 15, 30), false),
             LT(0, IT(3, 20, 15), false), LT(0, IT(4, 30, 30), false),
             LT(0, IT(5, 5, 10), false), LT(0, IT(5, 10, 10), false),
             LT(0, IT(6, 10, 10), false), LT(0, IT(7, 20, 5), false),
             LT(0, IT(7, 30, 20), false), LT(0, IT(7, 30, 30), false)}},
           {1, {LT(1, IT(9, 30, 32), false)}}}}});
  }
}
