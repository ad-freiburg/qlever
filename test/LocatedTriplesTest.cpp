//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./util/AllocatorTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "index/CompressedRelation.h"
#include "index/IndexImpl.h"
#include "index/IndexMetaData.h"
#include "index/LocatedTriples.h"
#include "index/Permutation.h"

// TODO: Why the namespace here? (copied from `test/IndexMetaDataTest.cpp`)
namespace {
auto V = ad_utility::testing::VocabId;
}

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

auto IT = [](const auto& c1, const auto& c2, const auto& c3) {
  return IdTriple{V(c1), V(c2), V(c3)};
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
  // A helper to check `numBlocks`, `numTriples` and `numTriples(blockIndex)`
  // for all blocks.
  auto checkNumbers =
      [&locatedTriplesPerBlock](
          size_t numBlocks, size_t numTriples,
          const ad_utility::HashMap<size_t, std::pair<size_t, size_t>>&
              numTriplesPerBlock) {
        ASSERT_EQ(locatedTriplesPerBlock.numBlocks(), numBlocks);
        ASSERT_EQ(locatedTriplesPerBlock.numTriples(), numTriples);

        for (auto [blockIndex, insertsAndDeletes] : numTriplesPerBlock) {
          ASSERT_EQ(locatedTriplesPerBlock.numTriples(blockIndex),
                    insertsAndDeletes);
        }
      };

  checkNumbers(3, 7, {{1, {1, 2}}, {2, {2, 0}}, {3, {0, 0}}, {4, {1, 1}}});

  locatedTriplesPerBlock.add({LT{3, IT(25, 5, 0), true}});

  checkNumbers(4, 8, {{1, {1, 2}}, {2, {2, 0}}, {3, {1, 0}}, {4, {1, 1}}});

  locatedTriplesPerBlock.clear();

  checkNumbers(0, 0, {{1, {0, 0}}, {2, {0, 0}}, {3, {0, 0}}, {4, {0, 0}}});
}

// Test the method that merges the matching `LocatedTriple`s from a block into
// an `IdTable`.
TEST_F(LocatedTriplesTest, mergeTriples) {
  using LT = LocatedTriple;

  auto dropFirstColumns = [](int64_t n, auto& table) {
    std::vector<ColumnIndex> indices(table.numColumns() - n);
    std::iota(indices.begin(), indices.end(), n);
    table.setColumnSubset(indices);
  };
  auto mergeAndCheck = [](IdTable block, const IdTable& expectedResult,
                          const LocatedTriplesPerBlock& locatedTriples) {
    IdTable result(expectedResult.numColumns(),
                   ad_utility::testing::makeAllocator());
    result.resize(expectedResult.size());
    locatedTriples.mergeTriples(1, std::move(block), result, 0);
    ASSERT_EQ(result, expectedResult);
  };

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

    mergeAndCheck(block.clone(), resultExpected, locatedTriplesPerBlock);
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

    mergeAndCheck(std::move(block), resultExpected, locatedTriplesPerBlock);
  }

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
        LocatedTriple{1, IT(1, 10, 12), false},  // Delete row 2
        LocatedTriple{1, IT(1, 10, 13), true},   // Insert before row 3
    });
    IdTable resultExpected = makeIdTableFromVector({
        {10},  // orig. Row 0
        {11},  // orig. Row 1
        {13},  // LT 2
        {20},  // orig. Row 3
        {23},  // orig. Row 4
        {30}   // orig. Row 5
    });
    mergeAndCheck(std::move(block), resultExpected, locatedTriplesPerBlock);
  }
}

// Test the locating of triples in a permutation using `locatedTriple`.
TEST_F(LocatedTriplesTest, locatedTriple) {
  // The actual test, for a given block size.
  //
  // TODO: Test for all permutations, right now it's only SPO.
  using LT = LocatedTriple;
  auto testWithGivenBlockSize =
      [](const IdTable& triplesInIndex,
         const std::vector<IdTriple>& triplesToLocate,
         const ad_utility::MemorySize& blockSize,
         const ad_utility::HashMap<size_t, std::vector<LT>>&
             expectedLocatedTriplesPerBlock) {
        std::string testIndexBasename = "LocatedTriplesTest.locatedTriple";

        // Collect the distinct relation `Id`s.
        std::vector<Id> relationIds;
        for (size_t i = 0; i < triplesInIndex.numRows(); ++i) {
          relationIds.push_back(triplesInIndex(i, 0));
          ASSERT_TRUE(i == 0 || relationIds[i - 1] <= relationIds[i]);
        }
        relationIds = ad_utility::removeDuplicates(relationIds);

        // Create a permutation pair from `triplesInIndex`.
        const auto& testAllocator = ad_utility::testing::makeAllocator();
        Permutation SPO{Permutation::SPO, {}, testAllocator};
        Permutation SOP{Permutation::SOP, {}, testAllocator};
        IndexImpl indexBuilder(testAllocator);
        indexBuilder.setOnDiskBase(testIndexBasename);
        indexBuilder.blocksizePermutationPerColumn() = blockSize;
        // The function `createPermutationPair` expects a generator.
        IndexImpl::BlocksOfTriples blocksOfTriples =
            [&triplesInIndex]() -> cppcoro::generator<IdTableStatic<0>> {
          co_yield triplesInIndex.clone();
        }();
        indexBuilder.createPermutationPair(3, std::move(blocksOfTriples), SPO,
                                           SOP);

        // Load the SPO permutation from disk.
        Permutation permutation(Permutation::SPO, {}, testAllocator);
        permutation.loadFromDisk(testIndexBasename);

        // Check that the permutation indeed consists of the relations that we
        // have written to it.
        {
          auto cancellationHandle =
              std::make_shared<ad_utility::CancellationHandle<>>();
          for (Id relationId : relationIds) {
            auto scanSpec = CompressedRelationReader::ScanSpecification(
                relationId, std::nullopt, std::nullopt);
            IdTable relation =
                permutation.scan(scanSpec, {}, cancellationHandle);
            std::cout << "Relation: " << relationId << " -> " << relation
                      << std::endl;
          }
          for (const CompressedBlockMetadata& block :
               permutation.metaData().blockData()) {
            // The permuted triples themselves already end with std::endl
            std::cout << "Block: size=" << block.numRows_ << " "
                      << block.firstTriple_ << " -> " << block.lastTriple_;
          }
        }

        // Now locate the triples from `triplesToLocate` in the permutation.
        LocatedTriplesPerBlock locatedTriplesPerBlock;
        // TODO<qup42> currently fixed to delete; find a way to reintroduce
        locatedTriplesPerBlock.add(LocatedTriple::locateTriplesInPermutation(
            triplesToLocate, permutation, false));

        // Check that the locations are as expected. Process in order of
        // increasing block index because it's easier to debug.
        std::cout << locatedTriplesPerBlock;
        std::vector<size_t> blockIndices;
        for (const auto& [blockIndex, expectedLocatedTriples] :
             expectedLocatedTriplesPerBlock) {
          blockIndices.push_back(blockIndex);
        }
        std::sort(blockIndices.begin(), blockIndices.end());
        for (auto blockIndex : blockIndices) {
          ASSERT_TRUE(locatedTriplesPerBlock.map_.contains(blockIndex))
              << "blockIndex = " << blockIndex << " not found";
          auto locatedTriplesSet = locatedTriplesPerBlock.map_.at(blockIndex);
          std::vector<LT> computedLocatedTriples(locatedTriplesSet.begin(),
                                                 locatedTriplesSet.end());
          const std::vector<LT>& expectedLocatedTriples =
              expectedLocatedTriplesPerBlock.at(blockIndex);
          ASSERT_EQ(computedLocatedTriples, expectedLocatedTriples)
              << "blockIndex = " << blockIndex;
        }
        ASSERT_EQ(locatedTriplesPerBlock.numBlocks(),
                  expectedLocatedTriplesPerBlock.size());

        // Delete the permutation files.
        for (const auto& permutation_suffix : {"spo", "sop"}) {
          std::string name = testIndexBasename + ".index." + permutation_suffix;
          ad_utility::deleteFile(name);
          ad_utility::deleteFile(name + MMAP_FILE_SUFFIX);
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
    // some of which do not, and which cover a variety of positons, including
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
    testWithGivenBlockSize(
        triplesInIndex, triplesToLocate, 8_B,
        {{0, {LT(0, IT(1, 5, 10), false)}},
         {1, {LT(1, IT(1, 15, 10), false), LT(1, IT(2, 10, 10), false)}},
         {2, {LT(2, IT(2, 14, 20), false)}},
         {4, {LT(4, IT(2, 20, 10), false)}},
         {6, {LT(6, IT(2, 30, 30), false)}},
         {7, {LT(7, IT(2, 30, 31), false)}},
         {8, {LT(8, IT(9, 30, 32), false)}}});

    // With block size 16, we have five blocks (Block 0 = Row 0, Block 1 = Row
    // 1+2, Block 2 = Row 3+4, Block 3 = Row 5+6, Block 4 = Row 7).
    testWithGivenBlockSize(
        triplesInIndex, triplesToLocate, 16_B,
        {{0, {LT(0, IT(1, 5, 10), false)}},
         {1,
          {LT(1, IT(1, 15, 10), false), LT(1, IT(2, 10, 10), false),
           LT(1, IT(2, 14, 20), false)}},
         {2, {LT(2, IT(2, 20, 10), false)}},
         {3, {LT(3, IT(2, 30, 30), false)}},
         {4, {LT(4, IT(2, 30, 31), false)}},
         {5, {LT(5, IT(9, 30, 32), false)}}});

    // With block size 32, we have four blocks (Block 0 = Row 0, Block 1 = Row
    // 1+2+3+4, Block 2 = Row 5+6, Block 3 = Row 7). Note that a
    // relation that spans multiple blocks has these blocks on its own.
    testWithGivenBlockSize(
        triplesInIndex, triplesToLocate, 32_B,
        {{0, {LT(0, IT(1, 5, 10), false)}},
         {1,
          {LT(1, IT(1, 15, 10), false), LT(1, IT(2, 10, 10), false),
           LT(1, IT(2, 14, 20), false), LT(1, IT(2, 20, 10), false)}},
         {2, {LT(2, IT(2, 30, 30), false)}},
         {3, {LT(3, IT(2, 30, 31), false)}},
         {4, {LT(4, IT(9, 30, 32), false)}}});

    // With block size 48, we have three blocks (Block 0 = Row 0, Block 1 = Row
    // 1+2+3+4+5+6, Block 2 = Row 7).
    testWithGivenBlockSize(
        triplesInIndex, triplesToLocate, 48_B,
        {{0, {LT(0, IT(1, 5, 10), false)}},
         {1,
          {LT(1, IT(1, 15, 10), false), LT(1, IT(2, 10, 10), false),
           LT(1, IT(2, 14, 20), false), LT(1, IT(2, 20, 10), false),
           LT(1, IT(2, 30, 30), false)}},
         {2, {LT(2, IT(2, 30, 31), false)}},
         {3, {LT(3, IT(9, 30, 32), false)}}});

    // With block size 100'000, we have one block.
    testWithGivenBlockSize(
        triplesInIndex, triplesToLocate, 100'000_B,
        {{0,
          {LT(0, IT(1, 5, 10), false), LT(0, IT(1, 15, 10), false),
           LT(0, IT(2, 10, 10), false), LT(0, IT(2, 14, 20), false),
           LT(0, IT(2, 20, 10), false), LT(0, IT(2, 30, 30), false),
           LT(0, IT(2, 30, 31), false)}},
         {1, {LT(1, IT(9, 30, 32), false)}}});
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

    testWithGivenBlockSize(
        triplesInIndex, triplesToLocate, 100'000_B,
        {{0,
          {LT(0, IT(1, 5, 20), false), LT(0, IT(1, 10, 10), false),
           LT(0, IT(2, 20, 10), false), LT(0, IT(3, 15, 30), false),
           LT(0, IT(3, 20, 15), false), LT(0, IT(4, 30, 30), false),
           LT(0, IT(5, 5, 10), false), LT(0, IT(5, 10, 10), false),
           LT(0, IT(6, 10, 10), false), LT(0, IT(7, 20, 5), false),
           LT(0, IT(7, 30, 20), false), LT(0, IT(7, 30, 30), false)}},
         {1, {LT(1, IT(9, 30, 32), false)}}});
  }
}
