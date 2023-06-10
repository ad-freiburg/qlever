//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "index/CompressedRelation.h"
#include "index/IndexMetaData.h"
#include "index/LocatedTriples.h"
#include "index/Permutations.h"

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
      std::vector<LocatedTriple> locatedTriples) {
    LocatedTriplesPerBlock result;
    for (auto locatedTriple : locatedTriples) {
      result.add(locatedTriple);
    }
    return result;
  }
};

// Test the method that counts the number of `LocatedTriple's in a block.
TEST_F(LocatedTriplesTest, numTriplesInBlock) {
  // Set up lists of located triples for three blocks.
  auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock(
      {LocatedTriple{1, 0, V(10), V(1), V(0), true},
       LocatedTriple{1, 0, V(10), V(2), V(1), true},
       LocatedTriple{1, 0, V(11), V(3), V(0), false},
       LocatedTriple{2, 0, V(20), V(4), V(0), false},
       LocatedTriple{2, 0, V(21), V(5), V(0), false},
       LocatedTriple{3, 0, V(30), V(6), V(0), false},
       LocatedTriple{3, 0, V(32), V(7), V(0), true}});
  ASSERT_EQ(locatedTriplesPerBlock.numBlocks(), 3);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(), 7);

  auto P = [](size_t n1, size_t n2) -> std::pair<size_t, size_t> {
    return {n1, n2};
  };

  // Check the total counts per block.
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(1), P(1, 2));
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(2), P(2, 0));
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(3), P(1, 1));

  // Check the counts per block for a given `id1`.
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(1, V(10)), P(0, 2));
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(1, V(11)), P(1, 0));
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(2, V(20)), P(1, 0));
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(2, V(21)), P(1, 0));
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(3, V(30)), P(1, 0));
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(3, V(32)), P(0, 1));

  // Check the counts per block for a given `id1` and `id2`.
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(1, V(10), V(1)), P(0, 1));
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(1, V(10), V(2)), P(0, 1));
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(1, V(11), V(3)), P(1, 0));
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(2, V(20), V(4)), P(1, 0));
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(2, V(21), V(5)), P(1, 0));
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(3, V(30), V(6)), P(1, 0));
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(3, V(32), V(7)), P(0, 1));
}

// Test the method that merges the matching `LocatedTriple`s from a block into a
// part of an `IdTable`.
TEST_F(LocatedTriplesTest, mergeTriples) {
  // A block, as it could come from an index scan.
  IdTable block = makeIdTableFromVector({{10, 10},    // Row 0
                                         {15, 20},    // Row 1
                                         {15, 30},    // Row 2
                                         {20, 10},    // Row 3
                                         {30, 20},    // Row 4
                                         {30, 30}});  // Row 5

  // A set of located triples for that block.
  auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock(
      {LocatedTriple{1, 0, V(1), V(10), V(10), true},    // Delete row 0
       LocatedTriple{1, 1, V(1), V(10), V(11), false},   // Insert before row 1
       LocatedTriple{1, 1, V(2), V(11), V(10), false},   // Insert before row 1
       LocatedTriple{1, 4, V(2), V(21), V(11), false},   // Insert before row 4
       LocatedTriple{1, 4, V(2), V(30), V(10), false},   // Insert before row 4
       LocatedTriple{1, 4, V(2), V(30), V(20), true},    // Delete row 4
       LocatedTriple{1, 5, V(3), V(30), V(30), true}});  // Delete row 5

  // Merge all these triples into `block` and check that the result is as
  // expected (four triples inserted and three triples deleted).
  {
    IdTable resultExpected = makeIdTableFromVector({{10, 11},    // Row 0
                                                    {11, 10},    // Row 1
                                                    {15, 20},    // Row 2
                                                    {15, 30},    // Row 3
                                                    {20, 10},    // Row 4
                                                    {21, 11},    // Row 5
                                                    {30, 10}});  // Row 6
    IdTable result(2, ad_utility::testing::makeAllocator());
    result.resize(resultExpected.size());
    locatedTriplesPerBlock.mergeTriples(1, block.clone(), result, 0);
    ASSERT_EQ(result, resultExpected);
  }

  // Merge only the triples with `id1 == V(2)` into `block` (three triples
  // inserted and one triple deleted).
  {
    IdTable resultExpected = makeIdTableFromVector({{10, 10},    // Row 0
                                                    {11, 10},    // Row 1
                                                    {15, 20},    // Row 2
                                                    {15, 30},    // Row 3
                                                    {20, 10},    // Row 4
                                                    {21, 11},    // Row 5
                                                    {30, 10},    // Row 6
                                                    {30, 30}});  // Row 7
    IdTable result(2, ad_utility::testing::makeAllocator());
    result.resize(resultExpected.size());
    locatedTriplesPerBlock.mergeTriples(1, block.clone(), result, 0, V(2));
    ASSERT_EQ(result, resultExpected);
  }

  // Repeat but with a partial block that leaves out the first two elements of
  // `block`.
  {
    IdTable resultExpected = makeIdTableFromVector({{15, 30},    // Row 0
                                                    {20, 10},    // Row 1
                                                    {21, 11},    // Row 2
                                                    {30, 10},    // Row 3
                                                    {30, 30}});  // Row 4
    IdTable result(2, ad_utility::testing::makeAllocator());
    result.resize(resultExpected.size());
    locatedTriplesPerBlock.mergeTriples(1, block.clone(), result, 0, V(2), 2);
    ASSERT_EQ(result, resultExpected);
  }

  // Merge only the triples with `id1 == V(2)` and `id2 == V(30)` into the
  // corresponding partial block (one triple inserted, one triple deleted).
  {
    IdTable blockColumnId3(1, ad_utility::testing::makeAllocator());
    blockColumnId3.resize(block.size());
    for (size_t i = 0; i < block.size(); ++i) {
      blockColumnId3(i, 0) = block(i, 1);
    }
    IdTable resultExpected = makeIdTableFromVector({{10}, {30}});
    IdTable result(1, ad_utility::testing::makeAllocator());
    result.resize(resultExpected.size());
    locatedTriplesPerBlock.mergeTriples(1, std::move(blockColumnId3), result, 0,
                                        V(2), V(30), 4, 6);
    ASSERT_EQ(result, resultExpected);
  }

  // Merge special triples.
  {
    size_t NRI = LocatedTriple::NO_ROW_INDEX;
    auto locatedTriplesPerBlock = makeLocatedTriplesPerBlock(
        {LocatedTriple{2, NRI, V(1), V(30), V(40), true},
         LocatedTriple{2, NRI, V(1), V(30), V(50), true},
         LocatedTriple{2, NRI, V(1), V(40), V(10), true}});
    IdTable resultExpected = makeIdTableFromVector({{30, 40},    // Row 0
                                                    {30, 50},    // Row 1
                                                    {40, 10}});  // Row 2
    IdTable result(2, ad_utility::testing::makeAllocator());
    result.resize(resultExpected.size());
    locatedTriplesPerBlock.mergeTriples(2, std::nullopt, result, 0, V(1));
  }
}

// Test the locating of triples in a permutation using `locatedTriple`.
TEST_F(LocatedTriplesTest, locatedTriple) {
  // The actual test, for a given block size.
  //
  // TODO: Also make the permutation an argument, right now it's only PSO.
  auto testWithGivenBlockSize =
      [](const IdTable& triplesInIndex, const IdTable& triplesToLocate,
         size_t blockSizeInBytes,
         const ad_utility::HashMap<size_t, std::string>&
             expectedLocatedTriplesPerBlock) {
        std::string basename = "LocatedTriplesTest.scanWithMergeTriples";
        std::string permutationFilename = basename + ".index.pso";

        // Collect the distinct relation `Id`s.
        std::vector<Id> relationIds;
        for (size_t i = 0; i < triplesInIndex.numRows(); ++i) {
          relationIds.push_back(triplesInIndex(i, 0));
          ASSERT_TRUE(i == 0 || relationIds[i - 1] <= relationIds[i]);
        }
        relationIds = ad_utility::removeDuplicates(relationIds);

        // Helper lambda for creating a `BufferedIdTable` from all triples in
        // the given `IdTable` matching `relationId`.
        //
        // This is needed need for `CompressedRelationWriter` below, which
        // expects a `BufferedIdTable` with two columns.
        //
        // TODO: Something like this is also used in `CompressedRelationsTest`,
        // so it should be in a helper class.
        auto getBufferedIdTable = [](const IdTable& idTable,
                                     Id relationId) -> BufferedIdTable {
          // Note that these files are never created because we set the
          // threshold for writing to disk so large.
          std::string bufferFilename1 = "compressedRelationWriter.buffer1.dat";
          std::string bufferFilename2 = "compressedRelationWriter.buffer2.dat";
          AD_CONTRACT_CHECK(idTable.numColumns() == 3);
          BufferedIdTable bufferedIdTable{
              2, std::array{
                     ad_utility::BufferedVector<Id>{
                         std::numeric_limits<size_t>::max(), bufferFilename1},
                     ad_utility::BufferedVector<Id>{
                         std::numeric_limits<size_t>::max(), bufferFilename2}}};
          for (size_t i = 0; i < idTable.size(); ++i) {
            if (idTable(i, 0) == relationId) {
              bufferedIdTable.push_back({idTable(i, 1), idTable(i, 2)});
            }
          }
          return bufferedIdTable;
        };

        // Write the permutation to disk (adapted from
        // `CompressedRelationsTest`, `IndexImpl::createPermutationPairImpl`,
        // and `IndexImpl::).
        {
          ad_utility::File permutationFileForWritingRelations{
              permutationFilename, "w"};
          IndexMetaDataMmap metadataMmap;
          metadataMmap.setup(permutationFilename + MMAP_FILE_SUFFIX,
                             ad_utility::CreateTag{});
          CompressedRelationWriter writer{
              std::move(permutationFileForWritingRelations), blockSizeInBytes};
          for (size_t i = 0; i < relationIds.size(); ++i) {
            // The third argument is the number of distinct elements. We set it
            // to 1 here because it is irrelevant for the purposes of this test.
            Id relationId = relationIds[i];
            auto relationMetadata = writer.addRelation(
                relationId, getBufferedIdTable(triplesInIndex, relationId), 1);
            metadataMmap.add(relationMetadata);
          }
          metadataMmap.blockData() = std::move(writer).getFinishedBlocks();
          ad_utility::File permutationFileForWritingMetadata(
              permutationFilename, "r+");
          metadataMmap.appendToFile(&permutationFileForWritingMetadata);
        }

        // Create a permutation based on this.
        Permutation permutation{"PSO", ".pso", {1, 0, 2}};
        permutation.loadFromDisk(basename);

        // Check that the permutation indeed consists of the relations that we
        // have written to it.
        {
          IdTable result(2, ad_utility::testing::makeAllocator());
          for (Id relationId : relationIds) {
            permutation.scan(relationId, &result);
            std::cout << "Relation: " << relationId << " -> " << result
                      << std::endl;
          }
        }

        // Now locate the triples from `triplesToLocate` in the permutation.
        LocatedTriplesPerBlock locatedTriplesPerBlock;
        for (size_t i = 0; i < triplesToLocate.size(); ++i) {
          locatedTriplesPerBlock.add(LocatedTriple::locateTripleInPermutation(
              triplesToLocate(i, 0), triplesToLocate(i, 1),
              triplesToLocate(i, 2), permutation));
        }

        // Check that the locations are as expected. Process in order of
        // increasing block index because it's easier to debug.
        std::cout << locatedTriplesPerBlock;
        std::vector<size_t> blockIndices;
        for (auto [blockIndex, expectedLocatedTriples] :
             expectedLocatedTriplesPerBlock) {
          blockIndices.push_back(blockIndex);
        }
        std::sort(blockIndices.begin(), blockIndices.end());
        for (auto blockIndex : blockIndices) {
          ASSERT_TRUE(locatedTriplesPerBlock.map_.contains(blockIndex))
              << "blockIndex = " << blockIndex << " not found";
          std::ostringstream os;
          os << locatedTriplesPerBlock.map_.at(blockIndex);
          std::string computedLocatedTriples = os.str();
          std::string expectedLocatedTriples =
              expectedLocatedTriplesPerBlock.at(blockIndex);
          ASSERT_EQ(computedLocatedTriples, expectedLocatedTriples)
              << "blockIndex = " << blockIndex;
        }
        ASSERT_EQ(locatedTriplesPerBlock.map_.size(),
                  expectedLocatedTriplesPerBlock.size());

        // Delete the permutation files.
        ad_utility::deleteFile(permutationFilename);
        ad_utility::deleteFile(permutationFilename + MMAP_FILE_SUFFIX);
      };

  // Triples in the index.
  IdTable triplesInIndex = makeIdTableFromVector({{1, 10, 10},    // Row 0
                                                  {2, 10, 10},    // Row 1
                                                  {2, 15, 20},    // Row 2
                                                  {2, 15, 30},    // Row 3
                                                  {2, 20, 10},    // Row 4
                                                  {2, 30, 20},    // Row 5
                                                  {2, 30, 30},    // Row 6
                                                  {3, 10, 10}});  // Row 7

  // Locate the following triples, some of which exist in the relation and some
  // of which do not, and which cover a variety of positons, including triples
  // that are larger than all existing triples.
  IdTable triplesToLocate =
      makeIdTableFromVector({{2, 15, 20},    // Equals Row 2
                             {2, 14, 20},    // Before Row 2
                             {2, 20, 10},    // Equals Row 4
                             {2, 30, 20},    // Equals Row 5
                             {2, 30, 30},    // Equals Row 6
                             {2, 30, 31},    // Before Row 7
                             {9, 30, 32}});  // Larger than all.

  // Now test for multiple block sizes (16 bytes is the minimum).
  // testing::internal::CaptureStdout();
  std::cout << "Index triples: " << triplesInIndex << std::endl;
  std::cout << "Delta triples: " << triplesToLocate << std::endl;

  // With block size 16, we have each triple in its own block.
  testWithGivenBlockSize(
      triplesInIndex, triplesToLocate, 16,
      {{2, "{ LT(2 0 V:2 V:14 V:20 0) LT(2 0 V:2 V:15 V:20 1) }"},
       {4, "{ LT(4 0 V:2 V:20 V:10 1) }"},
       {5, "{ LT(5 0 V:2 V:30 V:20 1) }"},
       {6, "{ LT(6 0 V:2 V:30 V:30 1) }"},
       {7, "{ LT(7 0 V:2 V:30 V:31 0) }"},
       {8, "{ LT(8 x V:9 V:30 V:32 0) }"}});

  // With block size 32, we have five blocks (Block 0 = Row 0, Block 1 = Row
  // 1+2, Block 2 = Row 3+4, Block 3 = Row 5+6, Block 4 = Row 7). Note that a
  // relation that spans multiple blocks has these blocks on its own.
  testWithGivenBlockSize(
      triplesInIndex, triplesToLocate, 32,
      {{1, "{ LT(1 1 V:2 V:14 V:20 0) LT(1 1 V:2 V:15 V:20 1) }"},
       {2, "{ LT(2 1 V:2 V:20 V:10 1) }"},
       {3, "{ LT(3 0 V:2 V:30 V:20 1) LT(3 1 V:2 V:30 V:30 1) }"},
       {4, "{ LT(4 0 V:2 V:30 V:31 0) }"},
       {5, "{ LT(5 x V:9 V:30 V:32 0) }"}});

  // With block size 48, we have four blocks (Block 0 = Row 0, Block 1 = Row
  // 1+2+3, Block 2 = Row 4+5+6, Block 3 = Row 7).
  testWithGivenBlockSize(
      triplesInIndex, triplesToLocate, 48,
      {{1, "{ LT(1 1 V:2 V:14 V:20 0) LT(1 1 V:2 V:15 V:20 1) }"},
       {2,
        "{ LT(2 0 V:2 V:20 V:10 1) LT(2 1 V:2 V:30 V:20 1)"
        " LT(2 2 V:2 V:30 V:30 1) }"},
       {3, "{ LT(3 0 V:2 V:30 V:31 0) }"},
       {4, "{ LT(4 x V:9 V:30 V:32 0) }"}});

  // With block size 100'000, we have one block.
  testWithGivenBlockSize(triplesInIndex, triplesToLocate, 100'000,
                         {{0,
                           "{ LT(0 2 V:2 V:14 V:20 0) LT(0 2 V:2 V:15 V:20 1) "
                           "LT(0 4 V:2 V:20 V:10 1) LT(0 5 V:2 V:30 V:20 1) "
                           "LT(0 6 V:2 V:30 V:30 1) LT(0 7 V:2 V:30 V:31 0) }"},
                          {1, "{ LT(1 x V:9 V:30 V:32 0) }"}});
}
