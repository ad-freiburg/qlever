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

// Fixture that ... TODO:explain.
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

// Test `Permutation::scan` (and hence also `CompressedRelation::scan`) with
// triples merged from a `locatedTriplesPerBlock` object.
TEST_F(LocatedTriplesTest, scanWithMergeTriples) {
  // The actual test, for a given block size.
  auto testWithGivenBlockSize = [](const size_t blockSizeInBytes,
                                   size_t numRelations, Id relationId) {
    std::string basename = "LocatedTriplesTest.scanWithMergeTriples";
    std::string permutationFilename = basename + ".index.pso";

    // Helper lambda for creating a `BufferedIdTable` (which we need for
    // `CompressedRelationWriter` from an ordinary `IdTable` with two columns).
    //
    // TODO: Something like this is also used in `CompressedRelationsTest`, so
    // it should be in a helper class.
    auto getBufferedIdTable = [](const IdTable& idTable) -> BufferedIdTable {
      // Note that these files are never created because we set the threshold
      // for writing to disk so large.
      std::string bufferFilename1 = "compressedRelationWriter.buffer1.dat";
      std::string bufferFilename2 = "compressedRelationWriter.buffer2.dat";
      AD_CONTRACT_CHECK(idTable.numColumns() == 2);
      BufferedIdTable bufferedIdTable{
          2,
          std::array{ad_utility::BufferedVector<Id>{
                         std::numeric_limits<size_t>::max(), bufferFilename1},
                     ad_utility::BufferedVector<Id>{
                         std::numeric_limits<size_t>::max(), bufferFilename2}}};
      for (size_t i = 0; i < idTable.size(); ++i) {
        bufferedIdTable.push_back({idTable(i, 0), idTable(i, 1)});
      }
      return bufferedIdTable;
    };

    // Our test relation.
    IdTable relation = makeIdTableFromVector({{10, 10},    // Row 0
                                              {15, 20},    // Row 1
                                              {15, 30},    // Row 2
                                              {20, 10},    // Row 3
                                              {30, 20},    // Row 4
                                              {30, 30}});  // Row 5

    // Write it to disk (adapted from `CompressedRelationsTest`). The last value
    // of the call to `addRelation` is the number of distinct elements.
    ad_utility::File permutationFileForWritingRelations{permutationFilename,
                                                        "w"};
    CompressedRelationWriter writer{
        std::move(permutationFileForWritingRelations), blockSizeInBytes};
    for (size_t i = 1; i <= numRelations; ++i) {
      writer.addRelation(V(i), getBufferedIdTable(relation), relation.size());
    }
    writer.finish();
    auto metadataPerRelation = writer.getFinishedMetaData();
    auto metadataPerBlock = writer.getFinishedBlocks();
    AD_CORRECTNESS_CHECK(metadataPerRelation.size() == numRelations);

    // Append the metadata to the index file.
    IndexMetaDataHmap metadata;
    std::ranges::for_each(metadataPerRelation,
                          [&metadata](auto& md) { metadata.add(md); });
    metadata.blockData() = metadataPerBlock;
    ad_utility::File permutationFileForWritingMetadata{permutationFilename,
                                                       "r+"};
    metadata.appendToFile(&permutationFileForWritingMetadata);
    permutationFileForWritingMetadata.close();

    // Create a permutation based on this.
    LocatedTriplesPerBlock locatedTriplesPerBlock;
    Permutation::PermutationImpl<SortByPSO, IndexMetaDataHmap> permutation{
        SortByPSO(), "PSO", ".pso", {1, 0, 2}, locatedTriplesPerBlock};
    permutation.loadFromDisk(basename);
    // ad_utility::File permutationFileForReading{permutationFilename, "r"};
    // permutation._file = std::move(permutationFileForReading);
    // permutation._meta = metadata;
    // permutation._isLoaded = true;

    // Read the (for this test: first and only) relation from disk and check
    // that it is the same.
    {
      IdTable result(2, ad_utility::testing::makeAllocator());
      permutation.scan(relationId, &result);
      ASSERT_EQ(result, relation);
    }

    // Helper lambda for adding to `locatedTriplesPerBlock`.
    auto locatedTriplesPerBlockAdd = [&locatedTriplesPerBlock, &relationId,
                                      &permutation](Id id2, Id id3) {
      locatedTriplesPerBlock.add(LocatedTriple::locateTripleInPermutation(
          relationId, id2, id3, permutation));
    };

    // Again, but with some located triples merged (three inserts, four
    // deletes).
    locatedTriplesPerBlockAdd(V(15), V(20));  // Delete.
    locatedTriplesPerBlockAdd(V(14), V(20));  // Insert.
    locatedTriplesPerBlockAdd(V(20), V(10));  // Delete.
    locatedTriplesPerBlockAdd(V(30), V(20));  // Delete.
    locatedTriplesPerBlockAdd(V(30), V(30));  // Delete.
    locatedTriplesPerBlockAdd(V(30), V(31));  // Insert at very end.
    locatedTriplesPerBlockAdd(V(30), V(32));  // Insert at very end.
    std::cout << locatedTriplesPerBlock;
    {
      IdTable result(2, ad_utility::testing::makeAllocator());
      permutation.scan(relationId, &result);
      IdTable resultExpected = makeIdTableFromVector({{10, 10},    // Row 0
                                                      {14, 20},    // Row 1
                                                      {15, 30},    // Row 2
                                                      {30, 31},    // Row 3
                                                      {30, 32}});  // Row 4
      ASSERT_EQ(result, resultExpected);
    }

    // Now a scan where two `Id`s are fixed.
    {
      IdTable result(1, ad_utility::testing::makeAllocator());
      result.resize(2);
      permutation.scan(relationId, V(30), &result);
      IdTable resultExpected = makeIdTableFromVector({{31}, {32}});
      ASSERT_EQ(result, resultExpected);
    }

    // Delete the file with the compressed relations.
    ad_utility::deleteFile(permutationFilename);
  };

  // Now test for multiple block sizes (16 bytes is the minimum), relation
  // sizes, and relations.
  //
  // TODO: Currently fails if `numRelations > 1`.
  size_t numRelations = 1;
  for (size_t i = 1; i <= numRelations; ++i) {
    testWithGivenBlockSize(16, numRelations, V(i));
    testWithGivenBlockSize(32, numRelations, V(i));
    testWithGivenBlockSize(48, numRelations, V(i));
    testWithGivenBlockSize(64, numRelations, V(i));
    testWithGivenBlockSize(100'000, numRelations, V(i));
  }
}
