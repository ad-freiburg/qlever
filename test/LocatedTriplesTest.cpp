//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "index/LocatedTriples.h"

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
       LocatedTriple{1, 0, V(11), V(3), V(0), true},
       LocatedTriple{2, 0, V(20), V(4), V(0), true},
       LocatedTriple{2, 0, V(21), V(5), V(0), true},
       LocatedTriple{3, 0, V(30), V(6), V(0), true},
       LocatedTriple{3, 0, V(32), V(7), V(0), true}});
  ASSERT_EQ(locatedTriplesPerBlock.numBlocks(), 3);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(), 7);

  // Check the total counts per block.
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(1), 3);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(2), 2);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(3), 2);

  // Check the counts per block for a given `id1`.
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(1, V(10)), 2);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(1, V(11)), 1);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(2, V(20)), 1);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(2, V(21)), 1);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(3, V(30)), 1);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(3, V(32)), 1);

  // Check the counts per block for a given `id1` and `id2`.
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(1, V(10), V(1)), 1);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(1, V(10), V(2)), 1);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(1, V(11), V(3)), 1);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(2, V(20), V(4)), 1);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(2, V(21), V(5)), 1);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(3, V(30), V(6)), 1);
  ASSERT_EQ(locatedTriplesPerBlock.numTriples(3, V(32), V(7)), 1);
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
    IdTable result(block.numColumns(), ad_utility::testing::makeAllocator());
    result.resize(resultExpected.size());
    locatedTriplesPerBlock.mergeTriples(1, block, result, 0);
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
    IdTable result(block.numColumns(), ad_utility::testing::makeAllocator());
    result.resize(resultExpected.size());
    locatedTriplesPerBlock.mergeTriples(1, block, result, 0, 0, V(2));
    ASSERT_EQ(result, resultExpected);
  }

  // Repeat but with a partial block that leaves out the first two elements of
  // `block` (and correspondingly `offsetOfBlock == 2` in `mergeTriples`).
  IdTable blockTruncated = block.clone();
  std::shift_left(blockTruncated.begin(), blockTruncated.end(), 2);
  blockTruncated.resize(block.size() - 2);
  {
    IdTable resultExpected = makeIdTableFromVector({{15, 30},    // Row 0
                                                    {20, 10},    // Row 1
                                                    {21, 11},    // Row 2
                                                    {30, 10},    // Row 3
                                                    {30, 30}});  // Row 4
    IdTable result(block.numColumns(), ad_utility::testing::makeAllocator());
    result.resize(resultExpected.size());
    locatedTriplesPerBlock.mergeTriples(1, blockTruncated, result, 0, 2, V(2));
    ASSERT_EQ(result, resultExpected);
  }

  // Merge only the triples with `id1 == V(2)` and `id2 == V(30)` into the same
  // truncated block as above (one triples inserted, one triple deleted).
  {
    IdTable resultExpected = makeIdTableFromVector({{15, 30},    // Row 0
                                                    {20, 10},    // Row 1
                                                    {30, 10},    // Row 2
                                                    {30, 30}});  // Row 3
    IdTable result(block.numColumns(), ad_utility::testing::makeAllocator());
    result.resize(resultExpected.size());
    locatedTriplesPerBlock.mergeTriples(1, blockTruncated, result, 0, 2, V(2),
                                        V(30));
    ASSERT_EQ(result, resultExpected);
  }
}
