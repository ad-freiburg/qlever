//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./IndexTestHelpers.h"
#include "absl/strings/str_split.h"
#include "index/DeltaTriples.h"
#include "index/IndexImpl.h"
#include "parser/TurtleParser.h"

// Fixture that sets up a test index.
class DeltaTriplesTest : public ::testing::Test {
 protected:
  // The triples in our test index (as a `std::vector` so that we have easy
  // access to each triple separately in the tests below).
  static constexpr const char* testTurtle =
      "<one> <next> 2 . "
      "<b> <upp> <B> . "
      "<c> <upp> <C> . "
      "<A> <low> <a> . "
      "<B> <low> <b> . "
      "<C> <low> <c> . "
      "<a> <next> <b> . "
      "<b> <next> <c> . "
      "<b> <prev> <a> . "
      "<c> <prev> <b> . "
      "<A> <next> <B> . "
      "<B> <next> <C> . "
      "<B> <prev> <A> . "
      "<C> <prev> <B>";

  // Query execution context with index for testing, see `IndexTestHelpers.h`.
  QueryExecutionContext* testQec = ad_utility::testing::getQec(testTurtle);

  // The individual triples (useful for testing below).
  //
  // NOTE: It looks like it would make more send to define a static class member
  // of type `std::vector<std::string>` in the first place that contains the
  // individual triples and then concatenate them with `absl::StrJoin` for
  // `getQec`, but C++ doesn't allow non-literal static class members.
  std::vector<std::string_view> testTriples() {
    return absl::StrSplit(testTurtle, " . ");
  }

  // Make `TurtleTriple` from given Turtle input.
  TurtleTriple makeTurtleTriple(std::string turtle) {
    TurtleStringParser<Tokenizer> parser;
    parser.parseUtf8String(std::move(turtle));
    AD_CONTRACT_CHECK(parser.getTriples().size() == 1);
    return parser.getTriples()[0];
  }

  // Make `IdTriple` from given Turtle input (the first argument is not `const`
  // because we might change the local vocabulary).
  DeltaTriples::IdTriple makeIdTriple(DeltaTriples& deltaTriples,
                                      std::string turtle) {
    return deltaTriples.getIdTriple(makeTurtleTriple(std::move(turtle)));
  }

  // Get the permutation with the given `enum` name.
  // ...
};

// Test the constructor.
TEST_F(DeltaTriplesTest, constructor) {
  DeltaTriples deltaTriples(testQec->getIndex());
  ASSERT_EQ(deltaTriples.numInserted(), 0);
  ASSERT_EQ(deltaTriples.numDeleted(), 0);
}

// Test clear after inserting or deleting a few triples.
TEST_F(DeltaTriplesTest, clear) {
  // Insert then clear.
  DeltaTriples deltaTriples(testQec->getIndex());
  deltaTriples.insertTriple(makeTurtleTriple("<a> <b> <c>"));
  ASSERT_EQ(deltaTriples.numInserted(), 1);
  ASSERT_EQ(deltaTriples.numDeleted(), 0);
  deltaTriples.clear();
  ASSERT_EQ(deltaTriples.numInserted(), 0);
  ASSERT_EQ(deltaTriples.numDeleted(), 0);

  // Delete then clear.
  deltaTriples.deleteTriple(makeTurtleTriple("<a> <b> <c>"));
  ASSERT_EQ(deltaTriples.numInserted(), 0);
  ASSERT_EQ(deltaTriples.numDeleted(), 1);
  deltaTriples.clear();
  ASSERT_EQ(deltaTriples.numInserted(), 0);
  ASSERT_EQ(deltaTriples.numInserted(), 0);
}

// Check that `findTripleInAllPermutations` locates triples correctly in all
// cases (triples that exist in the index, as well as those that do not).
TEST_F(DeltaTriplesTest, findTripleInAllPermutations) {
  const Index& index = testQec->getIndex();
  DeltaTriples deltaTriples(index);

  // Check the given `FindTripleResult` for the given permutation.
  auto checkFindTripleResult =
      [&](const DeltaTriples::FindTripleResult& findTripleResult,
          const auto& permutation) {
        auto& file = permutation._file;
        const auto& meta = permutation._meta;
        const auto& reader = permutation._reader;

        const size_t& blockIndex = findTripleResult.blockIndex;
        const size_t& rowIndexInBlock = findTripleResult.rowIndexInBlock;

        auto& pname = permutation._readableName;
        std::string name1 = deltaTriples.getNameForId(findTripleResult.id1);
        std::string name2 = deltaTriples.getNameForId(findTripleResult.id2);
        std::string name3 = deltaTriples.getNameForId(findTripleResult.id3);
        std::string tname = absl::StrCat(std::string{pname[0]}, "=", name1, " ",
                                         std::string{pname[1]}, "=", name2, " ",
                                         std::string{pname[2]}, "=", name3);
        std::string msg =
            absl::StrCat("Permutation ", pname, ", triple ", tname, "\n");

        const vector<CompressedBlockMetadata>& blocks = meta.blockData();
        ASSERT_LT(blockIndex, blocks.size()) << msg;
        const auto& block = blocks.at(blockIndex);
        const auto& blockTuples =
            reader.readAndDecompressBlock(block, file, std::nullopt);
        ASSERT_LT(rowIndexInBlock, blockTuples.size()) << msg;
        ASSERT_EQ(blockTuples(rowIndexInBlock, 0), findTripleResult.id2) << msg;
        ASSERT_EQ(blockTuples(rowIndexInBlock, 1), findTripleResult.id3) << msg;
      };

  // Check if each existing triple is located correctly in every permutation.
  size_t numTriples = 0;
  for (std::string_view triple : testTriples()) {
    DeltaTriples::IdTriple idTriple =
        makeIdTriple(deltaTriples, std::string{triple});
    deltaTriples.findTripleInAllPermutations(idTriple);
    ++numTriples;
    ASSERT_EQ(deltaTriples.posFindTripleResults_.size(), numTriples);
    checkFindTripleResult(deltaTriples.posFindTripleResults_.back(),
                          index.getImpl().POS());
    checkFindTripleResult(deltaTriples.psoFindTripleResults_.back(),
                          index.getImpl().PSO());
    checkFindTripleResult(deltaTriples.spoFindTripleResults_.back(),
                          index.getImpl().SPO());
    checkFindTripleResult(deltaTriples.sopFindTripleResults_.back(),
                          index.getImpl().SOP());
    checkFindTripleResult(deltaTriples.opsFindTripleResults_.back(),
                          index.getImpl().OPS());
    checkFindTripleResult(deltaTriples.ospFindTripleResults_.back(),
                          index.getImpl().OSP());
  }
}

// Visualize the result of `findTripleInPermutation` for one particular triple
// by showing the whole block (for understanding and debugging only, this will
// eventually be deleted).
TEST_F(DeltaTriplesTest, findTripleInAllPermutationsVisualize) {
  DeltaTriples deltaTriples(testQec->getIndex());
  std::string tripleAsString = "<a> <next> <b>";
  // std::string tripleAsString = "<c> <upp> <C>";
  // std::string tripleAsString = "<B> <low> <b>";
  // std::string tripleAsString = "<b> <0> <b>";
  // std::string tripleAsString = "<D> <low> <d>";
  std::cout << std::endl;
  std::cout << "Searching the following triple: " << tripleAsString
            << std::endl;
  std::cout
      << "For each permutation, find the first element that is not smaller"
      << std::endl;

  // Search the triple in all permutations.
  DeltaTriples::IdTriple idTriple = makeIdTriple(deltaTriples, tripleAsString);
  deltaTriples.findTripleInAllPermutations(idTriple, true);
  std::cout << std::endl;
}
