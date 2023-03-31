//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./IndexTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "absl/strings/str_split.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "index/DeltaTriples.h"
#include "index/IndexImpl.h"
#include "parser/TurtleParser.h"

// Shortcuts to these full type names used frequently in the following.
// using IdTriple;
static const std::vector<Index::Permutation> permutationEnums = {
    Index::Permutation::PSO, Index::Permutation::POS, Index::Permutation::SPO,
    Index::Permutation::SOP, Index::Permutation::OPS, Index::Permutation::OSP};

// Fixture that sets up a test index.
class DeltaTriplesTest : public ::testing::Test {
 protected:
  // The triples in our test index (as a `std::vector` so that we have easy
  // access to each triple separately in the tests below).
  static constexpr const char* testTurtle =
      "<a> <upp> <A> . "
      "<b> <upp> <B> . "
      "<c> <upp> <C> . "
      "<A> <low> <a> . "
      "<B> <low> <b> . "
      "<C> <low> <c> . "
      "<a> <next> <b> . "
      "<b> <next> <c> . "
      "<A> <next> <B> . "
      "<B> <next> <C> . "
      "<b> <prev> <a> . "
      "<c> <prev> <b> . "
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
  std::vector<std::string_view> getTestTriples() {
    return absl::StrSplit(testTurtle, " . ");
  }

  // Make `TurtleTriple` from given Turtle input.
  TurtleTriple makeTurtleTriple(std::string_view turtle) {
    TurtleStringParser<Tokenizer> parser;
    parser.parseUtf8String(std::string{turtle});
    AD_CONTRACT_CHECK(parser.getTriples().size() == 1);
    return parser.getTriples()[0];
  }

  // Make `IdTriple` from given Turtle input (the first argument is not `const`
  // because we might change the local vocabulary).
  IdTriple makeIdTriple(DeltaTriples& deltaTriples, std::string turtle) {
    return deltaTriples.getIdTriple(makeTurtleTriple(std::move(turtle)));
  }

  // Resolve the name for the given `Id` using the `Index` and `LocalVocab` from
  // the given `deltaTriples` object.
  std::string getNameForId(Id id, const DeltaTriples& deltaTriples) const {
    auto lookupResult = ExportQueryExecutionTrees::idToStringAndType(
        deltaTriples.getIndex(), id, deltaTriples.localVocab());
    AD_CONTRACT_CHECK(lookupResult.has_value());
    const auto& [value, type] = lookupResult.value();
    // std::ostringstream os;
    // os << "[" << id << "]";
    return type ? absl::StrCat("\"", value, "\"^^<", type, ">") : value;
    // : absl::StrCat(value, " ", os.str());
  };

  // Get human-readable names for the given `permutation` and `idTriple`. This
  // is needed for proper message when an assert fails in the tests below. The
  // `idTriple` is assumed to be already in the right permutation (for example,
  // for POS, `idTriple[0]` is the `Id` of the predicate).
  template <typename Permutation>
  std::pair<std::string, std::string> getNicePermutationAndTripleName(
      const DeltaTriples& deltaTriples, const Permutation& permutation,
      const IdTriple idTriple) {
    auto& namePermutation = permutation._readableName;
    std::string nameId1 = getNameForId(idTriple[0], deltaTriples);
    std::string nameId2 = getNameForId(idTriple[1], deltaTriples);
    std::string nameId3 = getNameForId(idTriple[2], deltaTriples);
    std::string nameTriple =
        absl::StrCat(std::string{namePermutation[0]}, "=", nameId1, " ",
                     std::string{namePermutation[1]}, "=", nameId2, " ",
                     std::string{namePermutation[2]}, "=", nameId3);
    return {namePermutation, nameTriple};
  }

  // Check that all six `triplesWithPositionsPerBlock` lists have the given
  // number of `LocatedTriple` objects.
  void checkTriplesWithPositionsPerBlockSize(const DeltaTriples& deltaTriples,
                                             size_t expectedSize) {
    for (Index::Permutation permutation : permutationEnums) {
      ASSERT_EQ(deltaTriples.getTriplesWithPositionsPerBlock(permutation)
                    .numTriples(),
                expectedSize);
    }
  }

  // Get the complete sequence of "relation" (most significant) `Id`s for the
  // given permutation. The result is a `std::vector` of `std::vector<Id>`,
  // where the index into the outer vector is a block index, and each inner
  // vector is as large as the corresponding block.
  //
  // NOTE: To save index storage space, these `Id`s are not stored explicitly in
  // the blocks, but implicitly in the `CompressedRelationMetadata` objects of a
  // permutation. For our test of `locateTripleInAllPermutations` below, we need
  // random access to these `Id`s.
  template <typename Permutation>
  std::vector<std::vector<Id>> getAllRelationIdsForPermutation(
      const Permutation& permutation) {
    // The metadata for each block (since our blocks are large, this is not a
    // lot of data).
    const std::vector<CompressedBlockMetadata>& metadataPerBlock =
        permutation._meta.blockData();

    // Make room for the `Id`s in our final result: one `std::vector<Id>`` per
    // block, and each of these is as large as the respective block.
    std::vector<std::vector<Id>> result(metadataPerBlock.size());
    for (size_t i = 0; i < result.size(); ++i) {
      result[i].resize(metadataPerBlock[i]._numRows, Id::makeUndefined());
    }

    // Iterate over all relations.
    //
    // NOTE: The metadata per "relation" is stored as a hash map for POS and PSO
    // (where there are typically few distinct "relations", that is,
    // predicates), and as a vector for the other four permutations (there are
    // typically many distinct subjects and objects). Whatever the type, we can
    // always iterate over the complete set, see `MetaDataHandler.h`.
    const auto& metadataPerRelation = permutation._meta._data;
    for (auto it = metadataPerRelation.begin(); it != metadataPerRelation.end();
         ++it) {
      // Get the `Id` of this relation, and where it starts in its (at this
      // unknown) block, and how many triples it has overall.
      const CompressedRelationMetadata& relationMetadata = it.getMetaData();
      Id relationId = relationMetadata._col0Id;
      size_t offsetInBlock = relationMetadata._offsetInBlock;
      size_t numTriples = relationMetadata._numRows;

      // Find the index of the first block that contains triples from this
      // relation.
      const auto block = std::lower_bound(
          metadataPerBlock.begin(), metadataPerBlock.end(), relationId,
          [&](const CompressedBlockMetadata& block, const Id& id) -> bool {
            return block._col0LastId < id;
          });
      size_t blockIndex = block - metadataPerBlock.begin();
      AD_CORRECTNESS_CHECK(blockIndex < metadataPerBlock.size());
      AD_CORRECTNESS_CHECK(block->_col0FirstId <= relationId);
      AD_CORRECTNESS_CHECK(block->_col0LastId >= relationId);

      // If the relation fits into a single block, we need to write the relation
      // `Id` only in one block of our result. Otherwise, we have a sequence of
      // blocks for only that relation `Id`.
      if (offsetInBlock != std::numeric_limits<size_t>::max()) {
        AD_CORRECTNESS_CHECK(offsetInBlock + numTriples <= block->_numRows);
        for (size_t i = offsetInBlock; i < offsetInBlock + numTriples; ++i) {
          result[blockIndex][i] = relationId;
        }
      } else {
        size_t count = 0;
        while (blockIndex < metadataPerBlock.size() &&
               metadataPerBlock[blockIndex]._col0FirstId == relationId) {
          const auto& block = metadataPerBlock[blockIndex];
          AD_CORRECTNESS_CHECK(block._col0LastId == relationId);
          for (size_t i = 0; i < block._numRows; ++i) {
            result[blockIndex][i] = relationId;
          }
          ++blockIndex;
          count += block._numRows;
        }
        AD_CORRECTNESS_CHECK(count == numTriples);
      }
    }

    // Check that all slots in `result` have been written and then return it.
    for (const auto& resultBlock : result) {
      for (const Id& id : resultBlock) {
        AD_CORRECTNESS_CHECK(id != Id::makeUndefined());
      }
    }
    return result;
  }
};

// Print relation `Id`s for selected permutation (for debugging only).
TEST_F(DeltaTriplesTest, showAllRelationIdsForPermutation) {
  bool runThisTest = false;
  if (runThisTest) {
    // Compute relation `Id`s for POS (choose another premutation if you wish).
    const Index& index = testQec->getIndex();
    DeltaTriples deltaTriples(index);
    const auto& permutation = index.getImpl().POS();
    const std::vector<std::vector<Id>> allRelationIdsForPermutation =
        getAllRelationIdsForPermutation(permutation);

    // Show them per block.
    std::cout << endl;
    std::cout << "All relation IDs for permutation "
              << permutation._readableName << ":" << std::endl;
    size_t blockCount = 0;
    for (const auto& block : allRelationIdsForPermutation) {
      std::cout << "Block #" << (++blockCount) << ":";
      for (const Id& id : block) {
        std::cout << " "
                  << (id != Id::makeUndefined() ? getNameForId(id, deltaTriples)
                                                : "UNDEF")
                  << std::flush;
      }
      std::cout << std::endl;
    }
    std::cout << std::endl;
  }
}

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
  deltaTriples.insertTriple(makeTurtleTriple("<a> <UPP> <A>"));
  ASSERT_EQ(deltaTriples.numInserted(), 1);
  ASSERT_EQ(deltaTriples.numDeleted(), 0);
  checkTriplesWithPositionsPerBlockSize(deltaTriples, 1);
  deltaTriples.clear();
  ASSERT_EQ(deltaTriples.numInserted(), 0);
  ASSERT_EQ(deltaTriples.numDeleted(), 0);
  checkTriplesWithPositionsPerBlockSize(deltaTriples, 0);

  // Delete then clear.
  deltaTriples.deleteTriple(makeTurtleTriple("<A> <low> <a>"));
  ASSERT_EQ(deltaTriples.numInserted(), 0);
  ASSERT_EQ(deltaTriples.numDeleted(), 1);
  checkTriplesWithPositionsPerBlockSize(deltaTriples, 1);
  deltaTriples.clear();
  ASSERT_EQ(deltaTriples.numInserted(), 0);
  ASSERT_EQ(deltaTriples.numInserted(), 0);
  checkTriplesWithPositionsPerBlockSize(deltaTriples, 0);
}

// Check that insert and delete work as they should. The core of this test is to
// check that `locateTripleInPermutation` and `locateTripleInAllPermutations`
// work correctly.
//
// TODO: Wouldn't it make more sense to test the mentioned functions instead of
// `insertTriple` and `deleteTriple`?
TEST_F(DeltaTriplesTest, insertAndDeleteTriples) {
  const Index& index = testQec->getIndex();
  DeltaTriples deltaTriples(index);

  // Check the given `locatedTriple` (a block index, an index in the
  // block, and a triple) is correct for the given permutation as follows:
  //
  // 1. If `locatedTriple.existsInIndex == true`, check that the
  // triple indeed occurs at that position in the respective triple.
  //
  // 2. If `locatedTriple.existsInIndex == false`, check that the
  // triple at the position is larger and the triple at the previous
  // position is smaller.
  auto checkTripleWithPositionInPermutation =
      [&](const LocatedTriple& locatedTriple, const auto& permutation,
          const std::vector<std::vector<Id>>& relationIdsPerBlock) {
        // Shortcuts for the tiples ids and its position.
        const size_t blockIndex = locatedTriple.blockIndex;
        const size_t rowIndexInBlock = locatedTriple.rowIndexInBlock;
        const bool existsInIndex = locatedTriple.existsInIndex;
        const IdTriple deltaTriple{locatedTriple.id1, locatedTriple.id2,
                                   locatedTriple.id3};

        // Members for accessing the data of a permutation.
        auto& file = permutation._file;
        const auto& meta = permutation._meta;
        const auto& reader = permutation._reader;

        // Prepare a message for when one of our assertions fails, with nice
        // names for the permutation and the `deltaTriple`.
        auto [namePermutation, nameTriple] = getNicePermutationAndTripleName(
            deltaTriples, permutation, deltaTriple);
        std::string msg =
            absl::StrCat("Permutation ", namePermutation, ", triple ",
                         nameTriple, ", block index ", blockIndex,
                         ", row index in block ", rowIndexInBlock, "\n");

        // If the `blockIndex` is beyond the last index, check the following:
        //
        // 1. The delta triple does not exist in the index
        // 2. The delta triple is larger than all triples in the index
        // 3. Exit this test (there is nothing more to test in that case)
        const vector<CompressedBlockMetadata>& metadataPerBlock =
            meta.blockData();
        AD_CONTRACT_CHECK(metadataPerBlock.size() > 0);
        IdTriple lastTriple{metadataPerBlock.back()._col0LastId,
                            metadataPerBlock.back()._col1LastId,
                            metadataPerBlock.back()._col2LastId};
        if (blockIndex >= metadataPerBlock.size()) {
          ASSERT_EQ(blockIndex, metadataPerBlock.size()) << msg;
          ASSERT_FALSE(existsInIndex);
          ASSERT_GT(deltaTriple, lastTriple);
          return;
        }

        // Read the triple at the block position and at the previous position
        // (which might be in the previous block).
        //
        // TODO: We assume here that `Id::makeUndefined()` is strictly smaller
        // than any regular `Id`. Is that correct?
        //
        // NOTE: When `blockIndex` is valid (we have handled the other case
        // already above), `rowIndexInBlock` should always be a valid index into
        // the block (and never one too large); check the semantics of
        // `locateTripleInAllPermutations`.
        const auto& blockMetadata = metadataPerBlock.at(blockIndex);
        const auto& blockTuples =
            reader.readAndDecompressBlock(blockMetadata, file, std::nullopt);
        ASSERT_LT(rowIndexInBlock, blockTuples.size()) << msg;
        IdTriple blockTriple{relationIdsPerBlock[blockIndex][rowIndexInBlock],
                             blockTuples(rowIndexInBlock, 0),
                             blockTuples(rowIndexInBlock, 1)};
        auto blockTriplePrevious = [&]() -> IdTriple {
          if (rowIndexInBlock > 0) {
            return IdTriple{
                relationIdsPerBlock[blockIndex][rowIndexInBlock - 1],
                blockTuples(rowIndexInBlock - 1, 0),
                blockTuples(rowIndexInBlock - 1, 1)};
          } else if (blockIndex > 0) {
            return IdTriple{metadataPerBlock[blockIndex - 1]._col0LastId,
                            metadataPerBlock[blockIndex - 1]._col1LastId,
                            metadataPerBlock[blockIndex - 1]._col2LastId};
          } else {
            return IdTriple{Id::makeUndefined(), Id::makeUndefined(),
                            Id::makeUndefined()};
          }
        }();

        // Now we can check whether our delta triple is exactly at the right
        // location.
        if (existsInIndex) {
          ASSERT_EQ(blockTriple, deltaTriple) << msg;
          ASSERT_LT(blockTriplePrevious, deltaTriple) << msg;
        } else {
          ASSERT_GT(blockTriple, deltaTriple) << msg;
          ASSERT_LT(blockTriplePrevious, deltaTriple) << msg;
        }
      };

  // Check that all `locatedTriple` in `positionsPerBlock` are
  // correct for the given permutation.
  auto checkAllTriplesWithPositionsForPermutation =
      [&](const LocatedTriplesPerBlock& triplesWithPositionsPerBlock,
          const auto& permutation) {
        std::vector<std::vector<Id>> allRelationIdsForPermutation =
            getAllRelationIdsForPermutation(permutation);
        for (const auto& [blockIndex, triplesWithPositions] :
             triplesWithPositionsPerBlock.map_) {
          for (const auto& locatedTriple : triplesWithPositions) {
            checkTripleWithPositionInPermutation(locatedTriple, permutation,
                                                 allRelationIdsForPermutation);
          }
        }
      };

  // Check that all `locatedTriple`s are correct (for all
  // permutations). the given permutation.
  auto checkAllTriplesWithPositionForAllPermutations = [&](const DeltaTriples&
                                                               deltaTriples) {
    checkAllTriplesWithPositionsForPermutation(
        deltaTriples.getTriplesWithPositionsPerBlock(Index::Permutation::POS),
        index.getImpl().POS());
    checkAllTriplesWithPositionsForPermutation(
        deltaTriples.getTriplesWithPositionsPerBlock(Index::Permutation::PSO),
        index.getImpl().PSO());
    checkAllTriplesWithPositionsForPermutation(
        deltaTriples.getTriplesWithPositionsPerBlock(Index::Permutation::SPO),
        index.getImpl().SPO());
    checkAllTriplesWithPositionsForPermutation(
        deltaTriples.getTriplesWithPositionsPerBlock(Index::Permutation::SOP),
        index.getImpl().SOP());
    checkAllTriplesWithPositionsForPermutation(
        deltaTriples.getTriplesWithPositionsPerBlock(Index::Permutation::OPS),
        index.getImpl().OPS());
    checkAllTriplesWithPositionsForPermutation(
        deltaTriples.getTriplesWithPositionsPerBlock(Index::Permutation::OSP),
        index.getImpl().OSP());
  };

  // Check if each existing triple is located correctly in every
  // permutation.
  //
  // TODO: Check that `existsInIndex` was set correctly. Test test routine
  // above just take it from the tested `LocatedTriple` objects
  // (which might be wrong)
  //
  // TODO: Check that each triple that was located was indeed added to
  // each of the `LocatedTriplesPerBlock` objects.
  //
  // TODO: Eventually, we should test `insertTriple` and `deleteTriple`,
  // which only insert a triple when it doesn't exist in the original
  // index, and which only delete a triple, when it does exist in the
  // original index. But let's first get `locateTripleInAllPermutations`
  // correct. Note that to check whether a triple exists or not in the
  // original index, looking at one permutation suffices.
  const std::vector<std::string_view>& testTriples = getTestTriples();
  for (std::string_view triple : testTriples) {
    deltaTriples.deleteTriple(makeTurtleTriple(triple));
  }
  checkTriplesWithPositionsPerBlockSize(deltaTriples, testTriples.size());
  checkAllTriplesWithPositionForAllPermutations(deltaTriples);

  // Inserting the triples a second time should throw an exception (and not
  // change anything about the internal data structures).
  for (std::string_view triple : testTriples) {
    AD_EXPECT_THROW_WITH_MESSAGE(
        deltaTriples.deleteTriple(makeTurtleTriple(triple)),
        ::testing::HasSubstr("this deletion therefore has no effect"));
  }
  checkTriplesWithPositionsPerBlockSize(deltaTriples, testTriples.size());
  checkAllTriplesWithPositionForAllPermutations(deltaTriples);

  // Check that new triples are located correctly in every permutation.
  for (std::string_view triple : testTriples) {
    std::string newTriple{triple};
    newTriple[1] = 'X';
    deltaTriples.insertTriple(makeTurtleTriple(newTriple));
  }
  checkTriplesWithPositionsPerBlockSize(deltaTriples, 2 * testTriples.size());
  checkAllTriplesWithPositionForAllPermutations(deltaTriples);

  // Deleting the triples a second time should throw an exception (and not
  // change anything about the internal data structures).
  for (std::string_view triple : testTriples) {
    std::string newTriple{triple};
    newTriple[1] = 'X';
    AD_EXPECT_THROW_WITH_MESSAGE(
        deltaTriples.insertTriple(makeTurtleTriple(newTriple)),
        ::testing::HasSubstr("this insertion therefore has no effect"));
  }
  checkTriplesWithPositionsPerBlockSize(deltaTriples, 2 * testTriples.size());
  checkAllTriplesWithPositionForAllPermutations(deltaTriples);
}

// Visualize the result of `findTripleInPermutation` for one particular
// triple by showing the whole block (for understanding and debugging
// only, this will eventually be deleted).
TEST_F(DeltaTriplesTest, findTripleInAllPermutationsVisualize) {
  const Index& index = testQec->getIndex();
  DeltaTriples deltaTriples(index);
  // std::string tripleAsString = "<X> <upp> <A>";
  std::string tripleAsString = "<a> <next> <b>";
  // std::string tripleAsString = "<c> <upp> <C>";
  // std::string tripleAsString = "<B> <low> <b>";
  // std::string tripleAsString = "<b> <0> <b>";
  // std::string tripleAsString = "<D> <low> <d>";
  std::cout << std::endl;
  std::cout << "Searching the following triple: " << tripleAsString
            << std::endl;
  std::cout << "For each permutation, find the first element that is not "
               "smaller"
            << std::endl;

  // Search the triple in all permutations.
  IdTriple idTriple = makeIdTriple(deltaTriples, tripleAsString);
  auto handles = deltaTriples.locateTripleInAllPermutations(idTriple);

  // Helper lambda for showing the block from the given permutation that
  // contains the given (via an iterator) `LocatedTriple` object.
  auto showBlock = [&](LocatedTriples::iterator& locatedTriple,
                       const auto& permutation) {
    // Shortcuts for the triple and its position.
    // AD_CORRECTNESS_CHECK(locatedTriple != locatedTriple.end());
    const size_t blockIndex = locatedTriple->blockIndex;
    const size_t rowIndexInBlock = locatedTriple->rowIndexInBlock;
    const bool existsInIndex = locatedTriple->existsInIndex;
    const IdTriple deltaTriple{locatedTriple->id1, locatedTriple->id2,
                               locatedTriple->id3};

    // Get nice names for the permutation and the triple.
    auto [namePermutation, nameTriple] =
        getNicePermutationAndTripleName(deltaTriples, permutation, deltaTriple);

    // If we are beyond the last block, there is nothing to show.
    const vector<CompressedBlockMetadata>& blockMetas =
        permutation._meta.blockData();
    if (blockIndex >= blockMetas.size()) {
      std::cout << endl;
      std::cout << "All triples in " << namePermutation << " are smaller than "
                << nameTriple << std::endl;
      return;
    }

    // Read the block and compute all relation `Id`s.
    const CompressedBlockMetadata& blockMetadata = blockMetas[blockIndex];
    DecompressedBlock blockTuples = permutation._reader.readAndDecompressBlock(
        blockMetadata, permutation._file, std::nullopt);
    std::vector<Id> blockRelationIds =
        getAllRelationIdsForPermutation(permutation).at(blockIndex);
    AD_CORRECTNESS_CHECK(blockRelationIds.size() == blockTuples.size());

    // Show the triples in the block.
    std::cout << std::endl;
    std::cout << "Block #" << blockIndex << " from " << namePermutation << " ("
              << nameTriple << "):" << std::endl;
    for (size_t i = 0; i < blockTuples.numRows(); ++i) {
      std::cout << "Row #" << i << ": "
                << getNameForId(blockRelationIds[i], deltaTriples);
      for (size_t j = 0; j < blockTuples.numColumns(); ++j) {
        std::cout << " " << getNameForId(blockTuples(i, j), deltaTriples);
      }
      if (i == rowIndexInBlock) {
        std::cout << " <-- "
                  << (existsInIndex ? "existing triple" : "new triple");
      }
      std::cout << std::endl;
    }
  };

  // Show block for each permutation.
  showBlock(handles.forPOS, index.getImpl().POS());
  showBlock(handles.forPSO, index.getImpl().PSO());
  showBlock(handles.forSPO, index.getImpl().SPO());
  showBlock(handles.forSOP, index.getImpl().SOP());
  showBlock(handles.forOSP, index.getImpl().OSP());
  showBlock(handles.forOPS, index.getImpl().OPS());
  std::cout << std::endl;
}
