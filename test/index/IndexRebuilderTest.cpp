//   Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

#include "../util/IndexTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "index/IndexRebuilder.h"
#include "index/IndexRebuilderImpl.h"

using namespace qlever::indexRebuilder;

// _____________________________________________________________________________
TEST(IndexRebuilder, materializeLocalVocab) {
  auto oldIndex = ad_utility::testing::makeTestIndex(
      "materializeLocalVocab", "<a> <c> <e> . <g> <i> <k> .");
  std::string vocabPrefix = "/tmp/materializeLocalVocab";
  // TODO<RobinTF> Cleanup generated test files.

  auto makeVocabEntry = [](std::string_view str) {
    return LocalVocabEntry{ad_utility::testing::iri(str)};
  };

  auto getId = ad_utility::testing::makeGetId(oldIndex);
  auto b = makeVocabEntry("<b>");
  auto c = getId("<c>");
  auto d = makeVocabEntry("<d>");
  auto e = getId("<e>");
  auto f = makeVocabEntry("<f>");
  auto g = getId("<g>");
  auto h = makeVocabEntry("<h>");
  auto j = makeVocabEntry("<j>");
  auto k = getId("<k>");
  auto l = makeVocabEntry("<l>");
  std::vector<LocalVocabIndex> entries{&b, &d, &f, &h, &j, &l};
  using OBE =
      ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry;
  std::vector ownedBlocks{OBE{{}, {4, 42}}, OBE{{}, {7, 77}}};

  auto [insertionPositions, localVocabMapping, flatBlockIndices] =
      materializeLocalVocab(entries, ownedBlocks, oldIndex.getVocab(),
                            vocabPrefix);
  EXPECT_THAT(
      insertionPositions,
      ::testing::ElementsAre(
          c.getVocabIndex(), e.getVocabIndex(), g.getVocabIndex(),
          Id::fromBits(h.positionInVocab().upperBound_.get()).getVocabIndex(),
          k.getVocabIndex(),
          Id::fromBits(l.positionInVocab().upperBound_.get()).getVocabIndex()));
  auto toBits = [](const LocalVocabEntry& entry) {
    return Id::makeFromLocalVocabIndex(&entry).getBits();
  };
  EXPECT_THAT(localVocabMapping,
              ::testing::UnorderedElementsAre(
                  std::make_pair(toBits(b),
                                 Id::makeFromVocabIndex(VocabIndex::make(1))),
                  std::make_pair(toBits(d),
                                 Id::makeFromVocabIndex(VocabIndex::make(3))),
                  std::make_pair(toBits(f),
                                 Id::makeFromVocabIndex(VocabIndex::make(5))),
                  std::make_pair(toBits(h),
                                 Id::makeFromVocabIndex(VocabIndex::make(7))),
                  std::make_pair(toBits(j),
                                 Id::makeFromVocabIndex(VocabIndex::make(14))),
                  std::make_pair(toBits(l), Id::makeFromVocabIndex(
                                                VocabIndex::make(16)))));
  EXPECT_THAT(flatBlockIndices, ::testing::ElementsAre(4, 7, 42, 77));

  // TODO<RobinTF> Add tests that the created vocabulary on disk is correct
}

// _____________________________________________________________________________
TEST(IndexRebuilder, remapVocabId) {
  // TODO<RobinTF> Add unit tests
}

// _____________________________________________________________________________
TEST(IndexRebuilder, remapBlankNodeId) {
  // TODO<RobinTF> Add unit tests
}

// _____________________________________________________________________________
TEST(IndexRebuilder, readIndexAndRemap) {
  // TODO<RobinTF> Add unit tests
}

// _____________________________________________________________________________
TEST(IndexRebuilder, getNumColumns) {
  // TODO<RobinTF> Add unit tests
}

// _____________________________________________________________________________
TEST(IndexRebuilder, createPermutationWriterTask) {
  // TODO<RobinTF> Add unit tests
}

// _____________________________________________________________________________
TEST(IndexRebuilder, materializeToIndex) {
  // TODO<RobinTF> Add unit tests
}
