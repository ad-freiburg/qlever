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
#include "index/vocabulary/VocabularyType.h"

using namespace qlever::indexRebuilder;
using namespace std::string_literals;

namespace {
// Read a file into a buffer.
std::vector<char> fileToBuffer(const std::string& filename) {
  std::ifstream f{filename, std::ios::binary};
  EXPECT_TRUE(f.is_open()) << "Could not open file " << filename;
  return std::vector(std::istreambuf_iterator<char>(f),
                     std::istreambuf_iterator<char>());
}

// Select the correct suffixes.
std::vector<std::string> getVocabSuffixesForType(
    ad_utility::VocabularyType::Enum type) {
  using enum ad_utility::VocabularyType::Enum;
  switch (type) {
    case InMemoryUncompressed:
      return {""};
    case OnDiskUncompressed:
      return {".external", ".external.offsets", ".internal", ".internal.ids"};
    case InMemoryCompressed:
      return {".codebooks", ".words"};
    case OnDiskCompressed:
      return {".codebooks", ".words.external", ".words.external.offsets",
              ".words.internal", ".words.internal.ids"};
    case OnDiskCompressedGeoSplit:
      return {".codebooks",
              ".words.external",
              ".words.external.offsets",
              ".words.internal",
              ".words.internal.ids",
              ".geometry.codebooks",
              ".geometry.geoinfo",
              ".geometry.words.external",
              ".geometry.words.external.offsets",
              ".geometry.words.internal",
              ".geometry.words.internal.ids"};
    default:
      AD_FAIL();
  }
}

// Helper function to clean up all vocabulary related files.
void deleteVocabFiles(const std::string& vocabBasename,
                      ad_utility::VocabularyType::Enum type) {
  for (const auto& suffix : getVocabSuffixesForType(type)) {
    ad_utility::deleteFile(vocabBasename + suffix);
  }
}
}  // namespace

// _____________________________________________________________________________
TEST(IndexRebuilder, materializeEmptyLocalVocab) {
  auto type = ad_utility::VocabularyType::random();
  ad_utility::testing::TestIndexConfig config{"<a> <c> <e> . <g> <i> <k> ."};
  config.vocabularyType = type;
  auto oldIndex = ad_utility::testing::makeTestIndex(
      "materializeEmptyLocalVocab", std::move(config));
  std::string vocabPrefix = "/tmp/materializeEmptyLocalVocab";
  std::string vocabFileName = vocabPrefix + VOCAB_SUFFIX;
  absl::Cleanup removeVocabFiles{[&vocabFileName, &type] {
    deleteVocabFiles(vocabFileName, type.value());
  }};

  auto getId = ad_utility::testing::makeGetId(oldIndex);
  auto [insertionPositions, localVocabMapping, flatBlockIndices] =
      materializeLocalVocab({}, {}, oldIndex.getVocab(), vocabPrefix);
  EXPECT_THAT(insertionPositions, ::testing::ElementsAre());
  EXPECT_THAT(localVocabMapping, ::testing::UnorderedElementsAre());
  EXPECT_THAT(flatBlockIndices, ::testing::ElementsAre());

  for (const auto& suffix : getVocabSuffixesForType(type.value())) {
    EXPECT_EQ(
        fileToBuffer("materializeEmptyLocalVocab"s + VOCAB_SUFFIX + suffix),
        fileToBuffer(vocabFileName + suffix));
  }
}

// _____________________________________________________________________________
TEST(IndexRebuilder, materializeLocalVocab) {
  auto type = ad_utility::VocabularyType::random();
  ad_utility::testing::TestIndexConfig config{"<a> <c> <e> . <g> <i> <k> ."};
  config.vocabularyType = type;
  auto oldIndex = ad_utility::testing::makeTestIndex("materializeLocalVocab",
                                                     std::move(config));
  std::string vocabPrefix = "/tmp/materializeLocalVocab";
  absl::Cleanup removeVocabFiles{[&vocabPrefix, &type] {
    deleteVocabFiles(vocabPrefix + VOCAB_SUFFIX, type.value());
  }};

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

  Index::Vocab newVocab;
  newVocab.resetToType(type);
  newVocab.readFromFile(vocabPrefix + VOCAB_SUFFIX);

  EXPECT_EQ(newVocab[VocabIndex::make(0)], "<a>");
  EXPECT_EQ(newVocab[VocabIndex::make(1)], "<b>");
  EXPECT_EQ(newVocab[VocabIndex::make(2)], "<c>");
  EXPECT_EQ(newVocab[VocabIndex::make(3)], "<d>");
  EXPECT_EQ(newVocab[VocabIndex::make(4)], "<e>");
  EXPECT_EQ(newVocab[VocabIndex::make(5)], "<f>");
  EXPECT_EQ(newVocab[VocabIndex::make(6)], "<g>");
  EXPECT_EQ(newVocab[VocabIndex::make(7)], "<h>");
  EXPECT_EQ(newVocab[VocabIndex::make(8)], DEFAULT_GRAPH_IRI);
  EXPECT_EQ(newVocab[VocabIndex::make(9)], HAS_PATTERN_PREDICATE);
  EXPECT_EQ(newVocab[VocabIndex::make(10)], HAS_PREDICATE_PREDICATE);
  EXPECT_EQ(newVocab[VocabIndex::make(11)], QLEVER_INTERNAL_GRAPH_IRI);
  EXPECT_EQ(newVocab[VocabIndex::make(12)], LANGUAGE_PREDICATE);
  EXPECT_EQ(newVocab[VocabIndex::make(13)], "<i>");
  EXPECT_EQ(newVocab[VocabIndex::make(14)], "<j>");
  EXPECT_EQ(newVocab[VocabIndex::make(15)], "<k>");
  EXPECT_EQ(newVocab[VocabIndex::make(16)], "<l>");
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
TEST(IndexRebuilder, getNumberOfColumnsAndAdditionalColumns) {
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
