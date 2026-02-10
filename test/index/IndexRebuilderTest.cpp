//   Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

#include "../util/IdTableHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "index/IndexRebuilder.h"
#include "index/IndexRebuilderImpl.h"
#include "index/vocabulary/VocabularyType.h"

using namespace qlever::indexRebuilder;
using namespace std::string_literals;

namespace {
auto V = ad_utility::testing::VocabId;
auto B = ad_utility::testing::BlankNodeId;

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
  auto m = makeVocabEntry("<m>");
  std::vector<LocalVocabIndex> entries{&b, &d, &f, &h, &j, &l, &m};
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
          Id::fromBits(l.positionInVocab().upperBound_.get()).getVocabIndex(),
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
                  std::make_pair(toBits(l),
                                 Id::makeFromVocabIndex(VocabIndex::make(16))),
                  std::make_pair(toBits(m), Id::makeFromVocabIndex(
                                                VocabIndex::make(17)))));
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
  EXPECT_EQ(newVocab[VocabIndex::make(17)], "<m>");
}

// _____________________________________________________________________________
TEST(IndexRebuilder, remapVocabId) {
  std::vector insertionPositionsA{VocabIndex::make(3), VocabIndex::make(5),
                                  VocabIndex::make(7)};

  EXPECT_EQ(remapVocabId(V(0), insertionPositionsA), V(0));
  EXPECT_EQ(remapVocabId(V(1), insertionPositionsA), V(1));
  EXPECT_EQ(remapVocabId(V(2), insertionPositionsA), V(2));
  EXPECT_EQ(remapVocabId(V(3), insertionPositionsA), V(4));
  EXPECT_EQ(remapVocabId(V(4), insertionPositionsA), V(5));
  EXPECT_EQ(remapVocabId(V(5), insertionPositionsA), V(7));
  EXPECT_EQ(remapVocabId(V(6), insertionPositionsA), V(8));
  EXPECT_EQ(remapVocabId(V(7), insertionPositionsA), V(10));
  EXPECT_EQ(remapVocabId(V(8), insertionPositionsA), V(11));

  std::vector insertionPositionsB{VocabIndex::make(0), VocabIndex::make(1)};
  EXPECT_EQ(remapVocabId(V(0), insertionPositionsB), V(1));
  EXPECT_EQ(remapVocabId(V(1), insertionPositionsB), V(3));
  EXPECT_EQ(remapVocabId(V(2), insertionPositionsB), V(4));
}

// _____________________________________________________________________________
TEST(IndexRebuilder, remapBlankNodeId) {
  std::vector<uint64_t> blankNodeBlocks{4, 42, 77};
  auto s = ad_utility::BlankNodeManager::blockSize_;

  EXPECT_EQ(remapBlankNodeId(B(4 * s), blankNodeBlocks, 0), B(0));
  EXPECT_EQ(remapBlankNodeId(B(4 * s + 1), blankNodeBlocks, 0), B(1));
  EXPECT_EQ(remapBlankNodeId(B(42 * s), blankNodeBlocks, 0), B(s));
  EXPECT_EQ(remapBlankNodeId(B(42 * s + 1), blankNodeBlocks, 0), B(s + 1));
  EXPECT_EQ(remapBlankNodeId(B(77 * s), blankNodeBlocks, 0), B(2 * s));
  EXPECT_EQ(remapBlankNodeId(B(77 * s + 1), blankNodeBlocks, 0), B(2 * s + 1));

  EXPECT_EQ(remapBlankNodeId(B(4 * s), blankNodeBlocks, 100000), B(4 * s));
  EXPECT_EQ(remapBlankNodeId(B(4 * s + 1), blankNodeBlocks, 100000),
            B(4 * s + 1));
  EXPECT_EQ(remapBlankNodeId(B(42 * s), blankNodeBlocks, 100000), B(42 * s));
  EXPECT_EQ(remapBlankNodeId(B(42 * s + 1), blankNodeBlocks, 100000),
            B(42 * s + 1));
  EXPECT_EQ(remapBlankNodeId(B(77 * s), blankNodeBlocks, 100000), B(77 * s));
  EXPECT_EQ(remapBlankNodeId(B(77 * s + 1), blankNodeBlocks, 100000),
            B(77 * s + 1));

  uint64_t o = 1337;
  EXPECT_EQ(remapBlankNodeId(B(0), blankNodeBlocks, o), B(0));
  EXPECT_EQ(remapBlankNodeId(B(o - 1), blankNodeBlocks, o), B(o - 1));
  EXPECT_EQ(remapBlankNodeId(B(4 * s + o), blankNodeBlocks, o), B(0 + o));
  EXPECT_EQ(remapBlankNodeId(B(4 * s + 1 + o), blankNodeBlocks, o), B(1 + o));
  EXPECT_EQ(remapBlankNodeId(B(42 * s + o), blankNodeBlocks, o), B(s + o));
  EXPECT_EQ(remapBlankNodeId(B(42 * s + 1 + o), blankNodeBlocks, o),
            B(s + 1 + o));
  EXPECT_EQ(remapBlankNodeId(B(77 * s + o), blankNodeBlocks, o), B(2 * s + o));
  EXPECT_EQ(remapBlankNodeId(B(77 * s + 1 + o), blankNodeBlocks, o),
            B(2 * s + 1 + o));
}

// _____________________________________________________________________________
TEST(IndexRebuilder, readIndexAndRemap) {
  // TODO<RobinTF> Add unit tests
}

// _____________________________________________________________________________
TEST(IndexRebuilder, getNumColumns) {
  EXPECT_EQ(getNumColumns({}), 4);
  ql::span<const CompressedBlockMetadata> emptySpan;
  EXPECT_EQ(
      getNumColumns({ql::ranges::subrange{emptySpan.begin(), emptySpan.end()}}),
      4);
  using C = CompressedBlockMetadataNoBlockIndex;
  std::array metadata{
      CompressedBlockMetadata{
          C{std::optional{std::vector<C::OffsetAndCompressedSize>(4)}, 0,
            C::PermutedTriple{}, C::PermutedTriple{}, std::nullopt, false},
          0},
      CompressedBlockMetadata{
          C{std::optional{std::vector<C::OffsetAndCompressedSize>(6)}, 0,
            C::PermutedTriple{}, C::PermutedTriple{}, std::nullopt, false},
          0},
      CompressedBlockMetadata{C{std::nullopt, 0, C::PermutedTriple{},
                                C::PermutedTriple{}, std::nullopt, false},
                              0}};
  EXPECT_EQ(getNumColumns(
                {ql::ranges::subrange{metadata.begin(), metadata.begin() + 1}}),
            4);
  EXPECT_EQ(getNumColumns({ql::ranges::subrange{metadata.begin() + 1,
                                                metadata.begin() + 2}}),
            6);
  EXPECT_EQ(getNumColumns({ql::ranges::subrange{metadata.begin() + 2,
                                                metadata.begin() + 3}}),
            4);
  EXPECT_EQ(getNumColumns({ql::ranges::subrange{metadata.begin() + 1,
                                                metadata.begin() + 3}}),
            6);
  EXPECT_EQ(
      getNumColumns(
          {ql::ranges::subrange{emptySpan.begin(), emptySpan.end()},
           ql::ranges::subrange{metadata.begin() + 1, metadata.begin() + 2}}),
      4);
}

// _____________________________________________________________________________
TEST(IndexRebuilder, getNumberOfColumnsAndAdditionalColumns) {
  using C = CompressedBlockMetadataNoBlockIndex;
  std::array metadata{CompressedBlockMetadata{
      C{std::optional{std::vector<C::OffsetAndCompressedSize>(6)}, 0,
        C::PermutedTriple{}, C::PermutedTriple{}, std::nullopt, false},
      0}};

  auto result = getNumberOfColumnsAndAdditionalColumns({});
  EXPECT_EQ(result.first, 4);
  EXPECT_EQ(result.second,
            (std::vector<ColumnIndex>{ADDITIONAL_COLUMN_GRAPH_ID}));

  result = getNumberOfColumnsAndAdditionalColumns(
      {ql::ranges::subrange{metadata.begin(), metadata.end()}});
  EXPECT_EQ(result.first, 6);
  EXPECT_EQ(result.second,
            (std::vector<ColumnIndex>{ADDITIONAL_COLUMN_GRAPH_ID,
                                      ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN,
                                      ADDITIONAL_COLUMN_INDEX_OBJECT_PATTERN}));
}

// _____________________________________________________________________________
TEST(IndexRebuilder, createPermutationWriterTask) {
  // TODO<RobinTF> Add unit tests
}

// _____________________________________________________________________________
TEST(IndexRebuilder, materializeToIndex) {
  // TODO<RobinTF> Add unit tests
}
