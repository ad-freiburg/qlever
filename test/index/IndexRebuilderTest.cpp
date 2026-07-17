//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/strings/str_cat.h>
#include <absl/time/time.h>
#include <gmock/gmock.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>

#include "../util/GTestHelpers.h"
#include "../util/HttpRequestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "backports/filesystem.h"
#include "engine/Server.h"
#include "global/Constants.h"
#include "index/IndexRebuilder.h"
#include "index/IndexRebuilderImpl.h"
#include "index/vocabulary/VocabularyType.h"
#include "libqlever/Qlever.h"
#include "util/FilesystemHelpers.h"

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
  std::string vocabPrefix = gtestCurrentTestName();
  std::string vocabFileName = vocabPrefix + VOCAB_SUFFIX;
  absl::Cleanup removeVocabFiles{[&vocabFileName, &type] {
    deleteVocabFiles(vocabFileName, type.value());
  }};

  auto getId = ad_utility::testing::makeGetId(oldIndex);
  auto [insertionPositions, localVocabMapping] =
      materializeLocalVocab({}, oldIndex.getVocab(), vocabPrefix);
  EXPECT_THAT(insertionPositions, ::testing::ElementsAre());
  EXPECT_THAT(localVocabMapping, ::testing::UnorderedElementsAre());

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
  std::string vocabPrefix = gtestCurrentTestName();
  absl::Cleanup removeVocabFiles{[&vocabPrefix, &type] {
    deleteVocabFiles(vocabPrefix + VOCAB_SUFFIX, type.value());
  }};

  auto makeVocabEntry = [&oldIndex](std::string_view str) {
    return LocalVocabEntry{ad_utility::testing::iri(str), oldIndex};
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

  auto [insertionPositions, localVocabMapping] =
      materializeLocalVocab(entries, oldIndex.getVocab(), vocabPrefix);
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
                                 Id::makeFromVocabIndex(VocabIndex::make(13))),
                  std::make_pair(toBits(l),
                                 Id::makeFromVocabIndex(VocabIndex::make(15))),
                  std::make_pair(toBits(m), Id::makeFromVocabIndex(
                                                VocabIndex::make(16)))));

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
  EXPECT_EQ(newVocab[VocabIndex::make(12)], "<i>");
  EXPECT_EQ(newVocab[VocabIndex::make(13)], "<j>");
  EXPECT_EQ(newVocab[VocabIndex::make(14)], "<k>");
  EXPECT_EQ(newVocab[VocabIndex::make(15)], "<l>");
  EXPECT_EQ(newVocab[VocabIndex::make(16)], "<m>");
}

// _____________________________________________________________________________
TEST(IndexRebuilder, flattenBlankNodeBlocks) {
  using OBE =
      ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry;
  std::vector ownedBlocks{OBE{{}, {4, 42}}, OBE{{}, {7, 77}}};

  auto flatBlockIndices = flattenBlankNodeBlocks(ownedBlocks);
  EXPECT_THAT(flatBlockIndices, ::testing::ElementsAre(4, 7, 42, 77));
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
// Same expected values as `remapVocabId`, but exercising the hinted overload.
// The hint is intentionally reused across all calls (the same way the
// production call site does it) and across non-monotone inputs to verify that
// it self-corrects.
TEST(IndexRebuilder, remapVocabIdHinted) {
  std::vector insertionPositionsA{VocabIndex::make(3), VocabIndex::make(5),
                                  VocabIndex::make(7)};

  size_t hint = 0;
  // Monotone forward sweep.
  EXPECT_EQ(remapVocabId(V(0), insertionPositionsA, hint), V(0));
  EXPECT_EQ(0, hint);
  EXPECT_EQ(remapVocabId(V(1), insertionPositionsA, hint), V(1));
  EXPECT_EQ(0, hint);
  EXPECT_EQ(remapVocabId(V(2), insertionPositionsA, hint), V(2));
  EXPECT_EQ(0, hint);
  EXPECT_EQ(remapVocabId(V(3), insertionPositionsA, hint), V(4));
  EXPECT_EQ(1, hint);
  EXPECT_EQ(remapVocabId(V(4), insertionPositionsA, hint), V(5));
  EXPECT_EQ(1, hint);
  EXPECT_EQ(remapVocabId(V(5), insertionPositionsA, hint), V(7));
  EXPECT_EQ(2, hint);
  EXPECT_EQ(remapVocabId(V(6), insertionPositionsA, hint), V(8));
  EXPECT_EQ(2, hint);
  EXPECT_EQ(remapVocabId(V(7), insertionPositionsA, hint), V(10));
  EXPECT_EQ(3, hint);
  EXPECT_EQ(remapVocabId(V(8), insertionPositionsA, hint), V(11));
  EXPECT_EQ(3, hint);

  // Backward jump (hint is now too high) - must self-correct.
  EXPECT_EQ(remapVocabId(V(2), insertionPositionsA, hint), V(2));
  EXPECT_EQ(0, hint);
  // Forward jump that lands several insertions later.
  EXPECT_EQ(remapVocabId(V(8), insertionPositionsA, hint), V(11));
  EXPECT_EQ(3, hint);
  // Repeated value (hint already correct).
  EXPECT_EQ(remapVocabId(V(8), insertionPositionsA, hint), V(11));
  EXPECT_EQ(3, hint);
  EXPECT_EQ(remapVocabId(V(8), insertionPositionsA, hint), V(11));
  EXPECT_EQ(3, hint);

  // Independent insertion-position vector with a fresh hint.
  std::vector insertionPositionsB{VocabIndex::make(0), VocabIndex::make(1)};
  size_t hintB = 0;
  EXPECT_EQ(remapVocabId(V(0), insertionPositionsB, hintB), V(1));
  EXPECT_EQ(1, hintB);
  EXPECT_EQ(remapVocabId(V(1), insertionPositionsB, hintB), V(3));
  EXPECT_EQ(2, hintB);
  EXPECT_EQ(remapVocabId(V(2), insertionPositionsB, hintB), V(4));
  EXPECT_EQ(2, hintB);

  // Empty insertion positions: every id is unchanged regardless of pattern.
  std::vector<VocabIndex> insertionPositionsEmpty;
  size_t hintE = 0;
  EXPECT_EQ(remapVocabId(V(0), insertionPositionsEmpty, hintE), V(0));
  EXPECT_EQ(0, hintE);
  EXPECT_EQ(remapVocabId(V(42), insertionPositionsEmpty, hintE), V(42));
  EXPECT_EQ(0, hintE);
  EXPECT_EQ(remapVocabId(V(7), insertionPositionsEmpty, hintE), V(7));
  EXPECT_EQ(0, hintE);
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

  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    EXPECT_THROW(remapBlankNodeId(B(0), blankNodeBlocks, 0),
                 ad_utility::Exception);
    EXPECT_THROW(remapBlankNodeId(B(100000), blankNodeBlocks, 0),
                 ad_utility::Exception);
  }
}

// _____________________________________________________________________________
TEST(IndexRebuilder, readIndexAndRemap) {
  auto index = ad_utility::testing::makeTestIndex(
      "readIndexAndRemap", "<a> <b> <c> . <d> <e> _:f .");
  const auto& permutation =
      index.getImpl().getPermutation(Permutation::Enum::PSO);
  auto cancellationHandle =
      std::make_shared<ad_utility::SharedCancellationHandle::element_type>();

  auto g = TripleComponent{ad_utility::triple_component::Iri::fromIriref(
                               DEFAULT_GRAPH_IRI)}
               .toValueId(index)
               .value();

  index.deltaTriplesManager().modify<void>(
      [&cancellationHandle, g, &index](DeltaTriples& deltaTriples) {
        LocalVocabEntry entry1 = LocalVocabEntry::fromIriref("<a2>", index);
        LocalVocabEntry entry2 = LocalVocabEntry::fromIriref("<d2>", index);
        auto a2 = Id::makeFromLocalVocabIndex(&entry1);
        auto d2 = Id::makeFromLocalVocabIndex(&entry2);
        deltaTriples.insertTriples(
            cancellationHandle,
            {IdTriple<0>{std::array{V(0), a2, Id::makeFromInt(1337), g}},
             IdTriple<0>{std::array{V(0), d2, B(1), g}}});
      });

  auto [state, vocabEntries, rawBlocks] =
      index.deltaTriplesManager()
          .getCurrentLocatedTriplesSharedStateWithVocab();
  auto blockMetadataRanges =
      permutation.getAugmentedMetadataForPermutation(*state);

  ql::ranges::sort(vocabEntries, {}, [](const LocalVocabEntry* entry) {
    return Id::makeFromLocalVocabIndex(entry);
  });

  ad_utility::HashMap<Id::T, Id> localVocabMapping{
      {Id::makeFromLocalVocabIndex(vocabEntries.at(0)).getBits(),
       Id::makeFromVocabIndex(VocabIndex::make(1))},
      {Id::makeFromLocalVocabIndex(vocabEntries.at(1)).getBits(),
       Id::makeFromVocabIndex(VocabIndex::make(5))}};

  std::vector<VocabIndex> insertionPositions{VocabIndex::make(1),
                                             VocabIndex::make(4)};
  std::vector<uint64_t> blankNodeBlocks{rawBlocks.at(0).blockIndices_.at(0)};
  uint64_t minBlankNodeIndex = 1;
  std::vector<ColumnIndex> additionalColumns{
      ADDITIONAL_COLUMN_GRAPH_ID, ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN,
      ADDITIONAL_COLUMN_INDEX_OBJECT_PATTERN};

  auto range = readIndexAndRemap(permutation, blockMetadataRanges, state,
                                 localVocabMapping, insertionPositions,
                                 blankNodeBlocks, minBlankNodeIndex,
                                 cancellationHandle, additionalColumns);

  std::vector<IdTableStatic<0>> idTables = ::ranges::to<std::vector>(
      ql::views::transform(range, ad_utility::staticCast<IdTableStatic<0>&&>));

  auto U = Id::makeUndefined();
  auto patternId = Id::makeFromInt(std::numeric_limits<int32_t>::max());
  auto newG = remapVocabId(g, insertionPositions);

  ASSERT_EQ(idTables.size(), 1);
  EXPECT_EQ(idTables.at(0),
            makeIdTableFromVector(
                {{V(1), V(0), Id::makeFromInt(1337), newG, U, U},
                 {V(2), V(0), V(3), newG, Id::makeFromInt(0), patternId},
                 {V(5), V(0), B(1), newG, U, U},
                 {V(6), V(4), B(0), newG, Id::makeFromInt(1), patternId}}));
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
  ql::span<const CompressedBlockMetadata> metaSpan{metadata};

  EXPECT_EQ(getNumColumns(
                {ql::ranges::subrange{metaSpan.begin(), metaSpan.begin() + 1}}),
            4);
  EXPECT_EQ(getNumColumns({ql::ranges::subrange{metaSpan.begin() + 1,
                                                metaSpan.begin() + 2}}),
            6);
  EXPECT_EQ(getNumColumns({ql::ranges::subrange{metaSpan.begin() + 2,
                                                metaSpan.begin() + 3}}),
            4);
  EXPECT_EQ(getNumColumns({ql::ranges::subrange{metaSpan.begin() + 1,
                                                metaSpan.begin() + 3}}),
            6);
  EXPECT_EQ(
      getNumColumns(
          {ql::ranges::subrange{emptySpan.begin(), emptySpan.end()},
           ql::ranges::subrange{metaSpan.begin() + 1, metaSpan.begin() + 2}}),
      4);
}

// _____________________________________________________________________________
TEST(IndexRebuilder, getNumberOfColumnsAndAdditionalColumns) {
  using C = CompressedBlockMetadataNoBlockIndex;
  CompressedBlockMetadata metadata{
      C{std::optional{std::vector<C::OffsetAndCompressedSize>(6)}, 0,
        C::PermutedTriple{}, C::PermutedTriple{}, std::nullopt, false},
      0};

  ql::span<const CompressedBlockMetadata> metaSpan{&metadata, 1};

  auto result = getNumberOfColumnsAndAdditionalColumns({});
  EXPECT_EQ(result.first, 4);
  EXPECT_EQ(result.second,
            (std::vector<ColumnIndex>{ADDITIONAL_COLUMN_GRAPH_ID}));

  result = getNumberOfColumnsAndAdditionalColumns(
      {ql::ranges::subrange{metaSpan.begin(), metaSpan.end()}});
  EXPECT_EQ(result.first, 6);
  EXPECT_EQ(result.second,
            (std::vector<ColumnIndex>{ADDITIONAL_COLUMN_GRAPH_ID,
                                      ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN,
                                      ADDITIONAL_COLUMN_INDEX_OBJECT_PATTERN}));
}

// _____________________________________________________________________________
TEST(IndexRebuilder, createPermutationWriterTask) {
  auto* qec = ad_utility::testing::getQec("<a> <b> <c> . <d> <e> _:f .");
  const auto& index = qec->getIndex();
  IndexImpl newIndex{ad_utility::makeUnlimitedAllocator<Id>()};
  std::string prefix = gtestCurrentTestName();
  std::array<std::string_view, 4> suffixes{".index.pos", ".index.pos.meta",
                                           ".index.pso", ".index.pso.meta"};
  newIndex.setOnDiskBase(prefix);
  auto cancellationHandle =
      std::make_shared<ad_utility::SharedCancellationHandle::element_type>();
  auto state =
      index.deltaTriplesManager().getCurrentLocatedTriplesSharedState();
  ad_utility::HashMap<Id::T, Id> localVocabMapping;
  std::vector<VocabIndex> insertionPositions;
  std::vector<uint64_t> blankNodeBlocks;
  auto task = createPermutationWriterTask(
      newIndex, index.getImpl().getPermutation(Permutation::Enum::PSO),
      index.getImpl().getPermutation(Permutation::Enum::POS), false, state,
      localVocabMapping, insertionPositions, blankNodeBlocks, 1,
      cancellationHandle);

  // Assert nothing has happened yet
  for (std::string_view suffix : suffixes) {
    EXPECT_FALSE(ql::filesystem::exists(prefix + suffix))
        << "File " << prefix + suffix
        << " should not exist before the task is executed.";
  }

  absl::Cleanup removePermutationFiles{[&prefix, &suffixes] {
    for (std::string_view suffix : suffixes) {
      ad_utility::deleteFile(prefix + suffix);
    }
  }};

  namespace net = boost::asio;
  net::thread_pool threadPool{1};

  net::co_spawn(threadPool, std::move(task), net::detached);
  threadPool.join();
  for (std::string_view suffix : suffixes) {
    EXPECT_TRUE(ql::filesystem::exists(prefix + suffix));
    EXPECT_EQ(fileToBuffer(index.getOnDiskBase() + suffix),
              fileToBuffer(prefix + suffix));
  }
}

// _____________________________________________________________________________
TEST(IndexRebuilder, materializeToIndex) {
  auto cancellationHandle =
      std::make_shared<ad_utility::SharedCancellationHandle::element_type>();
  std::string baseFolder = gtestCurrentTestName();
  std::string newIndexName = baseFolder + "/index";
  std::string logFile = newIndexName + ".log";

  for (auto [usePatterns, loadAllPermutations] :
       {std::pair{false, false}, std::pair{false, true},
        std::pair{true, true}}) {
    ad_utility::testing::TestIndexConfig config;
    config.turtleInput = "<a> <b> <c> . <d> <e> _:f .";
    config.loadAllPermutations = loadAllPermutations;
    config.usePatterns = usePatterns;
    auto index = ad_utility::testing::makeTestIndex("materializeToIndex",
                                                    std::move(config));
    index.deltaTriplesManager().modify<void>([&cancellationHandle, &index](
                                                 DeltaTriples& deltaTriples) {
      auto g = TripleComponent{ad_utility::triple_component::Iri::fromIriref(
                                   DEFAULT_GRAPH_IRI)}
                   .toValueId(index)
                   .value();
      deltaTriples.insertTriples(
          cancellationHandle, {IdTriple<0>{std::array{V(2), V(1), V(0), g}},
                               IdTriple<0>{std::array{B(1), B(2), B(3), g}}});
    });

    auto [state, vocab, blankNodes] =
        index.deltaTriplesManager()
            .getCurrentLocatedTriplesSharedStateWithVocab();

    ql::filesystem::create_directory(baseFolder);
    absl::Cleanup removeIndexFiles{
        [&baseFolder] { ql::filesystem::remove_all(baseFolder); }};

    auto sourceDate = index.getImpl().dateOfIndexBuild();

    qlever::materializeToIndex(index.getImpl(), newIndexName, state, vocab,
                               blankNodes, cancellationHandle, logFile);
    EXPECT_TRUE(ql::filesystem::exists(logFile));

    IndexImpl newIndex{ad_utility::makeUnlimitedAllocator<Id>()};
    newIndex.usePatterns() = usePatterns;
    newIndex.loadAllPermutations() = loadAllPermutations;
    newIndex.createFromOnDiskIndex(newIndexName, false);

    // The rebuilt index gets its own, more recent build date. Both dates are
    // recorded with second resolution, so the rebuild may happen within the
    // same second as the original build; hence we only assert "not older".
    auto parseDate = [](const std::string& date) {
      absl::Time result;
      std::string error;
      EXPECT_TRUE(absl::ParseTime(DATE_OF_INDEX_BUILD_FORMAT, date,
                                  absl::UTCTimeZone(), &result, &error))
          << error;
      return result;
    };
    EXPECT_GE(parseDate(newIndex.dateOfIndexBuild()), parseDate(sourceDate));

    EXPECT_EQ(newIndex.getBlankNodeManager()->minIndex_,
              index.getBlankNodeManager()->minIndex_ +
                  ad_utility::BlankNodeManager::blockSize_);
    EXPECT_EQ(newIndex.numTriples().normal, 4);
    EXPECT_EQ(newIndex.numTriples().internal, usePatterns ? 2 : 0);
    EXPECT_EQ(newIndex.numDistinctPredicates().normal, 3);
    EXPECT_EQ(newIndex.numDistinctPredicates().internal, usePatterns ? 1 : 0);
    if (newIndex.loadAllPermutations()) {
      EXPECT_EQ(newIndex.numDistinctSubjects().normal, 4);
      EXPECT_EQ(newIndex.numDistinctSubjects().internal, 0);
      EXPECT_EQ(newIndex.numDistinctObjects().normal, 4);
      EXPECT_EQ(newIndex.numDistinctObjects().internal, 0);
    }
  }
}

// _____________________________________________________________________________
TEST(IndexRebuilder, materializeToIndexWithZeroMemorySourceIndex) {
  // Build a regular source index (with the default, unlimited allocator), but
  // then load it with an allocator that has zero available memory. Rebuilding
  // such an index should still succeed, because the rebuild streams the data
  // and does not rely on the source index's allocator.
  auto cancellationHandle =
      std::make_shared<ad_utility::SharedCancellationHandle::element_type>();
  std::string sourceIndexName = gtestCurrentTestName();
  std::string baseFolder = absl::StrCat(sourceIndexName, "-new");
  std::string newIndexName = baseFolder + "/index";
  std::string logFile = newIndexName + ".log";

  // Build the on-disk source index using the default unlimited allocator.
  ad_utility::testing::makeTestIndex(sourceIndexName,
                                     "<a> <b> <c> . <d> <e> _:f .");

  // Load the source index with a zero-memory allocator.
  Index index{ad_utility::makeAllocatorWithLimit<Id>(0_B)};
  index.createFromOnDiskIndex(sourceIndexName, false);

  index.deltaTriplesManager().modify<void>(
      [&cancellationHandle, &index](DeltaTriples& deltaTriples) {
        auto g = TripleComponent{ad_utility::triple_component::Iri::fromIriref(
                                     DEFAULT_GRAPH_IRI)}
                     .toValueId(index)
                     .value();
        deltaTriples.insertTriples(
            cancellationHandle,
            {IdTriple<0>{std::array{Id::makeFromInt(1), Id::makeFromInt(2),
                                    Id::makeFromInt(3), g}}});
      });

  auto [state, vocab, blankNodes] =
      index.deltaTriplesManager()
          .getCurrentLocatedTriplesSharedStateWithVocab();

  ql::filesystem::create_directory(baseFolder);
  absl::Cleanup removeIndexFiles{
      [&baseFolder] { ql::filesystem::remove_all(baseFolder); }};

  EXPECT_NO_THROW(qlever::materializeToIndex(index.getImpl(), newIndexName,
                                             state, vocab, blankNodes,
                                             cancellationHandle, logFile));

  IndexImpl newIndex{ad_utility::makeUnlimitedAllocator<Id>()};
  newIndex.createFromOnDiskIndex(newIndexName, false);
  EXPECT_EQ(newIndex.numTriples().normal, 3);
}

// _____________________________________________________________________________
TEST(IndexRebuilder, materializeToIndexNoLogFileName) {
  auto cancellationHandle =
      std::make_shared<ad_utility::SharedCancellationHandle::element_type>();

  auto qec = ad_utility::testing::getQec();
  const auto& index = qec->getIndex();

  auto [state, vocab, blankNodes] =
      index.deltaTriplesManager()
          .getCurrentLocatedTriplesSharedStateWithVocab();

  EXPECT_THROW(
      qlever::materializeToIndex(index.getImpl(), "nexIndex", state, vocab,
                                 blankNodes, cancellationHandle, ""),
      ad_utility::Exception);
}

namespace {
// Get rid of previous files with the specified prefix.
void cleanFilesWithPrefix(std::string_view prefix) {
  AD_CONTRACT_CHECK(!prefix.empty(),
                    "This function is not meant to delete all files in the "
                    "current directory. Please specify a prefix.");
  // `deleteFilesInDirectory` collects the matching entries first and deletes
  // them only afterwards, and only deletes regular files (not directories).
  qlever::util::deleteFilesInDirectory(".", [prefix](const auto& path) {
    return ql::starts_with(path.filename().string(), prefix);
  });
}
}  // namespace

// _____________________________________________________________________________
TEST(IndexRebuilder, serverIntegration) {
#ifdef __EMSCRIPTEN__
  GTEST_SKIP() << "Skipped under Emscripten: this test hangs (threaded server "
                  "integration).";
#endif
  cleanFilesWithPrefix("my-name");
  cleanFilesWithPrefix("new_index");
  namespace net = boost::asio;
  net::thread_pool threadPool{1};

  std::string indexName = "IndexRebuilder_serverIntegration";
  ad_utility::testing::makeTestIndex(indexName, "<a> <b> <c> .");

  qlever::EngineConfig config;
  config.baseName_ = indexName;
  Server server{4321, 1, "accessToken", config};
  auto performRequest = [&threadPool, &server](auto& request) {
    using ResT = ad_utility::httpUtils::ResponseT;
    auto task =
        server.template onlyForTestingProcess<std::decay_t<decltype(request)>,
                                              ResT>(request);
    return net::co_spawn(threadPool, std::move(task), net::use_future);
  };

  // Without access token this operation is not allowed!
  auto request0 = ad_utility::testing::makeGetRequest(
      "/?cmd=rebuild-index&index-name=my-name");
  AD_EXPECT_THROW_WITH_MESSAGE(performRequest(request0).get(),
                               ::testing::HasSubstr("access token"));

  auto request1 = ad_utility::testing::makeGetRequest(
      "/?cmd=rebuild-index&index-name=my-name&access-token="
      "accessToken");
  auto future1 = performRequest(request1);
  auto request2 = ad_utility::testing::makeGetRequest(
      "/?cmd=rebuild-index&index-name=my-name&access-token="
      "accessToken");
  auto future2 = performRequest(request2);

  auto response1 = future1.get();
  auto response2 = future2.get();

  EXPECT_EQ(response1.base().result(), boost::beast::http::status::ok);
  EXPECT_EQ(response2.base().result(),
            boost::beast::http::status::too_many_requests);

  // We use this config as a proxy for the index rebuilder having finished
  // successfully.
  EXPECT_TRUE(ql::filesystem::exists("my-name.meta-data.json"));

  auto request3 = ad_utility::testing::makeGetRequest(
      "/?cmd=rebuild-index&access-token=accessToken");
  auto response3 = performRequest(request3).get();
  EXPECT_EQ(response3.base().result(), boost::beast::http::status::ok);
  // By default QLever should assign a default name for the new index.
  EXPECT_TRUE(ql::filesystem::exists("new_index.meta-data.json"));

  // The index with the same name already exists, so we don't want to overwrite
  // it.
  auto request4 = ad_utility::testing::makeGetRequest(
      "/?cmd=rebuild-index&access-token=accessToken");
  AD_EXPECT_THROW_WITH_MESSAGE(
      performRequest(request4).get(),
      ::testing::HasSubstr("already files with the same base name"));

  // The index has to reside within the same directory as the original index.
  auto request5 = ad_utility::testing::makeGetRequest(
      "/?cmd=rebuild-index&access-token=accessToken&index-name=%2Fmy-name");
  AD_EXPECT_THROW_WITH_MESSAGE(
      performRequest(request5).get(),
      ::testing::HasSubstr("not located in the same directory"));

  auto request6 = ad_utility::testing::makeGetRequest(
      "/?cmd=rebuild-index&access-token=accessToken&index-name=..%2Fother");
  AD_EXPECT_THROW_WITH_MESSAGE(
      performRequest(request6).get(),
      ::testing::HasSubstr("not located in the same directory"));

  threadPool.join();
}

// _____________________________________________________________________________
// The thread-count override for the rebuild's scans must be set on the
// dedicated reader created by `lazyScanWithUnlimitedReader` (and only there);
// the permutation's shared reader, which is used by the query scans, must
// never carry an override.
TEST(IndexRebuilder, lazyScanNumThreadsOverride) {
  auto index = ad_utility::testing::makeTestIndex("lazyScanNumThreadsOverride",
                                                  "<a> <b> <c> .");
  const auto& permutation =
      index.getImpl().getPermutation(Permutation::Enum::PSO);
  auto cancellationHandle =
      std::make_shared<ad_utility::SharedCancellationHandle::element_type>();
  auto state =
      index.deltaTriplesManager().getCurrentLocatedTriplesSharedState();
  ScanSpecification scanSpec{std::nullopt, std::nullopt, std::nullopt};
  std::array<ColumnIndex, 1> additionalColumns{ADDITIONAL_COLUMN_GRAPH_ID};

  auto scanWithOverride = [&](std::optional<size_t> numThreadsOverride) {
    return permutation.lazyScanWithUnlimitedReader(
        permutation.getScanSpecAndBlocks(scanSpec, *state), additionalColumns,
        cancellationHandle, *state, numThreadsOverride);
  };
  auto [reader, scan] = scanWithOverride(3);
  EXPECT_EQ(reader->lazyScanNumThreadsOverride_, std::optional<size_t>{3});
  auto [readerDefault, scanDefault] = scanWithOverride(std::nullopt);
  EXPECT_EQ(readerDefault->lazyScanNumThreadsOverride_, std::nullopt);
  EXPECT_EQ(permutation.reader().lazyScanNumThreadsOverride_, std::nullopt);

  // Recomputing the statistics with the throttle set must give exactly the
  // same result as with the default (0, which means "fall back to
  // `lazy-index-scan-num-threads`"). This exercises the translation of the
  // runtime parameter to the override at both of its use sites.
  auto statsDefault = index.getImpl().recomputeStatistics(state);
  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::rebuildIndexScanNumThreads_>(2);
  EXPECT_EQ(index.getImpl().recomputeStatistics(state), statsDefault);
}
