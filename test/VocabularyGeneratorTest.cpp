// Copyright 2018 - 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <absl/strings/str_format.h>
#include <gmock/gmock.h>

#include <array>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "./index/vocabulary_merger/VocabularyMergerTestHelpers.h"
#include "backports/StartsWithAndEndsWith.h"
#include "backports/filesystem.h"
#include "global/Constants.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/Index.h"
#include "index/VocabularyMerger.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/Vocabulary.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Algorithm.h"
#include "util/GTestHelpers.h"

using namespace ad_utility::vocabulary_merger;
using namespace vocabularyMergerTestHelpers;
namespace {
// The base name of the partial vocabulary files that the tests below create.
// Each of those tests runs in its own fresh working directory (see
// `makePartialVocabularyFilenamesInFreshDirectory`), so a short fixed name is
// unambiguous.
const std::string partialVocabBasename = "vocab-";

// Write the given `words` as a partial vocabulary file at `path`, assigning
// them consecutive local ids `0, 1, ...` in the given order and marking all of
// them as not external.
template <typename Range>
void writePartialVocabularyFile(const std::string& path, const Range& words) {
  ad_utility::serialization::FileWriteSerializer partialVocab(path);
  partialVocab << words.size();
  size_t localIdx = 0;
  for (const auto& word : words) {
    partialVocab << std::string_view{word};
    partialVocab << false;
    partialVocab << localIdx;
    ++localIdx;
  }
}
}  // namespace

// Test fixture that sets up the binary files for partial vocabulary and
// everything else connected with vocabulary merging.
class MergeVocabularyTest : public ::testing::Test {
 protected:
  // path of the 2 partial Vocabularies that are used by mergeVocabulary
  std::string path0_;
  std::string path1_;
  // the base directory for our test
  std::string basePath_;

  // The bool means "is in the external vocabulary and not in the internal
  // vocabulary".
  using ExpectedVocabulary = std::vector<std::pair<std::string, bool>>;
  ExpectedVocabulary expectedMergedVocabulary_;
  ExpectedVocabulary expectedMergedGeoVocabulary_;

  // The two expected ID maps from the partial to the global ids.
  IdMap expectedIdMap0_;
  IdMap expectedIdMap1_;

  // Constructor. TODO: Better write Setup method because of complex logic which
  // may throw?
  MergeVocabularyTest() {
    basePath_ = std::string("vocabularyGeneratorTestFiles");
    // those names are required by mergeVocabulary
    path0_ = std::string(PARTIAL_VOCAB_WORDS_INFIX + std::to_string(0));
    path1_ = std::string(PARTIAL_VOCAB_WORDS_INFIX + std::to_string(1));

    // Create a subdirectory for the test files in the working directory.
    basePath_ = basePath_ + "/";
    ql::error_code errorCode;
    ql::filesystem::create_directories(basePath_, errorCode);
    if (errorCode) {
      std::cerr << "Could not create the directory for the test files. This "
                   "might lead to test failures\n";
    }

    // Prepend the created directory to the paths.
    path0_ = basePath_ + path0_;
    path1_ = basePath_ + path1_;

    // these will be the contents of partial vocabularies, second element of
    // pair is the correct Id which is expected from mergeVocabulary
    std::vector<TripleComponentWithIndex> words0{
        {"\"ape\"", false, 0},
        {"\"bla\"", true, 2},
        {"\"gorilla\"", false, 3},
        {"\"LINESTRING(1 2, 3 4)\""
         "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
         true, 0},
        {"\"monkey\"", false, 4},
        {"_:blank", false, 0},
        {"_:blunk", false, 1}};
    std::vector<TripleComponentWithIndex> words1{
        {"\"bear\"", false, 1},
        {"\"monkey\"", true, 4},
        {"\"POLYGON((1 2, 3 4))\""
         "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
         true, 1},
        {"\"zebra\"", false, 5},
        {"_:blunk", false, 1},
    };

    // Note that the word "monkey" appears in both vocabularies, buth with
    // different settings for `isExternal`. In this case it is externalized.
    expectedMergedVocabulary_ = ExpectedVocabulary{
        {"\"ape\"", false},     {"\"bear\"", false},  {"\"bla\"", true},
        {"\"gorilla\"", false}, {"\"monkey\"", true}, {"\"zebra\"", false}};
    expectedMergedGeoVocabulary_ = ExpectedVocabulary{
        {"\"LINESTRING(1 2, 3 4)\""
         "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
         true},
        {"\"POLYGON((1 2, 3 4))\""
         "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
         true}};

    // open files for partial Vocabularies
    ad_utility::serialization::FileWriteSerializer partial0(path0_);
    ad_utility::serialization::FileWriteSerializer partial1(path1_);

    auto writePartialVocabulary = [](auto& partialVocab,
                                     const auto& tripleComponents,
                                     IdMap* idMap) {
      // write first partial vocabulary
      partialVocab << tripleComponents.size();
      size_t localIdx = 0;
      for (auto w : tripleComponents) {
        auto globalId = w.index_;
        w.index_ = localIdx;
        partialVocab << w;
        if (idMap) {
          if (w.isBlankNode({})) {
            idMap->push_back(
                {L(localIdx),
                 Id::makeFromBlankNodeIndex(BlankNodeIndex::make(globalId))});
          } else {
            using GeoVocab = SplitGeoVocabulary<
                CompressedVocabulary<VocabularyInternalExternal>>;
            if (GeoVocab::getMarkerForWord(w.iriOrLiteral()) == 1) {
              globalId = GeoVocab::addMarker(globalId, 1);
            }
            idMap->push_back({L(localIdx), V(globalId)});
          }
        }
        localIdx++;
      }
    };
    writePartialVocabulary(partial0, words0, &expectedIdMap0_);

    writePartialVocabulary(partial1, words1, &expectedIdMap1_);
  }

  // __________________________________________________________________
  ~MergeVocabularyTest() {
    // Delete the test files (to debug a test failure, comment this out).
    ql::error_code errorCode;
    ql::filesystem::remove_all(basePath_, errorCode);
  }

  // read all bytes from a file (e.g. to check equality of small test files)
  static std::pair<bool, std::vector<char>> readAllBytes(
      const std::string& filename) {
    using std::ifstream;
    ifstream ifs(filename, std::ios::binary | std::ios::ate);
    if (!ifs.is_open()) {
      return std::make_pair(false, std::vector<char>());
    }
    ifstream::pos_type pos = ifs.tellg();

    std::vector<char> result(pos);

    ifs.seekg(0, std::ios::beg);
    ifs.read(&result[0], pos);

    return std::make_pair(true, result);
  }
};

// Test for merge Vocabulary
TEST_F(MergeVocabularyTest, mergeVocabulary) {
  // mergeVocabulary only gets the name of the directory and the filename
  // suffixes of the partial vocabularies.
  VocabularyMetaData res;
  std::vector<std::pair<std::string, bool>> mergeResult;
  std::vector<std::pair<std::string, bool>> geoMergeResult;
  {
    // Simulate `Vocabulary::WordWriter::operation()` for testing purposes
    auto internalVocabularyAction = [&mergeResult, &geoMergeResult](
                                        const auto& word,
                                        bool isExternal) -> uint64_t {
      if (ql::starts_with(word, "\"") &&
          ql::ends_with(
              word, "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>")) {
        geoMergeResult.emplace_back(word, isExternal);
        return (geoMergeResult.size() - 1) | (1ull << 59);
      } else {
        mergeResult.emplace_back(word, isExternal);
        return mergeResult.size() - 1;
      }
    };

    TripleComponentComparator comparator;
    res = mergeVocabulary(
        basePath_, {"0", "1"},
        [&comparator](std::string_view a, std::string_view b) {
          return comparator(a, b, TripleComponentComparator::Level::TOTAL);
        },
        internalVocabularyAction, 1_GB);
  }

  EXPECT_THAT(mergeResult,
              ::testing::ElementsAreArray(expectedMergedVocabulary_));
  EXPECT_THAT(geoMergeResult,
              ::testing::ElementsAreArray(expectedMergedGeoVocabulary_));

  // No language tags in text file
  ASSERT_EQ(res.langTaggedPredicates().begin(), Id::makeUndefined());
  ASSERT_EQ(res.langTaggedPredicates().end(), Id::makeUndefined());
  // Also no internal entities there.
  ASSERT_EQ(res.internalEntities().begin(), Id::makeUndefined());
  ASSERT_EQ(res.internalEntities().end(), Id::makeUndefined());
  // Check that vocabulary has the right form.
  IdMap idMap0 = getIdMapFromFile(basePath_ + PARTIAL_VOCAB_IDMAP_INFIX +
                                  std::to_string(0));
  EXPECT_THAT(idMap0, ::testing::ElementsAreArray(expectedIdMap0_));
  IdMap idMap1 = getIdMapFromFile(basePath_ + PARTIAL_VOCAB_IDMAP_INFIX +
                                  std::to_string(1));
  EXPECT_THAT(idMap1, ::testing::ElementsAreArray(expectedIdMap1_));
}

// _____________________________________________________________________________
TEST(MergeVocabulary, mergeVocabularyAssertion) {
  // The violated order is only detected if the expensive checks are enabled
  // (see `WordBatchBuilder::addMergedWords`).
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP();
  }
  auto callback = [](const auto&, bool) { return uint64_t{0}; };

  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 2);

  // Intentionally in wrong order, so that the merge detects a violated order.
  std::array<std::string_view, 3> unorderedWords{"\"c\"", "\"b\"", "\"a\""};
  writePartialVocabularyFile(filenames.wordsFiles_[0], unorderedWords);
  writePartialVocabularyFile(filenames.wordsFiles_[1], unorderedWords);

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      mergeVocabulary(partialVocabBasename, filenames.suffixes_, std::less{},
                      callback, 1_GB),
      ::testing::HasSubstr("vocabulary order violated"), ad_utility::Exception);
}

// _____________________________________________________________________________
// Test that IRIs fully matched by one of the `blankNodeIriRegexes` are treated
// as blank nodes during `mergeVocabulary` (not passed to the vocabulary word
// callback, and mapped to blank node `Id`s), while non-matching IRIs and
// literals are left untouched. In particular, matching is a *full* match, so a
// regex that only matches a prefix of an IRI does not convert it.
TEST(MergeVocabulary, treatIrisAsBlankNodesViaRegex) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);

  // A single partial vocabulary. The words must be in ascending order according
  // to the comparator used below (plain `std::less`); note that literals (which
  // start with `"`) sort before IRIs (which start with `<`).
  std::array<std::string_view, 5> words{"\"bn_lit\"", "<http://ex/apple>",
                                        "<http://ex/bn_1>", "<http://ex/bn_2>",
                                        "<http://ex/cherry>"};
  writePartialVocabularyFile(filenames.wordsFiles_[0], words);

  // Collect the words that are actually written to the vocabulary (i.e. not
  // treated as blank nodes), together with the vocabulary index they get. None
  // of the words is external.
  std::vector<std::pair<std::string, bool>> vocabularyWords;
  auto wordCallback = makeCollectingWordCallback(vocabularyWords);

  // Two (compiled) regexes:
  // - `<http://ex/bn_.*>` fully matches the two `bn_` IRIs (and neither the
  //   `"bn_lit"` literal, which is not an IRI, nor the other IRIs).
  // - `<http://ex/apple` only matches a prefix of `<http://ex/apple>` (the
  //   closing `>` is missing), so with *full* match it converts nothing. With a
  //   partial match it would have wrongly converted `<http://ex/apple>`.
  ad_utility::RegexSet blankNodeIriRegexes{
      {"<http://ex/bn_.*>", "<http://ex/apple"}, "for the test"};
  mergeVocabulary(partialVocabBasename, filenames.suffixes_, std::less{},
                  wordCallback, 1_GB, blankNodeIriRegexes);

  // Only the two `bn_` IRIs became blank nodes; the two other IRIs and the
  // literal remain in the vocabulary, in sorted order.
  EXPECT_THAT(
      vocabularyWords,
      ::testing::ElementsAre(::testing::Pair("\"bn_lit\"", false),
                             ::testing::Pair("<http://ex/apple>", false),
                             ::testing::Pair("<http://ex/cherry>", false)));

  // Check the exact id mapping. The local ids `0..4` are assigned in the input
  // (sorted) order above; the two `bn_` IRIs get consecutive, distinct blank
  // node ids, the other three words get vocabulary ids in their appearance
  // order.
  IdMap idMap = getIdMapFromFile(filenames.idMapFiles_[0]);
  EXPECT_THAT(idMap, ::testing::ElementsAreArray(
                         IdMap{{L(0), V(0)},     // "bn_lit"
                               {L(1), V(1)},     // <http://ex/apple>
                               {L(2), BN(0)},    // <http://ex/bn_1>
                               {L(3), BN(1)},    // <http://ex/bn_2>
                               {L(4), V(2)}}));  // <http://ex/cherry>
}

TEST(VocabularyGeneratorTest, createInternalMapping) {
  ItemVec input;
  using S = PartialVocabIndexWithExternalFlag;
  input.emplace_back("alpha", S{5, false});
  input.emplace_back("beta", S{4, false});
  input.emplace_back("beta", S{42, false});
  input.emplace_back("d", S{8, false});
  input.emplace_back("e", S{9, false});
  input.emplace_back("e", S{38, false});
  input.emplace_back("xenon", S{0, false});

  auto res = createInternalMapping(input);
  ASSERT_EQ(0u, input[0].second.id());
  ASSERT_EQ(1u, input[1].second.id());
  ASSERT_EQ(1u, input[2].second.id());
  ASSERT_EQ(2u, input[3].second.id());
  ASSERT_EQ(3u, input[4].second.id());
  ASSERT_EQ(3u, input[5].second.id());
  ASSERT_EQ(4u, input[6].second.id());

  ASSERT_EQ(0u, res[5]);
  ASSERT_EQ(1u, res[4]);
  ASSERT_EQ(1u, res[42]);
  ASSERT_EQ(2u, res[8]);
  ASSERT_EQ(3u, res[9]);
  ASSERT_EQ(3u, res[38]);
  ASSERT_EQ(4u, res[0]);
}

// Regression test: previously, `createInternalMapping` left `lastWord` empty
// for the first iteration, so duplicates of the very first sorted word
// (which can occur when the same string is stored in two parallel
// `ItemMap`s with different `isExternal` flags) were assigned a *different*
// internal id than the first occurrence. The subsequent `std::unique` by id
// then failed to drop them, and the partial-vocab file ended up with two
// byte-identical entries for that word.
TEST(VocabularyGeneratorTest, createInternalMappingFirstWordDuplicates) {
  ItemVec input;
  using S = PartialVocabIndexWithExternalFlag;
  // The first word appears three times (e.g., from three parallel item
  // maps), then a second distinct word appears twice.
  input.emplace_back("alpha", S{7, true});
  input.emplace_back("alpha", S{12, false});
  input.emplace_back("alpha", S{99, false});
  input.emplace_back("beta", S{3, false});
  input.emplace_back("beta", S{55, true});

  auto res = createInternalMapping(input);
  // All three "alpha"s must collapse to the same id (0).
  EXPECT_EQ(0u, input[0].second.id());
  EXPECT_EQ(0u, input[1].second.id());
  EXPECT_EQ(0u, input[2].second.id());
  // Both "beta"s must collapse to the next id (1).
  EXPECT_EQ(1u, input[3].second.id());
  EXPECT_EQ(1u, input[4].second.id());

  EXPECT_EQ(0u, res[7]);
  EXPECT_EQ(0u, res[12]);
  EXPECT_EQ(0u, res[99]);
  EXPECT_EQ(1u, res[3]);
  EXPECT_EQ(1u, res[55]);
}

// _____________________________________________________________________________
// Merge words that occur in *every* partial vocabulary, such that the
// occurrences of a single word are spread over two consecutive batches of
// merged words. Such a word is only written to the vocabulary (and hence only
// gets its global ID) after the first of those batches has been handed to the
// writing thread, so this exercises the deliberate holding back of the last
// distinct word by the `WordBatchBuilder`.
TEST(MergeVocabulary, duplicateWordsAcrossBatchBoundaries) {
  // The words are currently collected in batches of 100000. Three partial
  // vocabularies with the same 120000 words yield 360000 index mappings, so we
  // get several batches, and as 100000 is not divisible by three, the
  // occurrences of a word are indeed split by a batch boundary.
  static constexpr size_t numWords = 120'000;
  static constexpr size_t numFiles = 3;
  auto [filenames, cleanup] = makePartialVocabularyFilenamesInFreshDirectory(
      partialVocabBasename, numFiles);

  // Each of the partial vocabularies contains all the words (zero-padded, such
  // that their lexicographic order is the same as the order of their indices),
  // with the local index `i` for the `i`-th word.
  std::vector<std::string> words;
  for (size_t i = 0; i < numWords; ++i) {
    words.push_back(absl::StrFormat("\"word%08d\"", i));
  }
  for (size_t i = 0; i < numFiles; ++i) {
    writePartialVocabularyFile(filenames.wordsFiles_[i], words);
  }

  size_t numWordsInCallback = 0;
  auto wordCallback = makeCountingWordCallback(numWordsInCallback);
  auto result = mergeVocabulary(partialVocabBasename, filenames.suffixes_,
                                std::less{}, wordCallback, 1_GB);
  // Each word is written to the vocabulary exactly once.
  EXPECT_EQ(numWordsInCallback, numWords);
  EXPECT_EQ(result.numWordsTotal(), numWords);

  // In each of the partial vocabularies, the word with local index `j` is the
  // word with global id `j`.
  IdMap expected;
  for (size_t j = 0; j < numWords; ++j) {
    expected.push_back({L(j), V(j)});
  }
  for (size_t f = 0; f < numFiles; ++f) {
    EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[f]),
                ::testing::ElementsAreArray(expected));
  }
}

// _____________________________________________________________________________
// Merge a word that occurs in two partial vocabularies with different
// `isExternal` flags, such that the two occurrences are split by a batch
// boundary. The merged word has to become external, which is only correct
// because the `WordBatchBuilder` holds the word back until no further
// occurrence of it can arrive.
TEST(MergeVocabulary, externalizationAcrossBatchBoundaries) {
  // The last word is the only one that occurs in both partial vocabularies,
  // and only its occurrence in the second one is marked as external.
  static constexpr size_t numWords = 100'000;
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 2);

  // The first partial vocabulary fills a whole batch and ends with `"zzz"`,
  // which is the only word of the second partial vocabulary, there marked as
  // external.
  {
    ad_utility::serialization::FileWriteSerializer partialVocab{
        filenames.wordsFiles_[0]};
    partialVocab << numWords;
    for (size_t i = 0; i + 1 < numWords; ++i) {
      partialVocab << absl::StrFormat("\"word%08d\"", i);
      partialVocab << false;
      partialVocab << i;
    }
    partialVocab << std::string{"\"zzz\""};
    partialVocab << false;
    partialVocab << numWords - 1;
  }
  {
    ad_utility::serialization::FileWriteSerializer partialVocab{
        filenames.wordsFiles_[1]};
    partialVocab << size_t{1};
    partialVocab << std::string{"\"zzz\""};
    partialVocab << true;
    partialVocab << size_t{0};
  }

  std::vector<std::pair<std::string, bool>> vocabulary;
  auto wordCallback = makeCollectingWordCallback(vocabulary);
  auto result = mergeVocabulary(partialVocabBasename, filenames.suffixes_,
                                std::less{}, wordCallback, 1_GB);
  EXPECT_EQ(result.numWordsTotal(), numWords);

  // `"zzz"` is written exactly once, and it is externalized because one of its
  // two occurrences was.
  ASSERT_EQ(vocabulary.size(), numWords);
  EXPECT_THAT(vocabulary.back(), ::testing::Pair("\"zzz\"", true));
  // Both occurrences of `"zzz"` map to its single global id.
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[1]),
              ::testing::ElementsAre(IdMapEntry{L(0), V(numWords - 1)}));
}
// _____________________________________________________________________________
// An exception that is thrown while a batch is written (here by the word
// callback, in practice e.g. by a full disk) happens on the writing thread. It
// must be propagated to the caller of `mergeVocabulary` and must not terminate
// the process, and the batches that are still queued must not be written.
TEST(MergeVocabulary, exceptionFromWritingThreadIsPropagated) {
  // More words than fit into a single batch (see
  // `VOCAB_MERGER_WORD_BATCH_SIZE`), so that there is a second batch that
  // has to be skipped after the first one has failed.
  static constexpr size_t numWords = 120'000;
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);
  std::vector<std::string> words;
  for (size_t i = 0; i < numWords; ++i) {
    words.push_back(absl::StrFormat("\"word%08d\"", i));
  }
  writePartialVocabularyFile(filenames.wordsFiles_[0], words);

  size_t numCalls = 0;
  auto wordCallback = [&numCalls](std::string_view, bool) -> uint64_t {
    ++numCalls;
    throw std::runtime_error{"The vocabulary could not be written"};
  };
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      mergeVocabulary(partialVocabBasename, filenames.suffixes_, std::less{},
                      wordCallback, 1_GB),
      ::testing::HasSubstr("could not be written"), std::runtime_error);
  // The first word of the first batch threw, and the second batch was skipped.
  EXPECT_EQ(numCalls, 1u);
}

// _____________________________________________________________________________
// Merge a number of words that is large enough for the index mappings to be
// handed to the asynchronous writer in several batches, and check that all the
// mappings arrive in the correct ID map, in the correct order.
TEST(MergeVocabulary, manyWordsWithSeveralIdMapBatches) {
  // The mappings are currently written in batches of 100000, so this leads to
  // several batches, of which the last one is only partially filled.
  static constexpr size_t numWords = 250'000;
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 2);

  // The `i`-th word (in sorted order) goes to the partial vocabulary
  // `i % 2`. The words are zero-padded, such that their lexicographic order is
  // the same as the order of their indices.
  std::array<std::vector<std::string>, 2> words;
  for (size_t i = 0; i < numWords; ++i) {
    words.at(i % 2).push_back(absl::StrFormat("\"word%08d\"", i));
  }
  writePartialVocabularyFile(filenames.wordsFiles_[0], words[0]);
  writePartialVocabularyFile(filenames.wordsFiles_[1], words[1]);

  // All the words are distinct, so they simply get the vocabulary indices
  // `0, 1, ...` in sorted order.
  size_t numWordsInCallback = 0;
  auto wordCallback = makeCountingWordCallback(numWordsInCallback);
  auto result = mergeVocabulary(partialVocabBasename, filenames.suffixes_,
                                std::less{}, wordCallback, 1_GB);
  EXPECT_EQ(numWordsInCallback, numWords);
  EXPECT_EQ(result.numWordsTotal(), numWords);

  // In the partial vocabulary `f`, the word with local id `j` is the word with
  // global id `2 * j + f`.
  for (size_t f = 0; f < 2; ++f) {
    IdMap expected;
    for (size_t j = 0; j < words.at(f).size(); ++j) {
      expected.push_back({L(j), V(2 * j + f)});
    }
    EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[f]),
                ::testing::ElementsAreArray(expected));
  }
}
