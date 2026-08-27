// Copyright 2018 - 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <re2/re2.h>

#include <array>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include "./util/IdTestHelpers.h"
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
#include "util/File.h"
#include "util/GTestHelpers.h"
#include "util/HashSet.h"

using namespace ad_utility::vocabulary_merger;
namespace {
// equality operator used in this test
bool vocabTestCompare(const IdMap& a, const std::vector<std::pair<Id, Id>>& b) {
  if (a.size() != b.size()) {
    return false;
  }

  for (size_t i = 0; i < a.size(); ++i) {
    if (a[i] != b[i]) {
      return false;
    }
  }

  return true;
}

auto V = ad_utility::testing::VocabId;

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
  std::string _path0;
  std::string _path1;
  // the base directory for our test
  std::string _basePath;

  // The bool means "is in the external vocabulary and not in the internal
  // vocabulary".
  using ExpectedVocabulary = std::vector<std::pair<std::string, bool>>;
  ExpectedVocabulary expectedMergedVocabulary_;
  ExpectedVocabulary expectedMergedGeoVocabulary_;

  // two std::vectors where we store the expected mapping
  // form partial to global ids;
  using Mapping = std::vector<std::pair<Id, Id>>;
  Mapping _expMapping0;
  Mapping _expMapping1;

  // Constructor. TODO: Better write Setup method because of complex logic which
  // may throw?
  MergeVocabularyTest() {
    _basePath = std::string("vocabularyGeneratorTestFiles");
    // those names are required by mergeVocabulary
    _path0 = std::string(PARTIAL_VOCAB_WORDS_INFIX + std::to_string(0));
    _path1 = std::string(PARTIAL_VOCAB_WORDS_INFIX + std::to_string(1));

    // Create a subdirectory for the test files in the working directory.
    _basePath = _basePath + "/";
    ql::error_code errorCode;
    ql::filesystem::create_directories(_basePath, errorCode);
    if (errorCode) {
      std::cerr << "Could not create the directory for the test files. This "
                   "might lead to test failures\n";
    }

    // Prepend the created directory to the paths.
    _path0 = _basePath + _path0;
    _path1 = _basePath + _path1;

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
    ad_utility::serialization::FileWriteSerializer partial0(_path0);
    ad_utility::serialization::FileWriteSerializer partial1(_path1);

    auto writePartialVocabulary =
        [](auto& partialVocab, const auto& tripleComponents, Mapping* mapping) {
          // write first partial vocabulary
          partialVocab << tripleComponents.size();
          size_t localIdx = 0;
          for (auto w : tripleComponents) {
            auto globalId = w.index_;
            w.index_ = localIdx;
            partialVocab << w;
            if (mapping) {
              if (w.isBlankNode({})) {
                mapping->emplace_back(
                    V(localIdx),
                    Id::makeFromBlankNodeIndex(BlankNodeIndex::make(globalId)));
              } else {
                using GeoVocab = SplitGeoVocabulary<
                    CompressedVocabulary<VocabularyInternalExternal>>;
                if (GeoVocab::getMarkerForWord(w.iriOrLiteral()) == 1) {
                  globalId = GeoVocab::addMarker(globalId, 1);
                }
                mapping->emplace_back(V(localIdx), V(globalId));
              }
            }
            localIdx++;
          }
        };
    writePartialVocabulary(partial0, words0, &_expMapping0);

    writePartialVocabulary(partial1, words1, &_expMapping1);
  }

  // __________________________________________________________________
  ~MergeVocabularyTest() {
    // Delete the test files (to debug a test failure, comment this out).
    ql::error_code errorCode;
    ql::filesystem::remove_all(_basePath, errorCode);
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
  // mergeVocabulary only gets name of directory and number of files.
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
        _basePath, 2,
        [&comparator](std::string_view a, bool aIsExternal, std::string_view b,
                      bool bIsExternal) {
          return comparator.isLessInTotalWithExternalFlag(a, aIsExternal, b,
                                                          bIsExternal);
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
  IdMap mapping0 = getIdMapFromFile(_basePath + PARTIAL_VOCAB_IDMAP_INFIX +
                                    std::to_string(0));
  ASSERT_TRUE(vocabTestCompare(mapping0, _expMapping0));
  IdMap mapping1 = getIdMapFromFile(_basePath + PARTIAL_VOCAB_IDMAP_INFIX +
                                    std::to_string(1));
  ASSERT_TRUE(vocabTestCompare(mapping1, _expMapping1));
}

// _____________________________________________________________________________
TEST(MergeVocabulary, mergeVocabularyAssertion) {
  auto callback = [](const auto&, bool) { return uint64_t{0}; };

  std::string basePath = gtestCurrentTestName();

  // Intentionally in wrong order, so that the merge detects a violated order.
  std::array<std::string_view, 3> unorderedWords{"\"c\"", "\"b\"", "\"a\""};
  writePartialVocabularyFile(
      absl::StrCat(basePath, PARTIAL_VOCAB_WORDS_INFIX, 0), unorderedWords);
  writePartialVocabularyFile(
      absl::StrCat(basePath, PARTIAL_VOCAB_WORDS_INFIX, 1), unorderedWords);
  absl::Cleanup cleanup = [&basePath] {
    for (size_t i = 0; i < 2; ++i) {
      ad_utility::deleteFile(
          absl::StrCat(basePath, PARTIAL_VOCAB_WORDS_INFIX, i), false);
      ad_utility::deleteFile(
          absl::StrCat(basePath, PARTIAL_VOCAB_IDMAP_INFIX, i), false);
    }
  };

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      mergeVocabulary(
          basePath, 2,
          [](std::string_view a, bool, std::string_view b, bool) {
            return std::less{}(a, b);
          },
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
  std::string basePath = gtestCurrentTestName();
  std::string wordsFile = absl::StrCat(basePath, PARTIAL_VOCAB_WORDS_INFIX, 0);
  std::string idMapFile = absl::StrCat(basePath, PARTIAL_VOCAB_IDMAP_INFIX, 0);
  absl::Cleanup cleanup = [&wordsFile, &idMapFile] {
    ad_utility::deleteFile(wordsFile, false);
    ad_utility::deleteFile(idMapFile, false);
  };

  // A single partial vocabulary. The words must be in ascending order according
  // to the comparator used below (plain `std::less`); note that literals (which
  // start with `"`) sort before IRIs (which start with `<`).
  std::array<std::string_view, 5> words{"\"bn_lit\"", "<http://ex/apple>",
                                        "<http://ex/bn_1>", "<http://ex/bn_2>",
                                        "<http://ex/cherry>"};
  writePartialVocabularyFile(wordsFile, words);

  // Collect the words that are actually written to the vocabulary (i.e. not
  // treated as blank nodes), together with the vocabulary index they get.
  std::vector<std::string> vocabularyWords;
  auto wordCallback = [&vocabularyWords](std::string_view word,
                                         bool) -> uint64_t {
    vocabularyWords.emplace_back(word);
    return vocabularyWords.size() - 1;
  };

  // Two (compiled) regexes:
  // - `<http://ex/bn_.*>` fully matches the two `bn_` IRIs (and neither the
  //   `"bn_lit"` literal, which is not an IRI, nor the other IRIs).
  // - `<http://ex/apple` only matches a prefix of `<http://ex/apple>` (the
  //   closing `>` is missing), so with *full* match it converts nothing. With a
  //   partial match it would have wrongly converted `<http://ex/apple>`.
  std::vector<std::unique_ptr<re2::RE2>> blankNodeIriRegexes;
  for (const char* pattern : {"<http://ex/bn_.*>", "<http://ex/apple"}) {
    blankNodeIriRegexes.push_back(std::make_unique<re2::RE2>(pattern));
  }
  mergeVocabulary(
      basePath, 1,
      [](std::string_view a, bool, std::string_view b, bool) {
        return std::less{}(a, b);
      },
      wordCallback, 1_GB, blankNodeIriRegexes);

  // Only the two `bn_` IRIs became blank nodes; the two other IRIs and the
  // literal remain in the vocabulary, in sorted order.
  EXPECT_THAT(vocabularyWords,
              ::testing::ElementsAre("\"bn_lit\"", "<http://ex/apple>",
                                     "<http://ex/cherry>"));

  // Check the exact id mapping. The local ids `0..4` are assigned in the input
  // (sorted) order above; the two `bn_` IRIs get consecutive, distinct blank
  // node ids, the other three words get vocabulary ids in their appearance
  // order.
  auto BN = [](uint64_t index) {
    return Id::makeFromBlankNodeIndex(BlankNodeIndex::make(index));
  };
  IdMap idMap = getIdMapFromFile(idMapFile);
  EXPECT_THAT(idMap, ::testing::ElementsAreArray(std::vector<std::pair<Id, Id>>{
                         {V(0), V(0)},     // "bn_lit"
                         {V(1), V(1)},     // <http://ex/apple>
                         {V(2), BN(0)},    // <http://ex/bn_1>
                         {V(3), BN(1)},    // <http://ex/bn_2>
                         {V(4), V(2)}}));  // <http://ex/cherry>
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
// The partial vocabularies that are written by
// `writePartialVocabularyToFile` end with a sparse splitter index, but files
// that were written by hand (as in this test) or by an older version of QLever
// have no such index. The merge then has to build the block metadata by a
// sequential scan, which this test exercises.
TEST(MergeVocabulary, mergeVocabularyWithoutSplitterIndex) {
  std::string basePath = gtestCurrentTestName();
  // The words of a single partial vocabulary have to be sorted; the word
  // `"c"` deliberately appears in both of them.
  std::array<std::string_view, 4> words0{"\"a\"", "\"c\"", "\"e\"", "\"g\""};
  std::array<std::string_view, 3> words1{"\"b\"", "\"c\"", "\"f\""};
  writePartialVocabularyFile(
      absl::StrCat(basePath, PARTIAL_VOCAB_WORDS_INFIX, 0), words0);
  writePartialVocabularyFile(
      absl::StrCat(basePath, PARTIAL_VOCAB_WORDS_INFIX, 1), words1);
  absl::Cleanup cleanup = [&basePath] {
    for (size_t i = 0; i < 2; ++i) {
      ad_utility::deleteFile(
          absl::StrCat(basePath, PARTIAL_VOCAB_WORDS_INFIX, i), false);
      ad_utility::deleteFile(
          absl::StrCat(basePath, PARTIAL_VOCAB_IDMAP_INFIX, i), false);
    }
  };

  std::vector<std::string> vocabularyWords;
  auto wordCallback = [&vocabularyWords](std::string_view word,
                                         bool) -> uint64_t {
    vocabularyWords.emplace_back(word);
    return vocabularyWords.size() - 1;
  };
  mergeVocabulary(
      basePath, 2,
      [](std::string_view a, bool, std::string_view b, bool) {
        return std::less{}(a, b);
      },
      wordCallback, 1_GB);

  EXPECT_THAT(vocabularyWords,
              ::testing::ElementsAre("\"a\"", "\"b\"", "\"c\"", "\"e\"",
                                     "\"f\"", "\"g\""));
  EXPECT_THAT(
      getIdMapFromFile(absl::StrCat(basePath, PARTIAL_VOCAB_IDMAP_INFIX, 0)),
      ::testing::ElementsAreArray(std::vector<std::pair<Id, Id>>{
          {V(0), V(0)}, {V(1), V(2)}, {V(2), V(3)}, {V(3), V(5)}}));
  EXPECT_THAT(
      getIdMapFromFile(absl::StrCat(basePath, PARTIAL_VOCAB_IDMAP_INFIX, 1)),
      ::testing::ElementsAreArray(std::vector<std::pair<Id, Id>>{
          {V(0), V(1)}, {V(1), V(2)}, {V(2), V(4)}}));
}

// _____________________________________________________________________________
// Merge many partial vocabularies with many words, such that the merge is
// actually performed in parallel. Every third word appears twice, namely once
// with `isExternal == false` and once with `isExternal == true`. Such a pair of
// byte-equal words is adjacent in the merged output, but the two words are
// *distinct* wrt the comparator, so a chunk boundary of the parallel merge may
// well fall between them. The merge has to keep them adjacent in that case,
// because the consumer deduplicates by comparing each word with its
// predecessor. The test explicitly checks that this case actually occurs.
TEST(MergeVocabulary, mergeVocabularyManyFilesAndDuplicates) {
  std::string basePath = gtestCurrentTestName();
  constexpr size_t numFiles = 8;
  constexpr size_t numDistinctWords = 70'000;
  // A tiny splitter interval, such that the merge has many blocks (and
  // therefore many candidates for its chunk boundaries) to choose from.
  constexpr size_t splitterInterval = 64;

  TripleComponentComparator comparator;
  auto sortPred = [&comparator](std::string_view a, bool aIsExternal,
                                std::string_view b, bool bIsExternal) {
    return comparator.isLessInTotalWithExternalFlag(a, aIsExternal, b,
                                                    bIsExternal);
  };

  // The distinct words. Every word with an index that is not divisible by three
  // is duplicated, see above.
  std::vector<std::string> words;
  for (size_t i = 0; i < numDistinctWords; ++i) {
    words.push_back(
        absl::StrCat("\"word", absl::Dec(i, absl::kZeroPad6), "\""));
  }
  auto isDuplicated = [](size_t i) { return i % 3 != 0; };

  // Distribute the words over the files: the words that are *not* external go
  // to the files `[0, numFiles / 2)`, the external ones to the remaining files.
  std::array<std::vector<std::pair<std::string_view, bool>>, numFiles>
      wordsPerFile;
  for (size_t i = 0; i < numDistinctWords; ++i) {
    wordsPerFile.at(i % (numFiles / 2)).emplace_back(words.at(i), false);
    if (isDuplicated(i)) {
      wordsPerFile.at(numFiles / 2 + i % (numFiles / 2))
          .emplace_back(words.at(i), true);
    }
  }

  // Write the partial vocabularies. The local ids are the positions of the
  // words within their (sorted) file.
  std::array<ItemVec, numFiles> elsPerFile;
  std::vector<std::string> fileNames;
  for (size_t file = 0; file < numFiles; ++file) {
    auto& wordsOfFile = wordsPerFile.at(file);
    ql::ranges::sort(wordsOfFile, [&sortPred](const auto& a, const auto& b) {
      return sortPred(a.first, a.second, b.first, b.second);
    });
    auto& els = elsPerFile.at(file);
    for (size_t i = 0; i < wordsOfFile.size(); ++i) {
      els.emplace_back(
          wordsOfFile.at(i).first,
          PartialVocabIndexWithExternalFlag{i, wordsOfFile.at(i).second});
    }
    fileNames.push_back(
        absl::StrCat(basePath, PARTIAL_VOCAB_WORDS_INFIX, file));
    writePartialVocabularyToFile(els, fileNames.back(), splitterInterval);
  }
  absl::Cleanup cleanup = [&basePath, &fileNames] {
    for (const auto& fileName : fileNames) {
      ad_utility::deleteFile(fileName, false);
    }
    for (size_t file = 0; file < numFiles; ++file) {
      ad_utility::deleteFile(
          absl::StrCat(basePath, PARTIAL_VOCAB_IDMAP_INFIX, file), false);
    }
  };

  // Check that at least one of the chunk boundaries that the merge will use
  // falls between two byte-equal words. The number of chunks is computed
  // exactly as in `parallelBlockMergeToRange`.
  namespace pbm = ad_utility::parallelBlockMerge;
  auto scheduler = pbm::defaultMergeScheduler();
  if (scheduler->maxParallelism() > 1) {
    auto lessThan = [&sortPred](const auto& a, const auto& b) {
      return sortPred(a.iriOrLiteral(), a.isExternal(), b.iriOrLiteral(),
                      b.isExternal());
    };
    PartialVocabRunsInput input{basePath, numFiles};
    pbm::MergeOptions defaultOptions;
    auto splitters = pbm::computeSplitters(
        input, lessThan,
        scheduler->maxParallelism() * defaultOptions.targetChunksPerThread);
    // A splitter that is a *non*-external word which is duplicated separates
    // that word from its external twin, because the external twin is the direct
    // predecessor of the splitter in the total order.
    ad_utility::HashSet<std::string_view> duplicatedWords;
    for (size_t i = 0; i < numDistinctWords; ++i) {
      if (isDuplicated(i)) {
        duplicatedWords.insert(words.at(i));
      }
    }
    size_t numSplitDuplicates = static_cast<size_t>(
        ql::ranges::count_if(splitters, [&duplicatedWords](const auto& key) {
          return !key.isExternal() && duplicatedWords.contains(key.word_);
        }));
    EXPECT_GT(numSplitDuplicates, 0u)
        << "None of the " << splitters.size()
        << " chunk boundaries of the merge separates two byte-equal words, so "
           "this test does not test what it is supposed to test";
  }

  // Merge the partial vocabularies. The memory limit is deliberately small,
  // such that the merge has to emit many small blocks of merged words.
  std::vector<std::pair<std::string, bool>> mergeResult;
  auto wordCallback = [&mergeResult](std::string_view word,
                                     bool isExternal) -> uint64_t {
    mergeResult.emplace_back(word, isExternal);
    return mergeResult.size() - 1;
  };
  auto metaData =
      mergeVocabulary(basePath, numFiles, sortPred, wordCallback, 8_MB);
  EXPECT_EQ(metaData.numWordsTotal(), numDistinctWords);

  // The expected vocabulary: every distinct word exactly once, in sorted order,
  // and externalized iff it appears with `isExternal == true` in one of the
  // partial vocabularies.
  std::vector<std::pair<std::string, bool>> expected;
  for (size_t i = 0; i < numDistinctWords; ++i) {
    expected.emplace_back(words.at(i), isDuplicated(i));
  }
  ql::ranges::sort(expected, [&sortPred](const auto& a, const auto& b) {
    return sortPred(a.first, a.second, b.first, b.second);
  });
  ASSERT_EQ(mergeResult.size(), expected.size());
  auto mismatch = ql::ranges::mismatch(mergeResult, expected);
  EXPECT_TRUE(mismatch.in1 == mergeResult.end())
      << "The first mismatch is at index "
      << (mismatch.in1 - mergeResult.begin()) << ": (" << mismatch.in1->first
      << ", " << mismatch.in1->second << ") instead of (" << mismatch.in2->first
      << ", " << mismatch.in2->second << ")";

  // The global index of a word is its position in the merged vocabulary.
  ad_utility::HashMap<std::string_view, size_t> globalIndices;
  for (size_t i = 0; i < expected.size(); ++i) {
    globalIndices[expected.at(i).first] = i;
  }
  // Every partial vocabulary maps its local ids (which are the positions of its
  // words within the file) to the global ids, in the order of the file.
  for (size_t file = 0; file < numFiles; ++file) {
    std::vector<std::pair<Id, Id>> expectedMapping;
    const auto& els = elsPerFile.at(file);
    for (size_t i = 0; i < els.size(); ++i) {
      expectedMapping.emplace_back(V(i), V(globalIndices.at(els.at(i).first)));
    }
    auto idMap = getIdMapFromFile(
        absl::StrCat(basePath, PARTIAL_VOCAB_IDMAP_INFIX, file));
    EXPECT_TRUE(vocabTestCompare(idMap, expectedMapping))
        << "The id map of the partial vocabulary " << file << " is wrong";
  }
}
