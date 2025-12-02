// Copyright 2018 - 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <string>

#include "./util/IdTestHelpers.h"
#include "backports/StartsWithAndEndsWith.h"
#include "global/Constants.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/Index.h"
#include "index/Vocabulary.h"
#include "index/VocabularyMerger.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Algorithm.h"

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

    // create random subdirectory in /tmp
    std::string tempPath = "";
    _basePath = tempPath + _basePath + "/";
    if (system(("mkdir -p " + _basePath).c_str())) {
      // system should return 0 on success
      std::cerr << "Could not create subfolder of tmp for test. this might "
                   "lead to test failures\n";
    }

    // make paths absolute under created tmp directory
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
              if (w.isBlankNode()) {
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
    // TODO: shall we delete the tmp files? doing so is cleaner, but makes it
    // harder to debug test failures
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

    res = mergeVocabulary(_basePath, 2, TripleComponentComparator(),
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

// Test for two-stage merge Vocabulary with small batch size
TEST_F(MergeVocabularyTest, mergeVocabularyTwoStage) {
  // Use a batch size of 1 to force two-stage merging with just 2 input files
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

    // Force two-stage merging by setting maxFilesPerBatch to 1
    res = mergeVocabulary(_basePath, 2, TripleComponentComparator(),
                          internalVocabularyAction, 1_GB, 1);
  }

  // Results should be identical to single-stage merge
  EXPECT_THAT(mergeResult,
              ::testing::ElementsAreArray(expectedMergedVocabulary_));
  EXPECT_THAT(geoMergeResult,
              ::testing::ElementsAreArray(expectedMergedGeoVocabulary_));

  // Verify metadata is correct
  ASSERT_EQ(res.langTaggedPredicates().begin(), Id::makeUndefined());
  ASSERT_EQ(res.langTaggedPredicates().end(), Id::makeUndefined());
  ASSERT_EQ(res.internalEntities().begin(), Id::makeUndefined());
  ASSERT_EQ(res.internalEntities().end(), Id::makeUndefined());

  // Check that ID mappings are correct (should be same as single-stage)
  IdMap mapping0 = getIdMapFromFile(_basePath + PARTIAL_VOCAB_IDMAP_INFIX +
                                    std::to_string(0));
  ASSERT_TRUE(vocabTestCompare(mapping0, _expMapping0));
  IdMap mapping1 = getIdMapFromFile(_basePath + PARTIAL_VOCAB_IDMAP_INFIX +
                                    std::to_string(1));
  ASSERT_TRUE(vocabTestCompare(mapping1, _expMapping1));
}

// Test fixture for comprehensive two-stage merge testing with multiple files
class MergeVocabularyMultiFileTest : public ::testing::Test {
 protected:
  std::string basePath_;
  size_t numFiles_ = 4;

  // Helper to create a partial vocabulary file
  void createPartialVocabFile(
      size_t fileIdx, const std::vector<TripleComponentWithIndex>& words) {
    std::string path = basePath_ + std::string(PARTIAL_VOCAB_WORDS_INFIX) +
                       std::to_string(fileIdx);
    ad_utility::serialization::FileWriteSerializer file(path);
    file << static_cast<uint64_t>(words.size());
    for (const auto& word : words) {
      file << word;
    }
  }

  void SetUp() override {
    basePath_ = "vocabMultiFileTest/";
    if (system(("mkdir -p " + basePath_).c_str())) {
      std::cerr << "Could not create test directory\n";
    }
  }

  void TearDown() override {
    // Clean up test files
    [[maybe_unused]] auto res = system(("rm -rf " + basePath_).c_str());
  }
};

// Test with 2 files per batch (4 files total, batch size 2)
TEST_F(MergeVocabularyMultiFileTest, twoFilesPerBatch) {
  // File 0: words "alpha", "delta"
  createPartialVocabFile(0, {{"\"alpha\"", false, 0}, {"\"delta\"", false, 1}});

  // File 1: words "beta", "delta" (delta appears in both files of batch 0)
  createPartialVocabFile(1, {{"\"beta\"", false, 0}, {"\"delta\"", false, 1}});

  // File 2: words "charlie", "foxtrot"
  createPartialVocabFile(
      2, {{"\"charlie\"", false, 0}, {"\"foxtrot\"", false, 1}});

  // File 3: words "echo", "foxtrot" (foxtrot appears in both files of batch 1)
  createPartialVocabFile(3,
                         {{"\"echo\"", false, 0}, {"\"foxtrot\"", false, 1}});

  std::vector<std::pair<std::string, bool>> mergeResult;
  auto internalVocabularyAction = [&mergeResult](const auto& word,
                                                 bool isExternal) -> uint64_t {
    mergeResult.emplace_back(word, isExternal);
    return mergeResult.size() - 1;
  };

  // Force 2 files per batch
  auto res = mergeVocabulary(basePath_, numFiles_, TripleComponentComparator(),
                             internalVocabularyAction, 1_GB, 2);

  // Expected merged vocabulary (sorted, deduplicated)
  std::vector<std::pair<std::string, bool>> expected = {
      {"\"alpha\"", false}, {"\"beta\"", false}, {"\"charlie\"", false},
      {"\"delta\"", false}, {"\"echo\"", false}, {"\"foxtrot\"", false}};

  EXPECT_THAT(mergeResult, ::testing::ElementsAreArray(expected));

  // Verify ID mappings are correct for all files
  for (size_t i = 0; i < numFiles_; ++i) {
    IdMap mapping = getIdMapFromFile(
        basePath_ + std::string(PARTIAL_VOCAB_IDMAP_INFIX) + std::to_string(i));
    ASSERT_GT(mapping.size(), 0) << "Mapping for file " << i << " is empty";
  }

  // Specifically check that "delta" (appears in files 0 and 1) maps to same
  // global ID
  IdMap map0 = getIdMapFromFile(basePath_ +
                                std::string(PARTIAL_VOCAB_IDMAP_INFIX) + "0");
  IdMap map1 = getIdMapFromFile(basePath_ +
                                std::string(PARTIAL_VOCAB_IDMAP_INFIX) + "1");

  // File 0: local ID 1 = "delta" should map to global ID 3
  EXPECT_EQ(map0[1].second, V(3));
  // File 1: local ID 1 = "delta" should also map to global ID 3
  EXPECT_EQ(map1[1].second, V(3));

  // Check that "foxtrot" (appears in files 2 and 3) maps to same global ID
  IdMap map2 = getIdMapFromFile(basePath_ +
                                std::string(PARTIAL_VOCAB_IDMAP_INFIX) + "2");
  IdMap map3 = getIdMapFromFile(basePath_ +
                                std::string(PARTIAL_VOCAB_IDMAP_INFIX) + "3");

  // File 2: local ID 1 = "foxtrot" should map to global ID 5
  EXPECT_EQ(map2[1].second, V(5));
  // File 3: local ID 1 = "foxtrot" should also map to global ID 5
  EXPECT_EQ(map3[1].second, V(5));
}

// Test with words appearing across different batches
TEST_F(MergeVocabularyMultiFileTest, wordAcrossBatches) {
  // File 0: "alpha", "shared"
  createPartialVocabFile(0,
                         {{"\"alpha\"", false, 0}, {"\"shared\"", false, 1}});

  // File 1: "beta"
  createPartialVocabFile(1, {{"\"beta\"", false, 0}});

  // File 2: "charlie", "shared" (shared appears in batch 0 and batch 1)
  createPartialVocabFile(2,
                         {{"\"charlie\"", false, 0}, {"\"shared\"", false, 1}});

  // File 3: "delta"
  createPartialVocabFile(3, {{"\"delta\"", false, 0}});

  std::vector<std::pair<std::string, bool>> mergeResult;
  auto internalVocabularyAction = [&mergeResult](const auto& word,
                                                 bool isExternal) -> uint64_t {
    mergeResult.emplace_back(word, isExternal);
    return mergeResult.size() - 1;
  };

  // Batch size 2: batch 0 = files [0,1], batch 1 = files [2,3]
  auto res = mergeVocabulary(basePath_, numFiles_, TripleComponentComparator(),
                             internalVocabularyAction, 1_GB, 2);

  // Expected: alpha, beta, charlie, delta, shared (sorted)
  std::vector<std::pair<std::string, bool>> expected = {{"\"alpha\"", false},
                                                        {"\"beta\"", false},
                                                        {"\"charlie\"", false},
                                                        {"\"delta\"", false},
                                                        {"\"shared\"", false}};

  EXPECT_THAT(mergeResult, ::testing::ElementsAreArray(expected));

  // Check that "shared" has same global ID from both files
  IdMap map0 = getIdMapFromFile(basePath_ +
                                std::string(PARTIAL_VOCAB_IDMAP_INFIX) + "0");
  IdMap map2 = getIdMapFromFile(basePath_ +
                                std::string(PARTIAL_VOCAB_IDMAP_INFIX) + "2");

  // File 0: local ID 1 = "shared" should map to global ID 4
  EXPECT_EQ(map0[1].second, V(4));
  // File 2: local ID 1 = "shared" should also map to global ID 4
  EXPECT_EQ(map2[1].second, V(4));
}

// Test with more complex scenario: 6 files, batch size 2
TEST_F(MergeVocabularyMultiFileTest, complexMultiBatchScenario) {
  numFiles_ = 6;

  // Batch 0: files 0-1
  createPartialVocabFile(0, {{"\"aaa\"", false, 0},
                             {"\"shared1\"", false, 1},
                             {"\"shared2\"", false, 2}});
  createPartialVocabFile(1, {{"\"bbb\"", false, 0}, {"\"shared1\"", false, 1}});

  // Batch 1: files 2-3
  createPartialVocabFile(2, {{"\"ccc\"", false, 0},
                             {"\"shared2\"", false, 1},
                             {"\"shared3\"", false, 2}});
  createPartialVocabFile(3, {{"\"ddd\"", false, 0}, {"\"shared3\"", false, 1}});

  // Batch 2: files 4-5
  createPartialVocabFile(4, {{"\"eee\"", false, 0}, {"\"shared1\"", false, 1}});
  createPartialVocabFile(5, {{"\"fff\"", false, 0}, {"\"shared3\"", false, 1}});

  std::vector<std::pair<std::string, bool>> mergeResult;
  auto internalVocabularyAction = [&mergeResult](const auto& word,
                                                 bool isExternal) -> uint64_t {
    mergeResult.emplace_back(word, isExternal);
    return mergeResult.size() - 1;
  };

  auto res = mergeVocabulary(basePath_, numFiles_, TripleComponentComparator(),
                             internalVocabularyAction, 1_GB, 2);

  // Expected: aaa, bbb, ccc, ddd, eee, fff, shared1, shared2, shared3
  std::vector<std::pair<std::string, bool>> expected = {
      {"\"aaa\"", false},     {"\"bbb\"", false},     {"\"ccc\"", false},
      {"\"ddd\"", false},     {"\"eee\"", false},     {"\"fff\"", false},
      {"\"shared1\"", false}, {"\"shared2\"", false}, {"\"shared3\"", false}};

  EXPECT_THAT(mergeResult, ::testing::ElementsAreArray(expected));

  // Verify shared1 appears in files 0, 1, 4 with same global ID (6)
  IdMap map0 = getIdMapFromFile(basePath_ +
                                std::string(PARTIAL_VOCAB_IDMAP_INFIX) + "0");
  IdMap map1 = getIdMapFromFile(basePath_ +
                                std::string(PARTIAL_VOCAB_IDMAP_INFIX) + "1");
  IdMap map4 = getIdMapFromFile(basePath_ +
                                std::string(PARTIAL_VOCAB_IDMAP_INFIX) + "4");

  EXPECT_EQ(map0[1].second, V(6));  // shared1 in file 0
  EXPECT_EQ(map1[1].second, V(6));  // shared1 in file 1
  EXPECT_EQ(map4[1].second, V(6));  // shared1 in file 4

  // Verify shared2 appears in files 0, 2 with same global ID (7)
  IdMap map2 = getIdMapFromFile(basePath_ +
                                std::string(PARTIAL_VOCAB_IDMAP_INFIX) + "2");
  EXPECT_EQ(map0[2].second, V(7));  // shared2 in file 0
  EXPECT_EQ(map2[1].second, V(7));  // shared2 in file 2

  // Verify shared3 appears in files 2, 3, 5 with same global ID (8)
  IdMap map3 = getIdMapFromFile(basePath_ +
                                std::string(PARTIAL_VOCAB_IDMAP_INFIX) + "3");
  IdMap map5 = getIdMapFromFile(basePath_ +
                                std::string(PARTIAL_VOCAB_IDMAP_INFIX) + "5");
  EXPECT_EQ(map2[2].second, V(8));  // shared3 in file 2
  EXPECT_EQ(map3[1].second, V(8));  // shared3 in file 3
  EXPECT_EQ(map5[1].second, V(8));  // shared3 in file 5
}

TEST(VocabularyGeneratorTest, createInternalMapping) {
  ItemVec input;
  using S = LocalVocabIndexAndSplitVal;
  TripleComponentComparator::SplitValNonOwningWithSortKey
      d;  // dummy value that is unused in this case.
  input.emplace_back("alpha", S{5, d});
  input.emplace_back("beta", S{4, d});
  input.emplace_back("beta", S{42, d});
  input.emplace_back("d", S{8, d});
  input.emplace_back("e", S{9, d});
  input.emplace_back("e", S{38, d});
  input.emplace_back("xenon", S{0, d});

  auto res = createInternalMapping(&input);
  ASSERT_EQ(0u, input[0].second.id_);
  ASSERT_EQ(1u, input[1].second.id_);
  ASSERT_EQ(1u, input[2].second.id_);
  ASSERT_EQ(2u, input[3].second.id_);
  ASSERT_EQ(3u, input[4].second.id_);
  ASSERT_EQ(3u, input[5].second.id_);
  ASSERT_EQ(4u, input[6].second.id_);

  ASSERT_EQ(0u, res[5]);
  ASSERT_EQ(1u, res[4]);
  ASSERT_EQ(1u, res[42]);
  ASSERT_EQ(2u, res[8]);
  ASSERT_EQ(3u, res[9]);
  ASSERT_EQ(3u, res[38]);
  ASSERT_EQ(4u, res[0]);
}
