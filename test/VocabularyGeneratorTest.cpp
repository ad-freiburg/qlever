#include <gmock/gmock.h>

#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <string>

#include "./util/IdTestHelpers.h"
#include "global/Constants.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/Index.h"
#include "index/VocabularyMerger.h"
#include "util/Algorithm.h"

using namespace ad_utility::vocabulary_merger;
namespace {
// equality operator used in this test
bool vocabTestCompare(const IdPairMMapVecView& a,
                      const std::vector<std::pair<Id, Id>>& b) {
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
    _path0 = std::string(PARTIAL_VOCAB_FILE_NAME + std::to_string(0));
    _path1 = std::string(PARTIAL_VOCAB_FILE_NAME + std::to_string(1));

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
        {"\"ape\"", false, 0},     {"\"bla\"", true, 2},
        {"\"gorilla\"", false, 3}, {"\"monkey\"", false, 4},
        {"_:blank", false, 0},     {"_:blunk", false, 1}};
    std::vector<TripleComponentWithIndex> words1{
        {"\"bear\"", false, 1},
        {"\"monkey\"", true, 4},
        {"\"zebra\"", false, 5},
        {"_:blunk", false, 1},
    };

    // Note that the word "monkey" appears in both vocabularies, buth with
    // different settings for `isExternal`. In this case it is externalized.
    expectedMergedVocabulary_ = ExpectedVocabulary{
        {"\"ape\"", false},     {"\"bear\"", false},  {"\"bla\"", true},
        {"\"gorilla\"", false}, {"\"monkey\"", true}, {"\"zebra\"", false}};

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
  {
    auto internalVocabularyAction =
        [&mergeResult](const auto& word,
                       [[maybe_unused]] bool isExternal) -> uint64_t {
      mergeResult.emplace_back(word, isExternal);
      return mergeResult.size() - 1;
    };
    res = mergeVocabulary(_basePath, 2, TripleComponentComparator(),
                          internalVocabularyAction, 1_GB);
  }

  EXPECT_THAT(mergeResult,
              ::testing::ElementsAreArray(expectedMergedVocabulary_));

  // No language tags in text file
  ASSERT_EQ(res.langTaggedPredicates().begin(), Id::makeUndefined());
  ASSERT_EQ(res.langTaggedPredicates().end(), Id::makeUndefined());
  // Also no internal entities there.
  ASSERT_EQ(res.internalEntities().begin(), Id::makeUndefined());
  ASSERT_EQ(res.internalEntities().end(), Id::makeUndefined());
  // Check that vocabulary has the right form.
  IdPairMMapVecView mapping0(_basePath + PARTIAL_MMAP_IDS + std::to_string(0));
  ASSERT_TRUE(vocabTestCompare(mapping0, _expMapping0));
  IdPairMMapVecView mapping1(_basePath + PARTIAL_MMAP_IDS + std::to_string(1));
  ASSERT_TRUE(vocabTestCompare(mapping1, _expMapping1));
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
