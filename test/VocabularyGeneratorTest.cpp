#include <gtest/gtest.h>

#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <string>

#include "./util/IdTestHelpers.h"
#include "global/Constants.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/Index.h"
#include "index/VocabularyGenerator.h"
#include "util/Algorithm.h"

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

auto makeItemMapArray = [] {
  auto alloc = ItemAlloc{std::pmr::get_default_resource()};
  auto make = [&](auto&&...) { return ItemMapAndBuffer{alloc}; };
  return ad_utility::transformArray(std::array<int, NUM_PARALLEL_ITEM_MAPS>{},
                                    make);
};
}  // namespace

// Test fixture that sets up the binary files vor partial vocabulary and
// everything else connected with vocabulary merging.
class MergeVocabularyTest : public ::testing::Test {
 protected:
  // path of the 2 partial Vocabularies that are used by mergeVocabulary
  std::string _path0;
  std::string _path1;
  // the base directory for our test
  std::string _basePath;
  // path to expected vocabulary text file
  std::string _pathVocabExp;
  // path to expected external vocabulary text file
  std::string _pathExternalVocabExp;

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

    // make paths abolute under created tmp directory
    _path0 = _basePath + _path0;
    _path1 = _basePath + _path1;
    _pathVocabExp = _basePath + std::string(".vocabExp");
    _pathExternalVocabExp = _basePath + std::string("externalTextFileExp");

    // these will be the contents of partial vocabularies, second element of
    // pair is the correct Id which is expected from mergeVocabulary
    std::vector<TripleComponentWithIndex> words0{{"\"ape\"", false, 0},
                                                 {"\"gorilla\"", false, 2},
                                                 {"\"monkey\"", false, 3},
                                                 {"\"bla\"", true, 5}};
    std::vector<TripleComponentWithIndex> words1{{"\"bear\"", false, 1},
                                                 {"\"monkey\"", false, 3},
                                                 {"\"zebra\"", false, 4}};

    // write expected vocabulary files
    std::ofstream expVoc(_pathVocabExp);
    std::ofstream expExtVoc(_pathExternalVocabExp);
    expVoc << "\"ape\"\n\"bear\"\n\"gorilla\"\n\"monkey\"\n\"zebra\"\n";
    expExtVoc << "\"bla\"\n";

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
              mapping->emplace_back(V(localIdx), V(globalId));
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

  // returns true if and only if the files with names n1 and n2 exist, can be
  // opened for reading and are bytewise equal.
  bool areBinaryFilesEqual(const std::string& n1, const std::string& n2) {
    auto p1 = readAllBytes(n1);
    auto p2 = readAllBytes(n2);
    auto s1 = p1.first;
    auto s2 = p2.first;
    auto& f1 = p1.second;
    auto& f2 = p2.second;

    return (s1 && s2 && f1.size() == f2.size() &&
            std::equal(f1.begin(), f1.end(), f2.begin()));
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
  VocabularyMerger::VocabularyMetaData res;
  {
    VocabularyMerger m;
    auto file = ad_utility::makeOfstream(_basePath + INTERNAL_VOCAB_SUFFIX);
    auto internalVocabularyAction = [&file](const auto& word) {
      file << RdfEscaping::escapeNewlinesAndBackslashes(word) << '\n';
    };
    res = m.mergeVocabulary(_basePath, 2, TripleComponentComparator(),
                            internalVocabularyAction, 1_GB);
  }

  // No language tags in text file
  ASSERT_EQ(res.langTaggedPredicates_.begin(), Id::makeUndefined());
  ASSERT_EQ(res.langTaggedPredicates_.end(), Id::makeUndefined());
  // Also no internal entities there.
  ASSERT_EQ(res.internalEntities_.begin(), Id::makeUndefined());
  ASSERT_EQ(res.internalEntities_.end(), Id::makeUndefined());
  // check that (external) vocabulary has the right form.
  ASSERT_TRUE(
      areBinaryFilesEqual(_pathVocabExp, _basePath + INTERNAL_VOCAB_SUFFIX));
  ASSERT_TRUE(areBinaryFilesEqual(_pathExternalVocabExp,
                                  _basePath + EXTERNAL_LITS_TEXT_FILE_NAME));

  IdPairMMapVecView mapping0(_basePath + PARTIAL_MMAP_IDS + std::to_string(0));
  ASSERT_TRUE(vocabTestCompare(mapping0, _expMapping0));
  IdPairMMapVecView mapping1(_basePath + PARTIAL_MMAP_IDS + std::to_string(1));
  ASSERT_TRUE(vocabTestCompare(mapping1, _expMapping1));
}

TEST(VocabularyGenerator, ReadAndWritePartial) {
  using S = TripleComponentComparator::SplitValNonOwningWithSortKey;
  {
    S dummy;

    ItemMapArray arr = makeItemMapArray();
    auto& s = arr[0].map_;
    s["A"] = {5, dummy};
    s["acb"] = {6, dummy};
    s["b"] = {7, dummy};
    s["Ba"] = {8, dummy};
    s["car"] = {9, dummy};
    TextVocabulary v;
    std::string basename = "_tmp_testidx";
    auto ptr = std::make_shared<const ItemMapArray>(std::move(arr));
    writePartialIdMapToBinaryFileForMerging(
        ptr, basename + PARTIAL_VOCAB_FILE_NAME + "0",
        [&v](const auto& a, const auto& b) {
          return v.getCaseComparator()(a.first, b.first);
        },
        false);

    {
      VocabularyMerger m;
      auto file = ad_utility::makeOfstream(basename + INTERNAL_VOCAB_SUFFIX);
      auto internalVocabularyAction = [&file](const auto& word) {
        file << RdfEscaping::escapeNewlinesAndBackslashes(word) << '\n';
      };
      m.mergeVocabulary(basename, 1, v.getCaseComparator(),
                        internalVocabularyAction, 1_GB);
    }
    auto idMap = IdMapFromPartialIdMapFile(basename + PARTIAL_MMAP_IDS + "0");
    EXPECT_EQ(V(0), idMap[V(5)]);
    EXPECT_EQ(V(1), idMap[V(6)]);
    EXPECT_EQ(V(2), idMap[V(7)]);
    EXPECT_EQ(V(3), idMap[V(8)]);
    EXPECT_EQ(V(4), idMap[V(9)]);
    auto res = system("rm _tmp_testidx*");
    (void)res;
  }

  // Again with the case-insensitive variant.
  try {
    RdfsVocabulary v;
    v.setLocale("en", "US", false);
    ItemMapArray arr = makeItemMapArray();
    auto& s = arr[0].map_;
    // The actual strings are never deallocated, but the
    // `monotonic_buffer_resource` deallocates them all at once. Note that
    // simply passing `std::pmr::get_default_resource` to the `alloc` below
    // would lead to memory leaks.
    std::pmr::monotonic_buffer_resource buffer;
    auto alloc = std::pmr::polymorphic_allocator<char>{&buffer};
    auto assign = [&](std::string_view str, size_t id) {
      s[str] = {
          id,
          v.getCaseComparator().extractAndTransformComparableNonOwning(
              str, TripleComponentComparator::Level::IDENTICAL, false, &alloc)};
    };
    assign("\"A\"", 5);
    assign("\"a\"", 6);
    assign("\"Ba\"", 7);
    assign("\"car\"", 8);
    assign("\"Ä\"", 9);
    std::string basename = "_tmp_testidx";
    auto ptr = std::make_shared<const ItemMapArray>(std::move(arr));
    writePartialIdMapToBinaryFileForMerging(
        ptr, basename + PARTIAL_VOCAB_FILE_NAME + "0",
        [&c = v.getCaseComparator()](const auto& a, const auto& b) {
          return c(a.second.splitVal_, b.second.splitVal_,
                   TripleComponentComparator::Level::IDENTICAL);
        },
        false);

    {
      VocabularyMerger m;
      auto file = ad_utility::makeOfstream(basename + INTERNAL_VOCAB_SUFFIX);
      auto internalVocabularyAction = [&file](const auto& word) {
        file << RdfEscaping::escapeNewlinesAndBackslashes(word) << '\n';
      };
      m.mergeVocabulary(basename, 1, v.getCaseComparator(),
                        internalVocabularyAction, 1_GB);
    }
    auto idMap = IdMapFromPartialIdMapFile(basename + PARTIAL_MMAP_IDS + "0");
    EXPECT_EQ(V(0), idMap[V(6)]);
    EXPECT_EQ(V(1), idMap[V(5)]);
    EXPECT_EQ(V(2), idMap[V(9)]);
    EXPECT_EQ(V(3), idMap[V(7)]);
    EXPECT_EQ(V(4), idMap[V(8)]);
    auto res = system("rm _tmp_testidx*");
    (void)res;

  } catch (const std::bad_cast& b) {
    std::cerr << "What the fuck\n";
  }
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
