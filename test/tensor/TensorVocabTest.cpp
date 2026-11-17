
#include <gtest/gtest.h>

#include "../util/MmapVectorLegacyFormat.h"
#include "./TensorTestHelpers.h"
#include "gmock/gmock.h"
#include <filesystem>
#include <random>
#include <thread>

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/TensorDataVocabulary.h"
#include "index/vocabulary/Vocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/File.h"

namespace {
using namespace TensorTestHelpers;
using namespace ad_utility;
using AnyTensorVocab = TensorDataVocabulary<VocabularyInMemory>;

// Define a typed test suite to test the `TensorDataVocabulary` on different
// types of underlying vocabularies.
using TensorVocabularyUnderlyingVocabTypes =
    ::testing::Types<VocabularyInMemory,
                     CompressedVocabulary<VocabularyInternalExternal>>;
template <typename T>
class TensorVocabularyUnderlyingVocabTypedTest : public ::testing::Test {
 public:
  // A function to test that a `TensorDataVocabulary` can correctly insert and
  // lookup literals and precompute tensor information. This test is
  // generic on the type of the underlying vocabulary, because the
  // `TensorDataVocabulary` should behave exactly the same no matter which
  // underlying vocabulary implementation is used.
  void testTensorVocabulary() {
    using TV = TensorDataVocabulary<T>;
    TV tensorDataVocab;
    const std::string fn = "tensordata-test1.dat";
    auto ww = tensorDataVocab.makeDiskWriterPtr(fn);
    ww->readableName() = "test";

    std::vector<std::string> testLiterals{
        // Invalid literal
        "\"Example non-tensor literal\"@en",
        R"("{\"data":[1.0,2.0,3.0],\"shape\":[3],\"type":\"invalid_type\"}")"
        "^^<https://w3id.org/rdf-tensor/datatypes#DataTensor>",
        R"("{\"data\":[1.0,2.0,3.0],\"shape":[3]}\"")"
        "^^<https://w3id.org/rdf-tensor/datatypes#DataTensor>",
        // Valid Vector Basis
        R"("{\"data\":[1.0,0.0,0.0],\"shape\":[3],\"type":\"float32\"}")"
        "^^<https://w3id.org/rdf-tensor/datatypes#DataTensor>",
        R"("{\"data\":[0.0,1.0,0.0],\"shape\":[3],\"type":\"float32\"}")"
        "^^<https://w3id.org/rdf-tensor/datatypes#DataTensor>",
        R"("{\"data\":[0.0,0.0,1.0],\"shape\":[3],\"type":\"float32\"}")"
        "^^<https://w3id.org/rdf-tensor/datatypes#DataTensor>",
    };
    std::sort(testLiterals.begin(), testLiterals.end());

    for (size_t i = 0; i < testLiterals.size(); i++) {
      auto lit = testLiterals[i];
      auto idx = (*ww)(lit, true);
      ASSERT_EQ(i, idx);
    }

    ww->finish();

    tensorDataVocab.open(fn);

    auto checkTensorVocabContents = [&testLiterals](TV& tensorVocab) {
      ASSERT_EQ(tensorVocab.size(), testLiterals.size());
      for (size_t i = 0; i < testLiterals.size(); i++) {
        ASSERT_EQ(tensorVocab[i], testLiterals[i]);
        ASSERT_EQ(tensorVocab.getUnderlyingVocabulary()[i], testLiterals[i]);
        auto tensorFromVocab = tensorVocab.getTensorData(i);
        auto tensorFromLiteral = TensorData::fromLiteral(testLiterals[i]);
        EXPECT_TENSOR_DATA(tensorFromVocab, tensorFromLiteral);
      }
    };

    checkTensorVocabContents(tensorDataVocab);

    // Test further methods
    ASSERT_EQ(tensorDataVocab.size(), testLiterals.size());
    ASSERT_EQ(tensorDataVocab.getUnderlyingVocabulary().size(),
              testLiterals.size());
    const auto& tensorVocabConstRef = tensorDataVocab;
    ASSERT_EQ(tensorVocabConstRef.getUnderlyingVocabulary().size(),
              testLiterals.size());

    tensorDataVocab.close();
  };
};

TYPED_TEST_SUITE(TensorVocabularyUnderlyingVocabTypedTest,
                 TensorVocabularyUnderlyingVocabTypes);

// _____________________________________________________________________________
TYPED_TEST(TensorVocabularyUnderlyingVocabTypedTest, TypedTest) {
  this->testTensorVocabulary();
}

// _____________________________________________________________________________
TEST(TensorVocabularyTest, VocabularyGetTensorInfoFromUnderlyingTensorVocab) {
  const VocabularyType tensorSplitVocabType{
      VocabularyType::Enum::OnDiskCompressedTensorSplit};
  const VocabularyType nonTensorVocabType{
      VocabularyType::Enum::OnDiskCompressed};

  // Generate test vocabulary
  RdfsVocabulary vocabulary;
  vocabulary.resetToType(tensorSplitVocabType);
  ASSERT_TRUE(vocabulary.isTensorDataAvailable());
  auto wordCallback = vocabulary.makeWordWriterPtr("tensorVocabTest.dat");
  auto nonTensorIdx = (*wordCallback)("<http://example.com/abc>", true);
  static constexpr std::string_view exampleTensorLit =
      R"("{\"data\":[1.0,2.0,3.0],\"shape\":[3],\"type":\"float32\"}")"
      "^^<https://w3id.org/rdf-tensor/datatypes#DataTensor>";
  auto exampleData =
      TensorData({1.0f, 2.0f, 3.0f}, {3}, TensorData::DType::FLOAT);
  auto tensorIdx = (*wordCallback)(exampleTensorLit, true);
  wordCallback->finish();

  // Load test vocabulary and try to retrieve precomputed `TensorData`
  vocabulary.readFromFile("tensorVocabTest.dat");
  ASSERT_TRUE(vocabulary.isTensorDataAvailable());
  ASSERT_FALSE(
      vocabulary.getTensorData(VocabIndex::make(nonTensorIdx)).has_value());
  auto tensor = vocabulary.getTensorData(VocabIndex::make(tensorIdx));
  ASSERT_TRUE(tensor.has_value());

  EXPECT_TENSOR_DATA(tensor, exampleData);
  // Cannot get `TensorData` from `PolymorphicVocabulary` with no underlying
  // `TensorDataVocabulary`
  RdfsVocabulary nonTensorVocab;
  nonTensorVocab.resetToType(nonTensorVocabType);
  ASSERT_FALSE(nonTensorVocab.isTensorDataAvailable());
  auto ngWordCallback =
      nonTensorVocab.makeWordWriterPtr("nonTensorVocabTest.dat");
  (*ngWordCallback)("<http://example.com/abc>", true);
  ngWordCallback->finish();
  nonTensorVocab.readFromFile("nonTensorVocabTest.dat");
  ASSERT_FALSE(nonTensorVocab.getTensorData(VocabIndex::make(0)).has_value());
}

// _____________________________________________________________________________
TEST(TensorVocabularyTest, InvalidTensorVersion) {
  const VocabularyType tensorSplitVocabType{
      VocabularyType::Enum::OnDiskCompressedTensorSplit};

  // Generate test vocabulary
  RdfsVocabulary vocabulary;
  vocabulary.resetToType(tensorSplitVocabType);
  auto wordCallback = vocabulary.makeWordWriterPtr("tensorVocabTest2.dat");
  (*wordCallback)("\"test\"@en", true);
  wordCallback->finish();

  // Overwrite the tensor data file with an invalid header
  ad_utility::File tensorDataFile{
      AnyTensorVocab::getTensorDataFilename("tensorVocabTest2.dat.tensors"),
      "w"};
  uint64_t fakeHeader = 0;
  tensorDataFile.write(&fakeHeader, 8);
  tensorDataFile.close();

  // Opening the vocabulary should throw an exception
  AD_EXPECT_THROW_WITH_MESSAGE(
      vocabulary.readFromFile("tensorVocabTest2.dat"),
      ::testing::HasSubstr(
          "The tensor vocab version of tensorVocabTest2.dat.tensors.tensors "
          "is 0"));
}

// _____________________________________________________________________________
TEST(TensorVocabularyTest, WordWriterDestructor) {
  const std::string lit =
      R"("{\"data\":[0.0,0.0,1.0],\"shape\":[3],\"type":\"float32\"}")"
      "^^<https://w3id.org/rdf-tensor/datatypes#DataTensor>";

  // Create a `TensorDataVocabulary::WordWriter` and destruct it without a call
  // to `finish()`.
  AnyTensorVocab sv1;
  auto wordWriter1 =
      sv1.makeDiskWriterPtr("TensorVocabularyWordWriterDestructor1.dat");
  (*wordWriter1)(lit, true);
  ASSERT_FALSE(wordWriter1->finishWasCalled());
  wordWriter1.reset();

  // Create a `TensorDataVocabulary::WordWriter` and destruct it after an
  // explicit call to `finish()`.
  AnyTensorVocab sv2;
  auto wordWriter2 =
      sv2.makeDiskWriterPtr("TensorVocabularyWordWriterDestructor2.dat");
  (*wordWriter2)(lit, true);
  wordWriter2->finish();
  ASSERT_TRUE(wordWriter2->finishWasCalled());
  wordWriter2.reset();
}

// _____________________________________________________________________________
// Helpers for the tests below.

// Make a tensor literal (with escaped JSON content) for the given values.
std::string makeTensorLiteral(const std::vector<float>& values,
                              std::string_view datatype = TENSOR_LITERAL) {
  return absl::StrCat(R"("{\"data\":[)", absl::StrJoin(values, ","),
                      R"(],\"shape\":[)", values.size(),
                      R"(],\"type\":\"float32\"}"^^<)", datatype, ">");
}

// Remove the files of a vocabulary with the given `suffixes` on destruction.
struct FileCleanup {
  std::string basename_;
  FileSuffixes suffixes_;
  ~FileCleanup() {
    for (const auto& suffix : suffixes_) {
      std::filesystem::remove(absl::StrCat(basename_, suffix));
    }
  }
};

// Check that `actual` equals `expected` (including the shape).
void expectSameTensor(const std::optional<TensorData>& actual,
                      const std::optional<TensorData>& expected,
                      ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto t = generateLocationTrace(l);
  ASSERT_EQ(actual.has_value(), expected.has_value());
  if (!actual.has_value()) {
    return;
  }
  EXPECT_TENSOR_DATA(actual, expected);
  EXPECT_EQ(actual->shape(), expected->shape());
}

// _____________________________________________________________________________
// Invalid tensor literals are stored (as words) but have no `TensorData`.
TEST(TensorVocabularyTest, InvalidTensorLiterals) {
  using TV = TensorDataVocabulary<VocabularyInMemory>;
  const std::string fn = "tensorVocabInvalidLiterals.dat";
  FileCleanup cleanup{fn, TV::fileSuffixes()};
  auto dt = absl::StrCat("^^<", TENSOR_LITERAL, ">");
  std::vector<std::string> invalid{
      // Not JSON at all.
      absl::StrCat("\"not a tensor\"", dt),
      // Empty content.
      absl::StrCat("\"\"", dt),
      // Missing `type`.
      absl::StrCat(R"("{\"data\":[1.0],\"shape\":[1]}")", dt),
      // Unknown dtype.
      absl::StrCat(R"("{\"data\":[1.0],\"shape\":[1],\"type\":\"xyz\"}")", dt),
      // Shape does not match the data.
      absl::StrCat(R"("{\"data\":[1.0,2.0],\"shape\":[3],\"type\":\"float32\"}")",
                   dt),
      // Empty shape.
      absl::StrCat(R"("{\"data\":[],\"shape\":[],\"type\":\"float32\"}")", dt),
      // Not a tensor literal at all.
      "\"plain\"@en",
  };
  std::vector<std::string> valid{makeTensorLiteral({1.0f, 2.0f}),
                                 makeTensorLiteral({-0.5f}),
                                 makeTensorLiteral({3.25f, 4.0f, 5.0f},
                                                   TENSOR_NUMERIC_LITERAL)};
  std::vector<std::string> words = invalid;
  ql::ranges::copy(valid, std::back_inserter(words));
  ql::ranges::sort(words);
  for (const auto& w : invalid) {
    ASSERT_FALSE(TensorData::fromLiteral(w).has_value()) << w;
  }

  TV vocab;
  {
    auto writer = vocab.makeDiskWriterPtr(fn);
    for (size_t i = 0; i < words.size(); ++i) {
      EXPECT_EQ((*writer)(words[i], false), i);
    }
    writer->finish();
  }
  vocab.open(fn);
  ASSERT_EQ(vocab.size(), words.size());
  size_t numValid = 0;
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(vocab[i], words[i]);
    bool isValid = ad_utility::contains(valid, words[i]);
    numValid += isValid;
    EXPECT_EQ(vocab.getTensorData(i).has_value(), isValid) << words[i];
    EXPECT_EQ(vocab.getSerializedTensorData(i).has_value(), isValid);
    expectSameTensor(vocab.getTensorData(i), TensorData::fromLiteral(words[i]));
  }
  EXPECT_EQ(numValid, valid.size());
  // Out of range.
  EXPECT_ANY_THROW(vocab.getTensorData(words.size()));
  vocab.close();
}

// _____________________________________________________________________________
// Many tensors of different sizes, read back after reopening, also
// concurrently from several threads.
TYPED_TEST(TensorVocabularyUnderlyingVocabTypedTest, ManyTensorsRoundTrip) {
  using TV = TensorDataVocabulary<TypeParam>;
  const std::string fn = "tensorVocabManyTensors.dat";
  FileCleanup cleanup{fn, TV::fileSuffixes()};

  std::mt19937 gen{42};
  std::uniform_int_distribution<size_t> sizeDist{1, 64};
  std::uniform_int_distribution<int> valueDist{-1000, 1000};
  std::vector<std::string> words;
  for (size_t i = 0; i < 3000; ++i) {
    std::vector<float> values(sizeDist(gen));
    for (auto& v : values) {
      v = static_cast<float>(valueDist(gen)) / 8.0f;
    }
    words.push_back(makeTensorLiteral(values));
  }
  ql::ranges::sort(words);
  words.erase(std::unique(words.begin(), words.end()), words.end());

  {
    TV vocab;
    auto writer = vocab.makeDiskWriterPtr(fn);
    for (size_t i = 0; i < words.size(); ++i) {
      ASSERT_EQ((*writer)(words[i], false), i);
    }
    writer->finish();
  }

  std::vector<TensorData> expected;
  for (const auto& w : words) {
    auto t = TensorData::fromLiteral(w);
    ASSERT_TRUE(t.has_value()) << w;
    expected.push_back(std::move(t.value()));
  }

  TV vocab;
  // Open twice to check that reopening works.
  vocab.open(fn);
  vocab.open(fn);
  ASSERT_EQ(vocab.size(), words.size());
  for (size_t i = 0; i < words.size(); ++i) {
    auto bytes = vocab.getSerializedTensorData(i);
    ASSERT_TRUE(bytes.has_value());
    auto serialized = expected[i].serialize();
    ASSERT_TRUE(ql::ranges::equal(bytes.value(), serialized));
    expectSameTensor(TV::deserializeTensorData(bytes.value()), expected[i]);
    expectSameTensor(vocab.getTensorData(i), expected[i]);
  }

  // Concurrent reads.
  std::atomic<size_t> numErrors = 0;
  std::vector<std::thread> threads;
  for (size_t t = 0; t < 4; ++t) {
    threads.emplace_back([&, t]() {
      for (size_t i = t; i < words.size(); i += 3) {
        auto tensor = vocab.getTensorData(i);
        if (!tensor.has_value() ||
            tensor->tensorData() != expected[i].tensorData()) {
          ++numErrors;
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(numErrors, 0u);

  // `lookupBatch` and `scanAll` are forwarded to the underlying vocabulary.
  std::vector<size_t> indices{0, words.size() - 1, words.size() / 2};
  auto batch = vocab.lookupBatch(indices);
  size_t j = 0;
  for (auto word : *batch) {
    EXPECT_EQ(word, words[indices[j]]);
    ++j;
  }
  EXPECT_EQ(j, indices.size());
  size_t numScanned = 0;
  for (const IndexAndWord& indexAndWord : vocab.scanAll()) {
    EXPECT_EQ(indexAndWord.index_, numScanned);
    EXPECT_EQ(indexAndWord.word_, words[numScanned]);
    ++numScanned;
  }
  EXPECT_EQ(numScanned, words.size());
  vocab.close();
}

// _____________________________________________________________________________
// An empty vocabulary can be written and opened.
TEST(TensorVocabularyTest, EmptyVocabulary) {
  using TV = TensorDataVocabulary<VocabularyInMemory>;
  const std::string fn = "tensorVocabEmpty.dat";
  FileCleanup cleanup{fn, TV::fileSuffixes()};
  TV vocab;
  vocab.makeDiskWriterPtr(fn)->finish();
  vocab.open(fn);
  EXPECT_EQ(vocab.size(), 0u);
  vocab.close();
}

// _____________________________________________________________________________
// A corrupt or inconsistent offsets file is detected when opening.
TEST(TensorVocabularyTest, InconsistentOffsetsFile) {
  using TV = TensorDataVocabulary<VocabularyInMemory>;
  const std::string fn = "tensorVocabInconsistent.dat";
  FileCleanup cleanup{fn, TV::fileSuffixes()};
  auto write = [&fn]() {
    TV vocab;
    auto writer = vocab.makeDiskWriterPtr(fn);
    (*writer)(makeTensorLiteral({1.0f}), false);
    (*writer)(makeTensorLiteral({2.0f}), false);
    writer->finish();
  };
  write();
  // A valid trailer, but the wrong number of offsets.
  ad_utility::testing::writeLegacyMmapVectorFile<uint64_t>(
      TV::getTensorDataOffsetsFilename(fn), {8, 20});
  {
    TV vocab;
    AD_EXPECT_THROW_WITH_MESSAGE(vocab.open(fn),
                                 ::testing::HasSubstr("tensor offsets file"));
  }
  // The right number of offsets, but inconsistent with the data file.
  write();
  auto offsets = ad_utility::testing::readLegacyMmapVectorFile<uint64_t>(
      TV::getTensorDataOffsetsFilename(fn));
  ASSERT_EQ(offsets.size(), 3u);
  offsets.back() += 1;
  ad_utility::testing::writeLegacyMmapVectorFile<uint64_t>(
      TV::getTensorDataOffsetsFilename(fn), offsets);
  {
    TV vocab;
    AD_EXPECT_THROW_WITH_MESSAGE(vocab.open(fn),
                                 ::testing::HasSubstr("inconsistent"));
  }
  // A truncated file without a valid trailer.
  write();
  std::filesystem::resize_file(TV::getTensorDataOffsetsFilename(fn),
                               sizeof(uint64_t));
  TV vocab;
  EXPECT_ANY_THROW(vocab.open(fn));
}

// _____________________________________________________________________________
// The on-disk format is the one of the original `TensorDataVocabulary` (before
// the removal of `MmapVector`): files written like the original code did can
// be read, and the current writer produces byte-identical files.
TYPED_TEST(TensorVocabularyUnderlyingVocabTypedTest, LegacyOnDiskFormat) {
  using TV = TensorDataVocabulary<TypeParam>;
  const std::string legacyFn = "tensorVocabLegacyFormatOld.dat";
  const std::string newFn = "tensorVocabLegacyFormatNew.dat";
  FileCleanup cleanupLegacy{legacyFn, TV::fileSuffixes()};
  FileCleanup cleanupNew{newFn, TV::fileSuffixes()};

  std::vector<std::string> words{
      makeTensorLiteral({1.0f, 2.0f, 3.0f}),
      makeTensorLiteral({-0.5f}),
      absl::StrCat("\"invalid\"^^<", TENSOR_LITERAL, ">"),
      makeTensorLiteral({4.0f, 5.0f}, TENSOR_NUMERIC_LITERAL),
      absl::StrCat("\"\"^^<", TENSOR_NUMERIC_LITERAL, ">"),
  };
  for (size_t i = 0; i < 300; ++i) {
    words.push_back(makeTensorLiteral({static_cast<float>(i), 0.25f}));
  }
  ql::ranges::sort(words);

  // Write the files exactly as the original code did: the underlying
  // vocabulary, the `.tensors` file with the version-1 header followed by the
  // serialized tensors (nothing for an invalid literal), and the offsets as a
  // legacy `MmapVector<uint64_t>` with `size() + 1` entries, the first of which
  // is the size of the header.
  std::vector<uint64_t> offsets;
  {
    TypeParam underlying;
    auto literalWriter = underlying.makeDiskWriterPtr(legacyFn);
    ad_utility::File tensorFile{TV::getTensorDataFilename(legacyFn), "w"};
    uint64_t version = 1;
    tensorFile.write(&version, sizeof(version));
    uint64_t currentOffset = sizeof(version);
    for (const auto& word : words) {
      (*literalWriter)(word, false);
      offsets.push_back(currentOffset);
      auto tensor = TensorData::fromLiteral(word);
      if (tensor.has_value()) {
        auto buffer = tensor->serialize();
        currentOffset += tensorFile.write(buffer.data(), buffer.size());
      }
    }
    offsets.push_back(currentOffset);
    tensorFile.close();
    literalWriter->finish();
    ad_utility::testing::writeLegacyMmapVectorFile(
        TV::getTensorDataOffsetsFilename(legacyFn), offsets);
  }
  EXPECT_EQ(TENSOR_VOCAB_VERSION, 1u);

  auto checkContents = [&words](const std::string& fn) {
    TV vocab;
    vocab.open(fn);
    ASSERT_EQ(vocab.size(), words.size());
    size_t numInvalid = 0;
    for (size_t i = 0; i < words.size(); ++i) {
      EXPECT_EQ(vocab[i], words[i]);
      auto expected = TensorData::fromLiteral(words[i]);
      numInvalid += !expected.has_value();
      expectSameTensor(vocab.getTensorData(i), expected);
      EXPECT_EQ(vocab.getSerializedTensorData(i).has_value(),
                expected.has_value());
    }
    EXPECT_EQ(numInvalid, 2u);
    vocab.close();
  };
  // The legacy files can be read.
  checkContents(legacyFn);

  // The current writer produces byte-identical tensor files.
  {
    TV vocab;
    auto writer = vocab.makeDiskWriterPtr(newFn);
    for (const auto& word : words) {
      (*writer)(word, false);
    }
    writer->finish();
  }
  checkContents(newFn);
  auto readBytes = [](const std::string& filename) {
    std::ifstream f{filename, std::ios::binary};
    EXPECT_TRUE(f.is_open()) << filename;
    return std::vector<char>(std::istreambuf_iterator<char>(f),
                             std::istreambuf_iterator<char>());
  };
  for (const auto& suffix : TV::fileSuffixes()) {
    EXPECT_EQ(readBytes(absl::StrCat(legacyFn, suffix)),
              readBytes(absl::StrCat(newFn, suffix)))
        << suffix;
  }
  // The offsets can also be read back by a legacy `MmapVectorView`.
  EXPECT_EQ(ad_utility::testing::readLegacyMmapVectorFile<uint64_t>(
                TV::getTensorDataOffsetsFilename(newFn)),
            offsets);
}

// _____________________________________________________________________________
TEST(TensorVocabularyTest, FileSuffixes) {
  using TV = TensorDataVocabulary<VocabularyInMemory>;
  EXPECT_THAT(TV::fileSuffixes(),
              ::testing::ElementsAre("", ".tensors", ".tensors.offsets"));
  using STV = SplitTensorVocabulary<VocabularyInMemory>;
  EXPECT_THAT(STV::fileSuffixes(),
              ::testing::ElementsAre("", ".tensors", ".tensors.tensors",
                                     ".tensors.tensors.offsets"));
  using CTV = TensorDataVocabulary<
      CompressedVocabulary<VocabularyInternalExternal>>;
  EXPECT_THAT(CTV::fileSuffixes(),
              ::testing::IsSupersetOf({".tensors", ".tensors.offsets"}));

  auto polySuffixes = PolymorphicVocabulary::fileSuffixes(
      VocabularyType{VocabularyType::Enum::OnDiskCompressedTensorSplit});
  EXPECT_THAT(polySuffixes, ::testing::IsSupersetOf(
                                {".codebooks", ".tensors.codebooks",
                                 ".tensors.tensors", ".tensors.tensors.offsets"}));
  // The suffixes are distinct.
  auto sorted = polySuffixes;
  ql::ranges::sort(sorted);
  EXPECT_EQ(std::unique(sorted.begin(), sorted.end()), sorted.end());

  // Exactly the declared files are written.
  const std::string fn = "tensorVocabFileSuffixes.dat";
  FileCleanup cleanup{fn, STV::fileSuffixes()};
  STV vocab;
  {
    auto writer = vocab.makeDiskWriterPtr(fn);
    (*writer)("<a>", false);
    (*writer)(makeTensorLiteral({1.0f}), false);
    writer->finish();
  }
  for (const auto& suffix : STV::fileSuffixes()) {
    EXPECT_TRUE(std::filesystem::exists(absl::StrCat(fn, suffix))) << suffix;
  }
}

// _____________________________________________________________________________
// The split function sends exactly the tensor literals to the
// `TensorDataVocabulary`, and the indices carry the corresponding marker.
TEST(TensorVocabularyTest, SplitVocabularyRouting) {
  using STV = SplitTensorVocabulary<VocabularyInMemory>;
  auto tensor = makeTensorLiteral({1.0f, 2.0f});
  auto numericTensor = makeTensorLiteral({3.0f}, TENSOR_NUMERIC_LITERAL);
  auto invalidTensor = absl::StrCat("\"garbage\"^^<", TENSOR_LITERAL, ">");
  EXPECT_EQ(STV::getMarkerForWord(tensor), 1);
  EXPECT_EQ(STV::getMarkerForWord(numericTensor), 1);
  EXPECT_EQ(STV::getMarkerForWord(invalidTensor), 1);
  EXPECT_EQ(STV::getMarkerForWord("<http://example.com/a>"), 0);
  EXPECT_EQ(STV::getMarkerForWord("\"abc\""), 0);
  EXPECT_EQ(STV::getMarkerForWord("\"abc\"@en"), 0);
  EXPECT_EQ(STV::getMarkerForWord(""), 0);
  EXPECT_EQ(STV::getMarkerForWord(
                "\"POINT(1 2)\"^^<http://www.opengis.net/ont/geosparql#"
                "wktLiteral>"),
            0);
  // A datatype that merely starts like the tensor datatype.
  EXPECT_EQ(STV::getMarkerForWord(absl::StrCat("\"x\"^^<", TENSOR_LITERAL,
                                               "Other>")),
            0);
  EXPECT_TRUE(STV::isTensorDataAvailable());
  EXPECT_FALSE(STV::isGeoInfoAvailable());

  const std::string fn = "tensorVocabSplitRouting.dat";
  FileCleanup cleanup{fn, STV::fileSuffixes()};
  std::vector<std::string> words{"\"abc\"", "<http://example.com/a>", tensor,
                                 numericTensor, invalidTensor};
  ql::ranges::sort(words);
  std::vector<uint64_t> indices;
  {
    STV vocab;
    auto writer = vocab.makeDiskWriterPtr(fn);
    for (const auto& w : words) {
      indices.push_back((*writer)(w, false));
    }
    writer->finish();
  }
  STV vocab;
  vocab.open(fn);
  EXPECT_EQ(vocab.size(), words.size());
  for (size_t i = 0; i < words.size(); ++i) {
    bool isTensorWord = STV::getMarkerForWord(words[i]) == 1;
    EXPECT_EQ(STV::getMarker(indices[i]), isTensorWord ? 1 : 0);
    EXPECT_EQ(vocab[indices[i]], words[i]);
    expectSameTensor(vocab.getTensorData(indices[i]),
                     isTensorWord ? TensorData::fromLiteral(words[i])
                                  : std::nullopt);
    EXPECT_FALSE(vocab.getGeoInfo(indices[i]).has_value());
  }
  EXPECT_FALSE(vocab.getGeoCellGrid().has_value());
  // Each underlying vocabulary has the right number of words.
  EXPECT_EQ(std::visit([](const auto& v) { return v.size(); },
                       vocab.getUnderlyingVocabulary(1)),
            3u);
  EXPECT_EQ(std::visit([](const auto& v) { return v.size(); },
                       vocab.getUnderlyingMainVocabulary()),
            2u);
  vocab.close();
}

// _____________________________________________________________________________
// The `PolymorphicVocabulary` with the tensor split type, written word by word
// and with the parallel (block) writer.
TEST(TensorVocabularyTest, PolymorphicVocabularyWithTensorType) {
  const VocabularyType type{VocabularyType::Enum::OnDiskCompressedTensorSplit};
  EXPECT_TRUE(type.isSupportedForIndexBuilding());
  std::vector<std::string> words{"\"abc\"", "<x>", "<y>"};
  for (size_t i = 0; i < 50; ++i) {
    words.push_back(makeTensorLiteral({static_cast<float>(i), 1.0f}));
  }
  ql::ranges::sort(words);

  auto check = [&](const std::string& fn, const std::vector<uint64_t>& ids) {
    PolymorphicVocabulary vocab;
    vocab.open(fn, type);
    EXPECT_TRUE(vocab.isTensorDataAvailable());
    EXPECT_FALSE(vocab.isGeoInfoAvailable());
    EXPECT_FALSE(vocab.getGeoCellGrid().has_value());
    vocab.setGeoCellGrid(std::nullopt);
    EXPECT_EQ(vocab.size(), words.size());
    for (size_t i = 0; i < words.size(); ++i) {
      EXPECT_EQ(vocab[ids[i]], words[i]);
      expectSameTensor(vocab.getTensorData(ids[i]),
                       TensorData::fromLiteral(words[i]));
      auto [lower, upper] = vocab.getPositionOfWord(words[i], std::less<>{});
      EXPECT_EQ(lower, ids[i]);
      EXPECT_EQ(upper, ids[i] + 1);
    }
    size_t numScanned = 0;
    for ([[maybe_unused]] const IndexAndWord& iw : vocab.scanAll()) {
      ++numScanned;
    }
    EXPECT_EQ(numScanned, words.size());
    vocab.close();
  };

  // Word by word.
  const std::string fn1 = "tensorVocabPolymorphic1.dat";
  FileCleanup cleanup1{fn1, PolymorphicVocabulary::fileSuffixes(type)};
  std::vector<uint64_t> ids1;
  {
    auto writer = PolymorphicVocabulary::makeDiskWriterPtr(fn1, type);
    for (const auto& w : words) {
      ids1.push_back((*writer)(w, false));
    }
    writer->finish();
  }
  check(fn1, ids1);

  // In blocks, via the parallel writer.
  const std::string fn2 = "tensorVocabPolymorphic2.dat";
  FileCleanup cleanup2{fn2, PolymorphicVocabulary::fileSuffixes(type)};
  std::vector<uint64_t> ids2;
  {
    PolymorphicVocabulary dummy;
    dummy.resetToType(type);
    auto writer = dummy.makeParallelWriterPtr(fn2);
    ASSERT_EQ(writer->numSubVocabularies(), 2);
    std::array<WordBlock, 2> blocks;
    for (const auto& w : words) {
      auto sub = writer->subVocabularyOf(w);
      auto& block = blocks.at(sub);
      ids2.push_back(writer->indexOf(sub, block.numWords(), w));
      block.push(w, false);
    }
    for (uint8_t sub = 0; sub < 2; ++sub) {
      std::move(writer->blockWriter(sub).prepare(std::move(blocks[sub])))();
    }
    writer->finish();
  }
  EXPECT_EQ(ids1, ids2);
  check(fn2, ids2);
}
}  // namespace
