
#include <gtest/gtest.h>

#include "./TensorTestHelpers.h"
#include "gmock/gmock.h"
#include "index/Vocabulary.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/TensorDataVocabulary.h"
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
  auto ngWordCallback = vocabulary.makeWordWriterPtr("nonTensorVocabTest.dat");
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

  // Overwrite the geoinfo file with an invalid header
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
          "The tensor vocab version of tensorVocabTest2.dat.tensors.tensors is 0"));
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
}  // namespace
