// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <swalter@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "global/Constants.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/EmbeddingVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "rdfTypes/EmbeddingVector.h"

#include "../../util/FileTestHelpers.h"

namespace {

using namespace ad_utility;

// Wrap an array body (e.g. `[1, 2, 3]`) into a serialized `emb:fp32Vector`
// literal, i.e. `"[1, 2, 3]"^^<...fp32Vector>`.
std::string makeVectorLiteral(std::string_view arrayBody) {
  return absl::StrCat("\"", arrayBody, EMBEDDING_FP32_LITERAL_SUFFIX);
}

// Typed test suite: an `EmbeddingVocabulary` must behave identically regardless
// of its underlying vocabulary implementation.
using EmbeddingVocabularyUnderlyingVocabTypes =
    ::testing::Types<VocabularyInMemory,
                     CompressedVocabulary<VocabularyInternalExternal>>;

template <typename T>
class EmbeddingVocabularyTypedTest : public ::testing::Test {};

TYPED_TEST_SUITE(EmbeddingVocabularyTypedTest,
                 EmbeddingVocabularyUnderlyingVocabTypes);

// _____________________________________________________________________________
TYPED_TEST(EmbeddingVocabularyTypedTest, RoundTrip) {
  using EV = EmbeddingVocabulary<TypeParam>;
  EV vocab;
  const std::string fn = "embvocab-test-roundtrip.dat";
  auto cleanup = ad_utility::testing::deleteFilesWithPrefixOnDestruction(fn);
  auto ww = vocab.makeDiskWriterPtr(fn);
  ww->readableName() = "test";

  // Pairs of (literal, expected decoded vector). All values are exactly
  // representable in `float32` so equality holds. The vectors have different
  // dimensions on purpose, to exercise the variable-length sidecar.
  std::vector<std::pair<std::string, std::vector<float>>> testData{
      {makeVectorLiteral("[1, 2, 3]"), {1.0f, 2.0f, 3.0f}},
      {makeVectorLiteral("[0.5, -0.25]"), {0.5f, -0.25f}},
      {makeVectorLiteral("[10, 20, 30, 40]"), {10.0f, 20.0f, 30.0f, 40.0f}},
      {makeVectorLiteral("[-1.5]"), {-1.5f}},
      {makeVectorLiteral("[0, 0, 0, 0, 0]"), {0.0f, 0.0f, 0.0f, 0.0f, 0.0f}},
  };
  // The vocabulary requires its words to be inserted in sorted order.
  std::sort(testData.begin(), testData.end());

  for (size_t i = 0; i < testData.size(); i++) {
    auto idx = (*ww)(testData[i].first, true);
    ASSERT_EQ(i, idx);
  }
  ww->finish();

  vocab.open(fn);

  ASSERT_EQ(vocab.size(), testData.size());
  for (size_t i = 0; i < testData.size(); i++) {
    // The original literal string is preserved (first-class RDF term).
    ASSERT_EQ(vocab[i], testData[i].first);
    ASSERT_EQ(vocab.getUnderlyingVocabulary()[i], testData[i].first);
    // The decoded vector round-trips exactly, with the correct dimension. The
    // returned `MaybeOwnedVector` borrows a `span` from the memory map.
    auto emb = vocab.getEmbedding(i);
    ASSERT_TRUE(emb.has_value());
    EXPECT_THAT(emb.value(), ::testing::ElementsAreArray(testData[i].second));
  }

  // An out-of-range index yields no vector.
  EXPECT_FALSE(vocab.getEmbedding(testData.size()).has_value());

  // `lower_bound`/`upper_bound` forward to the underlying vocabulary.
  auto wI = vocab.lower_bound(testData.front().first, ql::ranges::less{});
  ASSERT_EQ(wI.index(), 0);
  wI = vocab.upper_bound("\"~~~", ql::ranges::less{});
  ASSERT_TRUE(wI.isEnd());

  vocab.close();
}

// A malformed vector literal is a hard build error (strict), unlike the
// `GeoVocabulary`, which stores a placeholder for invalid geometries.
TEST(EmbeddingVocabularyTest, MalformedLiteralIsBuildError) {
  using EV = EmbeddingVocabulary<VocabularyInMemory>;
  EV vocab;
  const std::string fn = "embvocab-test-malformed.dat";
  auto cleanup = ad_utility::testing::deleteFilesWithPrefixOnDestruction(fn);
  auto ww = vocab.makeDiskWriterPtr(fn);
  // Not finite.
  EXPECT_ANY_THROW((*ww)(makeVectorLiteral("[1, nan, 3]"), true));
}

}  // namespace
