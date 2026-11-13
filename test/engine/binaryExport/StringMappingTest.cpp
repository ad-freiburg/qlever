// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <ranges>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "../../util/IndexTestHelpers.h"
#include "../../util/LocalVocabEntryTestHelpers.h"
#include "engine/binaryExport/StringMapping.h"
#include "global/Id.h"
#include "index/IndexImpl.h"
#include "index/vocabulary/SecondaryVocabulary.h"

using namespace qlever::binary_export;
using ad_utility::testing::localVocabEntryFromStringRepresentation;

namespace {
// An index with a text index, to resolve all datatypes that point to strings.
QueryExecutionContext* getQecWithTextIndex() {
  ad_utility::testing::TestIndexConfig config;
  config.turtleInput =
      "<a> <b> \"The quick brown fox jumps over the lazy dog\" .";
  config.createTextIndex = true;
  return ad_utility::testing::getQec(std::move(config));
}
}  // namespace

// Test that `remapId` assigns consecutive indices to the distinct IDs of a
// batch, resolves each new ID to its string, and counts the bytes.
TEST(StringMapping, remapId) {
  auto* qec = getQecWithTextIndex();
  auto toMappedId = [](size_t count) {
    return Id::fromBits(Datatype::LocalVocabIndex, count);
  };

  auto binaryEq = [](Id a, Id b,
                     ad_utility::source_location loc =
                         AD_CURRENT_SOURCE_LOC()) {
    auto g = generateLocationTrace(loc);
    EXPECT_EQ(a.getBits(), b.getBits());
  };

  // IDs of all datatypes that point to strings, where `id5` is a local vocab
  // entry with the same word as the vocabulary entry `id1`.
  const auto& index = qec->getIndex();
  LocalVocabEntry testWord = localVocabEntryFromStringRepresentation(
      "\"abc\"", index.getLocalVocabContext());
  LocalVocabEntry duplicateWord = localVocabEntryFromStringRepresentation(
      "<b>", index.getLocalVocabContext());
  StringMapping mapping{index};
  auto getLocalVocabId = ad_utility::testing::makeGetLocalVocabId();
  Id id1 = Id::makeFromVocabIndex(VocabIndex::make(2));
  Id id2 = getLocalVocabId(testWord);
  Id id3 = Id::makeFromTextRecordIndex(TextRecordIndex::make(0));
  Id id4 = Id::makeFromWordVocabIndex(WordVocabIndex::make(0));
  Id id5 = getLocalVocabId(duplicateWord);

  // The indices start at zero, a repeated ID keeps its index.
  binaryEq(mapping.remapId(id1), toMappedId(0));
  binaryEq(mapping.remapId(id2), toMappedId(1));
  binaryEq(mapping.remapId(id3), toMappedId(2));
  binaryEq(mapping.remapId(id4), toMappedId(3));
  binaryEq(mapping.remapId(id1), toMappedId(0));
  // The same word with a different ID gets a new index.
  binaryEq(mapping.remapId(id5), toMappedId(4));

  // A matcher for the entry of `id` in the string mapping.
  auto entry = [](Id id, uint64_t index) {
    return ::testing::Pair(id.getBits(), index);
  };
  EXPECT_THAT(mapping.stringMappingForTesting(),
              ::testing::UnorderedElementsAre(entry(id1, 0), entry(id2, 1),
                                              entry(id3, 2), entry(id4, 3),
                                              entry(id5, 4)));

  // The strings are `<b>`, `"abc"`, `""`, `"brown"`, `<b>` (in this order).
  EXPECT_EQ(mapping.size(), 5);
  EXPECT_EQ(mapping.numBytes(), 3 + 5 + 2 + 7 + 3);
  EXPECT_THAT(
      mapping.takeStrings(),
      ::testing::ElementsAre("<b>", "\"abc\"", "\"\"", "\"brown\"", "<b>"));
}

// Test that an ID of a secondary vocabulary is resolved to its string.
TEST(StringMapping, resolvesSecondaryVocabIds) {
  // An index with a secondary vocabulary. It is a fresh index instead of the
  // shared one of `getQec`, because `setSecondaryVocab` must not leak into
  // other tests.
  Index index = ad_utility::testing::makeTestIndex(gtestCurrentTestName(),
                                                   "<a> <b> <c> .");
  index.getImpl().setSecondaryVocab(std::make_shared<SecondaryVocabulary>(
      std::vector<std::string>{"<d>", "<e>"}));

  StringMapping mapping{index};
  Id vocabId = Id::makeFromVocabIndex(VocabIndex::make(1));
  Id secondaryVocabId =
      Id::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(1));
  mapping.remapId(vocabId);
  mapping.remapId(secondaryVocabId);

  EXPECT_THAT(mapping.takeStrings(), ::testing::ElementsAre("<b>", "<e>"));
}

// Test that `takeStrings` returns the strings of a batch in the order of their
// indices and starts a new batch.
TEST(StringMapping, takeStrings) {
  auto* qec = getQecWithTextIndex();
  const auto& index = qec->getIndex();
  StringMapping mapping{index};

  // IDs of all datatypes that point to strings.
  LocalVocabEntry testWord = localVocabEntryFromStringRepresentation(
      "\"abc\"", index.getLocalVocabContext());
  auto getLocalVocabId = ad_utility::testing::makeGetLocalVocabId();
  Id id0 = Id::makeFromVocabIndex(VocabIndex::make(1));
  Id id1 = Id::makeFromVocabIndex(VocabIndex::make(2));
  Id id2 = getLocalVocabId(testWord);
  Id id3 = Id::makeFromTextRecordIndex(TextRecordIndex::make(0));
  Id id4 = Id::makeFromWordVocabIndex(WordVocabIndex::make(0));

  // Every remapped ID has the `LocalVocabIndex` datatype, and repeated IDs
  // (`id0` twice) do not add a string.
  EXPECT_EQ(mapping.remapId(id0).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(mapping.remapId(id1).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(mapping.remapId(id0).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(mapping.remapId(id2).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(mapping.remapId(id3).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(mapping.remapId(id4).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(mapping.remapId(id0).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_THAT(
      mapping.takeStrings(),
      ::testing::ElementsAre("<a>", "<b>", "\"abc\"", "\"\"", "\"brown\""));

  // The next batch is empty and its indices start at zero again.
  EXPECT_EQ(mapping.size(), 0);
  EXPECT_EQ(mapping.numBytes(), 0);
  EXPECT_EQ(mapping.remapId(id1).getBits(),
            Id::makeFromLocalVocabIndex(nullptr).getBits());
  EXPECT_THAT(mapping.takeStrings(), ::testing::ElementsAre("<b>"));
}

// Test that `getTransmittedIndex` is the inverse of `makeTransmittedId`.
TEST(StringMapping, transmittedIds) {
  for (size_t index : {0ULL, 1ULL, 42ULL, 1ULL << 40}) {
    Id id = StringMapping::makeTransmittedId(index);
    EXPECT_EQ(id.getDatatype(), Datatype::LocalVocabIndex);
    EXPECT_EQ(StringMapping::getTransmittedIndex(id), index);
  }
}

// Test that the `TransmittedStringRewriter` resolves the IDs of a
// `StringMapping` to the local IDs of their strings.
TEST(TransmittedStringRewriter, roundTrip) {
  auto* qec = getQecWithTextIndex();
  const auto& index = qec->getIndex();
  StringMapping mapping{index};

  // A word of the vocabulary of the index, and a word that is not contained
  // in it.
  LocalVocabEntry testWord = localVocabEntryFromStringRepresentation(
      "\"abc\"", index.getLocalVocabContext());
  auto getLocalVocabId = ad_utility::testing::makeGetLocalVocabId();
  Id vocabId = Id::makeFromVocabIndex(VocabIndex::make(1));
  Id transmittedVocabId = mapping.remapId(vocabId);
  Id transmittedTestWord = mapping.remapId(getLocalVocabId(testWord));

  LocalVocab localVocab;
  TransmittedStringRewriter rewriter{mapping.takeStrings(), index, localVocab};
  // The word of the vocabulary becomes the same `VocabIndex` again (bitwise,
  // not only semantically), the other word is added to the `localVocab`.
  EXPECT_EQ(rewriter(transmittedVocabId).getBits(), vocabId.getBits());
  Id localTestWord = rewriter(transmittedTestWord);
  ASSERT_EQ(localTestWord.getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(localTestWord.getLocalVocabIndex()->toStringRepresentation(),
            "\"abc\"");
  EXPECT_EQ(localVocab.size(), 1);

  // A string that is not part of the batch.
  AD_EXPECT_THROW_WITH_MESSAGE(rewriter(StringMapping::makeTransmittedId(2)),
                               ::testing::HasSubstr("not transmitted"));
}

// Test that a transmitted IRI that can be encoded locally becomes an encoded
// IRI.
TEST(TransmittedStringRewriter, encodesIris) {
  ad_utility::testing::TestIndexConfig config;
  config.encodedPrefixesWithoutAngleBrackets = {"http://example.org/"};
  const auto& index = ad_utility::testing::getQec(config)->getIndex();

  LocalVocab localVocab;
  TransmittedStringRewriter rewriter{
      {"<http://example.org/42>"}, index, localVocab};
  EXPECT_EQ(rewriter(StringMapping::makeTransmittedId(0)),
            index.encodedIriManager().encode("<http://example.org/42>"));
  EXPECT_TRUE(localVocab.empty());
}

// Test that a transmitted string that is not the representation of an IRI or
// a literal is rejected, instead of failing an internal assertion.
TEST(TransmittedStringRewriter, malformedStrings) {
  const auto& index = ad_utility::testing::getQec()->getIndex();
  for (std::string string : {"", "x", "<x", "\"", "\"abc", "abc\"", "@en"}) {
    LocalVocab localVocab;
    AD_EXPECT_THROW_WITH_MESSAGE(
        (TransmittedStringRewriter{std::vector<std::string>{string}, index,
                                   localVocab}),
        ::testing::HasSubstr("malformed string"));
  }
}
