// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <ranges>
#include <vector>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/StringMapping.h"
#include "global/Id.h"
#include "index/IndexImpl.h"
#include "index/vocabulary/SecondaryVocabulary.h"

using namespace qlever::binary_export;

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

// _____________________________________________________________________________
TEST(StringMapping, remapId) {
  auto* qec = getQecWithTextIndex();
  auto toMappedId = [](size_t count) {
    return Id::makeFromLocalVocabIndex(
        reinterpret_cast<LocalVocabIndex>(count << ValueId::numDatatypeBits));
  };

  auto binaryEq = [](Id a, Id b,
                     ad_utility::source_location loc =
                         AD_CURRENT_SOURCE_LOC()) {
    auto g = generateLocationTrace(loc);
    EXPECT_EQ(a.getBits(), b.getBits());
  };

  const auto& index = qec->getIndex();
  LocalVocabEntry testWord = LocalVocabEntry::fromStringRepresentation(
      "\"abc\"", index.getLocalVocabContext());
  LocalVocabEntry duplicateWord = LocalVocabEntry::fromStringRepresentation(
      "<b>", index.getLocalVocabContext());
  StringMapping mapping{index};
  Id id1 = Id::makeFromVocabIndex(VocabIndex::make(2));
  Id id2 = Id::makeFromLocalVocabIndex(&testWord);
  Id id3 = Id::makeFromTextRecordIndex(TextRecordIndex::make(0));
  Id id4 = Id::makeFromWordVocabIndex(WordVocabIndex::make(0));
  Id id5 = Id::makeFromLocalVocabIndex(&duplicateWord);

  // Mapped ids start counting from zero.
  binaryEq(mapping.remapId(id1), toMappedId(0));
  binaryEq(mapping.remapId(id2), toMappedId(1));
  binaryEq(mapping.remapId(id3), toMappedId(2));
  binaryEq(mapping.remapId(id4), toMappedId(3));
  binaryEq(mapping.remapId(id1), toMappedId(0));
  // The same word with a different ID gets a new index.
  binaryEq(mapping.remapId(id5), toMappedId(4));

  EXPECT_THAT(
      mapping.stringMappingForTesting(),
      ::testing::UnorderedElementsAre(
          ::testing::Pair(id1.getBits(), 0), ::testing::Pair(id2.getBits(), 1),
          ::testing::Pair(id3.getBits(), 2), ::testing::Pair(id4.getBits(), 3),
          ::testing::Pair(id5.getBits(), 4)));

  EXPECT_EQ(mapping.size(), 5);
  // `<b>`, `"abc"`, `""`, `"brown"`, `<b>`.
  EXPECT_EQ(mapping.numBytes(), 3 + 5 + 2 + 7 + 3);
  EXPECT_THAT(
      mapping.takeStrings(),
      ::testing::ElementsAre("<b>", "\"abc\"", "\"\"", "\"brown\"", "<b>"));
}

// _____________________________________________________________________________
TEST(StringMapping, resolvesSecondaryVocabIds) {
  // `remapId` resolves the new `Id`s via `ql::exportIds::idToLiteralOrIri`,
  // so an `Id` of a secondary vocabulary requires the index to actually have
  // such a vocabulary. Use a fresh index instead of the shared one of `getQec`,
  // because `setSecondaryVocab` must not leak into other tests.
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

// _____________________________________________________________________________
TEST(StringMapping, takeStrings) {
  auto* qec = getQecWithTextIndex();
  const auto& index = qec->getIndex();
  StringMapping mapping{index};
  LocalVocabEntry testWord = LocalVocabEntry::fromStringRepresentation(
      "\"abc\"", index.getLocalVocabContext());
  Id id0 = Id::makeFromVocabIndex(VocabIndex::make(1));
  Id id1 = Id::makeFromVocabIndex(VocabIndex::make(2));
  Id id2 = Id::makeFromLocalVocabIndex(&testWord);
  Id id3 = Id::makeFromTextRecordIndex(TextRecordIndex::make(0));
  Id id4 = Id::makeFromWordVocabIndex(WordVocabIndex::make(0));

  EXPECT_EQ(mapping.remapId(id0).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(mapping.remapId(id1).getDatatype(), Datatype::LocalVocabIndex);
  // Ensure repetitions don't mess stuff up.
  EXPECT_EQ(mapping.remapId(id0).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(mapping.remapId(id2).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(mapping.remapId(id3).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(mapping.remapId(id4).getDatatype(), Datatype::LocalVocabIndex);
  // Another repetition.
  EXPECT_EQ(mapping.remapId(id0).getDatatype(), Datatype::LocalVocabIndex);

  EXPECT_THAT(
      mapping.takeStrings(),
      ::testing::ElementsAre("<a>", "<b>", "\"abc\"", "\"\"", "\"brown\""));

  // A new batch.
  EXPECT_EQ(mapping.size(), 0);
  EXPECT_EQ(mapping.numBytes(), 0);
  EXPECT_EQ(mapping.remapId(id1).getBits(),
            Id::makeFromLocalVocabIndex(nullptr).getBits());
  EXPECT_THAT(mapping.takeStrings(), ::testing::ElementsAre("<b>"));
}
