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

using namespace qlever::binary_export;

// _____________________________________________________________________________
TEST(StringMapping, remapId) {
  // This is important so we have a working comparator.
  ad_utility::testing::getQec("<a> <b> <c> .");
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

  LocalVocabEntry testWord{
      ad_utility::triple_component::Literal::fromStringRepresentation(
          "\"abc\"")};
  LocalVocabEntry duplicateWord{
      ad_utility::triple_component::Iri::fromStringRepresentation("<b>")};
  StringMapping mapping;
  Id id1 = Id::makeFromVocabIndex(VocabIndex::make(1));
  Id id2 = Id::makeFromLocalVocabIndex(&testWord);
  Id id3 = Id::makeFromTextRecordIndex(TextRecordIndex::make(42));
  Id id4 = Id::makeFromWordVocabIndex(WordVocabIndex::make(1010));
  Id id5 = Id::makeFromLocalVocabIndex(&duplicateWord);

  // Mapped ids start counting from zero.
  binaryEq(mapping.remapId(id1), toMappedId(0));
  binaryEq(mapping.remapId(id2), toMappedId(1));
  binaryEq(mapping.remapId(id3), toMappedId(2));
  binaryEq(mapping.remapId(id4), toMappedId(3));
  binaryEq(mapping.remapId(id1), toMappedId(0));
  binaryEq(mapping.remapId(id5), toMappedId(0));

  EXPECT_THAT(mapping.stringMappingForTesting(),
              ::testing::UnorderedElementsAre(
                  ::testing::Pair(id1, 0), ::testing::Pair(id2, 1),
                  ::testing::Pair(id3, 2), ::testing::Pair(id4, 3)));

  EXPECT_EQ(mapping.size(), 4);
}

// _____________________________________________________________________________
TEST(StringMapping, flush) {
  ad_utility::testing::TestIndexConfig config;
  config.turtleInput =
      "<a> <b> \"The quick brown fox jumps over the lazy dog\" .";
  config.createTextIndex = true;
  auto* qec = ad_utility::testing::getQec(std::move(config));
  StringMapping mapping;

  LocalVocabEntry testWord{
      ad_utility::triple_component::Literal::fromStringRepresentation(
          "\"abc\"")};
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
      mapping.flush(qec->getIndex()),
      ::testing::ElementsAre("<a>", "<b>", "\"abc\"", "\"\"", "\"brown\""));
}
