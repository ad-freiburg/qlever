// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "./util/GTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "backports/algorithm.h"
#include "global/Id.h"
#include "index/ExportIds.h"
#include "index/IndexImpl.h"
#include "index/LocalVocabEntry.h"
#include "index/vocabulary/AuxVocabulary.h"

namespace {

using ::testing::HasSubstr;
using ::testing::Optional;

// The words of the auxiliary vocabulary that the tests below use. In the order
// of the main vocabulary (see `makeIndexWithAuxVocab`), `"a"` is sorted before
// all of its words, `<b>` between `<a>` and `<c>`, and `<d>` between `<c>` and
// `<p>`.
//
// NOTE: An `AuxVocabulary` requires its words to be sorted with respect to
// `std::string`'s comparison, whereas the actual implementation will use the
// collation of the vocabulary of the main index (see `AuxVocabulary`). These
// words are deliberately chosen such that the two orders agree: their content
// is a single ASCII letter, for which the collation is the byte order, and `"`
// (the first byte of a literal) sorts before `<` (the first byte of an IRI) in
// both.
const std::vector<std::string> auxWords{"\"a\"", "<b>", "<d>"};

// The `Id` of the word of the auxiliary vocabulary at the given index.
Id auxId(uint64_t index) {
  return Id::makeFromAuxVocabIndex(AuxVocabIndex::make(index));
}

// An index whose main vocabulary holds `<a>`, `<c>`, `<p>`, and `<s>`, and
// whose auxiliary vocabulary holds `auxWords`.
Index makeIndexWithAuxVocab() {
  Index index = ad_utility::testing::makeTestIndex(
      gtestCurrentTestName(), "<s> <p> <a> . <s> <p> <c> .");
  index.getImpl().setAuxVocabForTesting(
      std::make_shared<AuxVocabulary>(auxWords));
  return index;
}

// ____________________________________________________________________________
TEST(AuxVocabulary, wordsAndLookup) {
  AuxVocabulary vocab{auxWords};
  EXPECT_EQ(vocab.numWords(), 3);
  EXPECT_EQ(vocab[AuxVocabIndex::make(0)], "\"a\"");
  EXPECT_EQ(vocab[AuxVocabIndex::make(1)], "<b>");
  EXPECT_EQ(vocab[AuxVocabIndex::make(2)], "<d>");
  EXPECT_EQ(vocab.getId("\"a\""), AuxVocabIndex::make(0));
  EXPECT_EQ(vocab.getId("<b>"), AuxVocabIndex::make(1));
  EXPECT_EQ(vocab.getId("<d>"), AuxVocabIndex::make(2));
  // Words that are not contained, before, between, and after the contained
  // ones.
  EXPECT_EQ(vocab.getId("\"A\""), std::nullopt);
  EXPECT_EQ(vocab.getId("<c>"), std::nullopt);
  EXPECT_EQ(vocab.getId("<e>"), std::nullopt);
  AD_EXPECT_THROW_WITH_MESSAGE(vocab[AuxVocabIndex::make(3)],
                               HasSubstr("index.get() < words_.size()"));

  // A default-constructed vocabulary is empty, which is how an index without an
  // auxiliary vocabulary behaves.
  AuxVocabulary empty{};
  EXPECT_EQ(empty.numWords(), 0);
  EXPECT_EQ(empty.getId("<b>"), std::nullopt);
}

// ____________________________________________________________________________
TEST(AuxVocabulary, wordsHaveToBeSortedAndDistinct) {
  // The words are looked up by binary search, so unsorted or duplicate words
  // are a programming error.
  AD_EXPECT_THROW_WITH_MESSAGE((AuxVocabulary{{"<d>", "<b>"}}),
                               HasSubstr("have to be sorted"));
  AD_EXPECT_THROW_WITH_MESSAGE((AuxVocabulary{{"<b>", "<b>"}}),
                               HasSubstr("have to be distinct"));
}

// ____________________________________________________________________________
TEST(AuxVocabIndex, valueIdBasics) {
  auto id = auxId(17);
  EXPECT_EQ(id.getDatatype(), Datatype::AuxVocabIndex);
  EXPECT_EQ(id.getAuxVocabIndex(), AuxVocabIndex::make(17));
  EXPECT_EQ(toString(Datatype::AuxVocabIndex), "AuxVocabIndex");
  // The words are stored in the index, so the `Id` is not self-contained and
  // has to be remapped when results of different indices are combined.
  EXPECT_FALSE(isDatatypeTrivial(Datatype::AuxVocabIndex));
  // Unlike a `LocalVocabIndex`, an `AuxVocabIndex` carries its value in its
  // bits.
  EXPECT_TRUE(id.canBeComparedBitwise());
  EXPECT_THROW(Id::makeFromAuxVocabIndex(AuxVocabIndex::make(Id::maxIndex + 1)),
               Id::IndexTooLargeException);

  auto visitor = [](const auto& value) -> std::optional<uint64_t> {
    if constexpr (std::is_same_v<std::decay_t<decltype(value)>,
                                 AuxVocabIndex>) {
      return value.get();
    } else {
      return std::nullopt;
    }
  };
  EXPECT_THAT(id.visit(visitor), Optional(17ULL));
}

// ____________________________________________________________________________
TEST(AuxVocabIndex, sortsAfterAllOtherDatatypes) {
  // One `Id` per datatype, except for `LocalVocabIndex`, which is not ordered
  // by its bits and is covered by the test below. If a datatype is added to the
  // `Datatype` enum, it has to be added here as well, which the check below
  // enforces.
  std::vector<Id> ids{Id::makeUndefined(),
                      Id::makeFromBool(true),
                      Id::makeFromInt(42),
                      Id::makeFromDouble(3.14),
                      Id::makeFromVocabIndex(VocabIndex::make(12)),
                      Id::makeFromTextRecordIndex(TextRecordIndex::make(3)),
                      Id::makeFromDate(DateYearOrDuration{Date{2013, 5, 16}}),
                      Id::makeFromGeoPoint(GeoPoint{3, 4}),
                      Id::makeFromWordVocabIndex(WordVocabIndex::make(3)),
                      Id::makeFromBlankNodeIndex(BlankNodeIndex::make(3)),
                      Id::makeFromEncodedVal(7),
                      auxId(0),
                      auxId(1)};
  for (size_t i = 0; i <= static_cast<size_t>(Datatype::MaxValue); ++i) {
    auto datatype = static_cast<Datatype>(i);
    bool isCovered = ql::ranges::any_of(
        ids, [datatype](Id id) { return id.getDatatype() == datatype; });
    ASSERT_EQ(isCovered, datatype != Datatype::LocalVocabIndex)
        << toString(datatype);
  }
  ql::ranges::sort(ids);
  // The two `Id`s of the auxiliary vocabulary are the largest ones, in
  // ascending order of their indices.
  EXPECT_EQ(ids[ids.size() - 2], auxId(0));
  EXPECT_EQ(ids[ids.size() - 1], auxId(1));
}

// ____________________________________________________________________________
// NOTE: All the comparisons in this test are in the *internal* order, that is,
// the order in which the index scans emit their `Id`s. That order deliberately
// is not the semantic (by string value) order as soon as an index has an
// auxiliary vocabulary, see the warning in `index/LocalVocabEntry.h`.
TEST(AuxVocabIndex, localVocabEntryInAuxVocabulary) {
  Index index = makeIndexWithAuxVocab();
  auto getId = ad_utility::testing::makeGetId(index);
  const auto& context = index.getImpl().getLocalVocabContext();

  // `<b>` is a word of the auxiliary vocabulary, `<e>` is contained in neither
  // vocabulary and would be sorted between `<c>` and `<p>` of the main one.
  auto entryB = LocalVocabEntry::fromIriref("<b>", context);
  auto entryE = LocalVocabEntry::fromIriref("<e>", context);
  auto idB = Id::makeFromLocalVocabIndex(&entryB);
  auto idE = Id::makeFromLocalVocabIndex(&entryE);

  // A local vocab entry that is stored in the auxiliary vocabulary is
  // positioned at the `Id` of that vocabulary, and hence compares equal to it.
  auto position = entryB.positionInVocab();
  EXPECT_EQ(Id::fromBits(position.lowerBound_.get()), auxId(1));
  EXPECT_NE(position.lowerBound_, position.upperBound_);
  EXPECT_EQ(idB, auxId(1));
  EXPECT_LT(auxId(0), idB);
  EXPECT_LT(idB, auxId(2));

  // It is greater than all `Id`s of the main vocabulary, and also greater than
  // all `Id`s of the unrelated datatypes, because those all have smaller
  // datatype bits than `Datatype::AuxVocabIndex`.
  EXPECT_LT(getId("<a>"), idB);
  EXPECT_LT(getId("<s>"), idB);
  EXPECT_LT(Id::makeFromInt(42), idB);
  EXPECT_LT(Id::makeFromBlankNodeIndex(BlankNodeIndex::make(3)), idB);

  // In particular, it is greater than a local vocab entry that is in neither
  // vocabulary, even though `<e>` is lexicographically greater than `<b>`. This
  // is required for the ordering to be consistent: `<p>` lies between the two.
  EXPECT_LT(idE, getId("<p>"));
  EXPECT_LT(getId("<p>"), idB);
  EXPECT_LT(idE, idB);
  // The two entries have to be compared in exactly the same way when they are
  // compared directly instead of via their `Id`s.
  EXPECT_LT(entryE, entryB);
  EXPECT_FALSE(entryB < entryE);

  // Two entries that both are in neither vocabulary are still compared by their
  // string values.
  auto entryF = LocalVocabEntry::fromIriref("<f>", context);
  EXPECT_LT(entryE, entryF);
  EXPECT_LT(Id::makeFromLocalVocabIndex(&entryE),
            Id::makeFromLocalVocabIndex(&entryF));
}

// ____________________________________________________________________________
TEST(AuxVocabIndex, localVocabEntryWithoutAuxVocabulary) {
  // Without an auxiliary vocabulary, the word `<b>` is simply positioned in the
  // main vocabulary, as it always was.
  Index index = ad_utility::testing::makeTestIndex(
      gtestCurrentTestName(), "<s> <p> <a> . <s> <p> <c> .");
  auto getId = ad_utility::testing::makeGetId(index);
  const auto& context = index.getImpl().getLocalVocabContext();
  EXPECT_EQ(index.getImpl().auxVocab(), nullptr);

  auto entryB = LocalVocabEntry::fromIriref("<b>", context);
  auto position = entryB.positionInVocab();
  EXPECT_EQ(position.lowerBound_, position.upperBound_);
  EXPECT_EQ(Id::fromBits(position.lowerBound_.get()), getId("<c>"));
  EXPECT_LT(Id::makeFromLocalVocabIndex(&entryB), getId("<c>"));
  EXPECT_LT(getId("<a>"), Id::makeFromLocalVocabIndex(&entryB));
}

// ____________________________________________________________________________
TEST(AuxVocabIndex, exportOfAuxVocabIds) {
  Index index = makeIndexWithAuxVocab();
  LocalVocab localVocab{};

  auto iriId = auxId(1);
  auto literalId = auxId(0);
  auto toStringRepresentation = [](const auto& literalOrIri) {
    return literalOrIri.toStringRepresentation();
  };

  EXPECT_THAT(
      ql::exportIds::idToLiteralOrIri(index.getImpl(), iriId, localVocab),
      Optional(::testing::ResultOf(toStringRepresentation, "<b>")));
  EXPECT_THAT(
      ql::exportIds::idToLiteralOrIri(index.getImpl(), literalId, localVocab),
      Optional(::testing::ResultOf(toStringRepresentation, "\"a\"")));

  // `idToLiteral` strips the angle brackets of an IRI and keeps a literal as it
  // is.
  EXPECT_THAT(ql::exportIds::idToLiteral(index.getImpl(), iriId, localVocab),
              Optional(::testing::ResultOf(toStringRepresentation, "\"b\"")));
  EXPECT_THAT(
      ql::exportIds::idToLiteral(index.getImpl(), literalId, localVocab),
      Optional(::testing::ResultOf(toStringRepresentation, "\"a\"")));
  // With `onlyReturnLiteralsWithXsdString`, the IRI is dropped, but the literal
  // is still returned.
  EXPECT_EQ(
      ql::exportIds::idToLiteral(index.getImpl(), iriId, localVocab, true),
      std::nullopt);
  EXPECT_THAT(
      ql::exportIds::idToLiteral(index.getImpl(), literalId, localVocab, true),
      Optional(::testing::ResultOf(toStringRepresentation, "\"a\"")));

  using StringAndType = std::pair<std::string, const char*>;
  EXPECT_THAT(ql::exportIds::idToStringAndType(index, iriId, localVocab),
              Optional(StringAndType{"<b>", nullptr}));
  EXPECT_THAT(ql::exportIds::idToStringAndType(index, literalId, localVocab),
              Optional(StringAndType{"\"a\"", nullptr}));
  // `returnOnlyLiterals` also has to work for the auxiliary vocabulary.
  EXPECT_EQ(
      (ql::exportIds::idToStringAndType<true, true>(index, iriId, localVocab)),
      std::nullopt);
  EXPECT_THAT((ql::exportIds::idToStringAndType<true, true>(index, literalId,
                                                            localVocab)),
              Optional(StringAndType{"a", nullptr}));
}

// ____________________________________________________________________________
TEST(AuxVocabIndex, exportWithoutAuxVocabularyThrows) {
  Index index = ad_utility::testing::makeTestIndex(gtestCurrentTestName(),
                                                   "<s> <p> <a> .");
  AD_EXPECT_THROW_WITH_MESSAGE(
      ql::exportIds::idToLiteralOrIri(index.getImpl(), auxId(0), LocalVocab{}),
      HasSubstr("the index has no auxiliary vocabulary"));
}

}  // namespace
