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

// The words of the auxiliary vocabulary that the tests below use. `<b>` is
// sorted between the words `<a>` and `<c>` of the main vocabulary (see
// `makeIndexWithAuxVocab`), `"lit"` after all of them.
const std::vector<std::string> auxWords{"<b>", "\"lit\""};

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
  EXPECT_EQ(vocab.numWords(), 2);
  EXPECT_EQ(vocab[AuxVocabIndex::make(0)], "<b>");
  EXPECT_EQ(vocab[AuxVocabIndex::make(1)], "\"lit\"");
  EXPECT_EQ(vocab.getId("<b>"), AuxVocabIndex::make(0));
  EXPECT_EQ(vocab.getId("\"lit\""), AuxVocabIndex::make(1));
  EXPECT_EQ(vocab.getId("<a>"), std::nullopt);
  AD_EXPECT_THROW_WITH_MESSAGE(vocab[AuxVocabIndex::make(2)],
                               HasSubstr("index.get() < words_.size()"));

  // A default-constructed vocabulary is empty, which is how an index without an
  // auxiliary vocabulary behaves.
  AuxVocabulary empty{};
  EXPECT_EQ(empty.numWords(), 0);
  EXPECT_EQ(empty.getId("<b>"), std::nullopt);
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
  // One `Id` per datatype, except for `LocalVocabIndex` (which is not ordered
  // by its bits and is covered by the test below).
  std::vector<Id> ids{Id::makeUndefined(),
                      Id::makeFromBool(true),
                      Id::makeFromInt(42),
                      Id::makeFromDouble(3.14),
                      Id::makeFromVocabIndex(VocabIndex::make(12)),
                      Id::makeFromTextRecordIndex(TextRecordIndex::make(3)),
                      Id::makeFromWordVocabIndex(WordVocabIndex::make(3)),
                      Id::makeFromBlankNodeIndex(BlankNodeIndex::make(3)),
                      auxId(0),
                      auxId(1)};
  ql::ranges::sort(ids);
  // The two `Id`s of the auxiliary vocabulary are the largest ones, in
  // ascending order of their indices.
  EXPECT_EQ(ids[ids.size() - 2], auxId(0));
  EXPECT_EQ(ids[ids.size() - 1], auxId(1));
}

// ____________________________________________________________________________
TEST(AuxVocabIndex, localVocabEntryInAuxVocabulary) {
  Index index = makeIndexWithAuxVocab();
  auto getId = ad_utility::testing::makeGetId(index);
  const auto& context = index.getImpl().getLocalVocabContext();

  // `<b>` is a word of the auxiliary vocabulary, `<e>` is contained in neither
  // vocabulary and would be sorted between `<c>` and `<p>`.
  auto entryB = LocalVocabEntry::fromIriref("<b>", context);
  auto entryE = LocalVocabEntry::fromIriref("<e>", context);
  auto idB = Id::makeFromLocalVocabIndex(&entryB);
  auto idE = Id::makeFromLocalVocabIndex(&entryE);

  // A local vocab entry that is stored in the auxiliary vocabulary is
  // positioned at the `Id` of that vocabulary, and hence compares equal to it.
  auto position = entryB.positionInVocab();
  EXPECT_EQ(Id::fromBits(position.lowerBound_.get()), auxId(0));
  EXPECT_NE(position.lowerBound_, position.upperBound_);
  EXPECT_EQ(idB, auxId(0));
  EXPECT_LT(idB, auxId(1));

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
TEST(AuxVocabIndex, export) {
  Index index = makeIndexWithAuxVocab();
  LocalVocab localVocab{};

  auto iriId = auxId(0);
  auto literalId = auxId(1);
  auto toStringRepresentation = [](const auto& literalOrIri) {
    return literalOrIri.toStringRepresentation();
  };

  EXPECT_THAT(
      ql::exportIds::idToLiteralOrIri(index.getImpl(), iriId, localVocab),
      Optional(::testing::ResultOf(toStringRepresentation, "<b>")));
  EXPECT_THAT(
      ql::exportIds::idToLiteralOrIri(index.getImpl(), literalId, localVocab),
      Optional(::testing::ResultOf(toStringRepresentation, "\"lit\"")));

  // `idToLiteral` strips the angle brackets of an IRI and keeps a literal as it
  // is.
  EXPECT_THAT(ql::exportIds::idToLiteral(index.getImpl(), iriId, localVocab),
              Optional(::testing::ResultOf(toStringRepresentation, "\"b\"")));
  EXPECT_THAT(
      ql::exportIds::idToLiteral(index.getImpl(), literalId, localVocab),
      Optional(::testing::ResultOf(toStringRepresentation, "\"lit\"")));
  // With `onlyReturnLiteralsWithXsdString`, the IRI is dropped, but the literal
  // is still returned.
  EXPECT_EQ(
      ql::exportIds::idToLiteral(index.getImpl(), iriId, localVocab, true),
      std::nullopt);
  EXPECT_THAT(
      ql::exportIds::idToLiteral(index.getImpl(), literalId, localVocab, true),
      Optional(::testing::ResultOf(toStringRepresentation, "\"lit\"")));

  using StringAndType = std::pair<std::string, const char*>;
  EXPECT_THAT(ql::exportIds::idToStringAndType(index, iriId, localVocab),
              Optional(StringAndType{"<b>", nullptr}));
  EXPECT_THAT(ql::exportIds::idToStringAndType(index, literalId, localVocab),
              Optional(StringAndType{"\"lit\"", nullptr}));
  // `returnOnlyLiterals` also has to work for the auxiliary vocabulary.
  EXPECT_EQ(
      (ql::exportIds::idToStringAndType<true, true>(index, iriId, localVocab)),
      std::nullopt);
  EXPECT_THAT((ql::exportIds::idToStringAndType<true, true>(index, literalId,
                                                            localVocab)),
              Optional(StringAndType{"lit", nullptr}));
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
