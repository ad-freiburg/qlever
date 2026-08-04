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
#include "parser/LiteralOrIri.h"

namespace {

// The functions that export an `Id`, and the type aliases for `LiteralOrIri`
// and `Literal` that come with them.
using namespace ql::exportIds;

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
  ad_utility::testing::TestIndexConfig config{"<s> <p> <a> . <s> <p> <c> ."};
  config.auxVocabWords = auxWords;
  return ad_utility::testing::makeTestIndex(gtestCurrentTestName(),
                                            std::move(config));
}

// A fixture for the tests that need an index with an auxiliary vocabulary, see
// `makeIndexWithAuxVocab`.
class AuxVocabIndexTest : public ::testing::Test {
 protected:
  Index index_ = makeIndexWithAuxVocab();
  std::function<Id(const std::string&)> getId_ =
      ad_utility::testing::makeGetId(index_);
  const LocalVocabContext& context_ = index_.getImpl().getLocalVocabContext();

  // A `LocalVocabEntry` for the given IRI, in the context of this index.
  LocalVocabEntry entry(std::string_view iriref) const {
    return LocalVocabEntry::fromIriref(iriref, context_);
  }

  // The `Id` that refers to the given entry. NOTE: The entry has to outlive the
  // returned `Id`, which only stores a pointer to it.
  static Id idOf(const LocalVocabEntry& entry) {
    return Id::makeFromLocalVocabIndex(&entry);
  }
};

// _____________________________________________________________________________
TEST(AuxVocabulary, wordsAndLookup) {
  AuxVocabulary vocab{auxWords};
  EXPECT_EQ(vocab.numWords(), auxWords.size());
  // Each word is stored at its index and is found again by that index.
  for (size_t i = 0; i < auxWords.size(); ++i) {
    SCOPED_TRACE(i);
    EXPECT_EQ(vocab[AuxVocabIndex::make(i)], auxWords.at(i));
    EXPECT_EQ(vocab.getId(auxWords.at(i)), AuxVocabIndex::make(i));
  }
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

// _____________________________________________________________________________
TEST(AuxVocabulary, wordsHaveToBeSortedAndDistinct) {
  // The words are looked up by binary search, so unsorted or duplicate words
  // are a programming error.
  AD_EXPECT_THROW_WITH_MESSAGE((AuxVocabulary{{"<d>", "<b>"}}),
                               HasSubstr("have to be sorted"));
  AD_EXPECT_THROW_WITH_MESSAGE((AuxVocabulary{{"<b>", "<b>"}}),
                               HasSubstr("have to be distinct"));
}

// _____________________________________________________________________________
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

// _____________________________________________________________________________
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
    bool isCovered = ::ranges::contains(ids, datatype, &Id::getDatatype);
    ASSERT_EQ(isCovered, datatype != Datatype::LocalVocabIndex)
        << toString(datatype);
  }
  ql::ranges::sort(ids);
  // The two `Id`s of the auxiliary vocabulary are the largest ones, in
  // ascending order of their indices.
  EXPECT_EQ(ids[ids.size() - 2], auxId(0));
  EXPECT_EQ(ids[ids.size() - 1], auxId(1));
}

// _____________________________________________________________________________
// NOTE: All the comparisons in this test are in the *internal* order, that is,
// the order in which the index scans emit their `Id`s. That order deliberately
// is not the semantic (by string value) order as soon as an index has an
// auxiliary vocabulary, see the warning in `index/LocalVocabEntry.h`.
TEST_F(AuxVocabIndexTest, localVocabEntryInAuxVocabulary) {
  EXPECT_TRUE(context_.hasAuxVocabulary());

  // `<b>` is a word of the auxiliary vocabulary, `<e>` is contained in neither
  // vocabulary and would be sorted between `<c>` and `<p>` of the main one.
  auto entryB = entry("<b>");
  auto entryE = entry("<e>");
  auto idB = idOf(entryB);
  auto idE = idOf(entryE);

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
  EXPECT_LT(getId_("<a>"), idB);
  EXPECT_LT(getId_("<s>"), idB);
  EXPECT_LT(Id::makeFromInt(42), idB);
  EXPECT_LT(Id::makeFromBlankNodeIndex(BlankNodeIndex::make(3)), idB);

  // In particular, it is greater than a local vocab entry that is in neither
  // vocabulary, even though `<e>` is lexicographically greater than `<b>`. This
  // is required for the ordering to be consistent: `<p>` lies between the two.
  EXPECT_LT(idE, getId_("<p>"));
  EXPECT_LT(getId_("<p>"), idB);
  EXPECT_LT(idE, idB);
  // The two entries have to be compared in exactly the same way when they are
  // compared directly instead of via their `Id`s.
  EXPECT_LT(entryE, entryB);
  EXPECT_FALSE(entryB < entryE);

  // Two entries that both are in neither vocabulary are still compared by their
  // string values.
  auto entryF = entry("<f>");
  EXPECT_LT(entryE, entryF);
  EXPECT_LT(idOf(entryE), idOf(entryF));
}

// _____________________________________________________________________________
// Unlike the ordering above, the equality of two `LocalVocabEntry`s is (and
// always was) the equality of their string representations, no matter which
// vocabulary the words are stored in, see `index/LocalVocabEntry.h`.
TEST_F(AuxVocabIndexTest, localVocabEntryEqualityIgnoresPositionInVocab) {
  // Check `==` and `!=` in both directions.
  auto expectEqual = [](const auto& a, const auto& b, bool equal) {
    EXPECT_EQ(a == b, equal);
    EXPECT_EQ(a != b, !equal);
    EXPECT_EQ(b == a, equal);
    EXPECT_EQ(b != a, !equal);
  };

  // `<b>` is a word of the auxiliary vocabulary, `<e>` is contained in neither
  // vocabulary, and `<a>` is a word of the main vocabulary.
  auto entryB = entry("<b>");
  auto entryE = entry("<e>");
  auto entryA = entry("<a>");

  expectEqual(entryB, entry("<b>"), true);
  expectEqual(entryB, entryE, false);
  expectEqual(entryB, entryA, false);

  // The same holds for the comparison against a plain `LiteralOrIri`, which the
  // entries inherit from.
  auto plainB = LiteralOrIri::fromStringRepresentation(
      std::string{entryB.toStringRepresentation()});
  expectEqual(entryB, plainB, true);
  expectEqual(entryE, plainB, false);
}

// _____________________________________________________________________________
TEST(AuxVocabIndex, localVocabEntryWithoutAuxVocabulary) {
  // Without an auxiliary vocabulary, the word `<b>` is simply positioned in the
  // main vocabulary, as it always was.
  Index index = ad_utility::testing::makeTestIndex(
      gtestCurrentTestName(), "<s> <p> <a> . <s> <p> <c> .");
  auto getId = ad_utility::testing::makeGetId(index);
  const auto& context = index.getImpl().getLocalVocabContext();
  EXPECT_EQ(index.getImpl().auxVocab(), nullptr);
  EXPECT_FALSE(context.hasAuxVocabulary());
  auto entry = [&context](std::string_view iriref) {
    return LocalVocabEntry::fromIriref(iriref, context);
  };

  auto entryB = entry("<b>");
  auto position = entryB.positionInVocab();
  EXPECT_EQ(position.lowerBound_, position.upperBound_);
  EXPECT_EQ(Id::fromBits(position.lowerBound_.get()), getId("<c>"));
  EXPECT_LT(Id::makeFromLocalVocabIndex(&entryB), getId("<c>"));
  EXPECT_LT(getId("<a>"), Id::makeFromLocalVocabIndex(&entryB));

  // Without an auxiliary vocabulary, two entries are compared by their string
  // values alone, without looking up their positions (see the documentation of
  // `LocalVocabEntry::compareThreeWay`). The result is the same as it would be
  // with the positions, no matter whether the words are contained in the
  // vocabulary of the index (`<a>` and `<c>`) or not (`<b>` and `<d>`).
  std::vector<LocalVocabEntry> entries;
  for (std::string_view word : {"<d>", "<b>", "<c>", "<a>"}) {
    entries.push_back(entry(word));
  }
  ql::ranges::sort(entries);
  EXPECT_THAT(entries, ::testing::ElementsAre(entry("<a>"), entry("<b>"),
                                              entry("<c>"), entry("<d>")));
}

// _____________________________________________________________________________
TEST_F(AuxVocabIndexTest, exportOfAuxVocabIds) {
  const IndexImpl& impl = index_.getImpl();
  LocalVocab localVocab{};

  auto iriId = auxId(1);
  auto literalId = auxId(0);
  // Matchers for the return values of the export functions below, which are an
  // optional `LiteralOrIri` resp. `Literal` with the given string
  // representation.
  auto isLiteralOrIri = [](std::string_view expected) {
    return Optional(
        AD_PROPERTY(LiteralOrIri, toStringRepresentation, expected));
  };
  auto isLiteral = [](std::string_view expected) {
    return Optional(AD_PROPERTY(Literal, toStringRepresentation, expected));
  };

  EXPECT_THAT(idToLiteralOrIri(impl, iriId, localVocab), isLiteralOrIri("<b>"));
  EXPECT_THAT(idToLiteralOrIri(impl, literalId, localVocab),
              isLiteralOrIri("\"a\""));

  // `idToLiteral` strips the angle brackets of an IRI and keeps a literal as it
  // is.
  EXPECT_THAT(idToLiteral(impl, iriId, localVocab), isLiteral("\"b\""));
  EXPECT_THAT(idToLiteral(impl, literalId, localVocab), isLiteral("\"a\""));
  // With `onlyReturnLiteralsWithXsdString`, the IRI is dropped, but the literal
  // is still returned.
  EXPECT_EQ(idToLiteral(impl, iriId, localVocab, true), std::nullopt);
  EXPECT_THAT(idToLiteral(impl, literalId, localVocab, true),
              isLiteral("\"a\""));

  using StringAndType = std::pair<std::string, const char*>;
  EXPECT_THAT(idToStringAndType(index_, iriId, localVocab),
              Optional(StringAndType{"<b>", nullptr}));
  EXPECT_THAT(idToStringAndType(index_, literalId, localVocab),
              Optional(StringAndType{"\"a\"", nullptr}));
  // `returnOnlyLiterals` also has to work for the auxiliary vocabulary.
  EXPECT_EQ((idToStringAndType<true, true>(index_, iriId, localVocab)),
            std::nullopt);
  EXPECT_THAT((idToStringAndType<true, true>(index_, literalId, localVocab)),
              Optional(StringAndType{"a", nullptr}));
}

// _____________________________________________________________________________
TEST(AuxVocabIndex, exportWithoutAuxVocabularyThrows) {
  Index index = ad_utility::testing::makeTestIndex(gtestCurrentTestName(),
                                                   "<s> <p> <a> .");
  AD_EXPECT_THROW_WITH_MESSAGE(
      idToLiteralOrIri(index.getImpl(), auxId(0), LocalVocab{}),
      HasSubstr("the index has no auxiliary vocabulary"));
}

}  // namespace
