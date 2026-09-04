// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "./util/GTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "backports/algorithm.h"
#include "engine/ExecuteUpdate.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/MaterializedViews.h"
#include "engine/NamedResultCache.h"
#include "engine/QueryPlanner.h"
#include "global/Id.h"
#include "index/ExportIds.h"
#include "index/IndexImpl.h"
#include "index/LocalVocab.h"
#include "index/LocalVocabEntry.h"
#include "index/TripleComponentConversions.h"
#include "index/vocabulary/SecondaryVocabulary.h"
#include "parser/LiteralOrIri.h"
#include "parser/SparqlParser.h"
#include "parser/TripleComponent.h"

namespace {

using namespace std::string_literals;

// The functions that export an `Id`, and the type aliases for `LiteralOrIri`
// and `Literal` that come with them.
using namespace ql::exportIds;

// NOTE: This alias has to be spelled out, because the SPARQL parser that the
// end-to-end tests below use pulls in an unrelated global `Literal` class (see
// `parser/data/Literal.h`), which would make the name ambiguous.
using Literal = ql::exportIds::Literal;

using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::Optional;
using ::testing::UnorderedElementsAre;

// The words of the secondary vocabulary that the tests below use. In the order
// of the main vocabulary (see `makeIndexWithSecondaryVocab`), `"a"` is sorted
// before all of its words, `<b>` between `<a>` and `<c>`, and `<d>` between
// `<c>` and `<p>`.
//
// NOTE: A `SecondaryVocabulary` requires its words to be sorted with respect
// to `std::string`'s comparison, whereas the actual implementation will use the
// collation of the vocabulary of the main index (see `SecondaryVocabulary`).
// These words are deliberately chosen such that the two orders agree: their
// content is a single ASCII letter, for which the collation is the byte order,
// and `"` (the first byte of a literal) sorts before `<` (the first byte of an
// IRI) in both.
const std::vector<std::string> secondaryVocabWords{"\"a\"", "<b>", "<d>"};

// The `Id` of the word of the secondary vocabulary at the given index.
Id secondaryVocabId(uint64_t index) {
  return Id::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(index));
}

// An index whose main vocabulary holds `<a>`, `<c>`, `<p>`, and `<s>`, and
// whose secondary vocabulary holds `secondaryVocabWords`.
Index makeIndexWithSecondaryVocab(const std::string& indexBasename) {
  ad_utility::testing::TestIndexConfig config{"<s> <p> <a> . <s> <p> <c> ."};
  config.secondaryVocabWords = secondaryVocabWords;
  return ad_utility::testing::makeTestIndex(indexBasename, std::move(config));
}

// A fixture for the tests that need an index with a secondary vocabulary, see
// `makeIndexWithSecondaryVocab`.
class SecondaryVocabIndexTest : public ::testing::Test {
 protected:
  Index index_ = makeIndexWithSecondaryVocab(gtestCurrentTestName());
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
TEST(SecondaryVocabulary, wordsAndLookup) {
  SecondaryVocabulary vocab{secondaryVocabWords};
  EXPECT_EQ(vocab.numWords(), secondaryVocabWords.size());
  // Each word is stored at its index and is found again by that index.
  for (size_t i = 0; i < secondaryVocabWords.size(); ++i) {
    SCOPED_TRACE(i);
    EXPECT_EQ(vocab[SecondaryVocabIndex::make(i)], secondaryVocabWords.at(i));
    EXPECT_EQ(vocab.getId(secondaryVocabWords.at(i)),
              SecondaryVocabIndex::make(i));
  }
  // Words that are not contained, before, between, and after the contained
  // ones.
  EXPECT_EQ(vocab.getId("\"A\""), std::nullopt);
  EXPECT_EQ(vocab.getId("<c>"), std::nullopt);
  EXPECT_EQ(vocab.getId("<e>"), std::nullopt);
  AD_EXPECT_THROW_WITH_MESSAGE(vocab[SecondaryVocabIndex::make(3)],
                               HasSubstr("index.get() < words_.size()"));

  // A default-constructed vocabulary is empty, which is how an index without a
  // secondary vocabulary behaves.
  SecondaryVocabulary empty{};
  EXPECT_EQ(empty.numWords(), 0);
  EXPECT_EQ(empty.getId("<b>"), std::nullopt);
}

// _____________________________________________________________________________
TEST(SecondaryVocabulary, wordsHaveToBeSortedAndDistinct) {
  // The words are looked up by binary search, so unsorted or duplicate words
  // are a programming error.
  AD_EXPECT_THROW_WITH_MESSAGE((SecondaryVocabulary{{"<d>", "<b>"}}),
                               HasSubstr("have to be sorted"));
  AD_EXPECT_THROW_WITH_MESSAGE((SecondaryVocabulary{{"<b>", "<b>"}}),
                               HasSubstr("have to be distinct"));
}

// _____________________________________________________________________________
// The semantic comparison of the words needs the comparator of the vocabulary
// of the main index, which a freshly constructed vocabulary does not have yet
// (`IndexImpl::setSecondaryVocabForTesting` attaches it, see the test below).
TEST(SecondaryVocabulary, semanticComparisonNeedsTheMainVocabComparator) {
  SecondaryVocabulary vocab{secondaryVocabWords};
  auto message =
      HasSubstr("comparator of the vocabulary of the main index has been set");
  AD_EXPECT_THROW_WITH_MESSAGE(
      vocab.compareWordTo(SecondaryVocabIndex::make(0), "<a>"), message);
  AD_EXPECT_THROW_WITH_MESSAGE(vocab.compareWords(SecondaryVocabIndex::make(0),
                                                  SecondaryVocabIndex::make(1)),
                               message);
}

// _____________________________________________________________________________
TEST(SecondaryVocabIndex, valueIdBasics) {
  auto id = secondaryVocabId(17);
  EXPECT_EQ(id.getDatatype(), Datatype::SecondaryVocabIndex);
  EXPECT_EQ(id.getSecondaryVocabIndex(), SecondaryVocabIndex::make(17));
  EXPECT_EQ(toString(Datatype::SecondaryVocabIndex), "SecondaryVocabIndex");
  // The words are stored in the index, so the `Id` is not self-contained and
  // has to be remapped when results of different indices are combined.
  EXPECT_FALSE(isDatatypeTrivial(Datatype::SecondaryVocabIndex));
  // Unlike a `LocalVocabIndex`, a `SecondaryVocabIndex` carries its value in
  // its bits.
  EXPECT_TRUE(id.canBeComparedBitwise());
  EXPECT_THROW(Id::makeFromSecondaryVocabIndex(
                   SecondaryVocabIndex::make(Id::maxIndex + 1)),
               Id::IndexTooLargeException);

  auto visitor = [](const auto& value) -> std::optional<uint64_t> {
    if constexpr (std::is_same_v<std::decay_t<decltype(value)>,
                                 SecondaryVocabIndex>) {
      return value.get();
    } else {
      return std::nullopt;
    }
  };
  EXPECT_THAT(id.visit(visitor), Optional(17ULL));
}

// _____________________________________________________________________________
TEST(SecondaryVocabIndex, sortsDirectlyAfterTheMainVocabulary) {
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
                      secondaryVocabId(0),
                      secondaryVocabId(1)};
  for (size_t i = 0; i <= static_cast<size_t>(Datatype::MaxValue); ++i) {
    auto datatype = static_cast<Datatype>(i);
    bool isCovered = ::ranges::contains(ids, datatype, &Id::getDatatype);
    ASSERT_EQ(isCovered, datatype != Datatype::LocalVocabIndex)
        << toString(datatype);
  }
  ql::ranges::sort(ids);
  // The `Id`s of the secondary vocabulary are greater than the `Id`s of the
  // main vocabulary (which is what makes the words of a secondary vocabulary
  // mergeable into a scan of the main index), and they directly follow them, in
  // ascending order of their indices.
  auto positionOfVocabIndex =
      ql::ranges::find(ids, Datatype::VocabIndex, &Id::getDatatype);
  ASSERT_GE(ids.end() - positionOfVocabIndex, 3);
  EXPECT_EQ(*(positionOfVocabIndex + 1), secondaryVocabId(0));
  EXPECT_EQ(*(positionOfVocabIndex + 2), secondaryVocabId(1));
}

// _____________________________________________________________________________
// NOTE: All the comparisons in this test are in the *internal* order, that is,
// the order in which the index scans emit their `Id`s. That order deliberately
// is not the semantic (by string value) order as soon as an index has an
// secondary vocabulary, see the warning in `index/LocalVocabEntry.h`.
TEST_F(SecondaryVocabIndexTest, localVocabEntryInSecondaryVocabulary) {
  EXPECT_TRUE(context_.hasSecondaryVocabulary());

  // `<b>` is a word of the secondary vocabulary, `<e>` is contained in neither
  // vocabulary and would be sorted between `<c>` and `<p>` of the main one.
  auto entryB = entry("<b>");
  auto entryE = entry("<e>");
  auto idB = idOf(entryB);
  auto idE = idOf(entryE);

  // A local vocab entry that is stored in the secondary vocabulary is
  // positioned at the `Id` of that vocabulary, and hence compares equal to it.
  auto position = entryB.positionInVocab();
  EXPECT_EQ(Id::fromBits(position.lowerBound_.get()), secondaryVocabId(1));
  EXPECT_NE(position.lowerBound_, position.upperBound_);
  EXPECT_EQ(idB, secondaryVocabId(1));
  EXPECT_LT(secondaryVocabId(0), idB);
  EXPECT_LT(idB, secondaryVocabId(2));

  // It is greater than all `Id`s of the main vocabulary, and it is ordered with
  // respect to the unrelated datatypes exactly like its position, that is, by
  // the datatype bits of `Datatype::SecondaryVocabIndex`.
  EXPECT_LT(getId_("<a>"), idB);
  EXPECT_LT(getId_("<s>"), idB);
  EXPECT_LT(Id::makeFromInt(42), idB);
  EXPECT_LT(idB, Id::makeFromBlankNodeIndex(BlankNodeIndex::make(3)));

  // More precisely: comparing either of the two entries to an `Id` of an
  // unrelated datatype yields exactly the same result as comparing the position
  // of that entry (`secondaryVocabId(1)` resp. the `VocabIndex` where `<e>`
  // would be sorted) to that `Id`. This is what makes the comparison of `Id`s a
  // valid strict weak ordering, and it is guaranteed by
  // `Datatype::SecondaryVocabIndex` being directly adjacent to
  // `Datatype::VocabIndex` and `Datatype::LocalVocabIndex`, which is also why
  // the comparison can be done on the bits alone for these datatypes (see
  // `ValueId::compareThreeWay`).
  for (Id other : {Id::makeUndefined(), Id::makeFromBool(true),
                   Id::makeFromInt(42), Id::makeFromDouble(3.14),
                   Id::makeFromTextRecordIndex(TextRecordIndex::make(3)),
                   Id::makeFromDate(DateYearOrDuration{Date{2013, 5, 16}}),
                   Id::makeFromGeoPoint(GeoPoint{3, 4}),
                   Id::makeFromWordVocabIndex(WordVocabIndex::make(3)),
                   Id::makeFromBlankNodeIndex(BlankNodeIndex::make(3))}) {
    SCOPED_TRACE(toString(other.getDatatype()));
    for (const auto& [id, position] :
         {std::pair{idB, secondaryVocabId(1)},
          std::pair{
              idE, Id::fromBits(entryE.positionInVocab().lowerBound_.get())}}) {
      SCOPED_TRACE(position);
      EXPECT_EQ(id < other, position < other);
      EXPECT_EQ(other < id, other < position);
    }
  }

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
TEST_F(SecondaryVocabIndexTest, localVocabEntryEqualityIgnoresPositionInVocab) {
  // Check `==` and `!=` in both directions.
  auto expectEqual = [](const auto& a, const auto& b, bool equal) {
    EXPECT_EQ(a == b, equal);
    EXPECT_EQ(a != b, !equal);
    EXPECT_EQ(b == a, equal);
    EXPECT_EQ(b != a, !equal);
  };

  // `<b>` is a word of the secondary vocabulary, `<e>` is contained in neither
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
TEST(SecondaryVocabIndex, localVocabEntryWithoutSecondaryVocabulary) {
  // Without a secondary vocabulary, the word `<b>` is simply positioned in the
  // main vocabulary, as it always was.
  Index index = ad_utility::testing::makeTestIndex(
      gtestCurrentTestName(), "<s> <p> <a> . <s> <p> <c> .");
  auto getId = ad_utility::testing::makeGetId(index);
  const auto& context = index.getImpl().getLocalVocabContext();
  EXPECT_EQ(index.getImpl().secondaryVocab(), nullptr);
  EXPECT_FALSE(context.hasSecondaryVocabulary());
  auto entry = [&context](std::string_view iriref) {
    return LocalVocabEntry::fromIriref(iriref, context);
  };

  auto entryB = entry("<b>");
  auto position = entryB.positionInVocab();
  EXPECT_EQ(position.lowerBound_, position.upperBound_);
  EXPECT_EQ(Id::fromBits(position.lowerBound_.get()), getId("<c>"));
  EXPECT_LT(Id::makeFromLocalVocabIndex(&entryB), getId("<c>"));
  EXPECT_LT(getId("<a>"), Id::makeFromLocalVocabIndex(&entryB));

  // Without a secondary vocabulary, two entries are compared by their string
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
// The semantic comparison of the words of the secondary vocabulary uses the
// comparator of the vocabulary of the main index, which the index attaches to
// the vocabulary when it is set.
TEST_F(SecondaryVocabIndexTest, semanticComparisonOfWords) {
  const SecondaryVocabulary* vocab = index_.getImpl().secondaryVocab();
  ASSERT_NE(vocab, nullptr);
  auto at = [](uint64_t i) { return SecondaryVocabIndex::make(i); };

  // Compare the words to words of the main vocabulary and to a word that is in
  // neither vocabulary. The words are `"a"` (index 0), `<b>` (1), and `<d>`
  // (2), see `secondaryVocabWords`.
  EXPECT_EQ(vocab->compareWordTo(at(1), "<b>"), 0);
  EXPECT_LT(vocab->compareWordTo(at(1), "<c>"), 0);
  EXPECT_GT(vocab->compareWordTo(at(1), "<a>"), 0);
  EXPECT_LT(vocab->compareWordTo(at(2), "<e>"), 0);
  EXPECT_GT(vocab->compareWordTo(at(2), "<c>"), 0);
  // A literal is smaller than every IRI in the order of the main vocabulary.
  EXPECT_LT(vocab->compareWordTo(at(0), "<a>"), 0);

  // Compare the words to each other.
  EXPECT_LT(vocab->compareWords(at(0), at(1)), 0);
  EXPECT_LT(vocab->compareWords(at(1), at(2)), 0);
  EXPECT_EQ(vocab->compareWords(at(1), at(1)), 0);
  EXPECT_GT(vocab->compareWords(at(2), at(0)), 0);
}

// _____________________________________________________________________________
// The semantic comparison of `Id`s, for all combinations of `VocabIndex`,
// `LocalVocabIndex`, and `SecondaryVocabIndex`. This is the comparison that
// SPARQL requires, and it deliberately differs from the *internal* order of the
// `Id`s, which the second half of this test pins down.
TEST_F(SecondaryVocabIndexTest, compareIdsSemantically) {
  // `"a"`, `<b>`, and `<d>` are words of the secondary vocabulary, `<a>`,
  // `<c>`, and `<p>` are words of the main vocabulary, and `<e>` is in neither.
  auto entryA = entry("<a>");
  auto entryB = entry("<b>");
  auto entryE = entry("<e>");
  Id idA = getId_("<a>");
  Id idC = getId_("<c>");
  Id idP = getId_("<p>");
  Id secLiteralA = secondaryVocabId(0);
  Id secB = secondaryVocabId(1);
  Id secD = secondaryVocabId(2);
  Id idOfA = idOf(entryA);
  Id idOfB = idOf(entryB);
  Id idOfE = idOf(entryE);

  // The sign of the semantic comparison, which is what the assertions below
  // are on.
  auto semantic = [this](Id a, Id b) {
    int result = context_.compareIdsSemantically(a, b);
    return result < 0 ? -1 : (result > 0 ? 1 : 0);
  };

  // Two `Id`s of the main vocabulary.
  EXPECT_EQ(semantic(idA, idC), -1);
  EXPECT_EQ(semantic(idC, idA), 1);
  EXPECT_EQ(semantic(idC, idC), 0);

  // An `Id` of the secondary vocabulary against one of the main vocabulary.
  EXPECT_EQ(semantic(secB, idA), 1);
  EXPECT_EQ(semantic(secB, idC), -1);
  EXPECT_EQ(semantic(idA, secB), -1);
  EXPECT_EQ(semantic(idC, secB), 1);
  EXPECT_EQ(semantic(secD, idP), -1);
  // A literal of the secondary vocabulary is smaller than every IRI.
  EXPECT_EQ(semantic(secLiteralA, idA), -1);
  EXPECT_EQ(semantic(idA, secLiteralA), 1);

  // Two `Id`s of the secondary vocabulary.
  EXPECT_EQ(semantic(secB, secD), -1);
  EXPECT_EQ(semantic(secD, secB), 1);
  EXPECT_EQ(semantic(secB, secB), 0);
  EXPECT_EQ(semantic(secLiteralA, secB), -1);

  // A `LocalVocabIndex` whose word is stored in the main vocabulary, one whose
  // word is stored in the secondary vocabulary, and one whose word is in
  // neither.
  EXPECT_EQ(semantic(idOfA, idA), 0);
  EXPECT_EQ(semantic(idOfB, secB), 0);
  EXPECT_EQ(semantic(idOfB, idA), 1);
  EXPECT_EQ(semantic(idOfB, idC), -1);
  EXPECT_EQ(semantic(idOfB, idOfE), -1);
  EXPECT_EQ(semantic(idOfE, secB), 1);
  EXPECT_EQ(semantic(idOfE, secD), 1);
  EXPECT_EQ(semantic(idOfE, idP), -1);
  EXPECT_EQ(semantic(idOfE, idOfE), 0);

  // The internal order is a different one: ALL words of the secondary
  // vocabulary come after ALL words of the main vocabulary, and after all words
  // that are in neither.
  for (Id secondary : {secLiteralA, secB, secD}) {
    SCOPED_TRACE(secondary);
    for (Id other : {idA, idC, idP, idOfA, idOfE}) {
      SCOPED_TRACE(other);
      EXPECT_LT(other, secondary);
    }
  }
  // In particular, the two orders disagree for `<a>` (main vocabulary) versus
  // `"a"` (secondary vocabulary), and for `<e>` (neither vocabulary) versus
  // `<b>` (secondary vocabulary).
  EXPECT_EQ(semantic(secLiteralA, idA), -1);
  EXPECT_LT(idA, secLiteralA);
  EXPECT_EQ(semantic(idOfE, secB), 1);
  EXPECT_LT(idOfE, secB);
}

// _____________________________________________________________________________
// `getSemanticPositionInMainVocab` always returns a position in the vocabulary
// of the MAIN index, also for a word that is stored in the secondary
// vocabulary. That position is what the binary search in
// `valueIdComparators::getRangesForId` needs.
TEST_F(SecondaryVocabIndexTest, getSemanticPositionInMainVocab) {
  auto entryA = entry("<a>");
  auto entryB = entry("<b>");
  auto entryE = entry("<e>");
  auto positionOf = [this](Id id) {
    return context_.getSemanticPositionInMainVocab(id);
  };
  auto vocabIndexOf = [this](const std::string& word) {
    return getId_(word).getVocabIndex();
  };

  // A word of the main vocabulary is at exactly its own position, so the range
  // consists of that single word. The same holds for a local vocab entry whose
  // word is stored there.
  auto positionA = positionOf(getId_("<a>"));
  EXPECT_EQ(positionA.first, vocabIndexOf("<a>"));
  EXPECT_EQ(positionA.second, VocabIndex::make(positionA.first.get() + 1));
  EXPECT_EQ(positionOf(idOf(entryA)), positionA);

  // A word that is not stored in the main vocabulary yields the empty range at
  // which it would be sorted into that vocabulary. For `<b>`, which is a word
  // of the secondary vocabulary, that is between `<a>` and `<c>`, no matter
  // whether it is addressed by its `SecondaryVocabIndex` or by a local vocab
  // entry.
  for (Id id : {secondaryVocabId(1), idOf(entryB)}) {
    SCOPED_TRACE(id);
    auto position = positionOf(id);
    EXPECT_EQ(position.first, position.second);
    EXPECT_GT(position.first.get(), vocabIndexOf("<a>").get());
    EXPECT_LE(position.first.get(), vocabIndexOf("<c>").get());
  }

  // For `<e>`, which is in neither vocabulary, it is between `<c>` and `<p>`.
  auto positionE = positionOf(idOf(entryE));
  EXPECT_EQ(positionE.first, positionE.second);
  EXPECT_GT(positionE.first.get(), vocabIndexOf("<c>").get());
  EXPECT_LE(positionE.first.get(), vocabIndexOf("<p>").get());

  // The literal `"a"` of the secondary vocabulary is positioned before all the
  // IRIs of the main vocabulary.
  auto positionLiteral = positionOf(secondaryVocabId(0));
  EXPECT_EQ(positionLiteral.first, positionLiteral.second);
  EXPECT_LE(positionLiteral.first.get(), vocabIndexOf("<a>").get());
}

// _____________________________________________________________________________
TEST_F(SecondaryVocabIndexTest, exportOfSecondaryVocabIds) {
  const IndexImpl& impl = index_.getImpl();
  LocalVocab localVocab{};

  auto iriId = secondaryVocabId(1);
  auto literalId = secondaryVocabId(0);
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
  // `returnOnlyLiterals` also has to work for the secondary vocabulary.
  EXPECT_EQ((idToStringAndType<true, true>(index_, iriId, localVocab)),
            std::nullopt);
  EXPECT_THAT((idToStringAndType<true, true>(index_, literalId, localVocab)),
              Optional(StringAndType{"a", nullptr}));
}

// _____________________________________________________________________________
TEST(SecondaryVocabIndex, exportWithoutSecondaryVocabularyThrows) {
  Index index = ad_utility::testing::makeTestIndex(gtestCurrentTestName(),
                                                   "<s> <p> <a> .");
  AD_EXPECT_THROW_WITH_MESSAGE(
      idToLiteralOrIri(index.getImpl(), secondaryVocabId(0), LocalVocab{}),
      HasSubstr("the index has no secondary vocabulary"));
}

// _____________________________________________________________________________
// Resolving a word to an `Id` has to consult the same vocabularies as
// `LocalVocabEntry::positionInVocab()`, else the two disagree about the
// position of a word of the secondary vocabulary, which yields wrong results
// and trips the check in the corresponding constructor of `LocalVocabEntry`.
TEST_F(SecondaryVocabIndexTest, toValueIdUsesAllVocabularies) {
  const IndexImpl& impl = index_.getImpl();

  // Resolve `iriref` via `toValueId`, which is the conversion that all callers
  // with a `LocalVocab` at hand use.
  auto toId = [&impl](std::string_view iriref, LocalVocab& localVocab) {
    TripleComponent tc{
        ad_utility::triple_component::Iri::fromIriref(std::string{iriref})};
    return toValueId(std::move(tc), impl, localVocab);
  };

  // A word of the secondary vocabulary becomes a bare `Id` of that vocabulary.
  {
    LocalVocab localVocab;
    Id id = toId("<b>", localVocab);
    EXPECT_EQ(id.getDatatype(), Datatype::SecondaryVocabIndex);
    EXPECT_EQ(id, secondaryVocabId(1));
    EXPECT_EQ(localVocab.size(), 0);
  }

  // A word of the vocabulary of the main index becomes a bare `Id` of that
  // vocabulary, as it always did.
  {
    LocalVocab localVocab;
    Id id = toId("<c>", localVocab);
    EXPECT_EQ(id.getDatatype(), Datatype::VocabIndex);
    EXPECT_EQ(id, getId_("<c>"));
    EXPECT_EQ(localVocab.size(), 0);
  }

  // A word that is in neither vocabulary becomes an entry of the local
  // vocabulary, whose position is the one that it would have in the main
  // vocabulary. That position has to be the very one that the entry computes
  // for itself, which is what the constructor of `LocalVocabEntry` checks.
  {
    LocalVocab localVocab;
    Id id = toId("<e>", localVocab);
    EXPECT_EQ(id.getDatatype(), Datatype::LocalVocabIndex);
    EXPECT_EQ(localVocab.size(), 1);
    auto expectedPosition = entry("<e>").positionInVocab();
    auto position = id.getLocalVocabIndex()->positionInVocab();
    EXPECT_EQ(position, expectedPosition);
    // The word is in none of the vocabularies, so the two bounds are equal and
    // point into the vocabulary of the main index.
    EXPECT_EQ(position.lowerBound_, position.upperBound_);
    EXPECT_EQ(Id::fromBits(position.lowerBound_.get()).getDatatype(),
              Datatype::VocabIndex);
  }
}

// _____________________________________________________________________________
// The same three cases, but for the `TripleComponent` overload that does not
// add to a local vocabulary. A word of the secondary vocabulary now has an
// `Id`, so this no longer reports it as "not found".
TEST_F(SecondaryVocabIndexTest, toValueIdWithoutLocalVocab) {
  const IndexImpl& impl = index_.getImpl();
  auto toId = [&impl](std::string_view iriref) {
    return toValueId(
        TripleComponent{
            ad_utility::triple_component::Iri::fromIriref(std::string{iriref})},
        impl);
  };
  EXPECT_THAT(toId("<b>"), Optional(secondaryVocabId(1)));
  EXPECT_THAT(toId("<c>"), Optional(getId_("<c>")));
  EXPECT_EQ(toId("<e>"), std::nullopt);
}

// The end-to-end tests below run actual SPARQL queries and updates against an
// index with a secondary vocabulary. Every query that mentions a word of that
// vocabulary goes through `toValueId` (see the tests above), so these are the
// tests that would have caught the mismatch between that function and
// `LocalVocabEntry::positionInVocab()` when it was introduced.

// A `QueryExecutionContext` for an index with the secondary vocabulary
// `secondaryVocabWords`, plus everything that such a context owns. NOTE: This
// deliberately does not use `ad_utility::testing::getQec`, because the updates
// below modify the index, which must not leak into the shared indices that that
// function caches.
struct ContextWithSecondaryVocab {
  std::string indexBasename_;
  std::shared_ptr<Index> index_;
  QueryResultCache cache_{};
  NamedResultCache namedResultCache_{};
  std::shared_ptr<MaterializedViewsManager> materializedViews_ =
      std::make_shared<MaterializedViewsManager>();
  QueryExecutionContext qec_;

  explicit ContextWithSecondaryVocab(std::string indexBasename)
      : indexBasename_{std::move(indexBasename)},
        index_{std::make_shared<Index>(
            makeIndexWithSecondaryVocab(indexBasename_))},
        qec_{index_,
             &cache_,
             ad_utility::testing::makeAllocator(
                 ad_utility::MemorySize::megabytes(100)),
             SortPerformanceEstimator{},
             &namedResultCache_,
             materializedViews_} {}

  // Delete the files of the index again. NOTE: The `Index` is still alive at
  // this point (the `QueryExecutionContext` also holds a reference to it), but
  // deleting a file that is still open is fine on the platforms that QLever
  // supports, and the index is only ever read from.
  ~ContextWithSecondaryVocab() {
    for (const std::string& filename :
         ad_utility::testing::getAllIndexFilenames(indexBasename_)) {
      ad_utility::deleteFile(filename, false);
    }
  }
};

// Run `query` on `qec` and return the result in TSV format.
std::string runQuery(QueryExecutionContext* qec, const std::string& query) {
  // The updates below change the result of a query, so a cached result of an
  // earlier run of the same query must not be reused.
  qec->clearCacheUnpinnedOnly();
  static const EncodedIriManager encodedIriManager;
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  auto parsedQuery = SparqlParser::parseQuery(&encodedIriManager, query);
  QueryPlanner queryPlanner{qec, cancellationHandle};
  auto executionTree = queryPlanner.createExecutionTree(parsedQuery);
  ad_utility::Timer timer{ad_utility::Timer::Started};
  std::string result;
  for (const auto& block : ExportQueryExecutionTrees::computeResult(
           parsedQuery, executionTree, ad_utility::MediaType::tsv, timer,
           cancellationHandle)) {
    result += block;
  }
  return result;
}

// Run `query` on `qec` and return the rows of its result, that is, the lines of
// the TSV without the header.
std::vector<std::string> runQueryAndGetRows(QueryExecutionContext* qec,
                                            const std::string& query) {
  std::vector<std::string> rows =
      absl::StrSplit(runQuery(qec, query), absl::ByChar('\n'));
  // The first line is the header, and the result ends with a newline, so the
  // last element is the empty string after it.
  AD_CORRECTNESS_CHECK(rows.size() >= 2 && rows.back().empty());
  rows.pop_back();
  rows.erase(rows.begin());
  return rows;
}

// Run the SPARQL `update` on `context`.
void runUpdate(ContextWithSecondaryVocab& context, const std::string& update) {
  static const EncodedIriManager encodedIriManager;
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  ad_utility::BlankNodeManager blankNodeManager;
  auto parsedQueries =
      SparqlParser::parseUpdate(&blankNodeManager, &encodedIriManager, update);
  context.index_->deltaTriplesManager().modify<void>(
      [&context, &parsedQueries,
       &cancellationHandle](DeltaTriples& deltaTriples) {
        context.qec_.setLocatedTriplesForEvaluation(
            deltaTriples.getLocatedTriplesSharedStateReference());
        for (auto& parsedQuery : parsedQueries) {
          deltaTriples.updateAugmentedMetadata();
          QueryPlanner queryPlanner{&context.qec_, cancellationHandle};
          auto executionTree = queryPlanner.createExecutionTree(parsedQuery);
          ExecuteUpdate::executeUpdate(*context.index_, parsedQuery,
                                       executionTree, deltaTriples,
                                       cancellationHandle);
        }
      });
}

// _____________________________________________________________________________
// Queries that mention a word of the secondary vocabulary. NOTE: The words of
// that vocabulary are not part of any permutation of the index (nothing but a
// unit test fills a secondary vocabulary yet, see `SecondaryVocabulary`), so a
// triple pattern with such a word matches nothing. The point of these tests is
// that the queries run at all and yield the words unchanged.
TEST(SecondaryVocabIndex, queriesWithWordsOfTheSecondaryVocabulary) {
  ContextWithSecondaryVocab context{gtestCurrentTestName()};

  // A word of the secondary vocabulary survives a round trip through `VALUES`,
  // no matter whether it is an IRI or a literal.
  EXPECT_EQ(runQuery(&context.qec_, "SELECT * WHERE { VALUES ?o { <b> } }"),
            "?o\n<b>\n");
  EXPECT_EQ(runQuery(&context.qec_, "SELECT * WHERE { VALUES ?o { \"a\" } }"),
            "?o\n\"a\"\n");
  // The same for a word that is in none of the vocabularies, and for one of the
  // main vocabulary.
  EXPECT_EQ(runQuery(&context.qec_, "SELECT * WHERE { VALUES ?o { <e> } }"),
            "?o\n<e>\n");
  EXPECT_EQ(runQuery(&context.qec_, "SELECT * WHERE { VALUES ?o { <a> } }"),
            "?o\n<a>\n");

  // A triple pattern with such a word as its object resp. its subject matches
  // nothing, but does not crash.
  EXPECT_EQ(runQuery(&context.qec_, "SELECT * WHERE { ?s ?p <b> }"),
            "?s\t?p\n");
  EXPECT_EQ(runQuery(&context.qec_, "SELECT * WHERE { ?s ?p \"a\" }"),
            "?s\t?p\n");
  EXPECT_EQ(runQuery(&context.qec_, "SELECT * WHERE { <b> ?p ?o }"),
            "?p\t?o\n");

  // A word of the secondary vocabulary can be combined with a scan of the main
  // index, here via an `OPTIONAL` that matches nothing.
  EXPECT_EQ(runQuery(&context.qec_,
                     "SELECT * WHERE { VALUES ?o { <b> } . "
                     "OPTIONAL { ?s <p> ?o } }"),
            "?o\t?s\n<b>\t\n");
}

// _____________________________________________________________________________
// An update that inserts a triple whose object is a word of the secondary
// vocabulary. This is the case that the secondary vocabulary exists for, and it
// goes through `toValueId` twice: once for the triple that is inserted, and
// once for the query that reads it back.
TEST(SecondaryVocabIndex, updateWithWordOfTheSecondaryVocabulary) {
  ContextWithSecondaryVocab context{gtestCurrentTestName()};
  auto objectsOfS = [&context]() {
    return runQueryAndGetRows(&context.qec_,
                              "SELECT ?o WHERE { <s> <p> ?o } ORDER BY ?o");
  };
  ASSERT_THAT(objectsOfS(), ElementsAre("<a>", "<c>"));

  // Insert a triple whose object is a word of the secondary vocabulary, and
  // then one whose object is in none of the vocabularies. Both have to become
  // visible. NOTE: These assertions deliberately ignore the order, which the
  // separate assertion below pins down.
  runUpdate(context, "INSERT DATA { <s> <p> <b> }");
  EXPECT_THAT(objectsOfS(), UnorderedElementsAre("<a>", "<b>", "<c>"));
  runUpdate(context, "INSERT DATA { <s> <p> <e> }");
  EXPECT_THAT(objectsOfS(), UnorderedElementsAre("<a>", "<b>", "<c>", "<e>"));

  // WARNING: The following assertion pins down the *order* of the result, which
  // currently is the internal order (see the warning in
  // `index/LocalVocabEntry.h`): a word of the secondary vocabulary is
  // positioned after all words of the main vocabulary and after all words that
  // are in none of the vocabularies, no matter what its string value is. So
  // `<b>` comes last here, instead of between `<a>` and `<c>` where its string
  // value belongs.
  //
  // This is deliberately NOT the order that SPARQL requires, see the detailed
  // note at `valueIdComparators::detail::compareIdsImpl`. The semantic
  // comparison that the fix needs now exists (see
  // `LocalVocabEntry::compareThreeWaySemantically` and
  // `LocalVocabContext::compareIdsSemantically`), but `valueIdComparators` does
  // not use it yet. The follow-up PR that makes it do so has to change this
  // expectation to `<a>, <b>, <c>, <e>`. A failure of this assertion is that
  // fix landing, not a regression.
  EXPECT_THAT(objectsOfS(), ElementsAre("<a>", "<c>", "<e>", "<b>"));
}

// _____________________________________________________________________________
// A query that does not mention any word of the secondary vocabulary has to
// yield exactly the same result as it does on the same index without such a
// vocabulary. This covers the whole query engine (scans over several blocks,
// joins, filters, prefilters, sorting, and grouping) at once, and it is the
// test that a future change of the secondary vocabulary has to keep passing.
TEST(SecondaryVocabIndex, sameResultsWithAndWithoutSecondaryVocabulary) {
  // A knowledge graph with enough triples to span several blocks per
  // permutation (a block holds two `Id`s per column in the tests, see
  // `TestIndexConfig::blocksizePermutations`), and with objects of several
  // datatypes.
  std::string kg;
  for (size_t i = 0; i < 12; ++i) {
    absl::StrAppend(&kg, "<s", i, "> <knows> <s", (i + 1) % 12, "> .\n");
    absl::StrAppend(&kg, "<s", i, "> <label> \"name", i, "\" .\n");
    absl::StrAppend(&kg, "<s", i, "> <rank> ", i, " .\n");
  }
  ad_utility::testing::TestIndexConfig configWithSecondaryVocab{kg};
  configWithSecondaryVocab.secondaryVocabWords = secondaryVocabWords;
  auto* qecWithout =
      ad_utility::testing::getQec(ad_utility::testing::TestIndexConfig{kg});
  auto* qecWith = ad_utility::testing::getQec(configWithSecondaryVocab);
  ASSERT_TRUE(qecWith->getIndex().getImpl().secondaryVocab() != nullptr);
  ASSERT_TRUE(qecWithout->getIndex().getImpl().secondaryVocab() == nullptr);

  for (const std::string& query :
       {"SELECT * WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o"s,
        "SELECT * WHERE { ?s <knows> ?o } ORDER BY ?o"s,
        "SELECT * WHERE { ?s <knows> ?o . ?o <label> ?l } ORDER BY ?s"s,
        "SELECT * WHERE { ?s ?p ?o . FILTER (?o > <s5>) } ORDER BY ?s ?p ?o"s,
        "SELECT * WHERE { ?s ?p ?o . FILTER (?o < \"name5\") } ORDER BY ?o"s,
        "SELECT * WHERE { ?s ?p ?o . FILTER isIRI(?o) } ORDER BY ?s ?p ?o"s,
        "SELECT * WHERE { ?s ?p ?o . FILTER isLiteral(?o) } ORDER BY ?o"s,
        "SELECT ?s (COUNT(?o) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?s "
        "ORDER BY ?s"s,
        "SELECT * WHERE { ?s <label> ?o . BIND(CONCAT(STR(?o), \"x\") AS ?b) } "
        "ORDER BY ?b"s,
        "SELECT * WHERE { ?s ?p ?o } ORDER BY DESC(?o)"s}) {
    SCOPED_TRACE(query);
    EXPECT_EQ(runQuery(qecWith, query), runQuery(qecWithout, query));
  }
}

}  // namespace
