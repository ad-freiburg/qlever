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
#include "index/vocabulary/AuxVocabulary.h"
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
Index makeIndexWithAuxVocab(const std::string& indexBasename) {
  ad_utility::testing::TestIndexConfig config{"<s> <p> <a> . <s> <p> <c> ."};
  config.auxVocabWords = auxWords;
  return ad_utility::testing::makeTestIndex(indexBasename, std::move(config));
}

// A fixture for the tests that need an index with an auxiliary vocabulary, see
// `makeIndexWithAuxVocab`.
class AuxVocabIndexTest : public ::testing::Test {
 protected:
  Index index_ = makeIndexWithAuxVocab(gtestCurrentTestName());
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
TEST(AuxVocabIndex, sortsDirectlyAfterTheMainVocabulary) {
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
  // The `Id`s of the auxiliary vocabulary are greater than the `Id`s of the
  // main vocabulary (which is what makes the words of an auxiliary vocabulary
  // mergeable into a scan of the main index), and they directly follow them, in
  // ascending order of their indices.
  auto positionOfVocabIndex =
      ql::ranges::find(ids, Datatype::VocabIndex, &Id::getDatatype);
  ASSERT_GE(ids.end() - positionOfVocabIndex, 3);
  EXPECT_EQ(*(positionOfVocabIndex + 1), auxId(0));
  EXPECT_EQ(*(positionOfVocabIndex + 2), auxId(1));
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

  // It is greater than all `Id`s of the main vocabulary, and it is ordered with
  // respect to the unrelated datatypes exactly like its position, that is, by
  // the datatype bits of `Datatype::AuxVocabIndex`.
  EXPECT_LT(getId_("<a>"), idB);
  EXPECT_LT(getId_("<s>"), idB);
  EXPECT_LT(Id::makeFromInt(42), idB);
  EXPECT_LT(idB, Id::makeFromBlankNodeIndex(BlankNodeIndex::make(3)));

  // More precisely: comparing either of the two entries to an `Id` of an
  // unrelated datatype yields exactly the same result as comparing the position
  // of that entry (`auxId(1)` resp. the `VocabIndex` where `<e>` would be
  // sorted) to that `Id`. This is what makes the comparison of `Id`s a valid
  // strict weak ordering, and it is guaranteed by `Datatype::AuxVocabIndex`
  // being directly adjacent to `Datatype::VocabIndex` and
  // `Datatype::LocalVocabIndex`, which is also why the comparison can be done
  // on the bits alone for these datatypes (see `ValueId::compareThreeWay`).
  for (Id other : {Id::makeUndefined(), Id::makeFromBool(true),
                   Id::makeFromInt(42), Id::makeFromDouble(3.14),
                   Id::makeFromTextRecordIndex(TextRecordIndex::make(3)),
                   Id::makeFromDate(DateYearOrDuration{Date{2013, 5, 16}}),
                   Id::makeFromGeoPoint(GeoPoint{3, 4}),
                   Id::makeFromWordVocabIndex(WordVocabIndex::make(3)),
                   Id::makeFromBlankNodeIndex(BlankNodeIndex::make(3))}) {
    SCOPED_TRACE(toString(other.getDatatype()));
    for (const auto& [id, position] :
         {std::pair{idB, auxId(1)},
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

// _____________________________________________________________________________
// Resolving a word to an `Id` has to consult the same vocabularies as
// `LocalVocabEntry::positionInVocab()`, else the two disagree about the
// position of a word of the auxiliary vocabulary, which yields wrong results
// and trips the check in the corresponding constructor of `LocalVocabEntry`.
TEST_F(AuxVocabIndexTest, toValueIdUsesAllVocabularies) {
  const IndexImpl& impl = index_.getImpl();

  // Resolve `iriref` via `toValueId`, which is the conversion that all callers
  // with a `LocalVocab` at hand use.
  auto toId = [&impl](std::string_view iriref, LocalVocab& localVocab) {
    TripleComponent tc{
        ad_utility::triple_component::Iri::fromIriref(std::string{iriref})};
    return toValueId(std::move(tc), impl, localVocab);
  };

  // A word of the auxiliary vocabulary becomes a bare `Id` of that vocabulary.
  {
    LocalVocab localVocab;
    Id id = toId("<b>", localVocab);
    EXPECT_EQ(id.getDatatype(), Datatype::AuxVocabIndex);
    EXPECT_EQ(id, auxId(1));
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
// add to a local vocabulary. A word of the auxiliary vocabulary now has an
// `Id`, so this no longer reports it as "not found".
TEST_F(AuxVocabIndexTest, toValueIdWithoutLocalVocab) {
  const IndexImpl& impl = index_.getImpl();
  auto toId = [&impl](std::string_view iriref) {
    return toValueId(
        TripleComponent{
            ad_utility::triple_component::Iri::fromIriref(std::string{iriref})},
        impl);
  };
  EXPECT_THAT(toId("<b>"), Optional(auxId(1)));
  EXPECT_THAT(toId("<c>"), Optional(getId_("<c>")));
  EXPECT_EQ(toId("<e>"), std::nullopt);
}

// The end-to-end tests below run actual SPARQL queries and updates against an
// index with an auxiliary vocabulary. Every query that mentions a word of that
// vocabulary goes through `toValueId` (see the tests above), so these are the
// tests that would have caught the mismatch between that function and
// `LocalVocabEntry::positionInVocab()` when it was introduced.

// A `QueryExecutionContext` for an index with the auxiliary vocabulary
// `auxWords`, plus everything that such a context owns. NOTE: This deliberately
// does not use `ad_utility::testing::getQec`, because the updates below modify
// the index, which must not leak into the shared indices that that function
// caches.
struct ContextWithAuxVocab {
  std::string indexBasename_;
  std::shared_ptr<Index> index_;
  QueryResultCache cache_{};
  NamedResultCache namedResultCache_{};
  std::shared_ptr<MaterializedViewsManager> materializedViews_ =
      std::make_shared<MaterializedViewsManager>();
  QueryExecutionContext qec_;

  explicit ContextWithAuxVocab(std::string indexBasename)
      : indexBasename_{std::move(indexBasename)},
        index_{std::make_shared<Index>(makeIndexWithAuxVocab(indexBasename_))},
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
  ~ContextWithAuxVocab() {
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
void runUpdate(ContextWithAuxVocab& context, const std::string& update) {
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
// Queries that mention a word of the auxiliary vocabulary. NOTE: The words of
// that vocabulary are not part of any permutation of the index (the auxiliary
// index that they belong to is not wired up yet, see `AuxVocabulary`), so a
// triple pattern with such a word matches nothing. The point of these tests is
// that the queries run at all and yield the words unchanged.
TEST(AuxVocabIndex, queriesWithWordsOfTheAuxVocabulary) {
  ContextWithAuxVocab context{gtestCurrentTestName()};

  // A word of the auxiliary vocabulary survives a round trip through `VALUES`,
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

  // A word of the auxiliary vocabulary can be combined with a scan of the main
  // index, here via an `OPTIONAL` that matches nothing.
  EXPECT_EQ(runQuery(&context.qec_,
                     "SELECT * WHERE { VALUES ?o { <b> } . "
                     "OPTIONAL { ?s <p> ?o } }"),
            "?o\t?s\n<b>\t\n");
}

// _____________________________________________________________________________
// An update that inserts a triple whose object is a word of the auxiliary
// vocabulary. This is the case that the auxiliary vocabulary exists for, and it
// goes through `toValueId` twice: once for the triple that is inserted, and
// once for the query that reads it back.
TEST(AuxVocabIndex, updateWithWordOfTheAuxVocabulary) {
  ContextWithAuxVocab context{gtestCurrentTestName()};
  auto objectsOfS = [&context]() {
    return runQueryAndGetRows(&context.qec_,
                              "SELECT ?o WHERE { <s> <p> ?o } ORDER BY ?o");
  };
  ASSERT_THAT(objectsOfS(), ElementsAre("<a>", "<c>"));

  // Insert a triple whose object is a word of the auxiliary vocabulary, and
  // then one whose object is in none of the vocabularies. Both have to become
  // visible. NOTE: These assertions deliberately ignore the order, which the
  // separate assertion below pins down.
  runUpdate(context, "INSERT DATA { <s> <p> <b> }");
  EXPECT_THAT(objectsOfS(), UnorderedElementsAre("<a>", "<b>", "<c>"));
  runUpdate(context, "INSERT DATA { <s> <p> <e> }");
  EXPECT_THAT(objectsOfS(), UnorderedElementsAre("<a>", "<b>", "<c>", "<e>"));

  // WARNING: The following assertion pins down the *order* of the result, which
  // currently is the internal order (see the warning in
  // `index/LocalVocabEntry.h`): a word of the auxiliary vocabulary is
  // positioned after all words of the main vocabulary and after all words that
  // are in none of the vocabularies, no matter what its string value is. So
  // `<b>` comes last here, instead of between `<a>` and `<c>` where its string
  // value belongs.
  //
  // This is deliberately NOT the order that SPARQL requires, see the detailed
  // note at `valueIdComparators::detail::compareIdsImpl`. The follow-up PR that
  // implements the semantic comparison (see the `TODO<joka921>` there) has to
  // change this expectation to `<a>, <b>, <c>, <e>`. A failure of this
  // assertion is that fix landing, not a regression.
  EXPECT_THAT(objectsOfS(), ElementsAre("<a>", "<c>", "<e>", "<b>"));
}

// _____________________________________________________________________________
// A query that does not mention any word of the auxiliary vocabulary has to
// yield exactly the same result as it does on the same index without such a
// vocabulary. This covers the whole query engine (scans over several blocks,
// joins, filters, prefilters, sorting, and grouping) at once, and it is the
// test that a future change of the auxiliary vocabulary has to keep passing.
TEST(AuxVocabIndex, sameResultsWithAndWithoutAuxVocabulary) {
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
  ad_utility::testing::TestIndexConfig configWithAuxVocab{kg};
  configWithAuxVocab.auxVocabWords = auxWords;
  auto* qecWithout =
      ad_utility::testing::getQec(ad_utility::testing::TestIndexConfig{kg});
  auto* qecWith = ad_utility::testing::getQec(configWithAuxVocab);
  ASSERT_TRUE(qecWith->getIndex().getImpl().auxVocab() != nullptr);
  ASSERT_TRUE(qecWithout->getIndex().getImpl().auxVocab() == nullptr);

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
