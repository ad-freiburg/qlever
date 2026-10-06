// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <s2/mutable_s2shape_index.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "../QueryPlannerTestHelpers.h"
#include "../index/SecondaryVocabularyTestHelpers.h"
#include "../util/AllocatorTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/ExplicitIdTableOperation.h"
#include "engine/NamedResultCache.h"
#include "index/LocalVocabEntry.h"
#include "index/vocabulary/SecondaryVocabulary.h"
#include "libqlever/NamedCacheSecondaryVocabRewriter.h"

using namespace qlever::namedCacheSecondaryVocab;
using secondaryVocabTestHelpers::secondaryVocabIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

namespace {
using Value = NamedResultCache::Value;

// Return the `Id` of the word of a secondary vocabulary at the given `index`.
Id secondaryId(uint64_t index) {
  return Id::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(index));
}

// Pin the result of the `query` under the `name` in the named result cache of
// `qec` (optionally with a geo index on the `geoIndexVar`), and return the
// pinned entry.
std::shared_ptr<const Value> pin(
    QueryExecutionContext* qec, const std::string& name,
    const std::string& query,
    std::optional<Variable> geoIndexVar = std::nullopt) {
  qec->pinResultWithName() =
      QueryExecutionContext::PinResultWithName{name, std::move(geoIndexVar)};
  auto plan = queryPlannerTestHelpers::parseAndPlan(query, qec);
  [[maybe_unused]] auto result = plan->getResult();
  qec->pinResultWithName() = std::nullopt;
  return qec->namedResultCache().get(name);
}

// Return the column with the given `index` of the result of `value`.
std::vector<Id> column(const Value& value, size_t index) {
  auto col = ExplicitIdTableOperation::viewOf(value.result_).getColumn(index);
  return {col.begin(), col.end()};
}

// Return the `Id` of the local vocab entry with the word `iriref` in the given
// `column`.
Id localVocabIdOf(const std::vector<Id>& column, std::string_view iriref) {
  for (Id id : column) {
    if (id.getDatatype() == Datatype::LocalVocabIndex &&
        id.getLocalVocabIndex()->toStringRepresentation() == iriref) {
      return id;
    }
  }
  AD_FAIL();
}

// Add the new words of the `value` to the `secondaryVocab`, and return the
// rewritten copy of the `value` (see `addNewWordsToSecondaryVocab` and
// `rewriteToSecondaryVocab`).
Value addAndRewrite(const std::shared_ptr<const Value>& value,
                    SecondaryVocabulary& secondaryVocab) {
  addNewWordsToSecondaryVocab({{"entry", value}}, secondaryVocab);
  return rewriteToSecondaryVocab(*value, secondaryVocab,
                                 ad_utility::testing::makeAllocator());
}

// The data of the tests below. Most of the new words that the tests use are
// sorted before (`<b>`, `<c>`) or after (`<y>`) all IRIs of this data.
constexpr std::string_view kb = "<m> <p> <o> . <m> <p> \"lit\" .";
}  // namespace

// Test `containsLocalVocabIds` on an entry with and without local vocab
// entries.
TEST(NamedCacheSecondaryVocabRewriter, containsLocalVocabIds) {
  auto qec = ad_utility::testing::getQec(std::string{kb});
  EXPECT_FALSE(containsLocalVocabIds(
      *pin(qec, "withoutLocalVocab", "SELECT * { ?s <p> ?o }")));
  EXPECT_TRUE(containsLocalVocabIds(*pin(
      qec, "withLocalVocab", "SELECT ?x { VALUES ?x { <m> <newWord> } }")));
}

// Test that `addNewWordsToSecondaryVocab` adds exactly the new words of
// several entries to a preexisting secondary vocabulary, and that
// `rewriteId` then rewrites the `Id`s of all words.
TEST(NamedCacheSecondaryVocabRewriter, addNewWordsAndRewriteIds) {
  auto qec = ad_utility::testing::getQec(std::string{kb});
  // `<y>` occurs twice, and in two different entries.
  auto first = pin(qec, "first", "SELECT ?x { VALUES ?x { <y> <m> <b> <y> } }");
  auto second = pin(qec, "second", "SELECT ?x { VALUES ?x { <y> <c> } }");
  Entries entries{{"first", first}, {"second", second}};

  // The secondary vocabulary already contains `<c>` (for example, because it
  // is the one of a previously written blob), which therefore keeps its `Id`.
  SecondaryVocabulary secondaryVocab{std::vector<std::string>{"<c>"}};
  EXPECT_EQ(addNewWordsToSecondaryVocab(entries, secondaryVocab), 2);
  // The new words form one sorted segment. Words of the main vocabulary
  // (`<m>`) are not part of it.
  EXPECT_THAT(secondaryVocab, secondaryVocabIs(2, {"<c>", "<b>", "<y>"}));
  EXPECT_EQ(secondaryVocab.getId("<m>"), std::nullopt);

  // Calling it again finds no more new words and adds no segment.
  EXPECT_EQ(addNewWordsToSecondaryVocab(entries, secondaryVocab), 0);
  EXPECT_EQ(secondaryVocab.numSegments(), 2);

  // All `Id`s are rewritten to the `Id`s in the secondary vocabulary, the `Id`
  // of the word of the main vocabulary is kept.
  auto firstColumn = column(*first, 0);
  auto secondColumn = column(*second, 0);
  EXPECT_EQ(rewriteId(localVocabIdOf(firstColumn, "<y>"), secondaryVocab),
            secondaryId(2));
  EXPECT_EQ(rewriteId(localVocabIdOf(firstColumn, "<b>"), secondaryVocab),
            secondaryId(1));
  EXPECT_EQ(rewriteId(localVocabIdOf(secondColumn, "<c>"), secondaryVocab),
            secondaryId(0));
  Id m = ad_utility::testing::makeGetId(qec->getIndex())("<m>");
  EXPECT_EQ(rewriteId(m, secondaryVocab), m);
  EXPECT_EQ(rewriteId(Id::makeFromInt(42), secondaryVocab),
            Id::makeFromInt(42));
  // A local vocab entry whose word is contained in the main vocabulary is
  // rewritten to the `Id` of that word.
  auto entryOfM =
      LocalVocabEntry::fromIriref("<m>", qec->getLocalVocabContext());
  EXPECT_EQ(rewriteId(Id::makeFromLocalVocabIndex(&entryOfM), secondaryVocab),
            m);

  // A word that has not been added to the secondary vocabulary cannot be
  // rewritten.
  AD_EXPECT_THROW_WITH_MESSAGE(
      rewriteId(localVocabIdOf(firstColumn, "<y>"), SecondaryVocabulary{}),
      HasSubstr("call `addNewWordsToSecondaryVocab` first"));
}

// Test that `rewriteToSecondaryVocab` sorts the rewritten copy again by the
// columns that the entry is sorted on, and leaves the entry unchanged.
TEST(NamedCacheSecondaryVocabRewriter, rewriteToSecondaryVocabSortsAgain) {
  auto qec = ad_utility::testing::getQec(std::string{kb});
  // The `DISTINCT` sorts the result by `?x` in the internal order, in which a
  // new word is sorted at the position where it would be sorted into the main
  // vocabulary, so the new word `<b>` comes first and `<y>` last.
  auto value = pin(qec, "sorted",
                   "SELECT DISTINCT ?x { VALUES ?x { <y> <o> <b> <m> } }");
  ASSERT_THAT(value->resultSortedOn_, ElementsAre(0));
  auto originalColumn = column(*value, 0);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  ASSERT_THAT(originalColumn,
              ElementsAre(localVocabIdOf(originalColumn, "<b>"), getId("<m>"),
                          getId("<o>"), localVocabIdOf(originalColumn, "<y>")));

  SecondaryVocabulary secondaryVocab;
  auto rewritten = addAndRewrite(value, secondaryVocab);

  // All `Id`s of the secondary vocabulary are greater than the ones of the
  // main vocabulary, so the rows had to be sorted again.
  EXPECT_THAT(
      column(rewritten, 0),
      ElementsAre(getId("<m>"), getId("<o>"), secondaryId(0), secondaryId(1)));
  EXPECT_THAT(rewritten.resultSortedOn_, ElementsAre(0));
  EXPECT_FALSE(containsLocalVocabIds(rewritten));
  EXPECT_EQ(rewritten.varToColMap_, value->varToColMap_);
  EXPECT_EQ(rewritten.cacheKey_, value->cacheKey_);
  EXPECT_FALSE(rewritten.cachedGeoIndex_.has_value());

  // The original entry is unchanged.
  EXPECT_EQ(column(*value, 0), originalColumn);
  EXPECT_TRUE(containsLocalVocabIds(*value));

  // A result that is not sorted keeps its order of rows.
  auto unsorted = pin(qec, "unsorted", "SELECT ?x { VALUES ?x { <y> <m> } }");
  ASSERT_TRUE(unsorted->resultSortedOn_.empty());
  EXPECT_THAT(column(addAndRewrite(unsorted, secondaryVocab), 0),
              ElementsAre(secondaryId(1), getId("<m>")));
}

// Test that the mapping from shapes to rows of a cached geo index is
// permuted together with the rows of the rewritten copy.
TEST(NamedCacheSecondaryVocabRewriter, rewriteToSecondaryVocabWithGeoIndex) {
  auto qec = ad_utility::testing::getQec(
      "<s1> <asWKT> \"LINESTRING(1 1, 2 2)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> . "
      "<s2> <asWKT> \"LINESTRING(3 3, 4 4)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> .");
  // The new subject `<a>` is sorted before `<s1>` and `<s2>`, but its `Id` in
  // the secondary vocabulary is sorted after them, so the rows are permuted by
  // the rewriting, and with them the rows of the geo index.
  auto value = pin(
      qec, "geo",
      "SELECT DISTINCT ?s ?geo { { ?s <asWKT> ?geo } UNION { VALUES (?s ?geo) "
      "{ (<a> \"LINESTRING(5 5, 6 6)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>) } } }",
      Variable{"?geo"});
  ASSERT_FALSE(value->resultSortedOn_.empty());
  ASSERT_TRUE(value->cachedGeoIndex_.has_value());
  const auto& originalGeoIndex = value->cachedGeoIndex_.value();
  auto numShapes = originalGeoIndex.getIndex()->num_shape_ids();
  ASSERT_EQ(numShapes, 3);

  SecondaryVocabulary secondaryVocab;
  auto rewritten = addAndRewrite(value, secondaryVocab);
  EXPECT_EQ(secondaryVocab.numWords(), 2);
  ASSERT_TRUE(rewritten.cachedGeoIndex_.has_value());
  const auto& rewrittenGeoIndex = rewritten.cachedGeoIndex_.value();
  EXPECT_EQ(rewrittenGeoIndex.getIndex().get(),
            originalGeoIndex.getIndex().get());

  // Each shape refers to the (rewritten) row that it referred to before.
  auto originalView = ExplicitIdTableOperation::viewOf(value->result_);
  auto rewrittenView = ExplicitIdTableOperation::viewOf(rewritten.result_);
  EXPECT_NE(rewrittenGeoIndex.getRow(0), originalGeoIndex.getRow(0));
  for (int shape = 0; shape < numShapes; ++shape) {
    for (size_t col = 0; col < originalView.numColumns(); ++col) {
      EXPECT_EQ(rewrittenView(rewrittenGeoIndex.getRow(shape), col),
                rewriteId(originalView(originalGeoIndex.getRow(shape), col),
                          secondaryVocab));
    }
  }
}

// Test local vocab entries whose words are already contained in the vocabulary
// of the main index or in the secondary vocabulary of the index itself (for
// example because it was loaded from a blob). No query produces such entries,
// so the entry of the named result cache is built by hand.
TEST(NamedCacheSecondaryVocabRewriter, localVocabEntriesOfExistingWords) {
  ad_utility::testing::TestIndexConfig config{std::string{kb}};
  config.secondaryVocabWords = std::vector<std::string>{"<a>"};
  auto qec = ad_utility::testing::getQec(std::move(config));
  const auto& context = qec->getLocalVocabContext();

  // The words are contained in the main vocabulary (`<m>`), in the secondary
  // vocabulary of the index (`<a>`), and in neither of them (`<y>`).
  LocalVocab localVocab;
  auto idOf = [&localVocab, &context](std::string_view iriref) {
    return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
        LocalVocabEntry::fromIriref(iriref, context)));
  };
  Id m = idOf("<m>");
  Id a = idOf("<a>");
  Id y = idOf("<y>");
  // The position of `<a>` is its `Id` in the secondary vocabulary of the index.
  auto positionOfA = a.getLocalVocabIndex()->positionInVocab();
  ASSERT_NE(positionOfA.lowerBound_, positionOfA.upperBound_);
  EXPECT_EQ(Id::fromBits(positionOfA.lowerBound_.get()), secondaryId(0));
  auto value = std::make_shared<const Value>(Value{
      std::make_shared<const IdTable>(makeIdTableFromVector({{m}, {a}, {y}})),
      VariableToColumnMap{}, std::vector<ColumnIndex>{}, std::move(localVocab),
      "handmade", std::nullopt});

  // The secondary vocabulary that is passed in has to be an extension of the
  // one of the index (see the precondition in
  // `NamedCacheSecondaryVocabRewriter.h`). Only `<y>` is new.
  SecondaryVocabulary secondaryVocab{std::vector<std::string>{"<a>"}};
  EXPECT_EQ(addNewWordsToSecondaryVocab({{"entry", value}}, secondaryVocab), 1);
  EXPECT_THAT(secondaryVocab, secondaryVocabIs(2, {"<a>", "<y>"}));

  // The word of the main vocabulary is rewritten to its `Id` there, and the
  // word of the secondary vocabulary of the index keeps its `Id`.
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  EXPECT_EQ(rewriteId(m, secondaryVocab), getId("<m>"));
  EXPECT_EQ(rewriteId(a, secondaryVocab), secondaryId(0));
  EXPECT_EQ(rewriteId(y, secondaryVocab), secondaryId(1));
  auto rewritten = rewriteToSecondaryVocab(
      *value, secondaryVocab, ad_utility::testing::makeAllocator());
  EXPECT_THAT(column(rewritten, 0),
              ElementsAre(getId("<m>"), secondaryId(0), secondaryId(1)));
}
