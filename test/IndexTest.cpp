// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "./IndexTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "global/Pattern.h"
#include "index/Index.h"
#include "index/IndexImpl.h"

using namespace ad_utility::testing;

namespace {
auto lit = ad_utility::testing::tripleComponentLiteral;

// Return a lambda that runs a scan for two fixed elements `c0` and `c1`
// on the `permutation` (e.g. a fixed P and S in the PSO permutation)
// of the `index` and checks whether the result of the
// scan matches `expected`.
auto makeTestScanWidthOne = [](const IndexImpl& index) {
  return [&index](const std::string& c0, const std::string& c1,
                  const auto& permutation,
                  const std::vector<std::vector<Id>>& expected) {
    IdTable result(1, makeAllocator());
    index.scan(c0, c1, &result, permutation);
    ASSERT_EQ(result, makeIdTableFromIdVector(expected));
  };
};

// Return a lambda that runs a scan for a fixed element `c0`
// on the `permutation` (e.g. a fixed P in the PSO permutation)
// of the `index` and checks whether the result of the
// scan matches `expected`.
auto makeTestScanWidthTwo = [](const IndexImpl& index) {
  return [&index](const std::string& c0, const auto& permutation,
                  const std::vector<std::vector<Id>>& expected) {
    IdTable wol(2, makeAllocator());
    index.scan(c0, &wol, permutation);
    ASSERT_EQ(wol, makeIdTableFromIdVector(expected));
  };
};
}  // namespace

TEST(IndexTest, createFromTurtleTest) {
  {
    std::string kb =
        "<a>  <b>  <c> . \n"
        "<a>  <b>  <c2> .\n"
        "<a>  <b2> <c> .\n"
        "<a2> <b2> <c2> .";
    const IndexImpl& index = getQec(kb)->getIndex().getImpl();

    auto getId = makeGetId(getQec(kb)->getIndex());
    Id a = getId("<a>");
    Id b = getId("<b>");
    Id c = getId("<c>");
    Id a2 = getId("<a2>");
    Id b2 = getId("<b2>");
    Id c2 = getId("<c2>");

    // TODO<joka921> We could also test the multiplicities here.
    ASSERT_TRUE(index._PSO.metaData().col0IdExists(b));
    ASSERT_TRUE(index._PSO.metaData().col0IdExists(b2));
    ASSERT_FALSE(index._PSO.metaData().col0IdExists(a));
    ASSERT_FALSE(index._PSO.metaData().col0IdExists(c));
    ASSERT_FALSE(index._PSO.metaData().col0IdExists(
        Id::makeFromVocabIndex(VocabIndex::make(735))));
    ASSERT_FALSE(index._PSO.metaData().getMetaData(b).isFunctional());
    ASSERT_TRUE(index._PSO.metaData().getMetaData(b2).isFunctional());

    ASSERT_TRUE(index.POS().metaData().col0IdExists(b));
    ASSERT_TRUE(index.POS().metaData().col0IdExists(b2));
    ASSERT_FALSE(index.POS().metaData().col0IdExists(a));
    ASSERT_FALSE(index.POS().metaData().col0IdExists(c));
    ASSERT_TRUE(index.POS().metaData().getMetaData(b).isFunctional());
    ASSERT_TRUE(index.POS().metaData().getMetaData(b2).isFunctional());

    // Relation b
    // Pair index
    auto testTwo = makeTestScanWidthTwo(index);
    testTwo("<b>", index.PSO(), {{a, c}, {a, c2}});
    std::vector<std::array<Id, 2>> buffer;

    // Relation b2
    testTwo("<b2>", index.PSO(), {{a, c}, {a2, c2}});

    {
      // Test for a previous bug in the scan of two fixed elements: An assertion
      // wrongly failed if the first Id existed in the permutation, but no
      // compressed block was found via binary search that could possibly
      // contain the combination of the ids. In this example <b2> is the largest
      // predicate that occurs and <c2> is larger than the largest subject that
      // appears with <b2>.
      auto testOne = makeTestScanWidthOne(index);
      testOne("<b2>", "<c2>", index.PSO(), {});
    }
  }
  {
    std::string kb =
        "<a> <is-a> <1> .\n"
        "<a> <is-a> <2> .\n"
        "<a> <is-a> <0> .\n"
        "<b> <is-a> <3> .\n"
        "<b> <is-a> <0> .\n"
        "<c> <is-a> <1> .\n"
        "<c> <is-a> <2> .\n";

    const IndexImpl& index = getQec(kb)->getIndex().getImpl();

    auto getId = makeGetId(getQec(kb)->getIndex());
    Id zero = getId("<0>");
    Id one = getId("<1>");
    Id two = getId("<2>");
    Id three = getId("<3>");
    Id a = getId("<a>");
    Id b = getId("<b>");
    Id c = getId("<c>");
    Id isA = getId("<is-a>");

    ASSERT_TRUE(index._PSO.metaData().col0IdExists(isA));
    ASSERT_FALSE(index._PSO.metaData().col0IdExists(a));

    ASSERT_FALSE(index._PSO.metaData().getMetaData(isA).isFunctional());

    ASSERT_TRUE(index.POS().metaData().col0IdExists(isA));
    ASSERT_FALSE(index.POS().metaData().getMetaData(isA).isFunctional());

    auto testTwo = makeTestScanWidthTwo(index);
    testTwo("<is-a>", index.PSO(),
            {{a, zero},
             {a, one},
             {a, two},
             {b, zero},
             {b, three},
             {c, one},
             {c, two}});

    // is-a for POS
    testTwo("<is-a>", index.POS(),
            {{zero, a},
             {zero, b},
             {one, a},
             {one, c},
             {two, a},
             {two, c},
             {three, b}});
  }
}

TEST(CreatePatterns, createPatterns) {
  {
    std::string kb =
        "<a>  <b>  <c>  .\n"
        "<a>  <b>  <c2> .\n"
        "<a>  <b2> <c>  .\n"
        "<a2> <b2> <c2> .\n"
        "<a2> <d>  <c2> .";

    const IndexImpl& index = getQec(kb)->getIndex().getImpl();

    ASSERT_EQ(2u, index.getHasPattern().size());
    ASSERT_EQ(0u, index.getHasPredicate().size());
    std::vector<VocabIndex> p0;
    std::vector<VocabIndex> p1;
    VocabIndex idx;
    // Pattern p0 (for subject <a>) consists of <b> and <b2)
    ASSERT_TRUE(index.getVocab().getId("<b>", &idx));
    p0.push_back(idx);
    ASSERT_TRUE(index.getVocab().getId("<b2>", &idx));
    p0.push_back(idx);

    // Pattern p1 (for subject <as>) consists of <b2> and <d>)
    p1.push_back(idx);
    ASSERT_TRUE(index.getVocab().getId("<d>", &idx));
    p1.push_back(idx);

    auto checkPattern = [](const auto& expected, const auto& actual) {
      for (size_t i = 0; i < actual.size(); i++) {
        ASSERT_EQ(Id::makeFromVocabIndex(expected[i]), actual[i]);
      }
    };

    ASSERT_TRUE(index.getVocab().getId("<a>", &idx));
    LOG(INFO) << idx << std::endl;
    for (size_t i = 0; i < index.getHasPattern().size(); ++i) {
      LOG(INFO) << index.getHasPattern()[i] << std::endl;
    }
    checkPattern(p0, index.getPatterns()[index.getHasPattern()[idx.get()]]);
    ASSERT_TRUE(index.getVocab().getId("<a2>", &idx));
    checkPattern(p1, index.getPatterns()[index.getHasPattern()[idx.get()]]);
  }
}

TEST(IndexTest, createFromOnDiskIndexTest) {
  std::string kb =
      "<a>  <b>  <c>  .\n"
      "<a>  <b>  <c2> .\n"
      "<a>  <b2> <c>  .\n"
      "<a2> <b2> <c2> .";
  const IndexImpl& index = getQec(kb)->getIndex().getImpl();

  auto getId = makeGetId(getQec(kb)->getIndex());
  Id b = getId("<b>");
  Id b2 = getId("<b2>");
  Id a = getId("<a>");
  Id c = getId("<c>");

  ASSERT_TRUE(index.PSO().metaData().col0IdExists(b));
  ASSERT_TRUE(index.PSO().metaData().col0IdExists(b2));
  ASSERT_FALSE(index.PSO().metaData().col0IdExists(a));
  ASSERT_FALSE(index.PSO().metaData().col0IdExists(c));
  ASSERT_FALSE(index.PSO().metaData().getMetaData(b).isFunctional());
  ASSERT_TRUE(index.PSO().metaData().getMetaData(b2).isFunctional());

  ASSERT_TRUE(index.POS().metaData().col0IdExists(b));
  ASSERT_TRUE(index.POS().metaData().col0IdExists(b2));
  ASSERT_FALSE(index.POS().metaData().col0IdExists(a));
  ASSERT_FALSE(index.POS().metaData().col0IdExists(c));
  ASSERT_TRUE(index.POS().metaData().getMetaData(b).isFunctional());
  ASSERT_TRUE(index.POS().metaData().getMetaData(b2).isFunctional());
};

TEST(IndexTest, scanTest) {
  std::string kb =
      "<a>  <b>  <c>  . \n"
      "<a>  <b>  <c2> . \n"
      "<a>  <b2> <c>  . \n"
      "<a2> <b2> <c2> .   ";
  {
    const IndexImpl& index = getQec(kb)->getIndex().getImpl();

    IdTable wol(1, makeAllocator());
    IdTable wtl(2, makeAllocator());

    auto getId = makeGetId(getQec(kb)->getIndex());
    Id a = getId("<a>");
    Id c = getId("<c>");
    Id a2 = getId("<a2>");
    Id c2 = getId("<c2>");
    auto testTwo = makeTestScanWidthTwo(index);

    testTwo("<b>", index._PSO, {{a, c}, {a, c2}});
    testTwo("<x>", index._PSO, {});
    testTwo("<c>", index._PSO, {});
    testTwo("<b>", index._POS, {{c, a}, {c2, a}});
    testTwo("<x>", index._POS, {});
    testTwo("<c>", index._POS, {});

    auto testOne = makeTestScanWidthOne(index);

    testOne("<b>", "<a>", index._PSO, {{c}, {c2}});
    testOne("<b>", "<c>", index._PSO, {});
    testOne("<b2>", "<c2>", index._POS, {{a2}});
    testOne("<notExisting>", "<a>", index._PSO, {});
  }
  kb = "<a> <is-a> <1> . \n"
       "<a> <is-a> <2> . \n"
       "<a> <is-a> <0> . \n"
       "<b> <is-a> <3> . \n"
       "<b> <is-a> <0> . \n"
       "<c> <is-a> <1> . \n"
       "<c> <is-a> <2> . \n";

  {
    const IndexImpl& index =
        ad_utility::testing::getQec(kb)->getIndex().getImpl();

    auto getId = makeGetId(ad_utility::testing::getQec(kb)->getIndex());
    Id a = getId("<a>");
    Id b = getId("<b>");
    Id c = getId("<c>");
    Id zero = getId("<0>");
    Id one = getId("<1>");
    Id two = getId("<2>");
    Id three = getId("<3>");

    auto testTwo = makeTestScanWidthTwo(index);
    testTwo("<is-a>", index._PSO,
            {{{a, zero},
              {a, one},
              {a, two},
              {b, zero},
              {b, three},
              {c, one},
              {c, two}}});
    testTwo("<is-a>", index._POS,
            {{zero, a},
             {zero, b},
             {one, a},
             {one, c},
             {two, a},
             {two, c},
             {three, b}});

    auto testWidthOne = makeTestScanWidthOne(index);

    testWidthOne("<is-a>", "<0>", index._POS, {{a}, {b}});
    testWidthOne("<is-a>", "<1>", index._POS, {{a}, {c}});
    testWidthOne("<is-a>", "<2>", index._POS, {{a}, {c}});
    testWidthOne("<is-a>", "<3>", index._POS, {{b}});
    testWidthOne("<is-a>", "<a>", index._PSO, {{zero}, {one}, {two}});
    testWidthOne("<is-a>", "<b>", index._PSO, {{zero}, {three}});
    testWidthOne("<is-a>", "<c>", index._PSO, {{one}, {two}});
  }
};

// Returns true iff `arg` (the first argument of `EXPECT_THAT` below) holds a
// `PossiblyExternalizedIriOrLiteral` that matches the string `content` and the
// bool `isExternal`.
MATCHER_P2(IsPossiblyExternalString, content, isExternal, "") {
  if (!std::holds_alternative<PossiblyExternalizedIriOrLiteral>(arg)) {
    return false;
  }
  const auto& el = std::get<PossiblyExternalizedIriOrLiteral>(arg);
  return (el._iriOrLiteral == content) && (isExternal == el._isExternal);
}

TEST(IndexTest, TripleToInternalRepresentation) {
  {
    IndexImpl index;
    TurtleTriple turtleTriple{"<subject>", "<predicate>", lit("\"literal\"")};
    LangtagAndTriple res =
        index.tripleToInternalRepresentation(std::move(turtleTriple));
    ASSERT_TRUE(res._langtag.empty());
    EXPECT_THAT(res._triple[0], IsPossiblyExternalString("<subject>", false));
    EXPECT_THAT(res._triple[1], IsPossiblyExternalString("<predicate>", false));
    EXPECT_THAT(res._triple[2], IsPossiblyExternalString("\"literal\"", false));
  }
  {
    IndexImpl index;
    index.getNonConstVocabForTesting().initializeExternalizePrefixes(
        std::vector{"<subj"s});
    TurtleTriple turtleTriple{"<subject>", "<predicate>",
                              lit("\"literal\"", "@fr")};
    LangtagAndTriple res =
        index.tripleToInternalRepresentation(std::move(turtleTriple));
    ASSERT_EQ(res._langtag, "fr");
    EXPECT_THAT(res._triple[0], IsPossiblyExternalString("<subject>", true));
    EXPECT_THAT(res._triple[1], IsPossiblyExternalString("<predicate>", false));
    // By default all languages other than English are externalized.
    EXPECT_THAT(res._triple[2],
                IsPossiblyExternalString("\"literal\"@fr", true));
  }
  {
    IndexImpl index;
    TurtleTriple turtleTriple{"<subject>", "<predicate>", 42.0};
    LangtagAndTriple res =
        index.tripleToInternalRepresentation(std::move(turtleTriple));
    ASSERT_EQ(Id::makeFromDouble(42.0), std::get<Id>(res._triple[2]));
  }
}

TEST(IndexTest, getIgnoredIdRanges) {
  const IndexImpl& index = getQec()->getIndex().getImpl();

  // First manually get the IDs of the vocabulary elements that might appear
  // in added triples.
  auto getId = [&index](const std::string& s) {
    Id id;
    bool success = index.getId(s, &id);
    AD_CONTRACT_CHECK(success);
    return id;
  };

  Id qlLangtag = getId("<QLever-internal-function/langtag>");
  Id en = getId("<QLever-internal-function/@en>");
  Id enLabel = getId("@en@<label>");
  Id label = getId("<label>");
  Id firstLiteral = getId("\"A\"");
  Id lastLiteral = getId("\"zz\"@en");
  Id x = getId("<x>");

  auto increment = [](Id id) {
    return Id::makeFromVocabIndex(id.getVocabIndex().incremented());
  };

  // The range of all entities that start with "<QLever-internal-function/"
  auto internalEntities = std::pair{en, increment(qlLangtag)};
  // The range of all entities that start with @ (like `@en@<label>`)
  auto predicatesWithLangtag = std::pair{enLabel, increment(enLabel)};
  // The range of all literals;
  auto literals = std::pair{firstLiteral, increment(lastLiteral)};

  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Index::Permutation::POS);
    ASSERT_FALSE(lambda(std::array{label, firstLiteral, x}));

    // Note: The following triple is added, but it should be filtered out via
    // the ranges and not via the lambda, because it can be retrieved using the
    // `ranges`.
    ASSERT_FALSE(lambda(std::array{enLabel, firstLiteral, x}));
    ASSERT_FALSE(lambda(std::array{x, x, x}));
    ASSERT_EQ(2u, ranges.size());

    ASSERT_EQ(ranges[0], internalEntities);
    ASSERT_EQ(ranges[1], predicatesWithLangtag);
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Index::Permutation::PSO);
    ASSERT_FALSE(lambda(std::array{label, x, firstLiteral}));

    // Note: The following triple is added, but it should be filtered out via
    // the ranges and not via the lambda, because it can be retrieved using the
    // `ranges`.
    ASSERT_FALSE(lambda(std::array{enLabel, x, firstLiteral}));
    ASSERT_FALSE(lambda(std::array{x, x, x}));
    ASSERT_EQ(2u, ranges.size());

    ASSERT_EQ(ranges[0], internalEntities);
    ASSERT_EQ(ranges[1], predicatesWithLangtag);
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Index::Permutation::SOP);
    ASSERT_TRUE(lambda(std::array{x, firstLiteral, enLabel}));
    ASSERT_FALSE(lambda(std::array{x, firstLiteral, label}));
    ASSERT_FALSE(lambda(std::array{x, x, label}));
    ASSERT_EQ(2u, ranges.size());

    ASSERT_EQ(ranges[0], internalEntities);
    ASSERT_EQ(ranges[1], literals);
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Index::Permutation::SPO);
    ASSERT_TRUE(lambda(std::array{x, enLabel, firstLiteral}));
    ASSERT_FALSE(lambda(std::array{x, label, firstLiteral}));
    ASSERT_FALSE(lambda(std::array{x, label, x}));
    ASSERT_EQ(2u, ranges.size());

    ASSERT_EQ(ranges[0], internalEntities);
    ASSERT_EQ(ranges[1], literals);
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Index::Permutation::OSP);
    ASSERT_TRUE(lambda(std::array{firstLiteral, x, enLabel}));
    ASSERT_FALSE(lambda(std::array{firstLiteral, x, label}));
    ASSERT_FALSE(lambda(std::array{x, x, label}));
    ASSERT_EQ(1u, ranges.size());
    ASSERT_EQ(ranges[0], internalEntities);
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Index::Permutation::OPS);
    ASSERT_TRUE(lambda(std::array{firstLiteral, enLabel, x}));
    ASSERT_FALSE(lambda(std::array{firstLiteral, label, x}));
    ASSERT_FALSE(lambda(std::array{x, label, x}));
    ASSERT_EQ(1u, ranges.size());
    ASSERT_EQ(ranges[0], internalEntities);
  }
}

TEST(IndexTest, NumDistinctEntities) {
  const IndexImpl& index = getQec()->getIndex().getImpl();
  // Note: Those numbers might change as the triples of the test index in
  // `IndexTestHelpers.cpp` change.
  // TODO<joka921> Also check the number of triples and the number of
  // added things.
  auto subjects = index.numDistinctSubjects();
  EXPECT_EQ(subjects.normal_, 3);
  // All literals with language tags are added subjects.
  EXPECT_EQ(subjects.internal_, 1);

  auto predicates = index.numDistinctPredicates();
  EXPECT_EQ(predicates.normal_, 2);
  // One added predicate is `ql:langtag` and one added predicate for
  // each combination of predicate+language that is actually used (e.g.
  // `@en@label`).
  EXPECT_EQ(predicates.internal_, 2);

  auto objects = index.numDistinctObjects();
  EXPECT_EQ(objects.normal_, 7);
  // One added object for each language that is used
  EXPECT_EQ(objects.internal_, 1);

  auto numTriples = index.numTriples();
  EXPECT_EQ(numTriples.normal_, 7);
  // Two added triples for each triple that has an object with a language tag.
  EXPECT_EQ(numTriples.internal_, 2);
}
