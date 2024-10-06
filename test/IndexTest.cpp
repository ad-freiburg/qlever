// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "./util/GTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "global/Pattern.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "util/IndexTestHelpers.h"

using namespace ad_utility::testing;

using ::testing::UnorderedElementsAre;

namespace {
using ad_utility::source_location;
auto lit = ad_utility::testing::tripleComponentLiteral;

// Return a lambda that runs a scan for two fixed elements `c0` and `c1`
// on the `permutation` (e.g. a fixed P and S in the PSO permutation)
// of the `index` and checks whether the result of the
// scan matches `expected`.
auto makeTestScanWidthOne = [](const IndexImpl& index) {
  return [&index](const TripleComponent& c0, const TripleComponent& c1,
                  Permutation::Enum permutation, const VectorTable& expected,
                  Permutation::ColumnIndices additionalColumns = {},
                  ad_utility::source_location l =
                      ad_utility::source_location::current()) {
    auto t = generateLocationTrace(l);
    IdTable result =
        index.scan({c0, c1, std::nullopt}, permutation, additionalColumns,
                   std::make_shared<ad_utility::CancellationHandle<>>());
    ASSERT_EQ(result.numColumns(), 1 + additionalColumns.size());
    ASSERT_EQ(result, makeIdTableFromVector(expected));
  };
};
// Return a lambda that runs a scan for a fixed element `c0`
// on the `permutation` (e.g. a fixed P in the PSO permutation)
// of the `index` and checks whether the result of the
// scan matches `expected`.
auto makeTestScanWidthTwo = [](const IndexImpl& index) {
  return [&index](const TripleComponent& c0, Permutation::Enum permutation,
                  const VectorTable& expected,
                  ad_utility::source_location l =
                      ad_utility::source_location::current()) {
    auto t = generateLocationTrace(l);
    IdTable wol =
        index.scan({c0, std::nullopt, std::nullopt}, permutation,
                   Permutation::ColumnIndicesRef{},
                   std::make_shared<ad_utility::CancellationHandle<>>());
    ASSERT_EQ(wol, makeIdTableFromVector(expected));
  };
};
}  // namespace

TEST(IndexTest, createFromTurtleTest) {
  auto runTest = [](bool loadAllPermutations, bool loadPatterns) {
    {
      std::string kb =
          "<a>  <b>  <c> . \n"
          "<a>  <b>  <c2> .\n"
          "<a>  <b2> <c> .\n"
          "<a2> <b2> <c2> .";

      auto getIndex = [&]() -> decltype(auto) {
        [[maybe_unused]] decltype(auto) ref =
            getQec(kb, loadAllPermutations, loadPatterns)->getIndex().getImpl();
        return ref;
      };
      if (!loadAllPermutations && loadPatterns) {
        AD_EXPECT_THROW_WITH_MESSAGE(
            getIndex(),
            ::testing::HasSubstr(
                "patterns can only be built when all 6 permutations"));
        return;
      }
      const IndexImpl& index = getIndex();

      auto getId = makeGetId(getQec(kb)->getIndex());
      Id a = getId("<a>");
      Id b = getId("<b>");
      Id c = getId("<c>");
      Id a2 = getId("<a2>");
      Id b2 = getId("<b2>");
      Id c2 = getId("<c2>");

      // TODO<joka921> We could also test the multiplicities here.
      ASSERT_TRUE(index.PSO().getMetadata(b).has_value());
      ASSERT_TRUE(index.PSO().getMetadata(b2).has_value());
      ASSERT_FALSE(index.PSO().getMetadata(a2).has_value());
      ASSERT_FALSE(index.PSO().getMetadata(c).has_value());
      ASSERT_FALSE(
          index.PSO()
              .getMetadata(Id::makeFromVocabIndex(VocabIndex::make(735)))
              .has_value());
      ASSERT_FALSE(index.PSO().getMetadata(b).value().isFunctional());
      ASSERT_TRUE(index.PSO().getMetadata(b2).value().isFunctional());

      ASSERT_TRUE(index.POS().getMetadata(b).has_value());
      ASSERT_TRUE(index.POS().getMetadata(b2).has_value());
      ASSERT_FALSE(index.POS().getMetadata(a2).has_value());
      ASSERT_FALSE(index.POS().getMetadata(c).has_value());
      ASSERT_FALSE(
          index.POS()
              .getMetadata(Id::makeFromVocabIndex(VocabIndex::make(735)))
              .has_value());
      ASSERT_TRUE(index.POS().getMetadata(b).value().isFunctional());
      ASSERT_TRUE(index.POS().getMetadata(b2).value().isFunctional());

      // Relation b
      // Pair index
      auto testTwo = makeTestScanWidthTwo(index);
      testTwo(iri("<b>"), Permutation::PSO, {{a, c}, {a, c2}});
      std::vector<std::array<Id, 2>> buffer;

      // Relation b2
      testTwo(iri("<b2>"), Permutation::PSO, {{a, c}, {a2, c2}});

      {
        // Test for a previous bug in the scan of two fixed elements: An
        // assertion wrongly failed if the first Id existed in the permutation,
        // but no compressed block was found via binary search that could
        // possibly contain the combination of the ids. In this example <b2> is
        // the largest predicate that occurs and <c2> is larger than the largest
        // subject that appears with <b2>.
        auto testOne = makeTestScanWidthOne(index);
        testOne(iri("<b2>"), iri("<c2>"), Permutation::PSO, {});
        // An empty scan result must still have the correct number of columns.
        testOne(iri("<notExisting>"), iri("<alsoNotExisting>"),
                Permutation::PSO, {},
                {ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN});
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

      ASSERT_TRUE(index.PSO().getMetadata(isA).has_value());
      ASSERT_FALSE(index.PSO().getMetadata(a).has_value());

      ASSERT_FALSE(index.PSO().getMetadata(isA).value().isFunctional());

      ASSERT_TRUE(index.POS().getMetadata(isA).has_value());
      ASSERT_FALSE(index.POS().getMetadata(isA).value().isFunctional());

      auto testTwo = makeTestScanWidthTwo(index);
      testTwo(iri("<is-a>"), Permutation::PSO,
              {{a, zero},
               {a, one},
               {a, two},
               {b, zero},
               {b, three},
               {c, one},
               {c, two}});

      // is-a for POS
      testTwo(iri("<is-a>"), Permutation::POS,
              {{zero, a},
               {zero, b},
               {one, a},
               {one, c},
               {two, a},
               {two, c},
               {three, b}});
    }
  };
  runTest(true, true);
  runTest(true, false);
  runTest(false, false);
  runTest(false, true);
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

  ASSERT_TRUE(index.PSO().getMetadata(b).has_value());
  ASSERT_TRUE(index.PSO().getMetadata(b2).has_value());
  ASSERT_FALSE(index.PSO().getMetadata(a).has_value());
  ASSERT_FALSE(index.PSO().getMetadata(c).has_value());
  ASSERT_FALSE(index.PSO().getMetadata(b).value().isFunctional());
  ASSERT_TRUE(index.PSO().getMetadata(b2).value().isFunctional());

  ASSERT_TRUE(index.POS().getMetadata(b).has_value());
  ASSERT_TRUE(index.POS().getMetadata(b2).has_value());
  ASSERT_FALSE(index.POS().getMetadata(a).has_value());
  ASSERT_FALSE(index.POS().getMetadata(c).has_value());
  ASSERT_TRUE(index.POS().getMetadata(b).value().isFunctional());
  ASSERT_TRUE(index.POS().getMetadata(b2).value().isFunctional());
};

TEST(IndexTest, indexId) {
  std::string kb =
      "<a1> <b> <c1> .\n"
      "<a2> <b> <c2> .\n"
      "<a2> <b> <c1> .\n"
      "<a3> <b> <c2> .";
  // Build index with all permutations (arg 2) and no patterns (arg 3). That
  // way, we get four triples, two distinct subjects, one distinct predicate
  // and two distinct objects.
  const Index& index = getQec(kb, true, false)->getIndex();
  ASSERT_EQ(index.getIndexId(), "#.4.3.1.2");
}

TEST(IndexTest, scanTest) {
  auto testWithAndWithoutPrefixCompression = [](bool useCompression) {
    using enum Permutation::Enum;
    std::string kb =
        "<a>  <b>  <c>  . \n"
        "<a>  <b>  <c2> . \n"
        "<a>  <b2> <c>  . \n"
        "<a2> <b2> <c2> .   ";
    {
      const IndexImpl& index =
          getQec(kb, true, true, useCompression)->getIndex().getImpl();

      IdTable wol(1, makeAllocator());
      IdTable wtl(2, makeAllocator());

      auto getId = makeGetId(getQec(kb)->getIndex());
      Id a = getId("<a>");
      Id c = getId("<c>");
      Id a2 = getId("<a2>");
      Id c2 = getId("<c2>");
      auto testTwo = makeTestScanWidthTwo(index);

      testTwo(iri("<b>"), PSO, {{a, c}, {a, c2}});
      testTwo(iri("<x>"), PSO, {});
      testTwo(iri("<c>"), PSO, {});
      testTwo(iri("<b>"), POS, {{c, a}, {c2, a}});
      testTwo(iri("<x>"), POS, {});
      testTwo(iri("<c>"), POS, {});

      auto testOne = makeTestScanWidthOne(index);

      testOne(iri("<b>"), iri("<a>"), PSO, {{c}, {c2}});
      testOne(iri("<b>"), iri("<c>"), PSO, {});
      testOne(iri("<b2>"), iri("<c2>"), POS, {{a2}});
      testOne(iri("<notExisting>"), iri("<a>"), PSO, {});
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
          ad_utility::testing::getQec(kb, true, true, useCompression)
              ->getIndex()
              .getImpl();

      auto getId = makeGetId(ad_utility::testing::getQec(kb)->getIndex());
      Id a = getId("<a>");
      Id b = getId("<b>");
      Id c = getId("<c>");
      Id zero = getId("<0>");
      Id one = getId("<1>");
      Id two = getId("<2>");
      Id three = getId("<3>");

      auto testTwo = makeTestScanWidthTwo(index);
      testTwo(iri("<is-a>"), PSO,
              {{{a, zero},
                {a, one},
                {a, two},
                {b, zero},
                {b, three},
                {c, one},
                {c, two}}});
      testTwo(iri("<is-a>"), POS,
              {{zero, a},
               {zero, b},
               {one, a},
               {one, c},
               {two, a},
               {two, c},
               {three, b}});

      auto testWidthOne = makeTestScanWidthOne(index);

      testWidthOne(iri("<is-a>"), iri("<0>"), POS, {{a}, {b}});
      testWidthOne(iri("<is-a>"), iri("<1>"), POS, {{a}, {c}});
      testWidthOne(iri("<is-a>"), iri("<2>"), POS, {{a}, {c}});
      testWidthOne(iri("<is-a>"), iri("<3>"), POS, {{b}});
      testWidthOne(iri("<is-a>"), iri("<a>"), PSO, {{zero}, {one}, {two}});
      testWidthOne(iri("<is-a>"), iri("<b>"), PSO, {{zero}, {three}});
      testWidthOne(iri("<is-a>"), iri("<c>"), PSO, {{one}, {two}});
    }
  };
  testWithAndWithoutPrefixCompression(true);
  testWithAndWithoutPrefixCompression(false);
};

// ______________________________________________________________
TEST(IndexTest, emptyIndex) {
  const IndexImpl& emptyIndexWithCompression =
      getQec("", true, true, true)->getIndex().getImpl();
  const IndexImpl& emptyIndexWithoutCompression =
      getQec("", true, true, false)->getIndex().getImpl();

  EXPECT_EQ(emptyIndexWithCompression.numTriples().normal, 0u);
  EXPECT_EQ(emptyIndexWithoutCompression.numTriples().normal, 0u);
  EXPECT_EQ(emptyIndexWithCompression.numTriples().internal, 0u);
  EXPECT_EQ(emptyIndexWithoutCompression.numTriples().internal, 0u);
  auto test = makeTestScanWidthTwo(emptyIndexWithCompression);
  // Test that scanning an empty index works, but yields an empty permutation.
  test(iri("<x>"), Permutation::PSO, {});
}

// Returns true iff `arg` (the first argument of `EXPECT_THAT` below) holds a
// `PossiblyExternalizedIriOrLiteral` that matches the string `content` and the
// bool `isExternal`.

auto IsPossiblyExternalString = [](TripleComponent content, bool isExternal) {
  return ::testing::VariantWith<PossiblyExternalizedIriOrLiteral>(
      ::testing::AllOf(AD_FIELD(PossiblyExternalizedIriOrLiteral, iriOrLiteral_,
                                ::testing::Eq(content)),
                       AD_FIELD(PossiblyExternalizedIriOrLiteral, isExternal_,
                                ::testing::Eq(isExternal))));
};

TEST(IndexTest, TripleToInternalRepresentation) {
  {
    IndexImpl index{ad_utility::makeUnlimitedAllocator<Id>()};
    TurtleTriple turtleTriple{iri("<subject>"), iri("<predicate>"),
                              lit("\"literal\"")};
    LangtagAndTriple res =
        index.tripleToInternalRepresentation(std::move(turtleTriple));
    EXPECT_TRUE(res.langtag_.empty());
    EXPECT_THAT(res.triple_[0],
                IsPossiblyExternalString(iri("<subject>"), false));
    EXPECT_THAT(res.triple_[1],
                IsPossiblyExternalString(iri("<predicate>"), false));
    EXPECT_THAT(res.triple_[2],
                IsPossiblyExternalString(lit("\"literal\""), false));
  }
  {
    IndexImpl index{ad_utility::makeUnlimitedAllocator<Id>()};
    index.getNonConstVocabForTesting().initializeExternalizePrefixes(
        std::vector{"<subj"s});
    TurtleTriple turtleTriple{iri("<subject>"), iri("<predicate>"),
                              lit("\"literal\"", "@fr")};
    LangtagAndTriple res =
        index.tripleToInternalRepresentation(std::move(turtleTriple));
    EXPECT_EQ(res.langtag_, "fr");
    EXPECT_THAT(res.triple_[0],
                IsPossiblyExternalString(iri("<subject>"), true));
    EXPECT_THAT(res.triple_[1],
                IsPossiblyExternalString(iri("<predicate>"), false));
    // By default all languages other than English are externalized.
    EXPECT_THAT(res.triple_[2],
                IsPossiblyExternalString(lit("\"literal\"", "@fr"), true));
  }
  {
    IndexImpl index{ad_utility::makeUnlimitedAllocator<Id>()};
    TurtleTriple turtleTriple{iri("<subject>"), iri("<predicate>"), 42.0};
    LangtagAndTriple res =
        index.tripleToInternalRepresentation(std::move(turtleTriple));
    EXPECT_EQ(Id::makeFromDouble(42.0), std::get<Id>(res.triple_[2]));
  }
}

TEST(IndexTest, NumDistinctEntities) {
  std::string turtleInput =
      "<x> <label> \"alpha\" . <x> <label> \"älpha\" . <x> <label> \"A\" . "
      "<x> "
      "<label> \"Beta\". <x> <is-a> <y>. <y> <is-a> <x>. <z> <label> "
      "\"zz\"@en";
  const IndexImpl& index = getQec(turtleInput)->getIndex().getImpl();
  // Note: Those numbers might change as the triples of the test index in
  // `IndexTestHelpers.cpp` change.
  // TODO<joka921> Also check the number of triples and the number of
  // added things.
  auto numSubjects = index.numDistinctSubjects();
  EXPECT_EQ(numSubjects.normal, 3);
  // All literals with language tags are added numSubjects.
  EXPECT_EQ(numSubjects, index.numDistinctCol0(Permutation::SPO));
  EXPECT_EQ(numSubjects, index.numDistinctCol0(Permutation::SOP));

  auto numPredicates = index.numDistinctPredicates();
  EXPECT_EQ(numPredicates.normal, 2);
  // The added numPredicates are `ql:has-pattern`, `ql:langtag`, and one added
  // predicate for each combination of predicate+language that is actually used
  // (e.g. `@en@label`).
  EXPECT_EQ(numPredicates.internal, 3);
  EXPECT_EQ(numPredicates, index.numDistinctCol0(Permutation::PSO));
  EXPECT_EQ(numPredicates, index.numDistinctCol0(Permutation::POS));

  auto objects = index.numDistinctObjects();
  EXPECT_EQ(objects.normal, 7);
  EXPECT_EQ(objects, index.numDistinctCol0(Permutation::OSP));
  EXPECT_EQ(objects, index.numDistinctCol0(Permutation::OPS));

  auto numTriples = index.numTriples();
  EXPECT_EQ(numTriples.normal, 7);
  // Two added triples for each triple that has an object with a language tag
  // and one triple per subject for the pattern.
  EXPECT_EQ(numTriples.internal, 5);

  auto multiplicities = index.getMultiplicities(Permutation::SPO);
  // 7 triples, three distinct numSubjects, 2 distinct numPredicates, 7 distinct
  // objects.
  EXPECT_FLOAT_EQ(multiplicities[0], 7.0 / 3.0);
  EXPECT_FLOAT_EQ(multiplicities[1], 7.0 / 2.0);
  EXPECT_FLOAT_EQ(multiplicities[2], 7.0 / 7.0);

  multiplicities = index.getMultiplicities(iri("<x>"), Permutation::SPO);
  EXPECT_FLOAT_EQ(multiplicities[0], 2.5);
  EXPECT_FLOAT_EQ(multiplicities[1], 1);
}

TEST(IndexTest, NumDistinctEntitiesCornerCases) {
  const IndexImpl& index = getQec("", false, false)->getIndex().getImpl();
  AD_EXPECT_THROW_WITH_MESSAGE(index.numDistinctSubjects(),
                               ::testing::ContainsRegex("if all 6"));
  AD_EXPECT_THROW_WITH_MESSAGE(index.numDistinctObjects(),
                               ::testing::ContainsRegex("if all 6"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      index.numDistinctCol0(static_cast<Permutation::Enum>(42)),
      ::testing::ContainsRegex("should be unreachable"));

  const IndexImpl& indexNoPatterns =
      getQec("", true, false)->getIndex().getImpl();
  AD_EXPECT_THROW_WITH_MESSAGE(
      indexNoPatterns.getAvgNumDistinctPredicatesPerSubject(),
      ::testing::ContainsRegex("requires a loaded patterns file"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      indexNoPatterns.getAvgNumDistinctSubjectsPerPredicate(),
      ::testing::ContainsRegex("requires a loaded patterns file"));
}

TEST(IndexTest, getPermutation) {
  using enum Permutation::Enum;
  const IndexImpl& index = getQec()->getIndex().getImpl();
  EXPECT_EQ(&index.PSO(), &index.getPermutation(PSO));
  EXPECT_EQ(&index.POS(), &index.getPermutation(POS));
  EXPECT_EQ(&index.SOP(), &index.getPermutation(SOP));
  EXPECT_EQ(&index.SPO(), &index.getPermutation(SPO));
  EXPECT_EQ(&index.OPS(), &index.getPermutation(OPS));
  EXPECT_EQ(&index.OSP(), &index.getPermutation(OSP));
}

TEST(IndexTest, trivialGettersAndSetters) {
  Index index{ad_utility::makeUnlimitedAllocator<Id>()};
  index.memoryLimitIndexBuilding() = 7_kB;
  EXPECT_EQ(index.memoryLimitIndexBuilding(), 7_kB);
  EXPECT_EQ(std::as_const(index).memoryLimitIndexBuilding(), 7_kB);
}
