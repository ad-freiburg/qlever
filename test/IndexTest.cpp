// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "./IndexTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "global/Pattern.h"
#include "index/Index.h"
#include "index/IndexImpl.h"

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
  return [&index](const std::string& c0, const std::string& c1,
                  Permutation::Enum permutation, const VectorTable& expected,
                  ad_utility::source_location l =
                      ad_utility::source_location::current()) {
    auto t = generateLocationTrace(l);
    TripleComponent c1Tc{c1};
    IdTable result = index.scan(
        c0, std::cref(c1Tc), permutation, Permutation::ColumnIndicesRef{},
        std::make_shared<ad_utility::CancellationHandle<>>());
    ASSERT_EQ(result, makeIdTableFromVector(expected));
  };
};

// Return a lambda that runs a scan for a fixed element `c0`
// on the `permutation` (e.g. a fixed P in the PSO permutation)
// of the `index` and checks whether the result of the
// scan matches `expected`.
auto makeTestScanWidthTwo = [](const IndexImpl& index) {
  return [&index](const std::string& c0, Permutation::Enum permutation,
                  const VectorTable& expected,
                  ad_utility::source_location l =
                      ad_utility::source_location::current()) {
    auto t = generateLocationTrace(l);
    IdTable wol = index.scan(
        c0, std::nullopt, permutation, Permutation::ColumnIndicesRef{},
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
      ASSERT_TRUE(index.PSO().metaData().col0IdExists(b));
      ASSERT_TRUE(index.PSO().metaData().col0IdExists(b2));
      ASSERT_FALSE(index.PSO().metaData().col0IdExists(a));
      ASSERT_FALSE(index.PSO().metaData().col0IdExists(c));
      ASSERT_FALSE(index.PSO().metaData().col0IdExists(
          Id::makeFromVocabIndex(VocabIndex::make(735))));
      ASSERT_FALSE(index.PSO().metaData().getMetaData(b).isFunctional());
      ASSERT_TRUE(index.PSO().metaData().getMetaData(b2).isFunctional());

      ASSERT_TRUE(index.POS().metaData().col0IdExists(b));
      ASSERT_TRUE(index.POS().metaData().col0IdExists(b2));
      ASSERT_FALSE(index.POS().metaData().col0IdExists(a));
      ASSERT_FALSE(index.POS().metaData().col0IdExists(c));
      ASSERT_TRUE(index.POS().metaData().getMetaData(b).isFunctional());
      ASSERT_TRUE(index.POS().metaData().getMetaData(b2).isFunctional());

      // Relation b
      // Pair index
      auto testTwo = makeTestScanWidthTwo(index);
      testTwo("<b>", Permutation::PSO, {{a, c}, {a, c2}});
      std::vector<std::array<Id, 2>> buffer;

      // Relation b2
      testTwo("<b2>", Permutation::PSO, {{a, c}, {a2, c2}});

      {
        // Test for a previous bug in the scan of two fixed elements: An
        // assertion wrongly failed if the first Id existed in the permutation,
        // but no compressed block was found via binary search that could
        // possibly contain the combination of the ids. In this example <b2> is
        // the largest predicate that occurs and <c2> is larger than the largest
        // subject that appears with <b2>.
        auto testOne = makeTestScanWidthOne(index);
        testOne("<b2>", "<c2>", Permutation::PSO, {});
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

      ASSERT_TRUE(index.PSO().metaData().col0IdExists(isA));
      ASSERT_FALSE(index.PSO().metaData().col0IdExists(a));

      ASSERT_FALSE(index.PSO().metaData().getMetaData(isA).isFunctional());

      ASSERT_TRUE(index.POS().metaData().col0IdExists(isA));
      ASSERT_FALSE(index.POS().metaData().getMetaData(isA).isFunctional());

      auto testTwo = makeTestScanWidthTwo(index);
      testTwo("<is-a>", Permutation::PSO,
              {{a, zero},
               {a, one},
               {a, two},
               {b, zero},
               {b, three},
               {c, one},
               {c, two}});

      // is-a for POS
      testTwo("<is-a>", Permutation::POS,
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

TEST(CreatePatterns, createPatterns) {
  {
    std::string kb =
        "<a>  <b>  <c>  .\n"
        "<a>  <b>  <c2> .\n"
        "<a>  <b2> <c>  .\n"
        "<a2> <b2> <c2> .\n"
        "<a2> <d>  <c2> .";

    const Index& indexNoImpl = getQec(kb)->getIndex();
    const IndexImpl& index = indexNoImpl.getImpl();

    ASSERT_EQ(2u, index.getHasPattern().size());
    ASSERT_EQ(0u, index.getHasPredicate().size());
    auto getId = ad_utility::testing::makeGetId(indexNoImpl);
    // Pattern p0 (for subject <a>) consists of <b> and <b2)
    std::vector<Id> p0{getId("<b>"), getId("<b2>")};
    // Pattern p1 (for subject <a2>) consists of <b2> and <d>)
    std::vector<Id> p1{getId("<b2>"), getId("<d>")};

    auto checkPattern = [&index](const auto& expected, Id subject) {
      PatternID patternIdx =
          index.getHasPattern()[subject.getVocabIndex().get()];
      const auto& actual = index.getPatterns()[patternIdx];
      for (size_t i = 0; i < actual.size(); i++) {
        ASSERT_EQ(expected[i], actual[i]);
      }
    };

    checkPattern(p0, getId("<a>"));
    checkPattern(p1, getId("<a2>"));
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

      testTwo("<b>", PSO, {{a, c}, {a, c2}});
      testTwo("<x>", PSO, {});
      testTwo("<c>", PSO, {});
      testTwo("<b>", POS, {{c, a}, {c2, a}});
      testTwo("<x>", POS, {});
      testTwo("<c>", POS, {});

      auto testOne = makeTestScanWidthOne(index);

      testOne("<b>", "<a>", PSO, {{c}, {c2}});
      testOne("<b>", "<c>", PSO, {});
      testOne("<b2>", "<c2>", POS, {{a2}});
      testOne("<notExisting>", "<a>", PSO, {});
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
      testTwo("<is-a>", PSO,
              {{{a, zero},
                {a, one},
                {a, two},
                {b, zero},
                {b, three},
                {c, one},
                {c, two}}});
      testTwo("<is-a>", POS,
              {{zero, a},
               {zero, b},
               {one, a},
               {one, c},
               {two, a},
               {two, c},
               {three, b}});

      auto testWidthOne = makeTestScanWidthOne(index);

      testWidthOne("<is-a>", "<0>", POS, {{a}, {b}});
      testWidthOne("<is-a>", "<1>", POS, {{a}, {c}});
      testWidthOne("<is-a>", "<2>", POS, {{a}, {c}});
      testWidthOne("<is-a>", "<3>", POS, {{b}});
      testWidthOne("<is-a>", "<a>", PSO, {{zero}, {one}, {two}});
      testWidthOne("<is-a>", "<b>", PSO, {{zero}, {three}});
      testWidthOne("<is-a>", "<c>", PSO, {{one}, {two}});
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

  EXPECT_EQ(emptyIndexWithCompression.numTriples().normal_, 0u);
  EXPECT_EQ(emptyIndexWithoutCompression.numTriples().normal_, 0u);
  EXPECT_EQ(emptyIndexWithCompression.numTriples().internal_, 0u);
  EXPECT_EQ(emptyIndexWithoutCompression.numTriples().internal_, 0u);
  auto test = makeTestScanWidthTwo(emptyIndexWithCompression);
  // Test that scanning an empty index works, but yields an empty permutation.
  test("<x>", Permutation::PSO, {});
}

// Returns true iff `arg` (the first argument of `EXPECT_THAT` below) holds a
// `PossiblyExternalizedIriOrLiteral` that matches the string `content` and the
// bool `isExternal`.
MATCHER_P2(IsPossiblyExternalString, content, isExternal, "") {
  if (!std::holds_alternative<PossiblyExternalizedIriOrLiteral>(arg)) {
    return false;
  }
  const auto& el = std::get<PossiblyExternalizedIriOrLiteral>(arg);
  return (el.iriOrLiteral_ == content) && (isExternal == el.isExternal_);
}

TEST(IndexTest, TripleToInternalRepresentation) {
  {
    IndexImpl index{ad_utility::makeUnlimitedAllocator<Id>()};
    TurtleTriple turtleTriple{"<subject>", "<predicate>", lit("\"literal\"")};
    LangtagAndTriple res =
        index.tripleToInternalRepresentation(std::move(turtleTriple));
    ASSERT_TRUE(res.langtag_.empty());
    EXPECT_THAT(res.triple_[0], IsPossiblyExternalString("<subject>", false));
    EXPECT_THAT(res.triple_[1], IsPossiblyExternalString("<predicate>", false));
    EXPECT_THAT(res.triple_[2], IsPossiblyExternalString("\"literal\"", false));
  }
  {
    IndexImpl index{ad_utility::makeUnlimitedAllocator<Id>()};
    index.getNonConstVocabForTesting().initializeExternalizePrefixes(
        std::vector{"<subj"s});
    TurtleTriple turtleTriple{"<subject>", "<predicate>",
                              lit("\"literal\"", "@fr")};
    LangtagAndTriple res =
        index.tripleToInternalRepresentation(std::move(turtleTriple));
    ASSERT_EQ(res.langtag_, "fr");
    EXPECT_THAT(res.triple_[0], IsPossiblyExternalString("<subject>", true));
    EXPECT_THAT(res.triple_[1], IsPossiblyExternalString("<predicate>", false));
    // By default all languages other than English are externalized.
    EXPECT_THAT(res.triple_[2],
                IsPossiblyExternalString("\"literal\"@fr", true));
  }
  {
    IndexImpl index{ad_utility::makeUnlimitedAllocator<Id>()};
    TurtleTriple turtleTriple{"<subject>", "<predicate>", 42.0};
    LangtagAndTriple res =
        index.tripleToInternalRepresentation(std::move(turtleTriple));
    ASSERT_EQ(Id::makeFromDouble(42.0), std::get<Id>(res.triple_[2]));
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

  Id qlLangtag =
      getId("<http://qlever.cs.uni-freiburg.de/builtin-functions/langtag>");
  Id en = getId("<http://qlever.cs.uni-freiburg.de/builtin-functions/@en>");
  Id enLabel = getId("@en@<label>");
  Id label = getId("<label>");
  Id firstLiteral = getId("\"A\"");
  Id lastLiteral = getId("\"zz\"@en");
  Id x = getId("<x>");

  auto increment = [](Id id) {
    return Id::makeFromVocabIndex(id.getVocabIndex().incremented());
  };

  // The range of all entities that start with
  // "<http://qlever.cs.uni-freiburg.de/builtin-functions/"
  auto internalEntities = std::pair{en, increment(qlLangtag)};
  // The range of all entities that start with @ (like `@en@<label>`)
  auto predicatesWithLangtag = std::pair{enLabel, increment(enLabel)};
  // The range of all literals;
  auto literals = std::pair{firstLiteral, increment(lastLiteral)};

  auto specialIds = qlever::getBoundsForSpecialIds();
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Permutation::POS);
    ASSERT_FALSE(lambda(std::array{label, firstLiteral, x}));

    // Note: The following triple is added, but it should be filtered out via
    // the ranges and not via the lambda, because it can be retrieved using the
    // `ranges`.
    ASSERT_FALSE(lambda(std::array{enLabel, firstLiteral, x}));
    ASSERT_FALSE(lambda(std::array{x, x, x}));
    EXPECT_THAT(ranges,
                UnorderedElementsAre(internalEntities, predicatesWithLangtag,
                                     specialIds));
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Permutation::PSO);
    ASSERT_FALSE(lambda(std::array{label, x, firstLiteral}));

    // Note: The following triple is added, but it should be filtered out via
    // the ranges and not via the lambda, because it can be retrieved using the
    // `ranges`.
    ASSERT_FALSE(lambda(std::array{enLabel, x, firstLiteral}));
    ASSERT_FALSE(lambda(std::array{x, x, x}));
    EXPECT_THAT(ranges,
                UnorderedElementsAre(internalEntities, predicatesWithLangtag,
                                     specialIds));
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Permutation::SOP);
    ASSERT_TRUE(lambda(std::array{x, firstLiteral, enLabel}));
    ASSERT_FALSE(lambda(std::array{x, firstLiteral, label}));
    ASSERT_FALSE(lambda(std::array{x, x, label}));
    EXPECT_THAT(ranges,
                UnorderedElementsAre(internalEntities, literals, specialIds));
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Permutation::SPO);
    ASSERT_TRUE(lambda(std::array{x, enLabel, firstLiteral}));
    ASSERT_FALSE(lambda(std::array{x, label, firstLiteral}));
    ASSERT_FALSE(lambda(std::array{x, label, x}));
    EXPECT_THAT(ranges,
                UnorderedElementsAre(internalEntities, literals, specialIds));
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Permutation::OSP);
    ASSERT_TRUE(lambda(std::array{firstLiteral, x, enLabel}));
    ASSERT_FALSE(lambda(std::array{firstLiteral, x, label}));
    ASSERT_FALSE(lambda(std::array{x, x, label}));
    EXPECT_THAT(ranges, UnorderedElementsAre(internalEntities, specialIds));
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Permutation::OPS);
    ASSERT_TRUE(lambda(std::array{firstLiteral, enLabel, x}));
    ASSERT_FALSE(lambda(std::array{firstLiteral, label, x}));
    ASSERT_FALSE(lambda(std::array{x, label, x}));
    auto hasPattern = qlever::specialIds.at(HAS_PATTERN_PREDICATE);
    ASSERT_TRUE(lambda(std::array{firstLiteral, hasPattern, x}));
    EXPECT_THAT(ranges, UnorderedElementsAre(internalEntities, specialIds));
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
  auto subjects = index.numDistinctSubjects();
  EXPECT_EQ(subjects.normal_, 3);
  // All literals with language tags are added subjects.
  EXPECT_EQ(subjects.internal_, 1);
  EXPECT_EQ(subjects, index.numDistinctCol0(Permutation::SPO));
  EXPECT_EQ(subjects, index.numDistinctCol0(Permutation::SOP));

  auto predicates = index.numDistinctPredicates();
  EXPECT_EQ(predicates.normal_, 2);
  // The added predicates are `ql:has-pattern`, `ql:langtag`, and one added
  // predicate for each combination of predicate+language that is actually used
  // (e.g. `@en@label`).
  EXPECT_EQ(predicates.internal_, 3);
  EXPECT_EQ(predicates, index.numDistinctCol0(Permutation::PSO));
  EXPECT_EQ(predicates, index.numDistinctCol0(Permutation::POS));

  auto objects = index.numDistinctObjects();
  EXPECT_EQ(objects.normal_, 7);
  // One added object for each language that is used.
  // Note: The pattern indices from the `ql:has-pattern` predicate are currently
  // not part of `objects.internal_`, but they are also not very important.
  EXPECT_EQ(objects.internal_, 1);
  EXPECT_EQ(objects, index.numDistinctCol0(Permutation::OSP));
  EXPECT_EQ(objects, index.numDistinctCol0(Permutation::OPS));

  auto numTriples = index.numTriples();
  EXPECT_EQ(numTriples.normal_, 7);
  // Two added triples for each triple that has an object with a language tag
  // and one triple per subject for the pattern.
  EXPECT_EQ(numTriples.internal_, 5);

  auto multiplicities = index.getMultiplicities(Permutation::SPO);
  EXPECT_FLOAT_EQ(multiplicities[0], 12.0 / 4.0);
  EXPECT_FLOAT_EQ(multiplicities[1], 12.0 / 5.0);
  EXPECT_FLOAT_EQ(multiplicities[2], 12.0 / 8.0);

  multiplicities = index.getMultiplicities("<x>", Permutation::SPO);
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

TEST(IndexTest, NewlinesInPrefixCompression) {
  std::string input;
  for (size_t i : ad_utility::integerRange(200UL)) {
    input.append(
        absl::StrCat("<a> <b> \"\"\"\nabc\t\n34as\n\ndj", i, "\"\"\".\n"));
  }
  const QueryExecutionContext* ctx = nullptr;

  ASSERT_NO_THROW(ctx = getQec(input));
  AD_CORRECTNESS_CHECK(ctx != nullptr);
  using namespace ::testing;

  const auto& prefixes = ctx->getIndex()
                             .getVocab()
                             .getInternalVocab()
                             .getUnderlyingVocabulary()
                             .getCompressor()
                             .prefixToCode();

  // There must be at least one of the compression prefixes that compresses the
  // common structure of the literals.
  EXPECT_THAT(prefixes, Contains(ContainsRegex("\nabc\t\n")));
}
