//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sstream>
#include <string>

#include "./util/TripleComponentTestHelpers.h"
#include "engine/Bind.h"
#include "engine/CountAvailablePredicates.h"
#include "engine/Distinct.h"
#include "engine/Filter.h"
#include "engine/GroupBy.h"
#include "engine/HasPredicateScan.h"
#include "engine/Join.h"
#include "engine/Minus.h"
#include "engine/MultiColumnJoin.h"
#include "engine/OptionalJoin.h"
#include "engine/OrderBy.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/Sort.h"
#include "engine/TransitivePathBase.h"
#include "engine/Union.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "global/Id.h"
#include "util/IndexTestHelpers.h"

namespace {
// Get test collection of words of a given size. The words are all distinct.

using TestWords = std::vector<LocalVocabEntry>;

TestWords getTestCollectionOfWords(size_t size) {
  using namespace ad_utility::triple_component;
  TestWords testCollectionOfWords;
  for (size_t i = 0; i < size; ++i) {
    testCollectionOfWords.push_back(
        LiteralOrIri::literalWithoutQuotes(std::to_string(i * 7635475567ULL)));
  }
  return testCollectionOfWords;
}
}  // namespace

// _____________________________________________________________________________
TEST(LocalVocab, constructionAndAccess) {
  // Test collection of words.
  TestWords testWords = getTestCollectionOfWords(1000);

  // Create empty local vocabulary.
  LocalVocab localVocab;
  ASSERT_TRUE(localVocab.empty());

  // Add the words from our test vocabulary and check that they get the expected
  // local vocab indexes.
  for (size_t i = 0; i < testWords.size(); ++i) {
    ASSERT_EQ(*localVocab.getIndexAndAddIfNotContained(testWords[i]),
              testWords[i]);
  }
  size_t localVocabSize = localVocab.size();
  ASSERT_EQ(localVocab.size(), testWords.size());

  // Check that we get the same indexes if we do this again, but that no new
  // words will be added.
  for (size_t i = 0; i < testWords.size(); ++i) {
    ASSERT_EQ(*localVocab.getIndexAndAddIfNotContained(testWords[i]),
              testWords[i]);
    ASSERT_EQ(localVocab.size(), localVocabSize);
  }

  // Check again that we get the right indexes, but with `getIndexOrNullopt`.
  for (size_t i = 0; i < testWords.size(); ++i) {
    std::optional<LocalVocabIndex> localVocabIndex =
        localVocab.getIndexOrNullopt(testWords[i]);
    ASSERT_TRUE(localVocabIndex.has_value());
    ASSERT_EQ(*localVocabIndex.value(), testWords[i]);
  }

  // Check that `getIndexOrNullopt` returns `std::nullopt` for words that are
  // not contained in the local vocab. This makes use of the fact that the words
  // in our test vocabulary only contain digits as letters, see above.
  for (size_t i = 0; i < testWords.size(); ++i) {
    std::string content{asStringViewUnsafe(testWords[i].getContent())};
    auto illegalWord =
        ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes(
            content + "A");
    ASSERT_FALSE(localVocab.getIndexOrNullopt(illegalWord));
  }

  // Check that a move gives the expected result.
  localVocabSize = localVocab.size();
  auto localVocabMoved = std::move(localVocab);
  // TODO<joka921> Should we reset the pointer?
  // ASSERT_EQ(localVocab.size(), 0);
  ASSERT_EQ(localVocabMoved.size(), testWords.size());
  // Check that we get the same indexes if we do this again, but that no new
  // words will be added.
  for (size_t i = 0; i < testWords.size(); ++i) {
    ASSERT_EQ(*localVocabMoved.getIndexAndAddIfNotContained(testWords[i]),
              testWords[i]);
    ASSERT_EQ(localVocabMoved.size(), localVocabSize);
  }
}

// _____________________________________________________________________________
TEST(LocalVocab, clone) {
  // Create a small local vocabulary.
  size_t localVocabSize = 100;
  LocalVocab localVocabOriginal;
  std::vector<LocalVocabIndex> indices;
  auto inputWords = getTestCollectionOfWords(localVocabSize);
  for (const auto& word : inputWords) {
    indices.push_back(localVocabOriginal.getIndexAndAddIfNotContained(word));
  }
  ASSERT_EQ(localVocabOriginal.size(), localVocabSize);

  // Clone it and test that the clone contains the same words.
  LocalVocab localVocabClone = localVocabOriginal.clone();
  ASSERT_EQ(localVocabOriginal.size(), localVocabSize);
  ASSERT_EQ(localVocabClone.size(), localVocabSize);
  ASSERT_THAT(localVocabClone.getAllWordsForTesting(),
              ::testing::UnorderedElementsAreArray(
                  localVocabOriginal.getAllWordsForTesting()));

  // Test that the indices are still valid after the original vocabulary was
  // destroyed.
  localVocabOriginal = LocalVocab{};

  for (size_t i = 0; i < inputWords.size(); ++i) {
    EXPECT_EQ(*indices[i], inputWords[i]);
  }

  // Test that a BlankNodeIndex obtained by a `LocalVocab` is also contained
  // in the clone.
  ad_utility::BlankNodeManager bnm;
  LocalVocab v;
  auto id = v.getBlankNodeIndex(&bnm);
  LocalVocab vClone = v.clone();
  EXPECT_TRUE(vClone.isBlankNodeIndexContained(id));
}
// _____________________________________________________________________________
TEST(LocalVocab, merge) {
  // Create a small local vocabulary.
  std::vector<LocalVocabIndex> indices;
  LocalVocab vocA;
  LocalVocab vocB;
  constexpr auto lit = [](std::string_view s) {
    return ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes(s);
  };
  indices.push_back(vocA.getIndexAndAddIfNotContained(lit("oneA")));
  indices.push_back(vocA.getIndexAndAddIfNotContained(lit("twoA")));
  indices.push_back(vocA.getIndexAndAddIfNotContained(lit("oneB")));
  indices.push_back(vocA.getIndexAndAddIfNotContained(lit("twoB")));

  // Clone it and test that the clone contains the same words.
  auto vocabs = std::vector{&std::as_const(vocA), &std::as_const(vocB)};
  LocalVocab localVocabMerged = LocalVocab::merge(vocabs);
  ASSERT_EQ(localVocabMerged.size(), 4u);
  ASSERT_THAT(localVocabMerged.getAllWordsForTesting(),
              ::testing::UnorderedElementsAre(lit("oneA"), lit("twoA"),
                                              lit("oneB"), lit("twoB")));
  vocA = LocalVocab{};
  vocB = LocalVocab{};
  EXPECT_EQ(*indices[0], lit("oneA"));
  EXPECT_EQ(*indices[1], lit("twoA"));
  EXPECT_EQ(*indices[2], lit("oneB"));
  EXPECT_EQ(*indices[3], lit("twoB"));

  // Test that the `LocalBlankNodeManager` of vocabs is merged correctly.
  ad_utility::BlankNodeManager bnm;
  LocalVocab localVocabMerged2;
  BlankNodeIndex id;
  {
    LocalVocab vocC, vocD;
    id = vocC.getBlankNodeIndex(&bnm);
    auto vocabs2 = std::vector{&std::as_const(vocC), &std::as_const(vocD)};
    localVocabMerged2 = LocalVocab::merge(vocabs2);
  }
  EXPECT_TRUE(localVocabMerged2.isBlankNodeIndexContained(id));

  LocalVocab vocE, vocF;
  auto id2 = vocE.getBlankNodeIndex(&bnm);
  auto vocabs3 =
      std::vector{&std::as_const(localVocabMerged2), &std::as_const(vocF)};
  vocE.mergeWith(vocabs3 | ql::views::transform(
                               [](const LocalVocab* l) -> const LocalVocab& {
                                 return *l;
                               }));
  EXPECT_TRUE(vocE.isBlankNodeIndexContained(id));
  EXPECT_TRUE(localVocabMerged2.isBlankNodeIndexContained(id));
  EXPECT_TRUE(vocE.isBlankNodeIndexContained(id2));
}

// _____________________________________________________________________________
TEST(LocalVocab, propagation) {
  // Query execution context (with small test index), see `IndexTestHelpers.h`.
  using ad_utility::AllocatorWithLimit;
  QueryExecutionContext* testQec = ad_utility::testing::getQec();

  // Lambda that checks the contents of the local vocabulary after the specified
  // operation.
  //
  // NOTE: This cannot be `const Operation&` because `Operation::getResult()` is
  // not `const`.
  auto checkLocalVocab =
      [&](Operation& operation,
          const std::vector<std::string>& expectedWordsAsStrings) -> void {
    TestWords expectedWords;
    auto toLitOrIri = [](const auto& word) {
      using namespace ad_utility::triple_component;
      if (word.starts_with('<')) {
        return LiteralOrIri::iriref(word);
      } else {
        return LiteralOrIri::literalWithoutQuotes(word);
      }
    };
    ql::ranges::transform(expectedWordsAsStrings,
                          std::back_inserter(expectedWords), toLitOrIri);
    std::shared_ptr<const Result> resultTable = operation.getResult();
    ASSERT_TRUE(resultTable)
        << "Operation: " << operation.getDescriptor() << std::endl;
    TestWords localVocabWords =
        resultTable->localVocab().getAllWordsForTesting();
    // We currently allow the local vocab to have multiple IDs for the same
    // word, so we have to deduplicate first.
    ql::ranges::sort(localVocabWords);
    localVocabWords.erase(std::ranges::unique(localVocabWords).begin(),
                          localVocabWords.end());
    ASSERT_THAT(localVocabWords,
                ::testing::UnorderedElementsAreArray(expectedWords))
        << "Operation: " << operation.getDescriptor() << std::endl;
  };

  // Lambda that returns a `std::shared_ptr` to a `QueryExecutionTree` with only
  // the given operation (and using `testQec`).
  auto qet = [&](const auto& operation) -> std::shared_ptr<QueryExecutionTree> {
    return std::make_shared<QueryExecutionTree>(
        testQec, std::make_shared<Values>(operation));
  };

  // VALUES operation with two variables and two rows. Adds four new literals.
  //
  // Note 1: It is important to pass a copy of `values1` (and `values2` below)
  // to `checkLocalVocab` because when the operation is executed, the parsed
  // values are moved out by `Values::writeValues` and would then no longer be
  // there in the subsequent uses of `values1` and `values2` if it were not
  // copied.
  //
  // Note 2: For literals, the quotes are part of the name, so if we wanted the
  // literal "x", we would have to write TripleComponent{"\"x\""}. For the
  // purposes of this test, we just want something that's not yet in the index,
  // so "x" etc. is just fine (and also different from the "<x>" below).
  auto iri = ad_utility::testing::iri;
  Values values1(
      testQec,
      {{Variable{"?x"}, Variable{"?y"}},
       {{TripleComponent{iri("<xN1>")}, TripleComponent{iri("<yN1>")}},
        {TripleComponent{iri("<xN1>")}, TripleComponent{iri("<yN2>")}}}});
  Values values1copy = values1;
  std::vector<std::string> localVocab1{"<xN1>", "<yN1>", "<yN2>"};
  checkLocalVocab(values1copy, localVocab1);

  // VALUES operation that uses an existing literal (from the test index).
  Values values2(
      testQec, {{Variable{"?x"}, Variable{"?y"}},
                {{TripleComponent{iri("<x>")}, TripleComponent{iri("<y>")}}}});
  Values values2copy = values2;
  checkLocalVocab(values2copy, std::vector<std::string>{});

  // Contains local vocab words that are (at least partially) disjoint from the
  // words in `values1`.
  Values values3(
      testQec,
      {{Variable{"?x"}, Variable{"?y"}},
       {{TripleComponent{iri("<xN1>")}, TripleComponent{iri("<yN1>")}},
        {TripleComponent{iri("<xN2>")}, TripleComponent{iri("<yN3>")}}}});
  std::vector<std::string> localVocab13{"<xN1>", "<yN1>", "<yN2>", "<xN2>",
                                        "<yN3>"};

  // JOIN operation with exactly one non-empty local vocab and with two
  // non-empty local vocabs (the last two arguments are the two join columns).
  Join join1(testQec, qet(values1), qet(values2), 0, 0);
  checkLocalVocab(join1, localVocab1);
  Join join2(testQec, qet(values1), qet(values3), 0, 0);
  checkLocalVocab(join2, localVocab13);

  // OPTIONAL JOIN operation with exactly one non-empty local vocab.
  OptionalJoin optJoin1(testQec, qet(values1), qet(values2));
  checkLocalVocab(optJoin1, localVocab1);

  // OPTIONAL JOIN operation with two non-empty local vocab.
  OptionalJoin optJoin2(testQec, qet(values1), qet(values3));
  checkLocalVocab(optJoin2, localVocab13);

  // MULTI-COLUMN JOIN operation with exactly one non-empty local vocab and with
  // two non-empty local vocabs.
  MultiColumnJoin multiJoin1(testQec, qet(values1), qet(values2));
  checkLocalVocab(multiJoin1, localVocab1);
  MultiColumnJoin multiJoin2(testQec, qet(values1), qet(values3));
  checkLocalVocab(multiJoin2, localVocab13);

  // ORDER BY operation (the third argument are the indices of the columns to be
  // sorted, and the sort order; not important for this test).
  OrderBy orderBy(testQec, qet(values1), {{0, true}, {1, true}});
  checkLocalVocab(orderBy, localVocab1);

  // SORT operation (the third operation is the sort column).
  Sort sort(testQec, qet(values1), {0});
  checkLocalVocab(sort, localVocab1);

  // DISTINCT operation (the third argument are the indices of the input columns
  // that are considered for the output; not important for this test).
  Distinct distinct1(testQec, qet(values1), {0, 1});
  checkLocalVocab(distinct1, localVocab1);

  // GROUP BY operation.
  auto groupConcatExpression =
      [](std::string variableName,
         std::string separator) -> sparqlExpression::SparqlExpressionPimpl {
    // NOTE: The last argument is a string representation of the expression,
    // which does not matter for the purposes of this test.
    return {std::make_unique<sparqlExpression::GroupConcatExpression>(
                false,
                std::make_unique<sparqlExpression::VariableExpression>(
                    Variable{variableName}),
                separator),
            "GROUP_CONCAT"};
  };
  GroupBy groupBy(
      testQec, {Variable{"?x"}},
      {Alias{groupConcatExpression("?y", "|"), Variable{"?concat"}}},
      qet(values1));
  checkLocalVocab(
      groupBy, std::vector<std::string>{"<xN1>", "<yN1>", "<yN2>", "yN1|yN2"});

  // DISTINCT again, but after something has been added to the local vocabulary
  // (to check that the "y1|y2" added by the GROUP BY does not also appear here,
  // as it did before GroupBy::computeResult copied it's local vocabulary).
  Distinct distinct2(testQec, qet(values1), {0});
  checkLocalVocab(distinct2, localVocab1);

  // UNION operation with exactly one non-empty local vocab and with two
  // non-empy local vocabs.
  Union union1(testQec, qet(values1), qet(values2));
  checkLocalVocab(union1, localVocab1);
  Union union2(testQec, qet(values1), qet(values3));
  checkLocalVocab(union2, localVocab13);

  // MINUS operation with exactly one non-empty local vocab and with
  // two non-empty local vocabs.
  Minus minus1(testQec, qet(values1), qet(values2));
  checkLocalVocab(minus1, localVocab1);
  Minus minus2(testQec, qet(values1), qet(values3));
  checkLocalVocab(minus2, localVocab13);

  // FILTER operation (the third argument is an expression; which one doesn't
  // matter for this test).
  Filter filter(
      testQec, qet(values1),
      {std::make_unique<sparqlExpression::VariableExpression>(Variable{"?x"}),
       "Expression ?x"});
  checkLocalVocab(filter, localVocab1);

  // BIND operation (the third argument is a `parsedQuery::Bind` object).
  Bind bind(
      testQec, qet(values1),
      {{std::make_unique<sparqlExpression::VariableExpression>(Variable{"?x"}),
        "Expression ?x"},
       Variable{"?z"}});
  checkLocalVocab(bind, localVocab1);

  // TRANSITIVE PATH operation.
  //
  // NOTE: As part of an actual query, the child of a transitive path
  // operation is always an index scan, which has a non-empty
  // local-vocabulary. Still, it doesn't harm to test this.
  TransitivePathSide left(std::nullopt, 0, Variable{"?x"});
  TransitivePathSide right(std::nullopt, 1, Variable{"?y"});
  auto transitivePath = TransitivePathBase::makeTransitivePath(
      testQec, qet(values1), std::move(left), std::move(right), 1, 1);
  checkLocalVocab(*transitivePath, localVocab1);

  // PATTERN TRICK operations.
  HasPredicateScan hasPredicateScan(testQec, qet(values1), 0, Variable{"?z"});
  checkLocalVocab(hasPredicateScan, localVocab1);
  Values valuesPatternTrick(
      testQec,
      {{Variable{"?x"}, Variable{"?y"}},
       {{TripleComponent{iri("<xN1>")}, TripleComponent{NO_PATTERN}},
        {TripleComponent{iri("<xN1>")}, TripleComponent{NO_PATTERN}}}});
  CountAvailablePredicates countAvailablePredictes(
      testQec, qet(valuesPatternTrick), 0, Variable{"?y"}, Variable{"?count"});
  checkLocalVocab(countAvailablePredictes, {"<xN1>"});
  // TEXT operations.
  //
  // TODO<joka921> Maybe add tests for the new TextIndexScanFor... classes,
  // they never introduce any local vocab.
}

// _____________________________________________________________________________
TEST(LocalVocab, getBlankNodeIndex) {
  ad_utility::BlankNodeManager bnm(0);
  LocalVocab v;
  BlankNodeIndex a = v.getBlankNodeIndex(&bnm);
  BlankNodeIndex b = v.getBlankNodeIndex(&bnm);
  EXPECT_NE(a, b);
}

// _____________________________________________________________________________
TEST(LocalVocab, otherWordSetIsTransitivelyPropagated) {
  using ad_utility::triple_component::LiteralOrIri;
  LocalVocab original;
  original.getIndexAndAddIfNotContained(
      LocalVocabEntry{LiteralOrIri::literalWithoutQuotes("test")});

  LocalVocab clone = original.clone();
  LocalVocab mergeCandidate;
  mergeCandidate.mergeWith(clone);

  EXPECT_EQ(mergeCandidate.size(), 1);
  EXPECT_THAT(mergeCandidate.getAllWordsForTesting(),
              ::testing::UnorderedElementsAre(
                  LiteralOrIri::literalWithoutQuotes("test")));
}

// _____________________________________________________________________________
TEST(LocalVocab, sizeIsProperlyUpdatedOnMerge) {
  using ad_utility::triple_component::LiteralOrIri;
  using ::testing::UnorderedElementsAre;
  LocalVocab original;
  original.getIndexAndAddIfNotContained(
      LocalVocabEntry{LiteralOrIri::literalWithoutQuotes("test")});

  LocalVocab clone1 = original.clone();
  LocalVocab clone2 = original.clone();
  clone2.mergeWith(original);
  original.mergeWith(clone1);

  // Make sure we deduplicate `otherWordSets_` with the primary word set.
  EXPECT_EQ(original.size(), 1);
  EXPECT_THAT(original.getAllWordsForTesting(),
              UnorderedElementsAre(LiteralOrIri::literalWithoutQuotes("test")));

  EXPECT_EQ(clone2.size(), 1);
  EXPECT_THAT(clone2.getAllWordsForTesting(),
              UnorderedElementsAre(LiteralOrIri::literalWithoutQuotes("test")));

  LocalVocab clone3 = original.clone();
  EXPECT_EQ(clone3.size(), 1);
  EXPECT_THAT(clone3.getAllWordsForTesting(),
              UnorderedElementsAre(LiteralOrIri::literalWithoutQuotes("test")));
}

// _____________________________________________________________________________
TEST(LocalVocab, modificationIsBlockedAfterCloneOrMerge) {
  using ad_utility::triple_component::LiteralOrIri;
  auto literal = LiteralOrIri::literalWithoutQuotes("test");
  auto otherLiteral = LiteralOrIri::literalWithoutQuotes("other");
  {
    LocalVocab original;
    original.getIndexAndAddIfNotContained(LocalVocabEntry{literal});
    (void)original.clone();
    EXPECT_NE(original.getIndexOrNullopt(LocalVocabEntry{literal}),
              std::nullopt);
    EXPECT_THROW(
        original.getIndexAndAddIfNotContained(LocalVocabEntry{literal}),
        ad_utility::Exception);
    EXPECT_THROW(
        original.getIndexAndAddIfNotContained(LocalVocabEntry{otherLiteral}),
        ad_utility::Exception);
    EXPECT_EQ(original.size(), 1);
  }
  {
    LocalVocab original;
    LocalVocab other;
    original.getIndexAndAddIfNotContained(LocalVocabEntry{literal});
    other.mergeWith(original);
    EXPECT_NE(original.getIndexOrNullopt(LocalVocabEntry{literal}),
              std::nullopt);
    EXPECT_THROW(
        original.getIndexAndAddIfNotContained(LocalVocabEntry{literal}),
        ad_utility::Exception);
    EXPECT_THROW(
        original.getIndexAndAddIfNotContained(LocalVocabEntry{otherLiteral}),
        ad_utility::Exception);
    EXPECT_EQ(original.size(), 1);
  }
}

// _____________________________________________________________________________
TEST(LocalVocab, modificationIsNotBlockedAfterAcquiringHolder) {
  using ad_utility::triple_component::LiteralOrIri;
  auto literal = LiteralOrIri::literalWithoutQuotes("test");
  auto otherLiteral = LiteralOrIri::literalWithoutQuotes("other");
  std::optional<LocalVocab::LifetimeExtender> extender = std::nullopt;
  LocalVocabIndex encodedTest;
  LocalVocabIndex encodedOther;
  {
    LocalVocab original;
    encodedTest =
        original.getIndexAndAddIfNotContained(LocalVocabEntry{literal});
    extender = original.getLifetimeExtender();

    EXPECT_EQ(original.getIndexOrNullopt(LocalVocabEntry{literal}),
              std::optional{encodedTest});

    EXPECT_EQ(original.getIndexAndAddIfNotContained(LocalVocabEntry{literal}),
              encodedTest);
    EXPECT_EQ(original.size(), 1);

    encodedOther =
        original.getIndexAndAddIfNotContained(LocalVocabEntry{otherLiteral});
    EXPECT_EQ(original.size(), 2);
  }
  // The `extender` keeps the `LocalVocabIndex`es valid even though the
  // corresponding `LocalVocab` has already been destroyed.
  (void)extender;
  EXPECT_EQ(*encodedTest, literal);
  EXPECT_EQ(*encodedOther, otherLiteral);
}
