//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <sstream>
#include <string>

#include "./IndexTestHelpers.h"
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
#include "engine/ResultTable.h"
#include "engine/Sort.h"
#include "engine/TextOperationWithFilter.h"
#include "engine/TextOperationWithoutFilter.h"
#include "engine/TransitivePath.h"
#include "engine/Union.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "global/Id.h"

namespace {
// Get test collection of words of a given size. The words are all distinct.
std::vector<std::string> getTestCollectionOfWords(size_t size) {
  std::vector<std::string> testCollectionOfWords(size);
  for (size_t i = 0; i < testCollectionOfWords.size(); ++i) {
    testCollectionOfWords[i] = std::to_string(i * 7635475567ULL);
  }
  return testCollectionOfWords;
}
}  // namespace

// _____________________________________________________________________________
TEST(LocalVocab, constructionAndAccess) {
  // Test collection of words.
  std::vector<std::string> testWords = getTestCollectionOfWords(1000);

  // Create empty local vocabulary.
  LocalVocab localVocab;
  ASSERT_TRUE(localVocab.empty());

  // Add the words from our test vocabulary and check that they get the expected
  // local vocab indexes.
  for (size_t i = 0; i < testWords.size(); ++i) {
    ASSERT_EQ(localVocab.getIndexAndAddIfNotContained(testWords[i]),
              LocalVocabIndex::make(i));
  }
  ASSERT_EQ(localVocab.size(), testWords.size());

  // Check that we get the same indexes if we do this again, but that no new
  // words will be added.
  for (size_t i = 0; i < testWords.size(); ++i) {
    ASSERT_EQ(localVocab.getIndexAndAddIfNotContained(testWords[i]),
              LocalVocabIndex::make(i));
  }
  ASSERT_EQ(localVocab.size(), testWords.size());

  // Check again that we get the right indexes, but with `getIndexOrNullopt`.
  for (size_t i = 0; i < testWords.size(); ++i) {
    std::optional<LocalVocabIndex> localVocabIndex =
        localVocab.getIndexOrNullopt(testWords[i]);
    ASSERT_TRUE(localVocabIndex.has_value());
    ASSERT_EQ(localVocabIndex.value(), LocalVocabIndex::make(i));
  }

  // Check that `getIndexOrNullopt` returns `std::nullopt` for words that are
  // not contained in the local vocab. This makes use of the fact that the words
  // in our test vocabulary only contain digits as letters, see above.
  for (size_t i = 0; i < testWords.size(); ++i) {
    ASSERT_FALSE(localVocab.getIndexOrNullopt(testWords[i] + "A"));
  }

  // Check that the lookup by ID gives the correct words.
  for (size_t i = 0; i < testWords.size(); ++i) {
    ASSERT_EQ(localVocab.getWord(LocalVocabIndex::make(i)), testWords[i]);
  }
  ASSERT_EQ(localVocab.size(), testWords.size());

  // Check that out-of-bound lookups are detected (this would have a caught the
  // one-off bug in #820, LocalVocab.cpp line 55).
  ASSERT_THROW(localVocab.getWord(LocalVocabIndex::make(testWords.size())),
               std::runtime_error);
  ASSERT_THROW(localVocab.getWord(LocalVocabIndex::make(-1)),
               std::runtime_error);

  // Check that a move gives the expected result.
  auto localVocabMoved = std::move(localVocab);
  ASSERT_EQ(localVocab.size(), 0);
  ASSERT_EQ(localVocabMoved.size(), testWords.size());
  for (size_t i = 0; i < testWords.size(); ++i) {
    ASSERT_EQ(localVocabMoved.getWord(LocalVocabIndex::make(i)), testWords[i]);
  }
}

// _____________________________________________________________________________
TEST(LocalVocab, clone) {
  // Create a small local vocabulary.
  size_t localVocabSize = 100;
  LocalVocab localVocabOriginal;
  for (auto& word : getTestCollectionOfWords(localVocabSize)) {
    localVocabOriginal.getIndexAndAddIfNotContained(word);
  }
  ASSERT_EQ(localVocabOriginal.size(), localVocabSize);

  // Clone it and test that the clone contains the same words, but under
  // different addresses (that is, the word strings were deeply copied).
  LocalVocab localVocabClone = localVocabOriginal.clone();
  ASSERT_EQ(localVocabOriginal.size(), localVocabSize);
  ASSERT_EQ(localVocabClone.size(), localVocabSize);
  for (size_t i = 0; i < localVocabSize; ++i) {
    LocalVocabIndex idx = LocalVocabIndex::make(i);
    const std::string& wordFromOriginal = localVocabOriginal.getWord(idx);
    const std::string& wordFromClone = localVocabClone.getWord(idx);
    ASSERT_EQ(wordFromOriginal, wordFromClone);
    ASSERT_NE(&wordFromOriginal, &wordFromClone);
  }

  // Check that `nextFreeIndex_` of the clone is OK by adding another word to
  // the clone.
  localVocabClone.getIndexAndAddIfNotContained("blubb");
  ASSERT_EQ(localVocabClone.getIndexAndAddIfNotContained("blubb"),
            LocalVocabIndex::make(localVocabSize));
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
  auto checkLocalVocab = [&](Operation& operation,
                             std::vector<std::string> expectedWords) -> void {
    std::shared_ptr<const ResultTable> resultTable = operation.getResult();
    ASSERT_TRUE(resultTable)
        << "Operation: " << operation.getDescriptor() << std::endl;
    std::vector<std::string> localVocabWords;
    for (size_t i = 0; i < resultTable->localVocab().size(); ++i) {
      localVocabWords.emplace_back(
          resultTable->localVocab().getWord(LocalVocabIndex::make(i)));
    }
    ASSERT_EQ(localVocabWords, expectedWords)
        << "Operation: " << operation.getDescriptor() << std::endl;
  };

  // Lambda that checks that `computeResult` throws an exception.
  auto checkThrow = [&](Operation& operation) -> void {
    ASSERT_THROW(operation.getResult(), ad_utility::AbortException)
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
  Values values1(testQec, {{Variable{"?x"}, Variable{"?y"}},
                           {{TripleComponent{"x"}, TripleComponent{"y1"}},
                            {TripleComponent{"x"}, TripleComponent{"y2"}}}});
  Values values1copy = values1;
  checkLocalVocab(values1copy, std::vector<std::string>{"x", "y1", "y2"});

  // VALUES operation that uses an existing literal (from the test index).
  Values values2(testQec, {{Variable{"?x"}, Variable{"?y"}},
                           {{TripleComponent{"<x>"}, TripleComponent{"<y>"}}}});
  Values values2copy = values2;
  checkLocalVocab(values2copy, std::vector<std::string>{});

  // JOIN operation with exactly one non-empty local vocab and with two
  // non-empty local vocabs (the last two arguments are the two join columns).
  Join join1(testQec, qet(values1), qet(values2), 0, 0);
  checkLocalVocab(join1, std::vector<std::string>{"x", "y1", "y2"});
  Join join2(testQec, qet(values1), qet(values1), 0, 0);
  checkThrow(join2);

  // OPTIONAL JOIN operation with exactly one non-empty local vocab.
  OptionalJoin optJoin1(testQec, qet(values1), qet(values2));
  checkLocalVocab(optJoin1, std::vector<std::string>{"x", "y1", "y2"});

  // OPTIONAL JOIN operation with two non-empty local vocab.
  OptionalJoin optJoin2(testQec, qet(values1), qet(values1));
  checkThrow(optJoin2);

  // MULTI-COLUMN JOIN operation with exactly one non-empty local vocab and with
  // two non-empty local vocabs.
  MultiColumnJoin multiJoin1(testQec, qet(values1), qet(values2));
  checkLocalVocab(multiJoin1, std::vector<std::string>{"x", "y1", "y2"});
  MultiColumnJoin multiJoin2(testQec, qet(values1), qet(values1));
  checkThrow(multiJoin2);

  // ORDER BY operation (the third argument are the indices of the columns to be
  // sorted, and the sort order; not important for this test).
  OrderBy orderBy(testQec, qet(values1), {{0, true}, {1, true}});
  checkLocalVocab(orderBy, std::vector<std::string>{"x", "y1", "y2"});

  // SORT operation (the third operation is the sort column).
  Sort sort(testQec, qet(values1), {0});
  checkLocalVocab(sort, std::vector<std::string>{"x", "y1", "y2"});

  // DISTINCT operation (the third argument are the indices of the input columns
  // that are considered for the output; not important for this test).
  Distinct distinct1(testQec, qet(values1), {0, 1});
  checkLocalVocab(distinct1, std::vector<std::string>{"x", "y1", "y2"});

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
  checkLocalVocab(groupBy, std::vector<std::string>{"x", "y1", "y2", "y1|y2"});

  // DISTINCT again, but after something has been added to the local vocabulary
  // (to check that the "y1|y2" added by the GROUP BY does not also appear here,
  // as it did before GroupBy::computeResult copied it's local vocabulary).
  Distinct distinct2(testQec, qet(values1), {0});
  checkLocalVocab(distinct2, std::vector<std::string>{"x", "y1", "y2"});

  // UNION operation with exactly one non-empty local vocab and with two
  // non-empy local vocabs.
  Union union1(testQec, qet(values1), qet(values2));
  checkLocalVocab(union1, std::vector<std::string>{"x", "y1", "y2"});
  Union union2(testQec, qet(values1), qet(values1));
  checkThrow(union2);

  // MINUS operation with exactly one non-empty local vocab and with
  // two non-empty local vocabs.
  Minus minus1(testQec, qet(values1), qet(values2));
  checkLocalVocab(minus1, std::vector<std::string>{"x", "y1", "y2"});
  Minus minus2(testQec, qet(values1), qet(values1));
  checkThrow(minus2);

  // FILTER operation (the third argument is an expression; which one doesn't
  // matter for this test).
  Filter filter(
      testQec, qet(values1),
      {std::make_unique<sparqlExpression::VariableExpression>(Variable{"?x"}),
       "Expression ?x"});
  checkLocalVocab(filter, std::vector<std::string>{"x", "y1", "y2"});

  // BIND operation (the third argument is a `parsedQuery::Bind` object).
  Bind bind(
      testQec, qet(values1),
      {{std::make_unique<sparqlExpression::VariableExpression>(Variable{"?x"}),
        "Expression ?x"},
       Variable{"?z"}});
  checkLocalVocab(bind, std::vector<std::string>{"x", "y1", "y2"});

  // TRANSITIVE PATH operation.
  //
  // NOTE: As part of an actual query, the child of a transitive path
  // operation is always an index scan, which has a non-empty
  // local-vocabulary. Still, it doesn't harm to test this.
  TransitivePathSide left(std::nullopt, 0, Variable{"?x"});
  TransitivePathSide right(std::nullopt, 1, Variable{"?y"});
  TransitivePath transitivePath(testQec, qet(values1), left, right, 1, 1);
  checkLocalVocab(transitivePath, std::vector<std::string>{"x", "y1", "y2"});

  // PATTERN TRICK operations.
  HasPredicateScan hasPredicateScan(testQec, qet(values1), 0, "?z");
  checkLocalVocab(hasPredicateScan, std::vector<std::string>{"x", "y1", "y2"});
  CountAvailablePredicates countAvailablePredictes(
      testQec, qet(values1), 0, Variable{"?x"}, Variable{"?y"});
  checkLocalVocab(countAvailablePredictes,
                  std::vector<std::string>{"x", "y1", "y2"});

  // TEXT operations.
  //
  // NOTE: `TextOperationWithoutFilter` takes no arguments, so the local
  // vocabulary of the operation remains empty (but it doesn't harm to check
  // that that is indeed the case).
  TextOperationWithFilter text1(
      testQec, "someWord", {Variable{"?x"}, Variable{"?y"}, Variable{"?text"}},
      Variable{"?text"}, qet(values1), 0);
  checkLocalVocab(text1, std::vector<std::string>{"x", "y1", "y2"});
  TextOperationWithoutFilter text2(testQec, {"someWord"}, {Variable{"?text"}},
                                   Variable{"?text"});
  checkLocalVocab(text2, {});
}
