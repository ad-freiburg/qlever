//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <sstream>
#include <string>

#include "./IndexTestHelpers.h"
#include "engine/Distinct.h"
#include "engine/GroupBy.h"
#include "engine/Join.h"
#include "engine/MultiColumnJoin.h"
#include "engine/OptionalJoin.h"
#include "engine/OrderBy.h"
#include "engine/QueryExecutionTree.h"
#include "engine/ResultTable.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "global/Id.h"

// _____________________________________________________________________________
TEST(LocalVocab, constructionAndAccess) {
  // Generate a test collection of words. For the tests below to work, these
  // must all be different.
  std::vector<std::string> testVocab(10'000);
  for (size_t i = 0; i < testVocab.size(); ++i) {
    testVocab[i] = std::to_string(i * 7635475567ULL);
  }
  // Create empty local vocabulary.
  LocalVocab localVocab;
  ASSERT_TRUE(localVocab.empty());

  // Add the words from our test vocabulary and check that they get the expected
  // local IDs.
  for (size_t i = 0; i < testVocab.size(); ++i) {
    ASSERT_EQ(localVocab.getIndexAndAddIfNotContained(testVocab[i]),
              LocalVocabIndex::make(i));
  }
  ASSERT_EQ(localVocab.size(), testVocab.size());

  // Check that we get the same IDs if we do this again, but that no new words
  // will be added.
  for (size_t i = 0; i < testVocab.size(); ++i) {
    ASSERT_EQ(localVocab.getIndexAndAddIfNotContained(testVocab[i]),
              LocalVocabIndex::make(i));
  }
  ASSERT_EQ(localVocab.size(), testVocab.size());

  // Check that the lookup by ID gives the correct words.
  for (size_t i = 0; i < testVocab.size(); ++i) {
    ASSERT_EQ(localVocab.getWord(LocalVocabIndex::make(i)), testVocab[i]);
  }
  ASSERT_EQ(localVocab.size(), testVocab.size());

  // Check that out-of-bound lookups are detected (this would have a caught the
  // one-off bug in #820, LocalVocab.cpp line 55).
  ASSERT_THROW(localVocab.getWord(LocalVocabIndex::make(testVocab.size())),
               std::runtime_error);
  ASSERT_THROW(localVocab.getWord(LocalVocabIndex::make(-1)),
               std::runtime_error);

  // Check that a move gives the expected result.
  auto localVocabMoved = std::move(localVocab);
  ASSERT_EQ(localVocab.size(), 0);
  ASSERT_EQ(localVocabMoved.size(), testVocab.size());
  for (size_t i = 0; i < testVocab.size(); ++i) {
    ASSERT_EQ(localVocabMoved.getWord(LocalVocabIndex::make(i)), testVocab[i]);
  }
}

// _____________________________________________________________________________
TEST(LocalVocab, propagation) {
  // Query execution context (with small test index) and allocator for testing,
  // see `IndexTestHelpers.h`.
  using ad_utility::AllocatorWithLimit;
  QueryExecutionContext* testQec = ad_utility::testing::getQec();
  AllocatorWithLimit<Id> testAllocator = ad_utility::testing::makeAllocator();

  // Lambda that checks the contents of the local vocabulary after the specified
  // operation.
  // NOTE: Clear the cache before each check
  // TODO: Does this solve the problem with the local vocab when doing several
  // operations on `values1`?
  auto checkLocalVocab = [&](auto* operation,
                             std::vector<std::string> expectedWords) -> void {
    AD_CHECK(operation);
    operation->getExecutionContext()->getQueryTreeCache().clearAll();
    std::shared_ptr<const ResultTable> resultTable = operation->getResult();
    ASSERT_TRUE(resultTable)
        << "Operation: " << operation->getDescriptor() << std::endl;
    std::vector<std::string> localVocabWords;
    for (size_t i = 0; i < resultTable->_localVocab->size(); ++i) {
      localVocabWords.emplace_back(
          resultTable->_localVocab->getWord(LocalVocabIndex::make(i)));
    }
    ASSERT_EQ(localVocabWords, expectedWords)
        << "Operation: " << operation->getDescriptor() << std::endl;
  };

  // Lambda that checks that `computeResult` throws an exception.
  auto checkThrow = [&](auto* operation) -> void {
    AD_CHECK(operation);
    operation->getExecutionContext()->getQueryTreeCache().clearAll();
    ASSERT_THROW(operation->getResult(), ad_semsearch::AbortException)
        << "Operation: " << operation->getDescriptor() << std::endl;
  };

  // Lambda that returns a `std::shared_ptr` to a `QueryExecutionTree` with only
  // the given operation (and using `testQec`).
  auto qet = [&](const auto& operation) -> std::shared_ptr<QueryExecutionTree> {
    return std::make_shared<QueryExecutionTree>(
        testQec, std::make_shared<Values>(operation));
  };

  // VALUES operation with two variables and two rows. Adds four new literals.
  //
  // Note: For literals, the quotes are part of the name, so if we wanted the
  // literal "x", we would have to write TripleComponent{"\"x\""}. For the
  // purposes of this test, we just want something that's not yet in the index,
  // so "x" etc. is just fine (and also different from the "<x>" below).
  //
  Values values1(testQec, {{Variable{"?x"}, Variable{"?y"}},
                           {{TripleComponent{"x"}, TripleComponent{"y1"}},
                            {TripleComponent{"x"}, TripleComponent{"y2"}}}});
  checkLocalVocab(&values1, std::vector<std::string>{"x", "y1", "y2"});

  // VALUES operation that uses an existing literal (from the test index).
  Values values2(testQec, {{Variable{"?x"}, Variable{"?y"}},
                           {{TripleComponent{"<x>"}, TripleComponent{"<y>"}}}});
  checkLocalVocab(&values2, std::vector<std::string>{});

  // JOIN operation with exactly one non-empty local vocab. The last arguments
  // are the two join columns.
  Join join1(testQec, qet(values1), qet(values2), 0, 0);
  checkLocalVocab(&join1, std::vector<std::string>{"x", "y1", "y2"});

  // JOIN operation with two non-empty local vocab.
  Join join2(testQec, qet(values1), qet(values1), 0, 0);
  checkThrow(&join2);

  // OPTIONAL JOIN operation with exactly one non-empty local vocab. The last
  // arguments are the two join columns.
  OptionalJoin optJoin1(testQec, qet(values1), true, qet(values2), true,
                        {{0, 0}});
  checkLocalVocab(&optJoin1, std::vector<std::string>{"x", "y1", "y2"});

  // OPTIONAL JOIN operation with two non-empty local vocab.
  OptionalJoin optJoin2(testQec, qet(values1), true, qet(values1), true,
                        {{0, 0}});
  checkThrow(&optJoin2);

  // MULTI-COLUMN JOIN operation with exactly one non-empty local vocab. The
  // last argument is a `std::vector` of join-column pairs.
  MultiColumnJoin multiJoin1(testQec, qet(values1), qet(values2),
                             {{0, 0}, {1, 1}});
  checkLocalVocab(&multiJoin1, std::vector<std::string>{"x", "y1", "y2"});

  // MULTI-COLUMN JOIN operation with two non-empty local vocab.
  MultiColumnJoin multiJoin2(testQec, qet(values1), qet(values1),
                             {{0, 0}, {1, 1}});
  checkThrow(&multiJoin2);

  // ORDER BY operation.
  //
  // Note: the third argument ar the indices of the columns to be sorted, and
  // the sort order. It's not important for this test.
  //
  OrderBy orderBy(testQec, qet(values1), {{0, true}, {1, true}});
  checkLocalVocab(&orderBy, std::vector<std::string>{"x", "y1", "y2"});

  // DISTINCT operation.
  //
  // Note: the third argument are those indices of the input columns that are
  // considered for the output.
  //
  Distinct distinct(testQec, qet(values1), {0, 1});
  checkLocalVocab(&distinct, std::vector<std::string>{"x", "y1", "y2"});

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
  checkLocalVocab(&groupBy, std::vector<std::string>{"x", "y1", "y2", "y1|y2"});

  // DISTINCT again, but after something has been added to the local
  // vocabulary.
  //
  // TODO: Without a clear cache (see `checkLocalVocab` above), the "y1|y2"
  // from the previous GROUP BY operation remains in the local vocabulary.
  // This is surprising behavior for me because I thought the local vocab is
  // tied to the result table (which is created from scratch in each call to
  // `checkLocalVocab`) and not to the operation (`values1` in this case).
  // `
  Distinct distinct2(testQec, qet(values1), {0});
  checkLocalVocab(&distinct2, std::vector<std::string>{"x", "y1", "y2"});
}
