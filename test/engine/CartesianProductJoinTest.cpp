//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../IndexTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "engine/CartesianProductJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/ValuesForTesting.h"

using namespace ad_utility::testing;
using ad_utility::source_location;

CartesianProductJoin makeJoin(std::vector<VectorTable> inputs,
                              bool useLimitInSuboperations = false) {
  auto qec = ad_utility::testing::getQec();
  // joka921 TODO shouldn't be necessary...
  qec->getQueryTreeCache().clearAll();
  std::vector<std::shared_ptr<QueryExecutionTree>> subtrees;
  auto v = [i = 0]() mutable { return Variable{"?" + std::to_string(i++)}; };
  for (const auto& input : inputs) {
    std::vector<Variable> vars;
    std::generate_n(std::back_inserter(vars),
                    input.empty() ? 0 : input.at(0).size(), v);
    subtrees.push_back(ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector(input), std::move(vars),
        useLimitInSuboperations));
  }
  return CartesianProductJoin{qec, std::move(subtrees)};
}

// Test that a cartesian product between the `inputs` yields the `expected`
// result.
void testCartesianProductImpl(VectorTable expected,
                              std::vector<VectorTable> inputs,
                              bool useLimitInSuboperations = false,
                              source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  auto join = makeJoin(std::move(inputs), useLimitInSuboperations);
  EXPECT_EQ(makeIdTableFromVector(expected),
            join.computeResultOnlyForTesting().idTable());

  for (size_t limit = 0; limit < expected.size(); ++limit) {
    for (size_t i = 0; i < expected.size(); ++i) {
      LimitOffsetClause limitClause{limit, 0, i};
      // joka921 TODO shouldn't be necessary...
      getQec()->getQueryTreeCache().clearAll();
      join.setLimit(limitClause);
      VectorTable partialResult;
      std::copy(expected.begin() + limitClause.actualOffset(expected.size()),
                expected.begin() + limitClause.upperBound(expected.size()),
                std::back_inserter(partialResult));
      EXPECT_EQ(makeIdTableFromVector(partialResult),
                join.computeResultOnlyForTesting().idTable())
          << "failed at offset " << i << " and limit " << limit;
    }
  }
}
void testCartesianProduct(VectorTable expected, std::vector<VectorTable> inputs,
                          source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  // testCartesianProductImpl(expected, inputs, true);
  testCartesianProductImpl(expected, inputs, false);
}

TEST(CartesianProductJoin, computeResult) {
  // Simple base cases.
  VectorTable v{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}};
  testCartesianProduct(v, {v});
  testCartesianProduct({}, {{}, v, {}});
  testCartesianProduct({}, {{}, {}});

  // Fails because of an empty input.
  EXPECT_ANY_THROW(makeJoin({}));

  // Fails because of the nullptrs.
  EXPECT_ANY_THROW(CartesianProductJoin(getQec(), {nullptr, nullptr}));

  // Join with a single result
  VectorTable v2{{1, 2, 3, 7}, {4, 5, 6, 7}, {7, 8, 9, 7}};
  testCartesianProduct(v2, {v, {{7}}});

  // A classic pattern
  testCartesianProduct({{0, 2, 4},
                        {1, 2, 4},
                        {0, 3, 4},
                        {1, 3, 4},
                        {0, 2, 5},
                        {1, 2, 5},
                        {0, 3, 5},
                        {1, 3, 5}},
                       {{{0}, {1}}, {{2}, {3}}, {{4}, {5}}});
  // Heterogenous sizes.
  testCartesianProduct(
      {{0, 2, 4}, {1, 2, 4}, {0, 2, 5}, {1, 2, 5}, {0, 2, 6}, {1, 2, 6}},
      {{{0}, {1}}, {{2}}, {{4}, {5}, {6}}});
}

TEST(CartesianProductJoin, basicMemberFunctions) {
  auto join = makeJoin({{{3}}});
  ASSERT_EQ(join.getDescriptor(), "Cartesian Product Join");
}
