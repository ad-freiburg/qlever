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

CartesianProductJoin makeJoin(const std::vector<VectorTable>& inputs,
                              bool useLimitInSuboperations = false) {
  auto qec = ad_utility::testing::getQec();
  std::vector<std::shared_ptr<QueryExecutionTree>> subtrees;
  size_t i = 0;
  auto v = [&i]() mutable { return Variable{"?" + std::to_string(i++)}; };
  for (const auto& input : inputs) {
    std::vector<std::optional<Variable>> vars;
    std::generate_n(std::back_inserter(vars),
                    input.empty() ? 0 : input.at(0).size(), std::ref(v));
    subtrees.push_back(ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector(input), std::move(vars),
        useLimitInSuboperations));
  }
  // Test that passing the same subtree in twice is illegal because it leads to
  // non-disjoint variable sets.
  if (!subtrees.empty() && i > 0) {
    auto subtrees2 = subtrees;
    subtrees2.insert(subtrees2.end(), subtrees.begin(), subtrees.end());
    EXPECT_ANY_THROW(CartesianProductJoin(qec, std::move(subtrees2)));
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
  {
    auto join = makeJoin(inputs, useLimitInSuboperations);
    EXPECT_EQ(makeIdTableFromVector(expected),
              join.computeResultOnlyForTesting().idTable());
  }

  for (size_t limit = 0; limit < expected.size(); ++limit) {
    for (size_t offset = 0; offset < expected.size(); ++offset) {
      LimitOffsetClause limitClause{limit, 0, offset};
      auto join = makeJoin(inputs, useLimitInSuboperations);
      join.setLimit(limitClause);
      VectorTable partialResult;
      std::copy(expected.begin() + limitClause.actualOffset(expected.size()),
                expected.begin() + limitClause.upperBound(expected.size()),
                std::back_inserter(partialResult));
      EXPECT_EQ(makeIdTableFromVector(partialResult),
                join.computeResultOnlyForTesting().idTable())
          << "failed at offset " << offset << " and limit " << limit;
    }
  }
}
void testCartesianProduct(VectorTable expected, std::vector<VectorTable> inputs,
                          source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  testCartesianProductImpl(expected, inputs, true);
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
  auto join = makeJoin({{{3, 5}, {7, 9}}, {{4}, {5}, {2}}});
  EXPECT_EQ(join.getDescriptor(), "Cartesian Product Join");
  EXPECT_FALSE(join.knownEmptyResult());
  EXPECT_EQ(join.getSizeEstimate(), 6);
  EXPECT_EQ(join.getResultWidth(), 3);
  EXPECT_EQ(join.getCostEstimate(), 11);
  EXPECT_EQ(join.getMultiplicity(1023), 1.0f);
  EXPECT_EQ(join.getMultiplicity(0), 1.0f);

  auto children = join.getChildren();
  EXPECT_THAT(join.asString(),
              ::testing::ContainsRegex("CARTESIAN PRODUCT JOIN"));
  EXPECT_THAT(join.asString(),
              ::testing::ContainsRegex("Values for testing with 2 columns"));
  EXPECT_THAT(join.asString(),
              ::testing::ContainsRegex("Values for testing with 1 col"));
  EXPECT_EQ(children.size(), 2u);
  EXPECT_NE(children.at(0), join.getChildren().at(1));
}

TEST(CartesianProductJoin, variableColumnMap) {
  auto qec = getQec();
  std::vector<std::shared_ptr<QueryExecutionTree>> subtrees;
  using Vars = std::vector<std::optional<Variable>>;
  subtrees.push_back(ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{3, 4}, {4, 7}}),
      Vars{Variable{"?x"}, std::nullopt}));
  subtrees.push_back(ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{3, 4, 3, Id::makeUndefined()}}),
      Vars{std::nullopt, Variable{"?y"}, std::nullopt, Variable{"?z"}}));
  CartesianProductJoin join{qec, std::move(subtrees)};

  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?x"}, {0, AlwaysDefined}},
      {Variable{"?y"}, {3, AlwaysDefined}},
      {Variable{"?z"}, {5, PossiblyUndefined}}};
  EXPECT_THAT(join.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
}
