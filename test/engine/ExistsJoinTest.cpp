// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/ExistsJoin.h"
#include "engine/IndexScan.h"
#include "engine/NeutralElementOperation.h"
#include "engine/QueryExecutionTree.h"

using namespace ad_utility::testing;

namespace {
void testExists(const VectorTable& leftInput, const VectorTable& rightInput,
                std::vector<bool> expectedAsBool, size_t numJoinColumns) {
  AD_CORRECTNESS_CHECK(leftInput.size() == expectedAsBool.size());
  auto left = makeIdTableFromVector(leftInput);
  auto right = makeIdTableFromVector(rightInput);
  AD_CORRECTNESS_CHECK(left.numColumns() >= numJoinColumns);
  AD_CORRECTNESS_CHECK(right.numColumns() >= numJoinColumns);

  auto qec = getQec();
  using V = Variable;
  using Vars = std::vector<std::optional<Variable>>;

  // TODO<joka921> Support more than one join column.
  // TODO<joka921> also randomly permute the join columns.

  auto joinCol = [](size_t i) { return V{absl::StrCat("?joinCol_", i)}; };
  auto nonJoinCol = [i = 0]() mutable {
    return V{absl::StrCat("?nonJoinCol_", i++)};
  };

  auto makeChild = [&](const IdTable& input) {
    Vars vars;
    for (size_t i : ad_utility::integerRange(numJoinColumns)) {
      vars.push_back(joinCol(i));
    };
    for ([[maybe_unused]] size_t i :
         ql::views::iota(numJoinColumns, input.numColumns())) {
      vars.push_back(nonJoinCol());
    }
    return ad_utility::makeExecutionTree<ValuesForTesting>(qec, input.clone(),
                                                           vars);
  };

  auto exists =
      ExistsJoin{qec, makeChild(left), makeChild(right), V{"?exists"}};

  EXPECT_EQ(exists.getResultWidth(), left.numColumns() + 1);

  auto res = exists.computeResultOnlyForTesting();
  const auto& table = res.idTable();
  ASSERT_EQ(table.numRows(), left.size());
  IdTable expected = left.clone();
  expected.addEmptyColumn();
  ql::ranges::transform(expectedAsBool, expected.getColumn(2).begin(),
                        &Id::makeFromBool);
  EXPECT_THAT(table, matchesIdTable(expected));
}
}  // namespace

TEST(Exists, computeResult) {
  // Single join column.
  testExists({{3, 6}, {4, 7}, {5, 8}}, {{3, 15}, {3, 19}, {5, 37}},
             {true, false, true}, 1);

  // UNDEF matches everything
  auto U = Id::makeUndefined();
  testExists({{U, 13}, {3, 6}, {4, 7}, {5, 8}}, {{3, 15}, {3, 19}, {5, 37}},
             {true, true, false, true}, 1);
  testExists({{3, 6}, {4, 7}, {5, 8}}, {{U, 15}}, {true, true, true}, 1);

  // Two join columns
  testExists({{3, 6}, {4, 7}, {5, 8}}, {{3, 15}, {3, 19}, {5, 37}},
             {false, false, false}, 2);
  testExists({{3, 6}, {4, 7}, {5, 8}},
             {{3, 6, 11}, {3, 19, 7}, {4, 8, 0}, {5, 8, 37}},
             {true, false, true}, 2);

  // Two join columns with UNDEF
  testExists({{2, 2}, {3, U}, {4, 8}, {5, 8}},
             {{U, 8}, {3, 15}, {3, 19}, {5, U}, {5, 37}},
             {false, true, true, true}, 2);
  testExists({{U, U}}, {{13, 17}}, {true}, 2);
  testExists({{13, 17}, {25, 38}}, {{U, U}}, {true, true}, 2);

  // TODO<joka921> Add tests with unsorted inputs.
  // TODO<joka921> Test empty inputs on one side.
}
