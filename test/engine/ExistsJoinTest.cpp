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
void testExistsFromIdTable(IdTable left, IdTable right,
                           std::vector<bool> expectedAsBool,
                           size_t numJoinColumns) {
  AD_CORRECTNESS_CHECK(left.numRows() == expectedAsBool.size());
  AD_CORRECTNESS_CHECK(left.numColumns() >= numJoinColumns);
  AD_CORRECTNESS_CHECK(right.numColumns() >= numJoinColumns);

  // Permute the join columns.
  auto colsLeft = ad_utility::integerRange(left.numColumns());
  std::vector<size_t> leftPermutation;
  ql::ranges::copy(colsLeft, std::back_inserter(leftPermutation));
  left.setColumnSubset(leftPermutation);

  auto colsRight = ad_utility::integerRange(right.numColumns());
  std::vector<size_t> rightPermutation;
  ql::ranges::copy(colsRight, std::back_inserter(rightPermutation));
  right.setColumnSubset(rightPermutation);

  // The expected output depends on the (sorted) input, even if we shuffle it
  // afterward.
  IdTable expected = left.clone();

  // Randomly shuffle the inputs, to ensure that the `existsJoin` correctly
  // pre-sorts its inputs.
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(left.begin(), left.end(), g);
  std::shuffle(right.begin(), right.end(), g);

  auto qec = getQec();
  using V = Variable;
  using Vars = std::vector<std::optional<Variable>>;

  auto joinCol = [](size_t i) { return V{absl::StrCat("?joinCol_", i)}; };
  auto nonJoinCol = [i = 0]() mutable {
    return V{absl::StrCat("?nonJoinCol_", i++)};
  };

  auto makeChild = [&](const IdTable& input, const auto& columnPermutation) {
    Vars vars;
    for (auto colIdx : columnPermutation) {
      if (colIdx < numJoinColumns) {
        vars.push_back(joinCol(colIdx));
      } else {
        vars.push_back(nonJoinCol());
      }
    }
    return ad_utility::makeExecutionTree<ValuesForTesting>(qec, input.clone(),
                                                           vars);
  };

  auto exists = ExistsJoin{qec, makeChild(left, leftPermutation),
                           makeChild(right, rightPermutation), V{"?exists"}};

  EXPECT_EQ(exists.getResultWidth(), left.numColumns() + 1);

  auto res = exists.computeResultOnlyForTesting();
  const auto& table = res.idTable();
  ASSERT_EQ(table.numRows(), left.size());
  expected.addEmptyColumn();
  ql::ranges::transform(expectedAsBool, expected.getColumn(2).begin(),
                        &Id::makeFromBool);
  EXPECT_THAT(table, matchesIdTable(expected));
}

void testExists(const VectorTable& leftInput, const VectorTable& rightInput,
                std::vector<bool> expectedAsBool, size_t numJoinColumns) {
  auto left = makeIdTableFromVector(leftInput);
  auto right = makeIdTableFromVector(rightInput);
  testExistsFromIdTable(std::move(left), std::move(right),
                        std::move(expectedAsBool), numJoinColumns);
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

  // Empty inputs
  auto alloc = ad_utility::testing::makeAllocator();
  testExistsFromIdTable(IdTable(2, alloc),
                        makeIdTableFromVector({{U, U}, {3, 7}}), {}, 1);
  testExistsFromIdTable(makeIdTableFromVector({{U, U}, {3, 7}}),
                        IdTable(2, alloc), {false, false}, 2);
}
