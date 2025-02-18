// Copyright 2024 - 2025, University of Freiburg
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

// Helper function that computes an `ExistsJoin` of the given `left` and
// `right` and checks that the result columns is equal to `expectedAsBool`.
// The first `numJoinColumns` columns of both `leftInput` and `rightInput` are
// used as join columns.
//
void testExistsFromIdTable(IdTable left, IdTable right,
                           std::vector<bool> expectedAsBool,
                           size_t numJoinColumns) {
  AD_CORRECTNESS_CHECK(left.numRows() == expectedAsBool.size());
  AD_CORRECTNESS_CHECK(left.numColumns() >= numJoinColumns);
  AD_CORRECTNESS_CHECK(right.numColumns() >= numJoinColumns);

  // Randomly permute the columns of the `input` and return the permutation that
  // was applied
  auto permuteColumns = [](auto& table) {
    auto colsView = ad_utility::integerRange(table.numColumns());
    std::vector<size_t> permutation;
    ql::ranges::copy(colsView, std::back_inserter(permutation));
    table.setColumnSubset(permutation);
    return permutation;
  };
  // Permute the columns.
  auto leftPermutation = permuteColumns(left);
  auto rightPermutation = permuteColumns(right);

  // We have to make the deep copy of `left` for the expected result at exactly
  // this point: The permutation of the columns (above) also affects the
  // expected result, while the permutation of the rows (which will be applied
  // below) doesn't affect it, as the `ExistsJoin` internally sorts its inputs.
  IdTable expected = left.clone();

  // Randomly shuffle the inputs, to ensure that the `existsJoin` correctly
  // pre-sorts its inputs.
  ad_utility::randomShuffle(left.begin(), left.end());
  ad_utility::randomShuffle(right.begin(), right.end());

  auto qec = getQec();
  using V = Variable;
  using Vars = std::vector<std::optional<Variable>>;

  // Helper lambda `makeChild` that turns a `VectorTable` input into a
  // `QueryExecutionTree` with a `ValuesForTesting` operation.
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

  // Compute the `ExistsJoin` and check the result.
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

// Same as the function above, but conveniently takes `VectorTable`s instead of
// `IdTable`s.
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

  // Single join column with one UNDEF (which always matches).
  auto U = Id::makeUndefined();
  testExists({{U, 13}, {3, 6}, {4, 7}, {5, 8}}, {{3, 15}, {3, 19}, {5, 37}},
             {true, true, false, true}, 1);
  testExists({{3, 6}, {4, 7}, {5, 8}}, {{U, 15}}, {true, true, true}, 1);

  // Two join columns.
  testExists({{3, 6}, {4, 7}, {5, 8}}, {{3, 15}, {3, 19}, {5, 37}},
             {false, false, false}, 2);
  testExists({{3, 6}, {4, 7}, {5, 8}},
             {{3, 6, 11}, {3, 19, 7}, {4, 8, 0}, {5, 8, 37}},
             {true, false, true}, 2);

  // Two join columns with UNDEFs in each column.
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
