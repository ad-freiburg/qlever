// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Co-Author: Andre Schlegel (November of 2022,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <tuple>

#include "./util/AllocatorTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"
#include "util/Forward.h"
#include "util/Random.h"
#include "util/SourceLocation.h"

using ad_utility::testing::makeAllocator;
using namespace ad_utility::testing;
namespace {
auto I = IntId;
auto V = VocabId;
constexpr auto U = Id::makeUndefined();
using JoinColumns = std::vector<std::array<ColumnIndex, 2>>;
}

TEST(EngineTest, distinctTest) {
  IdTable inp{makeIdTableFromVector(
      {{1, 1, 3, 7}, {6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}})};

  IdTable result{4, makeAllocator()};

  std::vector<size_t> keepIndices{{1, 2}};
  CALL_FIXED_SIZE(4, Engine::distinct, inp, keepIndices, &result);

  // For easier checking.
  IdTable expectedResult{
      makeIdTableFromVector({{1, 1, 3, 7}, {2, 2, 3, 5}, {3, 6, 5, 4}})};
  ASSERT_EQ(expectedResult, result);
}

void testOptionalJoin(const IdTable& inputA, const IdTable& inputB, JoinColumns jcls, const IdTable& expectedResult) {

  // TODO<joka921> Let this use the proper constructor of OptionalJoin.
  IdTable result{inputA.numColumns() + inputB.numColumns() - jcls.size(), makeAllocator()};
  // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
  // a have to equal those of column 2 of b and vice versa).
  int aWidth{static_cast<int>(inputA.numColumns())};
  int bWidth{static_cast<int>(inputB.numColumns())};
  int resultWidth{static_cast<int>(result.numColumns())};
  CALL_FIXED_SIZE((std::array{aWidth, bWidth, resultWidth}),
                  OptionalJoin::optionalJoin, inputA, inputB, jcls, &result);
  ASSERT_EQ(expectedResult, result);
}

// TODO<joka921> This function already exists in another PR in a better version, merge them.
IdTable makeTable(const std::vector<std::vector<Id>>& input) {
  size_t numCols = input[0].size();
  IdTable table{numCols, makeAllocator()};
  table.reserve(input.size());
  for (const auto& row : input) {
    table.emplace_back();
    for (size_t i = 0; i < table.numColumns(); ++i) {
      table.back()[i] = row.at(i);
    }
  }
  return table;
}

// TODO<joka921> This function already exists in another PR in a better version, merge them.
IdTable makeTableV(const std::vector<std::vector<std::variant<Id, int>>>& input) {
  size_t numCols = input[0].size();
  IdTable table{numCols, makeAllocator()};
  table.reserve(input.size());
  for (const auto& row : input) {
    table.emplace_back();
    for (size_t i = 0; i < table.numColumns(); ++i) {
      if (std::holds_alternative<Id>(row.at(i))) {
        table.back()[i] = std::get<Id>(row.at(i));
      } else {
        table.back()[i] = V(std::get<int>(row.at(i)));
      }
    }
  }
  return table;
}

TEST(JoinTest, optionalJoinTest) {
  {
    IdTable a{makeIdTableFromVector(
        {{4, 1, 2}, {2, 1, 3}, {1, 1, 4}, {2, 2, 1}, {1, 3, 1}})};
    IdTable b{
        makeIdTableFromVector({{3, 3, 1}, {1, 8, 1}, {4, 2, 2}, {1, 1, 3}})};
    // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
    // a have to equal those of column 2 of b and vice versa).
    JoinColumns jcls{{1, 2}, {2, 1}};

    IdTable expectedResult = makeTableV(
        {{4, 1, 2, U}, {2, 1, 3, 3}, {1, 1, 4, U}, {2, 2, 1, U}, {1, 3, 1, 1}});

    testOptionalJoin(a, b, jcls, expectedResult);
  }

  {
    // Test the optional join with variable sized data.
    IdTable va{makeIdTableFromVector(
        {{1, 2, 3, 4, 5, 6}, {1, 2, 3, 7, 5, 6}, {7, 6, 5, 4, 3, 2}})};

    IdTable vb{makeIdTableFromVector({{2, 3, 4}, {2, 3, 5}, {6, 7, 4}})};

    JoinColumns jcls{{1, 0}, {2, 1}};

    // For easier checking.
    auto expectedResult = makeTableV({{1, 2, 3, 4, 5, 6, 4},
                                 {1, 2, 3, 4, 5, 6, 5},
                                 {1, 2, 3, 7, 5, 6, 4},
                                 {1, 2, 3, 7, 5, 6, 5},
                                 {7, 6, 5, 4, 3, 2, U}});

    testOptionalJoin(va, vb, jcls, expectedResult);
    // TODO<joka921> Tests with nullopt in the input etc. are missing.
  }
}

TEST(OptionalJoinTest, SingleColumnPreexistingNulloptsLeft) {
  auto a = makeTableV({{U}, {U}, {2}, {3}, {4} });
  auto b = makeTableV({{3}, {5}});
  auto expected = makeTableV({{2}, {3}, {3}, {3}, {4}, {5}, {5}});
  testOptionalJoin(a, b, {{0, 0}}, expected);
}

TEST(OptionalJoinTest, SingleColumnPreexistingNulloptsRight) {
  auto a = makeTableV({{0}, {3}, {5}});
  auto b = makeTableV({{U}, {U}, {2}, {3}, {4} });
  auto expected = makeTableV({{0}, {0}, {3}, {3}, {3}, {5}, {5}});
  testOptionalJoin(a, b, {{0, 0}}, expected);
}
