// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Co-Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <tuple>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/OptionalJoin.h"
#include "util/Random.h"
#include "engine/QueryExecutionTree.h"
#include "util/Forward.h"
#include "util/SourceLocation.h"
#include "./util/GTestHelpers.h"
#include "engine/idTable/IdTable.h"

ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}

auto I = [](const auto& id) {
  return Id::makeFromVocabIndex(VocabIndex::make(id));
};

// For easier reading. We repeat that type combination so often, that this
// will make things a lot easier in terms of reading and writing.
using vectorTable = std::vector<std::vector<size_t>>;

/*
 * Return an 'IdTable' with the given 'tableContent'. all rows must have the
 * same length.
*/
IdTable makeIdTableFromVector(vectorTable tableContent) {
  AD_CHECK(!tableContent.empty());
  IdTable result{tableContent[0].size(), allocator()};

  // Copying the content into the table.
  for (const auto& row: tableContent) {
    AD_CHECK(row.size() == result.numColumns()); // All rows of an IdTable must have the same length.
    const size_t backIndex{result.size()};
    result.emplace_back();

    for (size_t c = 0; c < row.size(); c++) {
      result(backIndex, c) = I(row[c]);
    }
  }

  return result;
}

TEST(EngineTest, distinctTest) {
  IdTable inp{makeIdTableFromVector({{1, 1, 3, 7}, {6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}})};

  IdTable result{4, allocator()};

  std::vector<size_t> keepIndices{{1, 2}};
  CALL_FIXED_SIZE(4, Engine::distinct, inp, keepIndices, &result);
  
  // For easier checking.
  IdTable sampleSolution{makeIdTableFromVector({{1, 1, 3, 7}, {2, 2, 3, 5}, {3, 6, 5, 4}})};
  ASSERT_EQ(sampleSolution, result);
}

TEST(JoinTest, optionalJoinTest) {
  IdTable a{makeIdTableFromVector({{4, 1, 2}, {2, 1, 3}, {1, 1, 4}, {2, 2, 1}, {1, 3, 1}})};
  IdTable b{makeIdTableFromVector({{3, 3, 1}, {1, 8, 1}, {4, 2, 2}, {1, 1, 3}})};
  IdTable result{4, allocator()};
  vector<array<ColumnIndex, 2>> jcls{};
  jcls.push_back(array<ColumnIndex, 2>{{1, 2}});
  jcls.push_back(array<ColumnIndex, 2>{{2, 1}});

  // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
  // a have to equal those of column 2 of b and vice versa).
  int aWidth{static_cast<int>(a.numColumns())};
  int bWidth{static_cast<int>(b.numColumns())};
  int resultWidth{static_cast<int>(result.numColumns())};
  CALL_FIXED_SIZE((std::array{aWidth, bWidth, resultWidth}),
                  OptionalJoin::optionalJoin, a, b, false, true, jcls, &result);
  
  
  // For easier checking of the result.
  IdTable sampleSolution{makeIdTableFromVector({
          {4, 1, 2, 0},
          {2, 1, 3, 3},
          {1, 1, 4, 0},
          {2, 2, 1, 0},
          {1, 3, 1, 1} 
        })};
  sampleSolution(0, 3) = ID_NO_VALUE;
  sampleSolution(2, 3) = ID_NO_VALUE;
  sampleSolution(3, 3) = ID_NO_VALUE;

  ASSERT_EQ(sampleSolution, result);

  // Test the optional join with variable sized data.
  IdTable va{makeIdTableFromVector({{1, 2, 3, 4, 5, 6}, {1, 2, 3, 7, 5 ,6}, {7, 6, 5, 4, 3, 2}})};

  IdTable vb{makeIdTableFromVector({{2, 3, 4}, {2, 3, 5}, {6, 7, 4}})};

  IdTable vresult{7, allocator()};
  jcls.clear();
  jcls.push_back(array<ColumnIndex, 2>{{1, 0}});
  jcls.push_back(array<ColumnIndex, 2>{{2, 1}});

  aWidth = va.numColumns();
  bWidth = vb.numColumns();
  resultWidth = vresult.numColumns();
  CALL_FIXED_SIZE((std::array{aWidth, bWidth, resultWidth}),
                  OptionalJoin::optionalJoin, va, vb, true, false, jcls, &vresult);
  
  // For easier checking.
  sampleSolution = makeIdTableFromVector({
          {1, 2, 3, 4, 5, 6, 4},
          {1, 2, 3, 4, 5, 6, 5},
          {1, 2, 3, 7, 5, 6, 4},
          {1, 2, 3, 7, 5, 6, 5},
          {0, 6, 7, 0, 0, 0, 4}
        });
  sampleSolution(4, 0) = ID_NO_VALUE;
  sampleSolution(4, 3) = ID_NO_VALUE;
  sampleSolution(4, 4) = ID_NO_VALUE;
  sampleSolution(4, 5) = ID_NO_VALUE;

  ASSERT_EQ(sampleSolution, vresult);
}
