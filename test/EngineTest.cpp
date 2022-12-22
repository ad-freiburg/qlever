// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

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

/*
 * Does what it says on the tin: Save an IdTable with the corresponding
 * join column.
 */
struct IdTableAndJoinColumn {
  IdTable idTable;
  size_t joinColumn;
};

/*
 * A structure containing all information needed for a normal join test. A
 * normal join test is defined as two IdTables being joined, the resulting
 * IdTable tested, if it is sorted after the join column, or not, and this
 * IdTable is than compared with the expectedResult.
 */
struct normalJoinTest {
  IdTableAndJoinColumn leftInput;
  IdTableAndJoinColumn rightInput;
  IdTable expectedResult;
  bool resultMustBeSortedByJoinColumn;
};

/*
 * @brief Tests, whether the given IdTable has the same content as the sample
 * solution and, if the option was choosen, if the IdTable is sorted by
 * the join column.
 *
 * @param table The IdTable that should be tested.
 * @param expectedContent The sample solution. Doesn't need to be sorted,
 *  or the same order of rows as the table.
 * @param resultMustBeSortedByJoinColumn If this is true, it will also be tested, if the table
 *  is sorted by the join column.
 * @param joinColumn The join column of the table.
 * @param l Ignore it. It's only here for being able to make better messages,
 *  if a IdTable fails the comparison.
*/
void compareIdTableWithExpectedContent(const IdTable& table, 
    const IdTable& expectedContent,
    const bool resultMustBeSortedByJoinColumn = false,
    const size_t joinColumn = 0,
    ad_utility::source_location l = ad_utility::source_location::current()
    ) {
  // For generating more informative messages, when failing the comparison.
  std::stringstream traceMessage{};

  auto writeIdTableToStream = [&traceMessage](const IdTable& idTable){
      std::ranges::for_each(idTable, [&traceMessage](const auto& row){
            for (size_t i = 0; i < row.numColumns(); i++) {
              traceMessage << row[i] << " ";
            }
            traceMessage << "\n";
          }, {});
  };

  traceMessage << "compareIdTableWithExpectedContent comparing IdTable\n";
  writeIdTableToStream(table);
  traceMessage << "with IdTable \n";
  writeIdTableToStream(expectedContent);
  auto trace{generateLocationTrace(l, traceMessage.str())};

  // Because we compare tables later by sorting them, so that every table has
  // one definit form, we need to create local copies.
  IdTable localTable{table.clone()};
  IdTable localExpectedContent{expectedContent.clone()};

  if (resultMustBeSortedByJoinColumn) {
    // Is the table sorted by join column?
    ASSERT_TRUE(std::ranges::is_sorted(localTable, {}, [joinColumn](const auto& row){return row[joinColumn];}));
  }

  // Sort both the table and the expectedContent, so that both have a definite
  // form for comparison.
  auto sortFunction = [](
      const auto& row1,
      const auto& row2)
    {
      size_t i{0};
      while (i < (row1.numColumns() - 1) && row1[i] == row2[i]) {
        i++;
      }
      return row1[i] < row2[i];
    };
  std::sort(localTable.begin(), localTable.end(), sortFunction);
  std::sort(localExpectedContent.begin(), localExpectedContent.end(), sortFunction);

  ASSERT_EQ(localTable, localExpectedContent);
}

/*
 * @brief Join two IdTables togehter with the given join function and return
 * the result.
 *
 * @tparam JOIN_FUNCTION is used to allow the transfer of any type of
 *  lambda function, that could contain a join function. You never have to
 *  actually specify this parameter , just let inference do its job.
 *
 * @param tableA, tableB the tables with their join columns. 
 * @param func the function, that will be used for joining the two tables
 *  together. Look into src/engine/Join.h for how it should look like.
 *
 * @returns tableA and tableB joined together in a IdTable.
 */
template<typename JOIN_FUNCTION>
IdTable useJoinFunctionOnIdTables(
        const IdTableAndJoinColumn& tableA,
        const IdTableAndJoinColumn& tableB,
        JOIN_FUNCTION func
        ) {
  
  int resultWidth{static_cast<int>(tableA.idTable.numColumns() +
      tableB.idTable.numColumns()  - 1)};
  IdTable result{static_cast<size_t>(resultWidth), allocator()};
 
  // You need to use this special function for executing lambdas. The normal
  // function for functions won't work.
  // Additionaly, we need to cast the two size_t, because callFixedSize only takes arrays of int.
  ad_utility::callFixedSize((std::array{static_cast<int>(tableA.idTable.numColumns()),
        static_cast<int>(tableB.idTable.numColumns()), resultWidth}),
      func,
      tableA.idTable, tableA.joinColumn,
      tableB.idTable, tableB.joinColumn,
      &result);

  return result;
}

/*
 * @brief Goes through the sets of tests, joins them together with the given
 * join function and compares the results with the given expected content.
 *
 * @tparam JOIN_FUNCTION is used to allow the transfer of any type of
 *  lambda function, that could contain a join function. You never have to
 *  actually specify this parameter, just let inference do its job.
 *
 * @param testSet is an array of normalJoinTests, that describe what tests to
 *  do. For an explanation, how to read normalJoinTest, see the definition.
 * @param func the function, that will be used for joining the two tables
 *  together. Look into src/engine/Join.h, or this file, for how it should look like.
 */
template<typename JOIN_FUNCTION>
void goThroughSetOfTestsWithJoinFunction(
      std::vector<normalJoinTest> testSet,
      JOIN_FUNCTION func,
      ad_utility::source_location l = ad_utility::source_location::current()
    ) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "goThroughSetOfTestsWithJoinFunction")};

  for (normalJoinTest const& test: testSet) {
    IdTable resultTable{useJoinFunctionOnIdTables(test.leftInput,
        test.rightInput, func)};

    compareIdTableWithExpectedContent(resultTable, test.expectedResult, test.resultMustBeSortedByJoinColumn, test.leftInput.joinColumn);
  }
}

/* 
 * @brief Return a vector of normalJoinTest for testing with the normal join function. 
 */
std::vector<normalJoinTest> createNormalJoinTestSet() {
  std::vector<normalJoinTest> myTestSet{};

  // For easier creation of IdTables and readability.
  vectorTable leftIdTable{{{1, 1}, {1, 3}, {2, 1}, {2, 2}, {4, 1}}};
  vectorTable rightIdTable{{{1, 3}, {1, 8}, {3, 1}, {4, 2}}};
  vectorTable sampleSolution{{{1, 1, 3}, {1, 1, 8}, {1, 3, 3}, {1, 3, 8}, {4, 1, 2}}};
  myTestSet.push_back(normalJoinTest{
      IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
      IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
      makeIdTableFromVector(sampleSolution), true});

  leftIdTable = {{1, 1}, {1, 3}, {2, 1}, {2, 2}, {4, 1}};
  rightIdTable ={{1, 3}, {1, 8}, {3, 1}, {4, 2}};
  sampleSolution = {{1, 1, 3}, {1, 1, 8}, {1, 3, 3}, {1, 3, 8}, {4, 1, 2}, {400000, 200000, 200000}};
  for (size_t i = 1; i <= 10000; ++i) {
    rightIdTable.push_back({4 + i, 2 + i});
  }
  leftIdTable.push_back({400000, 200000});
  rightIdTable.push_back({400000, 200000});
  myTestSet.push_back(normalJoinTest{
      IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
      IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
      makeIdTableFromVector(sampleSolution), true});
  
  leftIdTable = {};
  rightIdTable = {};
  sampleSolution = {{40000, 200000, 200000}, {4000001, 200000, 200000}};
  for (size_t i = 1; i <= 10000; ++i) {
    leftIdTable.push_back({4 + i, 2 + i});
  }
  leftIdTable.push_back({40000, 200000});
  rightIdTable.push_back({40000, 200000});
  for (size_t i = 1; i <= 10000; ++i) {
    leftIdTable.push_back({40000 + i, 2 + i});
  }
  leftIdTable.push_back({4000001, 200000});
  rightIdTable.push_back({4000001, 200000});
  myTestSet.push_back(normalJoinTest{
      IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
      IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
      makeIdTableFromVector(sampleSolution), true});
  
  leftIdTable = {{0, 1}, {0, 2}, {1, 3}, {1, 4}};
  rightIdTable = {{0}};
  sampleSolution = {{0, 1}, {0, 2}};
  myTestSet.push_back(normalJoinTest{
      IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
      IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
      makeIdTableFromVector(sampleSolution), true});

  return myTestSet;
}


TEST(EngineTest, joinTest) {
  Join J{Join::InvalidOnlyForTestingJoinTag{}};
  auto JoinLambda = [&J]<int A, int B, int C>(auto&&... args) {
    return J.join<A, B, C>(AD_FWD(args)...);
  };
  goThroughSetOfTestsWithJoinFunction(createNormalJoinTestSet(), JoinLambda);
};

TEST(EngineTest, optionalJoinTest) {
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

TEST(EngineTest, distinctTest) {
  IdTable inp{makeIdTableFromVector({{1, 1, 3, 7}, {6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}})};

  IdTable result{4, allocator()};

  std::vector<size_t> keepIndices{{1, 2}};
  CALL_FIXED_SIZE(4, Engine::distinct, inp, keepIndices, &result);
  
  // For easier checking.
  IdTable sampleSolution{makeIdTableFromVector({{1, 1, 3, 7}, {2, 2, 3, 5}, {3, 6, 5, 4}})};
  ASSERT_EQ(sampleSolution, result);
}

TEST(EngineTest, hashJoinTest) {
  // Going through the tests of the normal lambda. The result for hashed join
  // is sorted, if the input was also sorted, so hashJoin can go through them,
  // without modifying them first.
  Join J{Join::InvalidOnlyForTestingJoinTag{}};
  auto HashJoinLambda = [&J]<int A, int B, int C>(auto&&... args) {
    return J.hashJoin<A, B, C>(AD_FWD(args)...);
  };
  auto JoinLambda = [&J]<int A, int B, int C>(auto&&... args) {
    return J.join<A, B, C>(AD_FWD(args)...);
  };
  goThroughSetOfTestsWithJoinFunction(createNormalJoinTestSet(), HashJoinLambda);

  /*
   * Create two sorted IdTables. Join them using the normal join and the
   * hashed join. Compare.
  */
  vectorTable a{{
      {34, 73, 92, 61, 18},
      {11, 80, 20, 43, 75},
      {96, 51, 40, 67, 23}
    }};
 
  vectorTable b{{
      {34, 73, 92, 61, 18},
      {96, 2, 76, 87, 38},
      {96, 16, 27, 22, 38},
      {7, 51, 40, 67, 23}
    }};
  
  // Saving the results and comparing them.
  IdTable result1{useJoinFunctionOnIdTables(
      IdTableAndJoinColumn{makeIdTableFromVector(a), 0},
      IdTableAndJoinColumn{makeIdTableFromVector(b), 0},
      JoinLambda)}; // Normal.
  IdTable result2{useJoinFunctionOnIdTables(
      IdTableAndJoinColumn{makeIdTableFromVector(a), 0},
      IdTableAndJoinColumn{makeIdTableFromVector(b), 0},
      HashJoinLambda)}; // Hash.
  ASSERT_EQ(result1, result2);

  // Checking if result1 and result2 are correct.
  compareIdTableWithExpectedContent(result1, makeIdTableFromVector(
      {
        {34, 73, 92, 61, 18, 73, 92, 61, 18},
        {96, 51, 40, 67, 23, 2, 76, 87, 38},
        {96, 51, 40, 67, 23, 16, 27, 22, 38}
      }
    ),
    true,
    0
  );
  compareIdTableWithExpectedContent(result2, makeIdTableFromVector(
      {
        {34, 73, 92, 61, 18, 73, 92, 61, 18},
        {96, 51, 40, 67, 23, 2, 76, 87, 38},
        {96, 51, 40, 67, 23, 16, 27, 22, 38}
      }
    ),
    true,
    0
  );

  // Random shuffle the tables, hash join them, sort the result and check,
  // if the result is still correct.
  randomShuffle(a.begin(), a.end());
  randomShuffle(b.begin(), b.end());
  result2 = useJoinFunctionOnIdTables(
      IdTableAndJoinColumn{makeIdTableFromVector(a), 0},
      IdTableAndJoinColumn{makeIdTableFromVector(b), 0},
      HashJoinLambda);

  // Sorting is done inside this function.
  compareIdTableWithExpectedContent(result2, makeIdTableFromVector(
      {
        {34, 73, 92, 61, 18, 73, 92, 61, 18},
        {96, 51, 40, 67, 23, 2, 76, 87, 38},
        {96, 51, 40, 67, 23, 16, 27, 22, 38}
      }
    ),
    false
  );
};
