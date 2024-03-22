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
#include "./util/JoinHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Values.h"
#include "engine/ValuesForTesting.h"
#include "engine/idTable/IdTable.h"
#include "util/Forward.h"
#include "util/IndexTestHelpers.h"
#include "util/Random.h"
#include "util/SourceLocation.h"

using ad_utility::testing::makeAllocator;
namespace {

using Vars = std::vector<std::optional<Variable>>;
auto iri = [](std::string_view s) {
  return TripleComponent::Iri::fromIriref(s);
};

/*
 * A structure containing all information needed for a normal join test. A
 * normal join test is defined as two IdTables being joined, the resulting
 * IdTable tested, if it is sorted after the join column, or not, and this
 * IdTable is than compared with the expectedResult.
 */
struct JoinTestCase {
  IdTableAndJoinColumn leftInput;
  IdTableAndJoinColumn rightInput;
  IdTable expectedResult;
  bool resultMustBeSortedByJoinColumn;
};

/*
 * @brief Goes through the sets of tests, joins them together with the given
 * join function and compares the results with the given expected content.
 *
 * @tparam JOIN_FUNCTION is used to allow the transfer of any type of
 *  lambda function, that could contain a join function. You never have to
 *  actually specify this parameter, just let inference do its job.
 *
 * @param testSet is an array of JoinTestCases, that describe what tests to
 *  do. For an explanation, how to read JoinTestCase, see the definition.
 * @param func the function, that will be used for joining the two tables
 *  together. Look into src/engine/Join.h, or this file, for how it should look
 * like.
 */
template <typename JOIN_FUNCTION>
void goThroughSetOfTestsWithJoinFunction(
    const std::vector<JoinTestCase>& testSet, JOIN_FUNCTION func,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "goThroughSetOfTestsWithJoinFunction")};

  for (JoinTestCase const& test : testSet) {
    IdTable resultTable{
        useJoinFunctionOnIdTables(test.leftInput, test.rightInput, func)};

    compareIdTableWithExpectedContent(resultTable, test.expectedResult,
                                      test.resultMustBeSortedByJoinColumn,
                                      test.leftInput.joinColumn);
  }
}

void runTestCasesForAllJoinAlgorithms(
    std::vector<JoinTestCase> testSet,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "runTestCasesForAllJoinAlgorithms")};

  // All normal join algorithm defined as lambda functions for easier handing
  // over to helper functions.
  auto hashJoinLambda = makeHashJoinLambda();
  auto joinLambda = makeJoinLambda();

  // For sorting IdTableAndJoinColumn by their join column.
  auto sortByJoinColumn = [](IdTableAndJoinColumn& idTableAndJC) {
    std::ranges::sort(idTableAndJC.idTable, {},
                      [&idTableAndJC](const auto& row) {
                        return row[idTableAndJC.joinColumn];
                      });
  };

  // Random shuffle both tables, run hashJoin, check result.
  std::ranges::for_each(testSet, [](JoinTestCase& testCase) {
    randomShuffle(testCase.leftInput.idTable.begin(),
                  testCase.leftInput.idTable.end());
    randomShuffle(testCase.rightInput.idTable.begin(),
                  testCase.rightInput.idTable.end());
    testCase.resultMustBeSortedByJoinColumn = false;
  });
  goThroughSetOfTestsWithJoinFunction(testSet, hashJoinLambda);

  // Sort the larger table by join column, run hashJoin, check result (this time
  // it's sorted).
  std::ranges::for_each(testSet, [&sortByJoinColumn](JoinTestCase& testCase) {
    IdTableAndJoinColumn& largerInputTable =
        (testCase.leftInput.idTable.size() >=
         testCase.rightInput.idTable.size())
            ? testCase.leftInput
            : testCase.rightInput;
    sortByJoinColumn(largerInputTable);
    testCase.resultMustBeSortedByJoinColumn = true;
  });
  goThroughSetOfTestsWithJoinFunction(testSet, hashJoinLambda);

  // Sort both tables, run merge join and hash join, check result. (Which has to
  // be sorted.)
  std::ranges::for_each(testSet, [&sortByJoinColumn](JoinTestCase& testCase) {
    sortByJoinColumn(testCase.leftInput);
    sortByJoinColumn(testCase.rightInput);
    testCase.resultMustBeSortedByJoinColumn = true;
  });
  goThroughSetOfTestsWithJoinFunction(testSet, joinLambda);
  goThroughSetOfTestsWithJoinFunction(testSet, hashJoinLambda);
}

/*
 * @brief Return a vector of JoinTestCase for testing with the join functions.
 */
std::vector<JoinTestCase> createJoinTestSet() {
  std::vector<JoinTestCase> myTestSet{};

  // For easier creation of IdTables and readability.
  VectorTable leftIdTable{{{1, 1}, {1, 3}, {2, 1}, {2, 2}, {4, 1}}};
  VectorTable rightIdTable{{{1, 3}, {1, 8}, {3, 1}, {4, 2}}};
  VectorTable expectedResult{
      {{1, 1, 3}, {1, 1, 8}, {1, 3, 3}, {1, 3, 8}, {4, 1, 2}}};
  myTestSet.push_back(
      JoinTestCase{IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
                   IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
                   makeIdTableFromVector(expectedResult), true});

  leftIdTable = {{1, 1}, {1, 3}, {2, 1}, {2, 2}, {4, 1}};
  rightIdTable = {{1, 3}, {1, 8}, {3, 1}, {4, 2}};
  expectedResult = {{1, 1, 3}, {1, 1, 8}, {1, 3, 3},
                    {1, 3, 8}, {4, 1, 2}, {400000, 200000, 200000}};
  for (int64_t i = 1; i <= 10000; ++i) {
    rightIdTable.push_back({4 + i, 2 + i});
  }
  leftIdTable.push_back({400000, 200000});
  rightIdTable.push_back({400000, 200000});
  myTestSet.push_back(
      JoinTestCase{IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
                   IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
                   makeIdTableFromVector(expectedResult), true});

  leftIdTable = {};
  rightIdTable = {};
  expectedResult = {{40000, 200000, 200000}, {4000001, 200000, 200000}};
  for (int64_t i = 1; i <= 10000; ++i) {
    leftIdTable.push_back({4 + i, 2 + i});
  }
  leftIdTable.push_back({40000, 200000});
  rightIdTable.push_back({40000, 200000});
  for (int64_t i = 1; i <= 10000; ++i) {
    leftIdTable.push_back({40000 + i, 2 + i});
  }
  leftIdTable.push_back({4000001, 200000});
  rightIdTable.push_back({4000001, 200000});
  myTestSet.push_back(
      JoinTestCase{IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
                   IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
                   makeIdTableFromVector(expectedResult), true});

  leftIdTable = {{0, 1}, {0, 2}, {1, 3}, {1, 4}};
  rightIdTable = {{0}};
  expectedResult = {{0, 1}, {0, 2}};
  myTestSet.push_back(
      JoinTestCase{IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
                   IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
                   makeIdTableFromVector(expectedResult), true});

  leftIdTable = {
      {34, 73, 92, 61, 18}, {11, 80, 20, 43, 75}, {96, 51, 40, 67, 23}};
  rightIdTable = {{34, 73, 92, 61, 18},
                  {96, 2, 76, 87, 38},
                  {96, 16, 27, 22, 38},
                  {7, 51, 40, 67, 23}};
  expectedResult = {{34, 73, 92, 61, 18, 73, 92, 61, 18},
                    {96, 51, 40, 67, 23, 2, 76, 87, 38},
                    {96, 51, 40, 67, 23, 16, 27, 22, 38}};
  myTestSet.push_back(
      JoinTestCase{IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
                   IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
                   makeIdTableFromVector(expectedResult), true});

  return myTestSet;
}
}  // namespace

TEST(JoinTest, joinTest) {
  runTestCasesForAllJoinAlgorithms(createJoinTestSet());
};

// Several helpers for the test cases below.
namespace {

// The exact order of the columns of a join result might change over time, for
// example we reorder inputs for simplicity or to more easily find them in the
// cache. That's why we only assert that the column associated with a given
// variable contains the expected contents, independent of the concrete column
// index that variable is assigned to.

// A hash map that connects variables to the expected contents of the
// corresponding result column and the `UndefStatus`.
using ExpectedColumns = ad_utility::HashMap<
    Variable,
    std::pair<std::span<const Id>, ColumnIndexAndTypeInfo::UndefStatus>>;

// Test that the result of the `join` matches the `expected` outcome.
void testJoinOperation(Join& join, const ExpectedColumns& expected) {
  auto res = join.getResult();
  const auto& varToCols = join.getExternallyVisibleVariableColumns();
  EXPECT_EQ(varToCols.size(), expected.size());
  const auto& table = res->idTable();
  ASSERT_EQ(table.numColumns(), expected.size());
  for (const auto& [var, columnAndStatus] : expected) {
    const auto& [colIndex, undefStatus] = varToCols.at(var);
    decltype(auto) columnSpan = table.getColumn(colIndex);
    std::vector column(columnSpan.begin(), columnSpan.end());
    EXPECT_EQ(undefStatus, columnAndStatus.second);
    EXPECT_THAT(column, ::testing::ElementsAreArray(columnAndStatus.first))
        << "Columns for variable " << var.name() << " did not match";
  }
}

// Convert a `VariableToColumnMap` (which assumes a fixed ordering of the
// columns), and an `idTable` to the `ExpectedColumns` format that is
// independent of the concrete assignment from variables to columns indices
ExpectedColumns makeExpectedColumns(const VariableToColumnMap& varToColMap,
                                    const IdTable& idTable) {
  ExpectedColumns result;
  for (const auto& [var, colIndexAndStatus] : varToColMap) {
    result[var] = {idTable.getColumn(colIndexAndStatus.columnIndex_),
                   colIndexAndStatus.mightContainUndef_};
  }
  return result;
}

// Create a `Values` clause with a single `variable` that stores the given
// `values`. The values must all be vocabulary entries (IRIs or literals) that
// are contained in the index of the `qec`, otherwise `std::bad_optional_access`
// will be thrown.
std::shared_ptr<QueryExecutionTree> makeValuesForSingleVariable(
    QueryExecutionContext* qec, std::string variable,
    std::vector<TripleComponent> values) {
  parsedQuery::SparqlValues sparqlValues;
  sparqlValues._variables.emplace_back(std::move(variable));
  for (auto& value : values) {
    sparqlValues._values.push_back({std::move(value)});
  }
  return ad_utility::makeExecutionTree<Values>(qec, sparqlValues);
}

using enum Permutation::Enum;
auto I = ad_utility::testing::IntId;
using Var = Variable;
}  // namespace

TEST(JoinTest, joinWithFullScanPSO) {
  auto qec = ad_utility::testing::getQec("<x> <p> 1. <x> <o> <x>. <x> <a> 3.");
  // Expressions in HAVING clauses are converted to special internal aliases.
  // Test the combination of parsing and evaluating such queries.
  auto fullScanPSO = ad_utility::makeExecutionTree<IndexScan>(
      qec, PSO, SparqlTriple{Var{"?s"}, "?p", Var{"?o"}});
  auto valuesTree =
      makeValuesForSingleVariable(qec, "?p", {iri("<o>"), iri("<a>")});

  auto join = Join{qec, fullScanPSO, valuesTree, 0, 0};

  auto id = ad_utility::testing::makeGetId(qec->getIndex());

  auto x = id("<x>");
  auto p = id("<p>");
  auto a = id("<a>");
  auto o = id("<o>");
  auto expected = makeIdTableFromVector({{a, x, I(3)}, {o, x, x}});
  VariableToColumnMap expectedVariables{
      {Variable{"?p"}, makeAlwaysDefinedColumn(0)},
      {Variable{"?s"}, makeAlwaysDefinedColumn(1)},
      {Variable{"?o"}, makeAlwaysDefinedColumn(2)}};
  testJoinOperation(join, makeExpectedColumns(expectedVariables, expected));

  auto joinSwitched = Join{qec, valuesTree, fullScanPSO, 0, 0};
  testJoinOperation(joinSwitched,
                    makeExpectedColumns(expectedVariables, expected));

  // A `Join` of two full scans.
  {
    auto fullScanSPO = ad_utility::makeExecutionTree<IndexScan>(
        qec, SPO, SparqlTriple{Var{"?s"}, "?p", Var{"?o"}});
    auto fullScanOPS = ad_utility::makeExecutionTree<IndexScan>(
        qec, OPS, SparqlTriple{Var{"?s2"}, "?p2", Var{"?s"}});
    // The knowledge graph is "<x> <p> 1 . <x> <o> <x> . <x> <a> 3 ."
    auto expected = makeIdTableFromVector(
        {{x, a, I(3), o, x}, {x, o, x, o, x}, {x, p, I(1), o, x}});
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, makeAlwaysDefinedColumn(0)},
        {Variable{"?p"}, makeAlwaysDefinedColumn(1)},
        {Variable{"?o"}, makeAlwaysDefinedColumn(2)},
        {Variable{"?p2"}, makeAlwaysDefinedColumn(3)},
        {Variable{"?s2"}, makeAlwaysDefinedColumn(4)}};
    auto join = Join{qec, fullScanSPO, fullScanOPS, 0, 0};
    testJoinOperation(join, makeExpectedColumns(expectedVariables, expected));
  }
}

// The following two tests run different code depending on the setting of the
// maximal size for materialized index scans. That's why they are run twice with
// different settings.
TEST(JoinTest, joinWithColumnAndScan) {
  auto test = [](size_t materializationThreshold) {
    auto qec = ad_utility::testing::getQec("<x> <p> 1. <x2> <p> 2. <x> <a> 3.");
    RuntimeParameters().set<"lazy-index-scan-max-size-materialization">(
        materializationThreshold);
    qec->getQueryTreeCache().clearAll();
    auto fullScanPSO = ad_utility::makeExecutionTree<IndexScan>(
        qec, PSO, SparqlTriple{Var{"?s"}, "<p>", Var{"?o"}});
    auto valuesTree = makeValuesForSingleVariable(qec, "?s", {iri("<x>")});

    auto join = Join{qec, fullScanPSO, valuesTree, 0, 0};
    EXPECT_EQ(join.getDescriptor(), "Join on ?s");

    auto getId = ad_utility::testing::makeGetId(qec->getIndex());
    auto idX = getId("<x>");
    auto expected = makeIdTableFromVector({{idX, I(1)}});
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, makeAlwaysDefinedColumn(0)},
        {Variable{"?o"}, makeAlwaysDefinedColumn(1)}};
    testJoinOperation(join, makeExpectedColumns(expectedVariables, expected));

    auto joinSwitched = Join{qec, valuesTree, fullScanPSO, 0, 0};
    testJoinOperation(joinSwitched,
                      makeExpectedColumns(expectedVariables, expected));
  };
  test(0);
  test(1);
  test(2);
  test(3);
  test(1'000'000);
}

TEST(JoinTest, joinWithColumnAndScanEmptyInput) {
  auto test = [](size_t materializationThreshold) {
    auto qec = ad_utility::testing::getQec("<x> <p> 1. <x2> <p> 2. <x> <a> 3.");
    RuntimeParameters().set<"lazy-index-scan-max-size-materialization">(
        materializationThreshold);
    qec->getQueryTreeCache().clearAll();
    auto fullScanPSO = ad_utility::makeExecutionTree<IndexScan>(
        qec, PSO, SparqlTriple{Var{"?s"}, "<p>", Var{"?o"}});
    auto valuesTree =
        ad_utility::makeExecutionTree<ValuesForTestingNoKnownEmptyResult>(
            qec, IdTable{1, qec->getAllocator()}, Vars{Variable{"?s"}});
    auto join = Join{qec, fullScanPSO, valuesTree, 0, 0};
    EXPECT_EQ(join.getDescriptor(), "Join on ?s");

    auto expected = IdTable{2, qec->getAllocator()};
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, makeAlwaysDefinedColumn(0)},
        {Variable{"?o"}, makeAlwaysDefinedColumn(1)}};
    testJoinOperation(join, makeExpectedColumns(expectedVariables, expected));

    auto joinSwitched = Join{qec, valuesTree, fullScanPSO, 0, 0};
    testJoinOperation(joinSwitched,
                      makeExpectedColumns(expectedVariables, expected));
  };
  test(0);
  test(1);
  test(2);
  test(3);
  test(1'000'000);
}

TEST(JoinTest, joinWithColumnAndScanUndefValues) {
  auto test = [](size_t materializationThreshold) {
    auto qec = ad_utility::testing::getQec("<x> <p> 1. <x2> <p> 2. <x> <a> 3.");
    RuntimeParameters().set<"lazy-index-scan-max-size-materialization">(
        materializationThreshold);
    qec->getQueryTreeCache().clearAll();
    auto fullScanPSO = ad_utility::makeExecutionTree<IndexScan>(
        qec, PSO, SparqlTriple{Var{"?s"}, "<p>", Var{"?o"}});
    auto U = Id::makeUndefined();
    auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{U}}), Vars{Variable{"?s"}});
    auto join = Join{qec, fullScanPSO, valuesTree, 0, 0};
    EXPECT_EQ(join.getDescriptor(), "Join on ?s");

    auto getId = ad_utility::testing::makeGetId(qec->getIndex());
    auto idX = getId("<x>");
    auto idX2 = getId("<x2>");
    auto expected = makeIdTableFromVector({{idX, I(1)}, {idX2, I(2)}});
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, makeAlwaysDefinedColumn(0)},
        {Variable{"?o"}, makeAlwaysDefinedColumn(1)}};
    testJoinOperation(join, makeExpectedColumns(expectedVariables, expected));

    auto joinSwitched = Join{qec, valuesTree, fullScanPSO, 0, 0};
    testJoinOperation(joinSwitched,
                      makeExpectedColumns(expectedVariables, expected));
  };
  test(0);
  test(1);
  test(2);
  test(3);
  test(1'000'000);
}

TEST(JoinTest, joinTwoScans) {
  auto test = [](size_t materializationThreshold) {
    auto qec = ad_utility::testing::getQec(
        "<x> <p> 1. <x2> <p> 2. <x> <p2> 3 . <x2> <p2> 4. <x3> <p2> 7. ");
    RuntimeParameters().set<"lazy-index-scan-max-size-materialization">(
        materializationThreshold);
    qec->getQueryTreeCache().clearAll();
    auto scanP = ad_utility::makeExecutionTree<IndexScan>(
        qec, PSO, SparqlTriple{Var{"?s"}, "<p>", Var{"?o"}});
    auto scanP2 = ad_utility::makeExecutionTree<IndexScan>(
        qec, PSO, SparqlTriple{Var{"?s"}, "<p2>", Var{"?q"}});
    auto join = Join{qec, scanP2, scanP, 0, 0};
    EXPECT_EQ(join.getDescriptor(), "Join on ?s");

    auto id = ad_utility::testing::makeGetId(qec->getIndex());
    auto expected = makeIdTableFromVector(
        {{id("<x>"), I(3), I(1)}, {id("<x2>"), I(4), I(2)}});
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, makeAlwaysDefinedColumn(0)},
        {Variable{"?q"}, makeAlwaysDefinedColumn(1)},
        {Variable{"?o"}, makeAlwaysDefinedColumn(2)}};
    testJoinOperation(join, makeExpectedColumns(expectedVariables, expected));

    auto joinSwitched = Join{qec, scanP2, scanP, 0, 0};
    testJoinOperation(joinSwitched,
                      makeExpectedColumns(expectedVariables, expected));
  };
  test(0);
  test(1);
  test(2);
  test(3);
  test(1'000'000);
}

TEST(JoinTest, invalidJoinVariable) {
  auto qec = ad_utility::testing::getQec(
      "<x> <p> 1. <x2> <p> 2. <x> <p2> 3 . <x2> <p2> 4. <x3> <p2> 7. ");
  auto valuesTree = makeValuesForSingleVariable(qec, "?s", {"<x>"});
  auto valuesTree2 = makeValuesForSingleVariable(qec, "?p", {"<x>"});

  ASSERT_ANY_THROW(Join(qec, valuesTree2, valuesTree, 0, 0));
}
