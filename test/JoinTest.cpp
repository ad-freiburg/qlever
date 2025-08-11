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
#include "engine/JoinHelpers.h"
#include "engine/NeutralOptional.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Values.h"
#include "engine/ValuesForTesting.h"
#include "engine/idTable/IdTable.h"
#include "util/Forward.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"
#include "util/Random.h"
#include "util/RuntimeParametersTestHelpers.h"
#include "util/SourceLocation.h"

using ad_utility::testing::makeAllocator;
namespace {

using qlever::joinHelpers::CHUNK_SIZE;

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

void removeJoinColFromVarColMap(const Variable& var, VariableToColumnMap& map) {
  auto colIdx = map.at(var).columnIndex_;
  map.erase(var);
  for (auto& [_, info] : map) {
    auto& [idx, status] = info;
    if (idx > colIdx) {
      --idx;
    }
  }
}

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
    ql::ranges::sort(idTableAndJC.idTable, {},
                     [&idTableAndJC](const auto& row) {
                       return row[idTableAndJC.joinColumn];
                     });
  };

  // Random shuffle both tables, run hashJoin, check result.
  ql::ranges::for_each(testSet, [](JoinTestCase& testCase) {
    randomShuffle(testCase.leftInput.idTable.begin(),
                  testCase.leftInput.idTable.end());
    randomShuffle(testCase.rightInput.idTable.begin(),
                  testCase.rightInput.idTable.end());
    testCase.resultMustBeSortedByJoinColumn = false;
  });
  goThroughSetOfTestsWithJoinFunction(testSet, hashJoinLambda);

  // Sort the larger table by join column, run hashJoin, check result (this time
  // it's sorted).
  ql::ranges::for_each(testSet, [&sortByJoinColumn](JoinTestCase& testCase) {
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
  ql::ranges::for_each(testSet, [&sortByJoinColumn](JoinTestCase& testCase) {
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
    std::pair<ql::span<const Id>, ColumnIndexAndTypeInfo::UndefStatus>>;

// Test that the result of the `join` matches the `expected` outcome.
// If `requestLaziness` is true, the join is requested to be lazy. If
// `expectLazinessParityWhenNonEmpty` is true, the laziness of the result is
// expected to be the same as `requestLaziness` if the result is not empty.
void testJoinOperation(Join& join, const ExpectedColumns& expected,
                       bool requestLaziness = false,
                       bool expectLazinessParityWhenNonEmpty = false,
                       ad_utility::source_location location =
                           ad_utility::source_location::current()) {
  auto lt = generateLocationTrace(location);
  auto res = join.getResult(false, requestLaziness
                                       ? ComputationMode::LAZY_IF_SUPPORTED
                                       : ComputationMode::FULLY_MATERIALIZED);
  const auto& varToCols = join.getExternallyVisibleVariableColumns();
  EXPECT_EQ(varToCols.size(), expected.size());
  if (expectLazinessParityWhenNonEmpty &&
      (!res->isFullyMaterialized() || !res->idTable().empty())) {
    EXPECT_EQ(res->isFullyMaterialized(), !requestLaziness);
  }
  IdTable table =
      res->isFullyMaterialized()
          ? res->idTable().clone()
          : aggregateTables(res->idTables(), join.getResultWidth()).first;
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

struct JoinTestParametrized : public ::testing::TestWithParam<bool> {};

TEST_P(JoinTestParametrized, joinWithFullScanPSO) {
  bool keepJoinCol = GetParam();
  auto qec = ad_utility::testing::getQec("<x> <p> 1. <x> <o> <x>. <x> <a> 3.");
  // Expressions in HAVING clauses are converted to special internal aliases.
  // Test the combination of parsing and evaluating such queries.
  auto fullScanPSO = ad_utility::makeExecutionTree<IndexScan>(
      qec, PSO, SparqlTripleSimple{Var{"?s"}, Var{"?p"}, Var{"?o"}});
  auto valuesTree =
      makeValuesForSingleVariable(qec, "?p", {iri("<o>"), iri("<a>")});

  auto join = Join{qec, fullScanPSO, valuesTree, 0, 0, keepJoinCol};

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

  if (!keepJoinCol) {
    removeJoinColFromVarColMap(Variable{"?p"}, expectedVariables);
    using C = ColumnIndex;
    expected.setColumnSubset(std::array{C{1}, C{2}});
  }

  testJoinOperation(join, makeExpectedColumns(expectedVariables, expected));

  auto joinSwitched = Join{qec, valuesTree, fullScanPSO, 0, 0, keepJoinCol};
  testJoinOperation(joinSwitched,
                    makeExpectedColumns(expectedVariables, expected));

  // A `Join` of two full scans.
  {
    auto fullScanSPO = ad_utility::makeExecutionTree<IndexScan>(
        qec, SPO, SparqlTripleSimple{Var{"?s"}, Var{"?p"}, Var{"?o"}});
    auto fullScanOPS = ad_utility::makeExecutionTree<IndexScan>(
        qec, OPS, SparqlTripleSimple{Var{"?s2"}, Var{"?p2"}, Var{"?s"}});
    // The knowledge graph is "<x> <p> 1 . <x> <o> <x> . <x> <a> 3 ."
    auto expected = makeIdTableFromVector(
        {{x, a, I(3), o, x}, {x, o, x, o, x}, {x, p, I(1), o, x}});
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, makeAlwaysDefinedColumn(0)},
        {Variable{"?p"}, makeAlwaysDefinedColumn(1)},
        {Variable{"?o"}, makeAlwaysDefinedColumn(2)},
        {Variable{"?p2"}, makeAlwaysDefinedColumn(3)},
        {Variable{"?s2"}, makeAlwaysDefinedColumn(4)}};
    auto join = Join{qec, fullScanSPO, fullScanOPS, 0, 0, keepJoinCol};

    if (!keepJoinCol) {
      removeJoinColFromVarColMap(Variable{"?s"}, expectedVariables);
      using C = ColumnIndex;
      expected.setColumnSubset(std::array{C{1}, C{2}, C{3}, C{4}});
    }
    testJoinOperation(join, makeExpectedColumns(expectedVariables, expected));
  }
}

// The following two tests run different code depending on the setting of the
// maximal size for materialized index scans. That's why they are run twice with
// different settings.
TEST_P(JoinTestParametrized, joinWithColumnAndScan) {
  bool keepJoinCol = GetParam();
  auto test = [keepJoinCol](size_t materializationThreshold) {
    auto qec = ad_utility::testing::getQec("<x> <p> 1. <x2> <p> 2. <x> <a> 3.");
    auto cleanup =
        setRuntimeParameterForTest<"lazy-index-scan-max-size-materialization">(
            materializationThreshold);
    qec->getQueryTreeCache().clearAll();
    auto fullScanPSO = ad_utility::makeExecutionTree<IndexScan>(
        qec, PSO, SparqlTripleSimple{Var{"?s"}, iri("<p>"), Var{"?o"}});
    auto valuesTree = makeValuesForSingleVariable(qec, "?s", {iri("<x>")});

    auto join = Join{qec, fullScanPSO, valuesTree, 0, 0, keepJoinCol};
    EXPECT_EQ(join.getDescriptor(), "Join on ?s");

    auto getId = ad_utility::testing::makeGetId(qec->getIndex());
    auto idX = getId("<x>");
    auto expected = makeIdTableFromVector({{idX, I(1)}});
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, makeAlwaysDefinedColumn(0)},
        {Variable{"?o"}, makeAlwaysDefinedColumn(1)}};
    if (!keepJoinCol) {
      removeJoinColFromVarColMap(Variable{"?s"}, expectedVariables);
      expected.setColumnSubset(std::array{ColumnIndex{1}});
    }
    testJoinOperation(join, makeExpectedColumns(expectedVariables, expected));

    auto joinSwitched = Join{qec, valuesTree, fullScanPSO, 0, 0, keepJoinCol};
    testJoinOperation(joinSwitched,
                      makeExpectedColumns(expectedVariables, expected));
  };
  test(0);
  test(1);
  test(2);
  test(3);
  test(1'000'000);
}

TEST_P(JoinTestParametrized, joinWithColumnAndScanEmptyInput) {
  auto keepJoinCol = GetParam();
  auto test = [keepJoinCol](size_t materializationThreshold,
                            bool lazyJoinValues) {
    auto qec = ad_utility::testing::getQec("<x> <p> 1. <x2> <p> 2. <x> <a> 3.");
    auto cleanup =
        setRuntimeParameterForTest<"lazy-index-scan-max-size-materialization">(
            materializationThreshold);
    qec->getQueryTreeCache().clearAll();
    auto fullScanPSO = ad_utility::makeExecutionTree<IndexScan>(
        qec, PSO, SparqlTripleSimple{Var{"?s"}, iri("<p>"), Var{"?o"}});
    auto valuesTree =
        ad_utility::makeExecutionTree<ValuesForTestingNoKnownEmptyResult>(
            qec, IdTable{1, qec->getAllocator()}, Vars{Variable{"?s"}}, false,
            std::vector<ColumnIndex>{0}, LocalVocab{}, std::nullopt,
            !lazyJoinValues);
    auto join = Join{qec, fullScanPSO, valuesTree, 0, 0, keepJoinCol};
    EXPECT_EQ(join.getDescriptor(), "Join on ?s");

    auto expected = IdTable{2, qec->getAllocator()};
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, makeAlwaysDefinedColumn(0)},
        {Variable{"?o"}, makeAlwaysDefinedColumn(1)}};

    if (!keepJoinCol) {
      removeJoinColFromVarColMap(Variable{"?s"}, expectedVariables);
      expected.setColumnSubset(std::array{ColumnIndex{1}});
    }
    testJoinOperation(join, makeExpectedColumns(expectedVariables, expected));

    auto joinSwitched = Join{qec, valuesTree, fullScanPSO, 0, 0, keepJoinCol};
    testJoinOperation(joinSwitched,
                      makeExpectedColumns(expectedVariables, expected));
  };
  for (bool lazyJoinValues : {true, false}) {
    test(0, lazyJoinValues);
    test(1, lazyJoinValues);
    test(2, lazyJoinValues);
    test(3, lazyJoinValues);
    test(1'000'000, lazyJoinValues);
  }
}

TEST_P(JoinTestParametrized, joinWithColumnAndScanUndefValues) {
  auto keepJoinCol = GetParam();
  auto test = [keepJoinCol](size_t materializationThreshold,
                            bool lazyJoinValues) {
    auto qec = ad_utility::testing::getQec("<x> <p> 1. <x2> <p> 2. <x> <a> 3.");
    auto cleanup =
        setRuntimeParameterForTest<"lazy-index-scan-max-size-materialization">(
            materializationThreshold);
    qec->getQueryTreeCache().clearAll();
    auto fullScanPSO = ad_utility::makeExecutionTree<IndexScan>(
        qec, PSO, SparqlTripleSimple{Var{"?s"}, iri("<p>"), Var{"?o"}});
    auto U = Id::makeUndefined();
    auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{U}}), Vars{Variable{"?s"}}, false,
        std::vector<ColumnIndex>{0}, LocalVocab{}, std::nullopt,
        !lazyJoinValues);
    auto join = Join{qec, fullScanPSO, valuesTree, 0, 0, keepJoinCol};
    EXPECT_EQ(join.getDescriptor(), "Join on ?s");

    auto getId = ad_utility::testing::makeGetId(qec->getIndex());
    auto idX = getId("<x>");
    auto idX2 = getId("<x2>");
    auto expected = makeIdTableFromVector({{idX, I(1)}, {idX2, I(2)}});
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, makeAlwaysDefinedColumn(0)},
        {Variable{"?o"}, makeAlwaysDefinedColumn(1)}};
    if (!keepJoinCol) {
      removeJoinColFromVarColMap(Variable{"?s"}, expectedVariables);
      expected.setColumnSubset(std::array{ColumnIndex{1}});
    }
    auto expectedColumns = makeExpectedColumns(expectedVariables, expected);

    qec->getQueryTreeCache().clearAll();
    testJoinOperation(join, expectedColumns, true,
                      materializationThreshold < 3);
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(join, expectedColumns, false);

    auto joinSwitched = Join{qec, valuesTree, fullScanPSO, 0, 0, keepJoinCol};
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(joinSwitched, expectedColumns, true,
                      materializationThreshold < 3);
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(joinSwitched, expectedColumns, false);
  };
  for (bool lazyJoinValues : {true, false}) {
    test(0, lazyJoinValues);
    test(1, lazyJoinValues);
    test(2, lazyJoinValues);
    test(3, lazyJoinValues);
    test(1'000'000, lazyJoinValues);
  }
}

TEST_P(JoinTestParametrized, joinTwoScans) {
  auto keepJoinCol = GetParam();
  auto test = [keepJoinCol](size_t materializationThreshold) {
    auto qec = ad_utility::testing::getQec(
        "<x> <p> 1. <x2> <p> 2. <x> <p2> 3 . <x2> <p2> 4. <x3> <p2> 7. ");
    auto cleanup =
        setRuntimeParameterForTest<"lazy-index-scan-max-size-materialization">(
            materializationThreshold);
    auto scanP = ad_utility::makeExecutionTree<IndexScan>(
        qec, PSO, SparqlTripleSimple{Var{"?s"}, iri("<p>"), Var{"?o"}});
    auto scanP2 = ad_utility::makeExecutionTree<IndexScan>(
        qec, PSO, SparqlTripleSimple{Var{"?s"}, iri("<p2>"), Var{"?q"}});
    auto join = Join{qec, scanP2, scanP, 0, 0, keepJoinCol};
    EXPECT_EQ(join.getDescriptor(), "Join on ?s");

    auto id = ad_utility::testing::makeGetId(qec->getIndex());
    auto expected = makeIdTableFromVector(
        {{id("<x>"), I(3), I(1)}, {id("<x2>"), I(4), I(2)}});
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, makeAlwaysDefinedColumn(0)},
        {Variable{"?q"}, makeAlwaysDefinedColumn(1)},
        {Variable{"?o"}, makeAlwaysDefinedColumn(2)}};
    if (!keepJoinCol) {
      removeJoinColFromVarColMap(Variable{"?s"}, expectedVariables);
      expected.setColumnSubset(std::array{ColumnIndex{1}, ColumnIndex{2}});
    }
    auto expectedColumns = makeExpectedColumns(expectedVariables, expected);

    qec->getQueryTreeCache().clearAll();
    testJoinOperation(join, expectedColumns, true,
                      materializationThreshold <= 3);
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(join, expectedColumns, false);

    auto joinSwitched = Join{qec, scanP2, scanP, 0, 0, keepJoinCol};
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(joinSwitched, expectedColumns, true,
                      materializationThreshold <= 3);
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(joinSwitched, expectedColumns, false);
  };
  test(0);
  test(1);
  test(2);
  test(3);
  test(1'000'000);
}

// This is a regression test for an issue that was reported in
// https://github.com/ad-freiburg/qlever/issues/1893 and heavily simplified so
// it can be reproduced in a unit test.
TEST_P(JoinTestParametrized, joinTwoScansWithDifferentGraphs) {
  auto keepJoinCol = GetParam();
  ad_utility::testing::TestIndexConfig config{
      "<x> <p1> <1> <g1> . <x> <p1> <2> <g1> . <x> <p2> <1> <g2> ."
      " <x> <p2> <2> <g2> ."};
  config.indexType = qlever::Filetype::NQuad;
  auto qec = ad_utility::testing::getQec(config);
  auto cleanup =
      setRuntimeParameterForTest<"lazy-index-scan-max-size-materialization">(0);
  using ad_utility::triple_component::Iri;
  auto scanP = ad_utility::makeExecutionTree<IndexScan>(
      qec, POS,
      SparqlTripleSimple{Var{"?s"}, iri("<p1>"), Iri::fromIriref("<1>")},
      std::optional{
          ad_utility::HashSet<TripleComponent>{Iri::fromIriref("<g1>")}});
  auto scanP2 = ad_utility::makeExecutionTree<IndexScan>(
      qec, POS,
      SparqlTripleSimple{Var{"?s"}, iri("<p1>"), Iri::fromIriref("<2>")},
      std::optional{
          ad_utility::HashSet<TripleComponent>{Iri::fromIriref("<g2>")}});
  auto join = Join{qec, scanP2, scanP, 0, 0, keepJoinCol};

  VariableToColumnMap expectedVariables{
      {Variable{"?s"}, makeAlwaysDefinedColumn(0)}};
  if (!keepJoinCol) {
    removeJoinColFromVarColMap(Variable{"?s"}, expectedVariables);
  }
  auto expected = keepJoinCol ? IdTable{1, qec->getAllocator()}
                              : IdTable{0, qec->getAllocator()};
  auto expectedColumns = makeExpectedColumns(expectedVariables, expected);

  qec->getQueryTreeCache().clearAll();
  testJoinOperation(join, expectedColumns, true, true);

  auto joinSwitched = Join{qec, scanP2, scanP, 0, 0, keepJoinCol};
  qec->getQueryTreeCache().clearAll();
  testJoinOperation(joinSwitched, expectedColumns, true, true);
}

// This is a regression test for a related issue found during the analysis of
// https://github.com/ad-freiburg/qlever/issues/1893 where the join of two index
// scans would fail if one element could potentially be found in multiple blocks
// of the respective other side.
TEST_P(JoinTestParametrized, joinTwoScansWithSubjectInMultipleBlocks) {
  // Default block size is 16 bytes for testing, so the triples are spread
  // across 3 blocks in total.
  auto keepJoinCol = GetParam();
  auto qec = ad_utility::testing::getQec(
      "<x> <p1> <1> . <x> <p1> <2> . <x> <p1> <3> . <x> <p1> <4> ."
      " <x> <p2> <5>");
  auto cleanup =
      setRuntimeParameterForTest<"lazy-index-scan-max-size-materialization">(0);
  using ad_utility::triple_component::Iri;
  auto scanP = ad_utility::makeExecutionTree<IndexScan>(
      qec, PSO, SparqlTripleSimple{Var{"?s"}, iri("<p1>"), Var{"?o1"}});
  auto scanP2 = ad_utility::makeExecutionTree<IndexScan>(
      qec, PSO, SparqlTripleSimple{Var{"?s"}, iri("<p2>"), Var{"?o2"}});
  auto join = Join{qec, scanP2, scanP, 0, 0, keepJoinCol};

  auto id = ad_utility::testing::makeGetId(qec->getIndex());
  auto expected = makeIdTableFromVector({{id("<x>"), id("<1>"), id("<5>")},
                                         {id("<x>"), id("<2>"), id("<5>")},
                                         {id("<x>"), id("<3>"), id("<5>")},
                                         {id("<x>"), id("<4>"), id("<5>")}});
  VariableToColumnMap expectedVariables{
      {Variable{"?s"}, makeAlwaysDefinedColumn(0)},
      {Variable{"?o1"}, makeAlwaysDefinedColumn(1)},
      {Variable{"?o2"}, makeAlwaysDefinedColumn(2)}};
  if (!keepJoinCol) {
    removeJoinColFromVarColMap(Variable{"?s"}, expectedVariables);
    expected.setColumnSubset(std::array{ColumnIndex{1}, ColumnIndex{2}});
  }
  auto expectedColumns = makeExpectedColumns(expectedVariables, expected);

  qec->getQueryTreeCache().clearAll();
  testJoinOperation(join, expectedColumns, true, true);

  auto joinSwitched = Join{qec, scanP2, scanP, 0, 0, keepJoinCol};
  qec->getQueryTreeCache().clearAll();
  testJoinOperation(joinSwitched, expectedColumns, true, true);
}

// _____________________________________________________________________________
TEST(JoinTest, invalidJoinVariable) {
  auto qec = ad_utility::testing::getQec(
      "<x> <p> 1. <x2> <p> 2. <x> <p2> 3 . <x2> <p2> 4. <x3> <p2> 7. ");
  auto valuesTree = makeValuesForSingleVariable(qec, "?s", {"<x>"});
  auto valuesTree2 = makeValuesForSingleVariable(qec, "?p", {"<x>"});

  ASSERT_ANY_THROW(Join(qec, valuesTree2, valuesTree, 0, 0));
}

// _____________________________________________________________________________
TEST_P(JoinTestParametrized, joinTwoLazyOperationsWithAndWithoutUndefValues) {
  auto keepJoinCol = GetParam();
  auto performJoin = [keepJoinCol](std::vector<IdTable> leftTables,
                                   std::vector<IdTable> rightTables,
                                   const IdTable& expectedIn,
                                   bool expectPossiblyUndefinedResult,
                                   ad_utility::source_location loc =
                                       ad_utility::source_location::current()) {
    IdTable expected = expectedIn.clone();
    auto l = generateLocationTrace(loc);
    auto qec = ad_utility::testing::getQec();
    auto cleanup =
        setRuntimeParameterForTest<"lazy-index-scan-max-size-materialization">(
            0);
    auto leftTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(leftTables), Vars{Variable{"?s"}}, false,
        std::vector<ColumnIndex>{0});
    auto rightTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(rightTables), Vars{Variable{"?s"}}, false,
        std::vector<ColumnIndex>{0});
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, expectPossiblyUndefinedResult
                             ? makePossiblyUndefinedColumn(0)
                             : makeAlwaysDefinedColumn(0)}};
    if (!keepJoinCol) {
      removeJoinColFromVarColMap(Variable{"?s"}, expectedVariables);
      expected.setColumnSubset(std::array<ColumnIndex, 0>{});
    }
    auto expectedColumns = makeExpectedColumns(expectedVariables, expected);
    auto join = Join{qec, leftTree, rightTree, 0, 0, keepJoinCol};
    EXPECT_EQ(join.getDescriptor(), "Join on ?s");

    qec->getQueryTreeCache().clearAll();
    testJoinOperation(join, expectedColumns, true, true);
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(join, expectedColumns, false);

    auto joinSwitched = Join{qec, rightTree, leftTree, 0, 0, keepJoinCol};
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(joinSwitched, expectedColumns, true, true);
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(joinSwitched, expectedColumns, false);
  };
  auto U = Id::makeUndefined();
  std::vector<IdTable> leftTables;
  std::vector<IdTable> rightTables;
  IdTable expected1{1, ad_utility::makeUnlimitedAllocator<Id>()};
  performJoin(std::move(leftTables), std::move(rightTables), expected1, false);

  leftTables.push_back(makeIdTableFromVector({{U}}));
  rightTables.push_back(makeIdTableFromVector({{U}}));
  auto expected2 = makeIdTableFromVector({{U}});
  performJoin(std::move(leftTables), std::move(rightTables), expected2, true);

  leftTables.push_back(makeIdTableFromVector({{U}, {I(0)}}));
  rightTables.push_back(makeIdTableFromVector({{U}}));
  auto expected3 = makeIdTableFromVector({{U}, {I(0)}});
  performJoin(std::move(leftTables), std::move(rightTables), expected3, true);

  leftTables.push_back(makeIdTableFromVector({{U}, {I(0)}}));
  leftTables.push_back(makeIdTableFromVector({{I(1)}}));
  rightTables.push_back(makeIdTableFromVector({{I(0)}}));
  auto expected4 = makeIdTableFromVector({{I(0)}, {I(0)}});
  performJoin(std::move(leftTables), std::move(rightTables), expected4, false);

  leftTables.push_back(makeIdTableFromVector({{U}, {I(0)}}));
  leftTables.push_back(makeIdTableFromVector({{I(1)}}));
  rightTables.push_back(IdTable{1, ad_utility::makeUnlimitedAllocator<Id>()});
  IdTable expected5{1, ad_utility::makeUnlimitedAllocator<Id>()};
  performJoin(std::move(leftTables), std::move(rightTables), expected5, false);

  leftTables.push_back(makeIdTableFromVector({{I(0)}}));
  leftTables.push_back(makeIdTableFromVector({{I(1)}}));
  rightTables.push_back(makeIdTableFromVector({{I(1)}}));
  rightTables.push_back(makeIdTableFromVector({{I(2)}}));
  auto expected6 = makeIdTableFromVector({{I(1)}});
  performJoin(std::move(leftTables), std::move(rightTables), expected6, false);

  leftTables.push_back(makeIdTableFromVector({{U}}));
  leftTables.push_back(makeIdTableFromVector({{I(2)}}));
  rightTables.push_back(createIdTableOfSizeWithValue(CHUNK_SIZE, I(1)));
  auto expected7 = createIdTableOfSizeWithValue(CHUNK_SIZE, I(1));
  performJoin(std::move(leftTables), std::move(rightTables), expected7, false);

  leftTables.push_back(makeIdTableFromVector({{U}}));
  leftTables.push_back(makeIdTableFromVector({{I(1)}}));
  rightTables.push_back(makeIdTableFromVector({{I(2)}}));
  rightTables.push_back(makeIdTableFromVector({{I(2)}}));
  auto expected8 = createIdTableOfSizeWithValue(2, I(2));
  performJoin(std::move(leftTables), std::move(rightTables), expected8, false);
}

// _____________________________________________________________________________
TEST_P(JoinTestParametrized,
       joinLazyAndNonLazyOperationWithAndWithoutUndefValues) {
  auto keepJoinCol = GetParam();
  auto performJoin = [keepJoinCol](IdTable leftTable,
                                   std::vector<IdTable> rightTables,
                                   const IdTable& expectedIn,
                                   bool expectPossiblyUndefinedResult,
                                   ad_utility::source_location loc =
                                       ad_utility::source_location::current()) {
    auto l = generateLocationTrace(loc);
    IdTable expected = expectedIn.clone();
    auto qec = ad_utility::testing::getQec();
    auto cleanup =
        setRuntimeParameterForTest<"lazy-index-scan-max-size-materialization">(
            0);
    auto leftTree =
        ad_utility::makeExecutionTree<ValuesForTestingNoKnownEmptyResult>(
            qec, std::move(leftTable), Vars{Variable{"?s"}}, false,
            std::vector<ColumnIndex>{0}, LocalVocab{}, std::nullopt, true);
    auto rightTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(rightTables), Vars{Variable{"?s"}}, false,
        std::vector<ColumnIndex>{0});
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, expectPossiblyUndefinedResult
                             ? makePossiblyUndefinedColumn(0)
                             : makeAlwaysDefinedColumn(0)}};
    if (!keepJoinCol) {
      removeJoinColFromVarColMap(Variable{"?s"}, expectedVariables);
      expected.setColumnSubset(std::array<ColumnIndex, 0>{});
    }
    auto expectedColumns = makeExpectedColumns(expectedVariables, expected);
    auto join = Join{qec, leftTree, rightTree, 0, 0, keepJoinCol};
    EXPECT_EQ(join.getDescriptor(), "Join on ?s");

    qec->getQueryTreeCache().clearAll();
    testJoinOperation(join, expectedColumns, true);
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(join, expectedColumns, false);

    auto joinSwitched = Join{qec, rightTree, leftTree, 0, 0, keepJoinCol};
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(joinSwitched, expectedColumns, true);
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(joinSwitched, expectedColumns, false);
  };
  auto U = Id::makeUndefined();
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector({{U}}));
  auto expected1 = makeIdTableFromVector({{U}});
  performJoin(makeIdTableFromVector({{U}}), std::move(rightTables), expected1,
              true);

  rightTables.push_back(makeIdTableFromVector({{U}}));
  auto expected2 = makeIdTableFromVector({{U}, {I(0)}});
  performJoin(makeIdTableFromVector({{U}, {I(0)}}), std::move(rightTables),
              expected2, true);

  rightTables.push_back(makeIdTableFromVector({{I(0)}}));
  auto expected3 = makeIdTableFromVector({{I(0)}, {I(0)}});
  performJoin(makeIdTableFromVector({{U}, {I(0)}, {I(1)}}),
              std::move(rightTables), expected3, false);

  rightTables.push_back(makeIdTableFromVector({{U}, {I(0)}}));
  auto expected4 = makeIdTableFromVector({{I(0)}, {I(0)}, {I(1)}});
  performJoin(makeIdTableFromVector({{I(0)}, {I(1)}}), std::move(rightTables),
              expected4, false);

  rightTables.push_back(IdTable{1, ad_utility::makeUnlimitedAllocator<Id>()});
  IdTable expected5{1, ad_utility::makeUnlimitedAllocator<Id>()};
  performJoin(makeIdTableFromVector({{U}, {I(0)}, {I(1)}}),
              std::move(rightTables), expected5, false);

  rightTables.push_back(makeIdTableFromVector({{I(1)}}));
  rightTables.push_back(makeIdTableFromVector({{I(2)}}));
  auto expected6 = makeIdTableFromVector({{I(1)}});
  performJoin(makeIdTableFromVector({{I(0)}, {I(1)}}), std::move(rightTables),
              expected6, false);

  rightTables.push_back(makeIdTableFromVector({{U}}));
  rightTables.push_back(makeIdTableFromVector({{I(2)}}));
  auto expected7 = createIdTableOfSizeWithValue(CHUNK_SIZE, I(1));
  performJoin(createIdTableOfSizeWithValue(CHUNK_SIZE, I(1)),
              std::move(rightTables), expected7, false);
}

// _____________________________________________________________________________
TEST_P(JoinTestParametrized, errorInSeparateThreadIsPropagatedCorrectly) {
  auto keepJoinCol = GetParam();
  auto qec = ad_utility::testing::getQec();
  auto cleanup =
      setRuntimeParameterForTest<"lazy-index-scan-max-size-materialization">(0);
  auto leftTree =
      ad_utility::makeExecutionTree<AlwaysFailOperation>(qec, Variable{"?s"});
  auto rightTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{I(1)}}), Vars{Variable{"?s"}}, false,
      std::vector<ColumnIndex>{0});
  Join join{qec, leftTree, rightTree, 0, 0, keepJoinCol};

  auto result = join.getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
  ASSERT_FALSE(result->isFullyMaterialized());

  auto idTables = result->idTables();
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(idTables.begin(),
                                        testing::StrEq("AlwaysFailOperation"),
                                        std::runtime_error);
}

// _____________________________________________________________________________
TEST_P(JoinTestParametrized, verifyColumnPermutationsAreAppliedCorrectly) {
  auto keepJoinCol = GetParam();
  auto qec =
      ad_utility::testing::getQec("<x> <p> <g>. <x2> <p> <h>. <x> <a> <i>.");
  auto cleanup =
      setRuntimeParameterForTest<"lazy-index-scan-max-size-materialization">(0);
  auto U = Id::makeUndefined();
  {
    auto leftTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{U, I(1), U}, {U, I(3), U}}),
        Vars{Variable{"?t"}, Variable{"?s"}, Variable{"?u"}}, false,
        std::vector<ColumnIndex>{1});
    auto rightTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{U, I(10), I(1)}, {U, U, I(2)}}),
        Vars{Variable{"?v"}, Variable{"?w"}, Variable{"?s"}}, false,
        std::vector<ColumnIndex>{2});
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, makeAlwaysDefinedColumn(0)},
        {Variable{"?t"}, makePossiblyUndefinedColumn(1)},
        {Variable{"?u"}, makePossiblyUndefinedColumn(2)},
        {Variable{"?v"}, makePossiblyUndefinedColumn(3)},
        {Variable{"?w"}, makePossiblyUndefinedColumn(4)}};
    auto expected = makeIdTableFromVector({{I(1), U, U, U, I(10)}});
    if (!keepJoinCol) {
      removeJoinColFromVarColMap(Variable{"?s"}, expectedVariables);
      expected.setColumnSubset(std::array<ColumnIndex, 4>{1, 2, 3, 4});
    }
    auto expectedColumns = makeExpectedColumns(expectedVariables, expected);
    auto join = Join{qec, leftTree, rightTree, 1, 2, keepJoinCol};
    EXPECT_EQ(join.getDescriptor(), "Join on ?s");

    qec->getQueryTreeCache().clearAll();
    testJoinOperation(join, expectedColumns, true, true);
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(join, expectedColumns, false);
  }
  {
    auto leftTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{I(1), I(2), U}}),
        Vars{Variable{"?p"}, Variable{"?q"}, Variable{"?s"}}, false,
        std::vector<ColumnIndex>{2}, LocalVocab{}, std::nullopt, true);
    auto fullScanPSO = ad_utility::makeExecutionTree<IndexScan>(
        qec, PSO, SparqlTripleSimple{Var{"?s"}, iri("<p>"), Var{"?o"}});
    VariableToColumnMap expectedVariables{
        {Variable{"?s"}, makeAlwaysDefinedColumn(0)},
        {Variable{"?p"}, makeAlwaysDefinedColumn(1)},
        {Variable{"?q"}, makeAlwaysDefinedColumn(2)},
        {Variable{"?o"}, makeAlwaysDefinedColumn(3)}};
    auto id = ad_utility::testing::makeGetId(qec->getIndex());
    auto expected =
        makeIdTableFromVector({{id("<x>"), I(1), I(2), id("<g>")},
                               {id("<x2>"), I(1), I(2), id("<h>")}});
    if (!keepJoinCol) {
      removeJoinColFromVarColMap(Variable{"?s"}, expectedVariables);
      expected.setColumnSubset(std::array<ColumnIndex, 3>{
          1,
          2,
          3,
      });
    }
    auto expectedColumns = makeExpectedColumns(expectedVariables, expected);
    auto join = Join{qec, leftTree, fullScanPSO, 2, 0, keepJoinCol};
    EXPECT_EQ(join.getDescriptor(), "Join on ?s");

    qec->getQueryTreeCache().clearAll();
    testJoinOperation(join, expectedColumns, true, true);
    qec->getQueryTreeCache().clearAll();
    testJoinOperation(join, expectedColumns, false);
  }
}

// _____________________________________________________________________________
TEST(JoinTest, clone) {
  auto qec = ad_utility::testing::getQec();
  auto leftTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{I(1), I(1), I(1)}}),
      Vars{Variable{"?t"}, Variable{"?s"}, Variable{"?u"}}, false,
      std::vector<ColumnIndex>{1});
  auto rightTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{I(1), I(1), I(1)}}),
      Vars{Variable{"?v"}, Variable{"?w"}, Variable{"?s"}}, false,
      std::vector<ColumnIndex>{2});
  Join join{qec, leftTree, rightTree, 1, 2};

  auto clone = join.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(join, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), join.getDescriptor());
}

// _____________________________________________________________________________
TEST_P(JoinTestParametrized, columnOriginatesFromGraphOrUndef) {
  auto keepJoinCol = GetParam();
  using ad_utility::triple_component::Iri;
  auto* qec = ad_utility::testing::getQec();
  // Not in graph no undef
  auto values1 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?c"}});
  auto values2 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}});
  // Not in graph, potentially undef
  auto values3 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{Id::makeUndefined(), Id::makeUndefined()}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?c"}});
  auto values4 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{Id::makeUndefined(), Id::makeUndefined()}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}});
  // In graph, no undef
  auto index1 = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::PSO,
      SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                         Variable{"?c"}});
  auto index2 = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::PSO,
      SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                         Variable{"?b"}});
  // In graph, potential undef
  auto index3 = ad_utility::makeExecutionTree<NeutralOptional>(
      qec, ad_utility::makeExecutionTree<IndexScan>(
               qec, Permutation::PSO,
               SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                                  Variable{"?c"}}));
  auto index4 = ad_utility::makeExecutionTree<NeutralOptional>(
      qec, ad_utility::makeExecutionTree<IndexScan>(
               qec, Permutation::PSO,
               SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                                  Variable{"?b"}}));

  auto testWithTrees = [qec, keepJoinCol](
                           std::shared_ptr<QueryExecutionTree> left,
                           std::shared_ptr<QueryExecutionTree> right, bool a,
                           bool b, bool c,
                           ad_utility::source_location location =
                               ad_utility::source_location::current()) {
    auto trace = generateLocationTrace(location);

    Join join{qec, std::move(left), std::move(right), 0, 0, keepJoinCol, false};
    if (keepJoinCol) {
      EXPECT_EQ(join.columnOriginatesFromGraphOrUndef(Variable{"?a"}), a);
    } else {
      EXPECT_THROW(join.columnOriginatesFromGraphOrUndef(Variable{"?a"}),
                   ad_utility::Exception);
    }
    EXPECT_EQ(join.columnOriginatesFromGraphOrUndef(Variable{"?b"}), b);
    EXPECT_EQ(join.columnOriginatesFromGraphOrUndef(Variable{"?c"}), c);
    EXPECT_THROW(
        join.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
        ad_utility::Exception);
  };

  testWithTrees(index3, index4, true, true, true);
  testWithTrees(index3, index2, true, true, true);
  testWithTrees(index3, values4, false, false, true);
  testWithTrees(index3, values2, false, false, true);
  testWithTrees(index1, index4, true, true, true);
  testWithTrees(index1, index2, true, true, true);
  testWithTrees(index1, values4, true, false, true);
  testWithTrees(index1, values2, true, false, true);
  testWithTrees(values4, index3, false, false, true);
  testWithTrees(values4, index1, true, false, true);
  testWithTrees(values4, values3, false, false, false);
  testWithTrees(values4, values1, false, false, false);
  testWithTrees(values2, index3, false, false, true);
  testWithTrees(values2, index1, true, false, true);
  testWithTrees(values2, values3, false, false, false);
  testWithTrees(values2, values1, false, false, false);
}

// _____________________________________________________________________________
INSTANTIATE_TEST_SUITE_P(JoinTestWithAndWithoutKeptJoinColumn,
                         JoinTestParametrized, ::testing::Values(true, false));
