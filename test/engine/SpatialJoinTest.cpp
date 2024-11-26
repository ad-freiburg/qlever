//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: @Jonathan24680
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>

#include <cstdlib>
#include <fstream>
#include <memory>
#include <regex>

#include "../util/IndexTestHelpers.h"
#include "./../../src/global/ValueId.h"
#include "./../../src/util/GeoSparqlHelpers.h"
#include "./SpatialJoinTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/SpatialJoin.h"
#include "engine/VariableToColumnMap.h"
#include "global/Constants.h"
#include "gtest/gtest.h"
#include "parser/data/Variable.h"

namespace {  // anonymous namespace to avoid linker problems

using namespace ad_utility::testing;
using namespace SpatialJoinTestHelpers;

namespace childrenTesting {

void testAddChild(bool addLeftChildFirst) {
  auto checkVariable = [](SpatialJoin* spatialJoin, bool checkLeftVariable) {
    if (checkLeftVariable) {
      std::shared_ptr<Operation> op =
          spatialJoin->onlyForTestingGetLeftChild()->getRootOperation();
      IndexScan* scan = static_cast<IndexScan*>(op.get());
      ASSERT_EQ(scan->subject().getVariable().name(), "?obj1");
      ASSERT_EQ(scan->object().getVariable().name(), "?point1");
    } else {
      std::shared_ptr<Operation> op =
          spatialJoin->onlyForTestingGetRightChild()->getRootOperation();
      IndexScan* scan = static_cast<IndexScan*>(op.get());
      ASSERT_EQ(scan->subject().getVariable().name(), "?obj2");
      ASSERT_EQ(scan->object().getVariable().name(), "?point2");
    }
  };

  auto qec = buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  TripleComponent point1{Variable{"?point1"}};
  TripleComponent point2{Variable{"?point2"}};
  auto leftChild =
      buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
  auto rightChild =
      buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(
          qec,
          std::make_shared<SpatialJoinConfiguration>(MaxDistanceConfig{1000},
                                                     point1.getVariable(),
                                                     point2.getVariable()),
          std::nullopt, std::nullopt);

  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  auto firstChild = addLeftChildFirst ? leftChild : rightChild;
  auto secondChild = addLeftChildFirst ? rightChild : leftChild;
  Variable firstVariable =
      addLeftChildFirst ? point1.getVariable() : point2.getVariable();
  Variable secondVariable =
      addLeftChildFirst ? point2.getVariable() : point1.getVariable();
  std::string firstSubject = addLeftChildFirst ? "?obj1" : "?obj2";
  std::string secondSubject = addLeftChildFirst ? "?obj2" : "?obj1";
  std::string firstObject = addLeftChildFirst ? "?point1" : "?point2";
  std::string secondObject = addLeftChildFirst ? "?point2" : "?point1";

  ASSERT_EQ(spatialJoin->onlyForTestingGetLeftChild(), nullptr);
  ASSERT_EQ(spatialJoin->onlyForTestingGetRightChild(), nullptr);

  ASSERT_ANY_THROW(spatialJoin->addChild(firstChild, Variable{"?wrongVar"}));
  ASSERT_ANY_THROW(spatialJoin->addChild(secondChild, Variable{"?wrongVar"}));

  ASSERT_EQ(spatialJoin->onlyForTestingGetLeftChild(), nullptr);
  ASSERT_EQ(spatialJoin->onlyForTestingGetRightChild(), nullptr);

  auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
  spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());

  checkVariable(spatialJoin, addLeftChildFirst);

  auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
  spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());

  checkVariable(spatialJoin, (!addLeftChildFirst));
}

TEST(SpatialJoin, addChild) {
  testAddChild(true);
  testAddChild(false);
}

TEST(SpatialJoin, isConstructed) {
  auto qec = buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  TripleComponent point1{Variable{"?point1"}};
  TripleComponent point2{Variable{"?point2"}};
  auto leftChild =
      buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
  auto rightChild =
      buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(
          qec,
          std::make_shared<SpatialJoinConfiguration>(MaxDistanceConfig{1000},
                                                     point1.getVariable(),
                                                     point2.getVariable()),
          std::nullopt, std::nullopt);

  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  ASSERT_FALSE(spatialJoin->isConstructed());

  auto spJoin1 = spatialJoin->addChild(leftChild, point1.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());

  ASSERT_FALSE(spatialJoin->isConstructed());

  auto spJoin2 = spatialJoin->addChild(rightChild, point2.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());

  ASSERT_TRUE(spatialJoin->isConstructed());
}

TEST(SpatialJoin, getChildren) {
  auto qec = buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  TripleComponent point1{Variable{"?point1"}};
  TripleComponent point2{Variable{"?point2"}};
  auto leftChild =
      buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
  auto rightChild =
      buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(
          qec,
          std::make_shared<SpatialJoinConfiguration>(MaxDistanceConfig{1000},
                                                     point1.getVariable(),
                                                     point2.getVariable()),
          std::nullopt, std::nullopt);

  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  ASSERT_EQ(spatialJoin->getChildren().size(), 0);

  auto spJoin1 = spatialJoin->addChild(leftChild, point1.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());

  ASSERT_EQ(spatialJoin->getChildren().size(), 1);

  auto spJoin2 = spatialJoin->addChild(rightChild, point2.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());

  ASSERT_EQ(spatialJoin->getChildren().size(), 2);

  auto assertScanVariables = [](IndexScan* scan1, IndexScan* scan2,
                                bool isSubjectNotObject, std::string varName1,
                                std::string varName2) {
    std::string value1 = scan1->subject().getVariable().name();
    std::string value2 = scan2->subject().getVariable().name();
    if (!isSubjectNotObject) {
      value1 = scan1->object().getVariable().name();
      value2 = scan2->object().getVariable().name();
    }
    ASSERT_TRUE(value1 == varName1 || value1 == varName2);
    ASSERT_TRUE(value2 == varName1 || value2 == varName2);
    ASSERT_TRUE(value1 != value2);
  };

  std::shared_ptr<Operation> op1 =
      spatialJoin->getChildren().at(0)->getRootOperation();
  IndexScan* scan1 = static_cast<IndexScan*>(op1.get());

  std::shared_ptr<Operation> op2 =
      spatialJoin->getChildren().at(1)->getRootOperation();
  IndexScan* scan2 = static_cast<IndexScan*>(op2.get());

  assertScanVariables(scan1, scan2, true, "?obj1", "?obj2");
  assertScanVariables(scan1, scan2, false, "?point1", "?point2");
}

}  // namespace childrenTesting

namespace variableColumnMapAndResultWidth {

// Represents from left to right: leftSideBigChild, rightSideBigChild,
// addLeftChildFirst, testVarToColMap
using VarColTestSuiteParam = std::tuple<bool, bool, bool, bool>;

class SpatialJoinVarColParamTest
    : public ::testing::TestWithParam<VarColTestSuiteParam> {
 public:
  // Helper function to construct a child for testing
  std::shared_ptr<QueryExecutionTree> getChild(QueryExecutionContext* qec,
                                               bool getBigChild,
                                               std::string numberOfChild) {
    std::string obj = absl::StrCat("?obj", numberOfChild);
    std::string name = absl::StrCat("?name", numberOfChild);
    std::string geo = absl::StrCat("?geo", numberOfChild);
    std::string point = absl::StrCat("?point", numberOfChild);
    if (getBigChild) {
      return buildMediumChild(qec, {obj, std::string{"<name>"}, name},
                              {obj, std::string{"<hasGeometry>"}, geo},
                              {geo, std::string{"<asWKT>"}, point}, obj, geo);
    } else {
      return buildSmallChild(qec, {obj, std::string{"<hasGeometry>"}, geo},
                             {geo, std::string{"<asWKT>"}, point}, geo);
    }
  }

  // Helper function to construct a vector of the expected columns in the
  // example child generated by getChild
  std::vector<std::pair<std::string, std::string>> addExpectedColumns(
      std::vector<std::pair<std::string, std::string>> expectedColumns,
      bool bigChild, std::string numberOfChild) {
    std::string obj = absl::StrCat("?obj", numberOfChild);
    std::string name = absl::StrCat("?name", numberOfChild);
    std::string geo = absl::StrCat("?geo", numberOfChild);
    std::string point = absl::StrCat("?point", numberOfChild);
    expectedColumns.push_back({obj, "<node_"});
    expectedColumns.push_back({geo, "<geometry"});
    expectedColumns.push_back({point, "\"POINT("});
    if (bigChild) {
      expectedColumns.push_back({name, "\""});
    }
    return expectedColumns;
  };

  // only test one at a time. Then the gtest will fail on the test, which
  // failed, instead of failing for both getResultWidth() and
  // computeVariableToColumnMap() if only one of them is wrong
  void testGetResultWidthOrVariableToColumnMap(
      VarColTestSuiteParam parameters) {
    auto [leftSideBigChild, rightSideBigChild, addLeftChildFirst,
          testVarToColMap] = parameters;
    auto qec = buildTestQEC();

    auto leftChild = getChild(qec, leftSideBigChild, "1");
    auto rightChild = getChild(qec, rightSideBigChild, "2");

    std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<SpatialJoin>(
            qec,
            std::make_shared<SpatialJoinConfiguration>(
                MaxDistanceConfig{0}, Variable{"?point1"}, Variable{"?point2"},
                Variable{"?distOfTheTwoObjectsAddedInternally"}),
            std::nullopt, std::nullopt);
    std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
    SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
    auto firstChild = addLeftChildFirst ? leftChild : rightChild;
    auto secondChild = addLeftChildFirst ? rightChild : leftChild;
    Variable firstVariable =
        addLeftChildFirst ? Variable{"?point1"} : Variable{"?point2"};
    Variable secondVariable =
        addLeftChildFirst ? Variable{"?point2"} : Variable{"?point1"};
    auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
    spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
    auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
    spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());

    size_t expectedResultWidth =
        (leftSideBigChild ? 4 : 3) + (rightSideBigChild ? 4 : 3) + 1;

    auto numTriples = qec->getIndex().numTriples().normal;
    ASSERT_EQ(numTriples, 15);

    if (!testVarToColMap) {
      ASSERT_EQ(spatialJoin->getResultWidth(), expectedResultWidth);
    } else {
      std::vector<std::pair<std::string, std::string>> expectedColumns{};

      expectedColumns =
          addExpectedColumns(expectedColumns, leftSideBigChild, "1");
      expectedColumns =
          addExpectedColumns(expectedColumns, rightSideBigChild, "2");

      expectedColumns.push_back({"?distOfTheTwoObjectsAddedInternally", "0"});

      auto varColMap = spatialJoin->computeVariableToColumnMap();
      auto resultTable = spatialJoin->computeResult(false);

      // if the size of varColMap and expectedColumns is the same and each
      // element of expectedColumns is contained in varColMap, then they are the
      // same (assuming that each element is unique)
      ASSERT_EQ(varColMap.size(), expectedColumns.size());

      for (size_t i = 0; i < expectedColumns.size(); i++) {
        ASSERT_TRUE(varColMap.contains(Variable{expectedColumns.at(i).first}));

        // test, that the column contains the correct values
        ColumnIndex ind =
            varColMap[Variable{expectedColumns.at(i).first}].columnIndex_;
        const IdTable* r = &resultTable.idTable();
        ASSERT_LT(0, r->numRows());
        ASSERT_LT(ind, r->numColumns());
        ValueId tableEntry = r->at(0, ind);

        if (tableEntry.getDatatype() == Datatype::VocabIndex) {
          std::string value = ExportQueryExecutionTrees::idToStringAndType(
                                  qec->getIndex(), tableEntry, {})
                                  .value()
                                  .first;
          ASSERT_TRUE(value.find(expectedColumns.at(i).second, 0) !=
                      string::npos);
        } else if (tableEntry.getDatatype() == Datatype::Int) {
          std::string value = ExportQueryExecutionTrees::idToStringAndType(
                                  qec->getIndex(), tableEntry, {})
                                  .value()
                                  .first;
          ASSERT_EQ(value, expectedColumns.at(i).second);
        } else if (tableEntry.getDatatype() == Datatype::GeoPoint) {
          auto [value, type] = ExportQueryExecutionTrees::idToStringAndType(
                                   qec->getIndex(), tableEntry, {})
                                   .value();
          value = absl::StrCat("\"", value, "\"^^<", type, ">");
          ASSERT_TRUE(value.find(expectedColumns.at(i).second, 0) !=
                      string::npos);
        }
      }
    }
  }
};

TEST_P(SpatialJoinVarColParamTest, variableToColumnMap) {
  testGetResultWidthOrVariableToColumnMap(GetParam());
}

TEST_P(SpatialJoinVarColParamTest, payloadVariables) {
  // TODO<ullingerc> Test payload variables
  ;
}

INSTANTIATE_TEST_SUITE_P(SpatialJoin, SpatialJoinVarColParamTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Bool(),
                                            ::testing::Bool(),
                                            ::testing::Bool()));

}  // namespace variableColumnMapAndResultWidth

namespace knownEmptyResult {

// Represents from left to right: leftSideEmptyChild, rightSideEmptyChild,
// addLeftChildFirst
using KnownEmptyParam = std::tuple<bool, bool, bool>;

class SpatialJoinKnownEmptyTest
    : public ::testing::TestWithParam<KnownEmptyParam> {
 public:
  void testKnownEmptyResult(bool leftSideEmptyChild, bool rightSideEmptyChild,
                            bool addLeftChildFirst) {
    auto checkEmptyResult = [](SpatialJoin* spatialJoin_, bool shouldBeEmpty) {
      ASSERT_EQ(spatialJoin_->knownEmptyResult(), shouldBeEmpty);
    };

    auto getChild = [](QueryExecutionContext* qec, bool emptyChild) {
      std::string predicate =
          emptyChild ? "<notExistingPred>" : "<hasGeometry>";
      return buildSmallChild(qec, {"?obj1", predicate, "?geo1"},
                             {"?geo1", std::string{"<asWKT>"}, "?point1"},
                             "?geo1");
    };

    auto qec = buildTestQEC();
    auto numTriples = qec->getIndex().numTriples().normal;
    ASSERT_EQ(numTriples, 15);

    auto leftChild = getChild(qec, leftSideEmptyChild);
    auto rightChild = getChild(qec, rightSideEmptyChild);

    std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<SpatialJoin>(
            qec,
            std::make_shared<SpatialJoinConfiguration>(
                MaxDistanceConfig{0}, Variable{"?point1"}, Variable{"?point2"}),
            std::nullopt, std::nullopt);
    std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
    SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
    auto firstChild = addLeftChildFirst ? leftChild : rightChild;
    auto secondChild = addLeftChildFirst ? rightChild : leftChild;
    Variable firstVariable =
        addLeftChildFirst ? Variable{"?point1"} : Variable{"?point2"};
    Variable secondVariable =
        addLeftChildFirst ? Variable{"?point2"} : Variable{"?point1"};
    bool firstChildEmpty =
        addLeftChildFirst ? leftSideEmptyChild : rightSideEmptyChild;
    bool secondChildEmpty =
        addLeftChildFirst ? rightSideEmptyChild : leftSideEmptyChild;

    checkEmptyResult(spatialJoin, false);

    auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
    spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
    checkEmptyResult(spatialJoin, firstChildEmpty);

    auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
    spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());
    checkEmptyResult(spatialJoin, (firstChildEmpty || secondChildEmpty));
  }
};

TEST_P(SpatialJoinKnownEmptyTest, knownEmpyResult) {
  auto [leftSideEmptyChild, rightSideEmptyChild, addLeftChildFirst] =
      GetParam();
  testKnownEmptyResult(leftSideEmptyChild, rightSideEmptyChild,
                       addLeftChildFirst);
}

INSTANTIATE_TEST_SUITE_P(SpatialJoin, SpatialJoinKnownEmptyTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Bool(),
                                            ::testing::Bool()));

}  // namespace knownEmptyResult

namespace resultSortedOn {

TEST(SpatialJoin, resultSortedOn) {
  std::string kg = createSmallDatasetWithPoints();

  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);

  TripleComponent obj1{Variable{"?point1"}};
  TripleComponent obj2{Variable{"?point2"}};
  auto leftChild = buildIndexScan(qec, {"?geometry1", "<asWKT>", "?point1"});
  auto rightChild = buildIndexScan(qec, {"?geometry2", "<asWKT>", "?point2"});

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(
          qec,
          std::make_shared<SpatialJoinConfiguration>(
              MaxDistanceConfig{10000000}, Variable{"?point1"},
              Variable{"?point2"}),
          std::nullopt, std::nullopt);

  // add children and test, that multiplicity is a dummy return before all
  // children are added
  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
  ASSERT_EQ(spatialJoin->getResultSortedOn().size(), 0);
  auto spJoin1 = spatialJoin->addChild(leftChild, obj1.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
  ASSERT_EQ(spatialJoin->getResultSortedOn().size(), 0);
  auto spJoin2 = spatialJoin->addChild(rightChild, obj2.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());
  ASSERT_EQ(spatialJoin->getResultSortedOn().size(), 0);
}

}  // namespace resultSortedOn

namespace stringRepresentation {

TEST(SpatialJoin, getDescriptor) {
  auto qec = getQec();
  TripleComponent subject{Variable{"?subject"}};
  TripleComponent object{Variable{"?object"}};

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(
          qec,
          std::make_shared<SpatialJoinConfiguration>(MaxDistanceConfig{1000},
                                                     subject.getVariable(),
                                                     object.getVariable()),
          std::nullopt, std::nullopt);
  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  auto description = spatialJoin->getDescriptor();
  ASSERT_THAT(description, ::testing::HasSubstr(std::to_string(
                               spatialJoin->getMaxDist().value_or(-1))));
  ASSERT_TRUE(description.find("?subject") != std::string::npos);
  ASSERT_TRUE(description.find("?object") != std::string::npos);
}

TEST(SpatialJoin, getCacheKeyImpl) {
  auto qec = buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  Variable subj{"?point1"};
  Variable obj{"?point2"};
  auto leftChild =
      buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
  auto rightChild =
      buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(
          qec,
          std::make_shared<SpatialJoinConfiguration>(MaxDistanceConfig{1000},
                                                     subj, obj),
          std::nullopt, std::nullopt);
  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  ASSERT_EQ(spatialJoin->getCacheKeyImpl(), "incomplete SpatialJoin class");

  auto spJoin1 = spatialJoin->addChild(leftChild, subj);
  spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());

  ASSERT_EQ(spatialJoin->getCacheKeyImpl(), "incomplete SpatialJoin class");

  auto spJoin2 = spatialJoin->addChild(rightChild, obj);
  spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());

  auto cacheKeyString = spatialJoin->getCacheKeyImpl();
  auto leftCacheKeyString =
      spatialJoin->onlyForTestingGetLeftChild()->getCacheKey();
  auto rightCacheKeyString =
      spatialJoin->onlyForTestingGetRightChild()->getCacheKey();

  ASSERT_TRUE(cacheKeyString.find(
                  std::to_string(spatialJoin->getMaxDist().value_or(-1))) !=
              std::string::npos);
  ASSERT_TRUE(cacheKeyString.find(leftCacheKeyString) != std::string::npos);
  ASSERT_TRUE(cacheKeyString.find(rightCacheKeyString) != std::string::npos);
}

}  // namespace stringRepresentation

namespace getMultiplicityAndSizeEstimate {

// Represents addLeftChildFirst, MultiplicityOrSizeEst
enum class MultiplicityOrSizeEst { Multiplicity, SizeEstimate };
using MultiplicityAndSizeEstimateParam =
    std::tuple<bool, MultiplicityOrSizeEst>;

class SpatialJoinMultiplicityAndSizeEstimateTest
    : public ::testing::TestWithParam<MultiplicityAndSizeEstimateParam> {
 public:
  void testMultiplicitiesOrSizeEstimate(
      bool addLeftChildFirst, MultiplicityOrSizeEst testMultiplicities) {
    auto multiplicitiesBeforeAllChildrenAdded = [&](SpatialJoin* spatialJoin) {
      for (size_t i = 0; i < spatialJoin->getResultWidth(); i++) {
        ASSERT_EQ(spatialJoin->getMultiplicity(i), 1);
      }
    };

    const double doubleBound = 0.00001;
    std::string kg = createSmallDatasetWithPoints();

    // add multiplicities to test knowledge graph
    kg += "<node_1> <name> \"testing multiplicity\" .";
    kg += "<node_1> <name> \"testing multiplicity 2\" .";

    ad_utility::MemorySize blocksizePermutations = 16_MB;
    auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
    auto numTriples = qec->getIndex().numTriples().normal;
    const unsigned int nrTriplesInput = 17;
    ASSERT_EQ(numTriples, nrTriplesInput);

    Variable subj{"?point1"};
    Variable obj{"?point2"};

    // ===================== build the first child
    // ===============================
    auto leftChild = buildMediumChild(
        qec, {"?obj1", std::string{"<name>"}, "?name1"},
        {"?obj1", std::string{"<hasGeometry>"}, "?geo1"},
        {"?geo1", std::string{"<asWKT>"}, "?point1"}, "?obj1", "?geo1");
    // result table of leftChild:
    // ?obj1    ?name1                    ?geo1       ?point1
    // node_1   Uni Freiburg TF           geometry1   Point(7.83505 48.01267)
    // node_1   testing multiplicity      geometry1   Point(7.83505 48.01267)
    // node_1   testing multiplicity 2    geometry1   Point(7.83505 48.01267)
    // node_2   Minster Freiburg          geometry2   POINT(7.85298 47.99557)
    // node_3   London Eye                geometry3   POINT(-0.11957 51.50333)
    // node_4   Statue of Liberty         geometry4   POINT(-74.04454 40.68925)
    // node_5   eiffel tower              geometry5   POINT(2.29451 48.85825)

    // ======================= build the second child
    // ============================
    auto rightChild = buildMediumChild(
        qec, {"?obj2", std::string{"<name>"}, "?name2"},
        {"?obj2", std::string{"<hasGeometry>"}, "?geo2"},
        {"?geo2", std::string{"<asWKT>"}, "?point2"}, "?obj2", "?geo2");
    // result table of rightChild is identical to leftChild, see above

    std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<SpatialJoin>(
            qec,
            std::make_shared<SpatialJoinConfiguration>(
                MaxDistanceConfig{10000000}, subj, obj),
            std::nullopt, std::nullopt);

    // add children and test, that multiplicity is a dummy return before all
    // children are added
    std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
    SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
    auto firstChild = addLeftChildFirst ? leftChild : rightChild;
    auto secondChild = addLeftChildFirst ? rightChild : leftChild;
    Variable firstVariable = addLeftChildFirst ? subj : obj;
    Variable secondVariable = addLeftChildFirst ? obj : subj;

    if (testMultiplicities == MultiplicityOrSizeEst::Multiplicity) {
      // expected behavior:
      // assert (result table at column i has the same distinctness as the
      // corresponding input table (via varToColMap))
      // assert, that distinctness * multiplicity = sizeEstimate

      multiplicitiesBeforeAllChildrenAdded(spatialJoin);
      auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
      spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
      multiplicitiesBeforeAllChildrenAdded(spatialJoin);
      auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
      spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());
      auto varColsMap = spatialJoin->getExternallyVisibleVariableColumns();
      auto varColsVec = copySortedByColumnIndex(varColsMap);
      auto leftVarColMap = leftChild->getVariableColumns();
      auto rightVarColMap = rightChild->getVariableColumns();
      for (size_t i = 0; i < spatialJoin->getResultWidth(); i++) {
        // get variable at column 0 of the result table
        Variable var = varColsVec.at(i).first;
        auto varChildLeft = leftVarColMap.find(var);
        auto varChildRight = rightVarColMap.find(var);
        auto inputChild = leftChild;
        if (varChildLeft == leftVarColMap.end()) {
          inputChild = rightChild;
        }
        auto distanceVariable =
            spatialJoin->onlyForTestingGetDistanceVariable();
        if (  // varChildRight == rightVarColMap.end() &&
            distanceVariable.has_value() && var == distanceVariable.value()) {
          // as each distance is very likely to be unique (even if only after
          // a few decimal places), no multiplicities are assumed
          ASSERT_EQ(spatialJoin->getMultiplicity(i), 1);
        } else {
          ColumnIndex colIndex;
          if (inputChild == leftChild) {
            colIndex = varChildLeft->second.columnIndex_;
          } else {
            colIndex = varChildRight->second.columnIndex_;
          }
          auto multiplicityChild = inputChild->getMultiplicity(colIndex);
          auto sizeEstimateChild = inputChild->getSizeEstimate();
          double distinctnessChild = sizeEstimateChild / multiplicityChild;
          auto mult = spatialJoin->getMultiplicity(i);
          auto sizeEst = std::pow(sizeEstimateChild, 2);
          double distinctness = sizeEst / mult;
          // multiplicity, distinctness and size are related via the formula
          // size = distinctness * multiplicity. Therefore if we have two of
          // them, we can calculate the third one. Here we check, that this
          // formula holds true. The distinctness must not change after the
          // operation, the other two variables can change. Therefore we check
          // the correctness via distinctness.
          ASSERT_NEAR(distinctnessChild, distinctness, doubleBound);
        }
      }
    } else {
      // test getSizeEstimate
      ASSERT_EQ(spatialJoin->getSizeEstimate(), 1);
      auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
      spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
      ASSERT_EQ(spatialJoin->getSizeEstimate(), 1);
      auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
      spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());
      ASSERT_NE(spatialJoin->onlyForTestingGetLeftChild(), nullptr);
      ASSERT_NE(spatialJoin->onlyForTestingGetRightChild(), nullptr);
      // the size should be at most 49, because both input tables have 7 rows
      // and it is assumed, that in the worst case the whole cross product is
      // build
      auto estimate =
          spatialJoin->onlyForTestingGetLeftChild()->getSizeEstimate() *
          spatialJoin->onlyForTestingGetRightChild()->getSizeEstimate();
      ASSERT_LE(spatialJoin->getSizeEstimate(), estimate);
    }

    {  // new block for hard coded testing
      // ======================== hard coded test
      // ================================ here the children are only index
      // scans, as they are perfectly predictable in relation to size and
      // multiplicity estimates
      std::string kg = createSmallDatasetWithPoints();

      // add multiplicities to test knowledge graph
      kg += "<geometry1> <asWKT> \"POINT(7.12345 48.12345)\".";
      kg += "<geometry1> <asWKT> \"POINT(7.54321 48.54321)\".";

      ad_utility::MemorySize blocksizePermutations = 16_MB;
      auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
      auto numTriples = qec->getIndex().numTriples().normal;
      const unsigned int nrTriplesInput = 17;
      ASSERT_EQ(numTriples, nrTriplesInput);

      Variable subj{"?point1"};
      Variable obj{"?point2"};

      TripleComponent subj1{Variable{"?geometry1"}};
      TripleComponent obj1{Variable{"?point1"}};
      TripleComponent subj2{Variable{"?geometry2"}};
      TripleComponent obj2{Variable{"?point2"}};
      auto leftChild =
          buildIndexScan(qec, {"?geometry1", "<asWKT>", "?point1"});
      auto rightChild =
          buildIndexScan(qec, {"?geometry2", "<asWKT>", "?point2"});

      std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
          ad_utility::makeExecutionTree<SpatialJoin>(
              qec,
              std::make_shared<SpatialJoinConfiguration>(
                  MaxDistanceConfig{10000000}, subj, obj,
                  Variable{"?distanceForTesting"}),
              std::nullopt, std::nullopt);

      // add children and test, that multiplicity is a dummy return before all
      // children are added
      std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
      SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
      auto firstChild = addLeftChildFirst ? leftChild : rightChild;
      auto secondChild = addLeftChildFirst ? rightChild : leftChild;
      Variable firstVariable = addLeftChildFirst ? subj : obj;
      Variable secondVariable = addLeftChildFirst ? obj : subj;

      // each of the input child result tables should look like this:
      // ?geometry           ?point
      // <geometry1>         POINT(7.83505 48.01267)
      // <geometry1>         POINT(7.12345 48.12345)
      // <geometry1>         POINT(7.54321 48.54321)
      // <geometry2>         POINT(7.85298 47.99557)
      // <geometry3>         POINT(-0.11957 51.50333)
      // <geometry4>         POINT(-74.04454 40.68925)
      // <geometry5>         POINT(2.29451 48.85825)
      // multiplicity of ?geometry: 1,4   multiplicity of ?point: 1   size: 7

      if (testMultiplicities == MultiplicityOrSizeEst::Multiplicity) {
        auto assertMultiplicity = [&](Variable var, double multiplicity,
                                      SpatialJoin* spatialJoin,
                                      VariableToColumnMap& varColsMap) {
          ASSERT_NEAR(
              spatialJoin->getMultiplicity(varColsMap[var].columnIndex_),
              multiplicity, doubleBound);
        };
        multiplicitiesBeforeAllChildrenAdded(spatialJoin);
        auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
        spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
        multiplicitiesBeforeAllChildrenAdded(spatialJoin);
        auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
        spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());
        auto varColsMap = spatialJoin->getExternallyVisibleVariableColumns();

        assertMultiplicity(subj1.getVariable(), 9.8, spatialJoin, varColsMap);
        assertMultiplicity(obj1.getVariable(), 7.0, spatialJoin, varColsMap);
        assertMultiplicity(subj2.getVariable(), 9.8, spatialJoin, varColsMap);
        assertMultiplicity(obj2.getVariable(), 7.0, spatialJoin, varColsMap);
        ASSERT_TRUE(
            spatialJoin->onlyForTestingGetDistanceVariable().has_value());
        assertMultiplicity(Variable{"?distanceForTesting"}, 1, spatialJoin,
                           varColsMap);
      } else {
        ASSERT_EQ(leftChild->getSizeEstimate(), 7);
        ASSERT_EQ(rightChild->getSizeEstimate(), 7);
        auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
        spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
        auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
        spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());
        ASSERT_LE(spatialJoin->getSizeEstimate(), 49);
      }
    }
  }
};

TEST_P(SpatialJoinMultiplicityAndSizeEstimateTest,
       MultiplicityAndSizeEstimate) {
  auto [addLeftChildFirst, testMultiplicity] = GetParam();
  testMultiplicitiesOrSizeEstimate(addLeftChildFirst, testMultiplicity);
}

INSTANTIATE_TEST_SUITE_P(
    SpatialJoin, SpatialJoinMultiplicityAndSizeEstimateTest,
    ::testing::Combine(::testing::Bool(),
                       ::testing::Values(MultiplicityOrSizeEst::Multiplicity,
                                         MultiplicityOrSizeEst::SizeEstimate)));

}  // namespace getMultiplicityAndSizeEstimate

}  // anonymous namespace
