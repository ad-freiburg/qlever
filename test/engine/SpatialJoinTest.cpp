//  Copyright 2024 - 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: @Jonathan24680
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>

#include <cstdlib>
#include <fstream>
#include <memory>
#include <regex>
#include <sstream>

#include "../printers/PayloadVariablePrinters.h"
#include "../printers/VariablePrinters.h"
#include "../printers/VariableToColumnMapPrinters.h"
#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "./SpatialJoinTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryPlanner.h"
#include "engine/SpatialJoin.h"
#include "engine/VariableToColumnMap.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/ValueId.h"
#include "index/ExportIds.h"
#include "parser/SparqlParser.h"
#include "rdfTypes/Variable.h"
#include "util/GeoSparqlHelpers.h"

using qlever::EncodedIriManager;
using qlever::SparqlParser;

namespace {  // anonymous namespace to avoid linker problems

using namespace ad_utility::testing;
using namespace SpatialJoinTestHelpers;

namespace childrenTesting {

void testAddChild(bool addLeftChildFirst) {
  auto checkVariable = [](qlever::SpatialJoin* spatialJoin,
                          bool checkLeftVariable) {
    if (checkLeftVariable) {
      std::shared_ptr<qlever::Operation> op =
          spatialJoin->onlyForTestingGetLeftChild()->getRootOperation();
      qlever::IndexScan* scan = static_cast<qlever::IndexScan*>(op.get());
      ASSERT_EQ(scan->subject().getVariable().name(), "?obj1");
      ASSERT_EQ(scan->object().getVariable().name(), "?point1");
    } else {
      std::shared_ptr<qlever::Operation> op =
          spatialJoin->onlyForTestingGetRightChild()->getRootOperation();
      qlever::IndexScan* scan = static_cast<qlever::IndexScan*>(op.get());
      ASSERT_EQ(scan->subject().getVariable().name(), "?obj2");
      ASSERT_EQ(scan->object().getVariable().name(), "?point2");
    }
  };

  auto qec = buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  qlever::TripleComponent point1{qlever::Variable{"?point1"}};
  qlever::TripleComponent point2{qlever::Variable{"?point2"}};
  auto leftChild =
      buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
  auto rightChild =
      buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

  std::shared_ptr<qlever::QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<qlever::SpatialJoin>(
          qec,
          SpatialJoinConfiguration{MaxDistanceConfig{1000},
                                   point1.getVariable(), point2.getVariable()},
          std::nullopt, std::nullopt);

  std::shared_ptr<qlever::Operation> op =
      spatialJoinOperation->getRootOperation();
  qlever::SpatialJoin* spatialJoin =
      static_cast<qlever::SpatialJoin*>(op.get());

  auto firstChild = addLeftChildFirst ? leftChild : rightChild;
  auto secondChild = addLeftChildFirst ? rightChild : leftChild;
  qlever::Variable firstVariable =
      addLeftChildFirst ? point1.getVariable() : point2.getVariable();
  qlever::Variable secondVariable =
      addLeftChildFirst ? point2.getVariable() : point1.getVariable();
  std::string firstSubject = addLeftChildFirst ? "?obj1" : "?obj2";
  std::string secondSubject = addLeftChildFirst ? "?obj2" : "?obj1";
  std::string firstObject = addLeftChildFirst ? "?point1" : "?point2";
  std::string secondObject = addLeftChildFirst ? "?point2" : "?point1";

  ASSERT_EQ(spatialJoin->onlyForTestingGetLeftChild(), nullptr);
  ASSERT_EQ(spatialJoin->onlyForTestingGetRightChild(), nullptr);

  ASSERT_ANY_THROW(
      spatialJoin->addChild(firstChild, qlever::Variable{"?wrongVar"}));
  ASSERT_ANY_THROW(
      spatialJoin->addChild(secondChild, qlever::Variable{"?wrongVar"}));

  ASSERT_EQ(spatialJoin->onlyForTestingGetLeftChild(), nullptr);
  ASSERT_EQ(spatialJoin->onlyForTestingGetRightChild(), nullptr);

  auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
  spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin1.get());

  checkVariable(spatialJoin, addLeftChildFirst);

  auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
  spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());

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
  qlever::TripleComponent point1{qlever::Variable{"?point1"}};
  qlever::TripleComponent point2{qlever::Variable{"?point2"}};
  auto leftChild =
      buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
  auto rightChild =
      buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

  std::shared_ptr<qlever::QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<qlever::SpatialJoin>(
          qec,
          SpatialJoinConfiguration{MaxDistanceConfig{1000},
                                   point1.getVariable(), point2.getVariable()},
          std::nullopt, std::nullopt);

  std::shared_ptr<qlever::Operation> op =
      spatialJoinOperation->getRootOperation();
  qlever::SpatialJoin* spatialJoin =
      static_cast<qlever::SpatialJoin*>(op.get());

  ASSERT_FALSE(spatialJoin->isConstructed());

  auto spJoin1 = spatialJoin->addChild(leftChild, point1.getVariable());
  spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin1.get());

  ASSERT_FALSE(spatialJoin->isConstructed());

  auto spJoin2 = spatialJoin->addChild(rightChild, point2.getVariable());
  spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());

  ASSERT_TRUE(spatialJoin->isConstructed());
}

TEST(SpatialJoin, getChildren) {
  auto qec = buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  qlever::TripleComponent point1{qlever::Variable{"?point1"}};
  qlever::TripleComponent point2{qlever::Variable{"?point2"}};
  auto leftChild =
      buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
  auto rightChild =
      buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

  std::shared_ptr<qlever::QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<qlever::SpatialJoin>(
          qec,
          SpatialJoinConfiguration{MaxDistanceConfig{1000},
                                   point1.getVariable(), point2.getVariable()},
          std::nullopt, std::nullopt);

  std::shared_ptr<qlever::Operation> op =
      spatialJoinOperation->getRootOperation();
  qlever::SpatialJoin* spatialJoin =
      static_cast<qlever::SpatialJoin*>(op.get());

  ASSERT_EQ(spatialJoin->getChildren().size(), 0);

  auto spJoin1 = spatialJoin->addChild(leftChild, point1.getVariable());
  spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin1.get());

  ASSERT_EQ(spatialJoin->getChildren().size(), 1);

  auto spJoin2 = spatialJoin->addChild(rightChild, point2.getVariable());
  spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());

  ASSERT_EQ(spatialJoin->getChildren().size(), 2);

  auto assertScanVariables =
      [](qlever::IndexScan* scan1, qlever::IndexScan* scan2,
         bool isSubjectNotObject, std::string varName1, std::string varName2) {
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

  std::shared_ptr<qlever::Operation> op1 =
      spatialJoin->getChildren().at(0)->getRootOperation();
  qlever::IndexScan* scan1 = static_cast<qlever::IndexScan*>(op1.get());

  std::shared_ptr<qlever::Operation> op2 =
      spatialJoin->getChildren().at(1)->getRootOperation();
  qlever::IndexScan* scan2 = static_cast<qlever::IndexScan*>(op2.get());

  assertScanVariables(scan1, scan2, true, "?obj1", "?obj2");
  assertScanVariables(scan1, scan2, false, "?point1", "?point2");
}

}  // namespace childrenTesting

namespace variableColumnMapAndResultWidth {

// Represents from left to right: leftSideBigChild, rightSideBigChild,
// addLeftChildFirst, testVarToColMap
using VarColTestSuiteParam = std::tuple<bool, bool, bool, bool>;

// Internal testing shorthands
using V = qlever::Variable;
using VarToColVec = std::vector<std::pair<V, qlever::ColumnIndexAndTypeInfo>>;

// Helper function to create a spatial join from VALUEs
std::shared_ptr<qlever::SpatialJoin> makeSpatialJoinFromValues(
    qlever::QueryExecutionContext* qec,
    PayloadVariables pv = PayloadVariables::all(),
    SpatialJoinAlgorithm alg = SPATIAL_JOIN_DEFAULT_ALGORITHM) {
  EncodedIriManager encodedIriManager;
  const auto sharedHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  // also include some garbage input geometries
  auto pqLeft = SparqlParser::parseQuery(
      &encodedIriManager,
      "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\nSELECT ?a {VALUES "
      "(?a) {(\"POLYGON((8.529 47.375, 8.549 47.375, 8.549 47.395, 8.529 "
      "47.395, 8.529 47.375))\"^^geo:wktLiteral) (\"garbage\") (5) (<>)}}");
  auto pqRight = SparqlParser::parseQuery(
      &encodedIriManager,
      "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\nSELECT ?b {VALUES "
      "(?b) {(\"POINT(8.542 47.385)\"^^geo:wktLiteral)}}");
  qlever::QueryPlanner qp{qec, sharedHandle};

  auto leftChild = std::make_shared<qlever::QueryExecutionTree>(
      qp.createExecutionTree(pqLeft));
  auto rightChild = std::make_shared<qlever::QueryExecutionTree>(
      qp.createExecutionTree(pqRight));

  std::shared_ptr<qlever::QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<qlever::SpatialJoin>(
          qec,
          SpatialJoinConfiguration{MaxDistanceConfig{0}, qlever::Variable{"?a"},
                                   qlever::Variable{"?b"}, std::nullopt, pv,
                                   alg, SpatialJoinType::INTERSECTS},
          std::nullopt, std::nullopt);
  std::shared_ptr<qlever::Operation> op =
      spatialJoinOperation->getRootOperation();
  qlever::SpatialJoin* spatialJoin =
      static_cast<qlever::SpatialJoin*>(op.get());
  auto spJoin1 = spatialJoin->addChild(leftChild, qlever::Variable{"?a"});
  spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin1.get());
  auto spJoin2 = spatialJoin->addChild(rightChild, qlever::Variable{"?b"});
  return spJoin2;
}

class SpatialJoinVarColParamTest
    : public ::testing::TestWithParam<VarColTestSuiteParam> {
 public:
  // Helper function to construct a child for testing
  std::shared_ptr<qlever::QueryExecutionTree> getChild(
      qlever::QueryExecutionContext* qec, bool getBigChild,
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

  std::shared_ptr<qlever::SpatialJoin> makeSpatialJoin(
      qlever::QueryExecutionContext* qec, VarColTestSuiteParam parameters,
      bool addDist = true, PayloadVariables pv = PayloadVariables::all(),
      SpatialJoinAlgorithm alg = SPATIAL_JOIN_DEFAULT_ALGORITHM,
      SpatialJoinType joinType = SpatialJoinType::WITHIN_DIST) {
    auto [leftSideBigChild, rightSideBigChild, addLeftChildFirst,
          testVarToColMap] = parameters;
    auto leftChild = getChild(qec, leftSideBigChild, "1");
    auto rightChild = getChild(qec, rightSideBigChild, "2");

    std::optional<qlever::Variable> dist = std::nullopt;
    if (addDist) {
      dist = qlever::Variable{"?distOfTheTwoObjectsAddedInternally"};
    }
    std::shared_ptr<qlever::QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<qlever::SpatialJoin>(
            qec,
            SpatialJoinConfiguration{
                MaxDistanceConfig{0}, qlever::Variable{"?point1"},
                qlever::Variable{"?point2"}, dist, pv, alg, joinType},
            std::nullopt, std::nullopt);
    std::shared_ptr<qlever::Operation> op =
        spatialJoinOperation->getRootOperation();
    qlever::SpatialJoin* spatialJoin =
        static_cast<qlever::SpatialJoin*>(op.get());
    auto firstChild = addLeftChildFirst ? leftChild : rightChild;
    auto secondChild = addLeftChildFirst ? rightChild : leftChild;
    qlever::Variable firstVariable = addLeftChildFirst
                                         ? qlever::Variable{"?point1"}
                                         : qlever::Variable{"?point2"};
    qlever::Variable secondVariable = addLeftChildFirst
                                          ? qlever::Variable{"?point2"}
                                          : qlever::Variable{"?point1"};
    auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
    spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin1.get());
    auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
    return spJoin2;
  }

  // only test one at a time. Then the gtest will fail on the test, which
  // failed, instead of failing for both getResultWidth() and
  // computeVariableToColumnMap() if only one of them is wrong
  void testGetResultWidthOrVariableToColumnMap(
      VarColTestSuiteParam parameters,
      SpatialJoinAlgorithm alg = SPATIAL_JOIN_DEFAULT_ALGORITHM) {
    auto [leftSideBigChild, rightSideBigChild, addLeftChildFirst,
          testVarToColMap] = parameters;
    auto qec = buildTestQEC();
    auto spJoin2 =
        makeSpatialJoin(qec, parameters, true, PayloadVariables::all(), alg);
    auto spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());

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
        ASSERT_TRUE(
            varColMap.contains(qlever::Variable{expectedColumns.at(i).first}));

        // test, that the column contains the correct values
        qlever::ColumnIndex ind =
            varColMap[qlever::Variable{expectedColumns.at(i).first}]
                .columnIndex_;
        const IdTableView<0>* r = &resultTable.idTableView();
        ASSERT_LT(0, r->numRows());
        ASSERT_LT(ind, r->numColumns());
        qlever::ValueId tableEntry = r->at(0, ind);

        if (tableEntry.getDatatype() == qlever::Datatype::VocabIndex) {
          std::string value =
              ql::exportIds::idToStringAndType(qec->getIndex(), tableEntry, {})
                  .value()
                  .first;
          ASSERT_TRUE(value.find(expectedColumns.at(i).second, 0) !=
                      std::string::npos);
        } else if (tableEntry.getDatatype() == qlever::Datatype::Int) {
          std::string value =
              ql::exportIds::idToStringAndType(qec->getIndex(), tableEntry, {})
                  .value()
                  .first;
          ASSERT_EQ(value, expectedColumns.at(i).second);
        } else if (tableEntry.getDatatype() == qlever::Datatype::GeoPoint) {
          auto [value, type] =
              ql::exportIds::idToStringAndType(qec->getIndex(), tableEntry, {})
                  .value();
          value = absl::StrCat("\"", value, "\"^^<", type, ">");
          ASSERT_TRUE(value.find(expectedColumns.at(i).second, 0) !=
                      std::string::npos);
        }
      }
    }
  }

  // Test spatial join on contains
  void testGetResultWidthOrVariableToColumnMapSpatialJoinContains(
      VarColTestSuiteParam parameters) {
    auto [leftSideBigChild, rightSideBigChild, addLeftChildFirst,
          testVarToColMap] = parameters;
    auto qec = buildNonSelfJoinDataset();

    auto spJoin2 = makeSpatialJoin(
        qec, parameters, false, PayloadVariables::all(),
        SpatialJoinAlgorithm::LIBSPATIALJOIN, SpatialJoinType::CONTAINS);
    auto spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());

    size_t expectedResultWidth =
        (leftSideBigChild ? 4 : 3) + (rightSideBigChild ? 4 : 3);

    auto numTriples = qec->getIndex().numTriples().normal;
    ASSERT_EQ(numTriples, 22);

    if (!testVarToColMap) {
      ASSERT_EQ(spatialJoin->getResultWidth(), expectedResultWidth);
    } else {
      std::vector<std::pair<std::string, std::string>> expectedColumns{};

      expectedColumns =
          addExpectedColumns(expectedColumns, leftSideBigChild, "1");
      expectedColumns =
          addExpectedColumns(expectedColumns, rightSideBigChild, "2");

      auto varColMap = spatialJoin->computeVariableToColumnMap();
      auto resultTable = spatialJoin->computeResult(false);

      // if the size of varColMap and expectedColumns is the same and each
      // element of expectedColumns is contained in varColMap, then they are the
      // same (assuming that each element is unique)
      ASSERT_EQ(varColMap.size(), expectedColumns.size());

      for (size_t i = 0; i < expectedColumns.size(); i++) {
        ASSERT_TRUE(
            varColMap.contains(qlever::Variable{expectedColumns.at(i).first}));

        // test, that the column contains the correct values
        qlever::ColumnIndex ind =
            varColMap[qlever::Variable{expectedColumns.at(i).first}]
                .columnIndex_;
        const IdTableView<0>* r = &resultTable.idTableView();
        ASSERT_LT(0, r->numRows());
        ASSERT_LT(ind, r->numColumns());

        auto col = r->getColumn(ind);
        std::vector<std::string> columnEntries;
        columnEntries.reserve(r->numRows());
        for (const auto& valueId : col) {
          auto [value, type] =
              ql::exportIds::idToStringAndType(qec->getIndex(), valueId, {})
                  .value();
          if (valueId.getDatatype() == qlever::Datatype::GeoPoint) {
            value = absl::StrCat("\"", value, "\"^^<", type, ">");
          }
          columnEntries.push_back(value);
        }

        auto expected = expectedColumns.at(i).second;
        EXPECT_THAT(columnEntries,
                    ::testing::Contains(::testing::StartsWith(expected)));
      }
    }
  }

  // Function to test the variable to column map that is generated by spatial
  // join with the left and right children as given by the parameters. It will
  // try all possible, valid payloadVariables settings and check the variable
  // to column maps using a comprehensible and easy to debug string
  // representation.
  void testPayloadVariablesVarToColMap(VarColTestSuiteParam parameters) {
    auto [leftSideBigChild, rightSideBigChild, addLeftChildFirst,
          testVarToColMap] = parameters;
    if (!testVarToColMap) {
      return;
    }

    auto computeAndCompareVarToColMaps =
        [&](bool addDist, PayloadVariables pv,
            qlever::VariableToColumnMap expectedVarToColMap,
            std::optional<std::string> matchWarning = std::nullopt) {
          auto qec = buildTestQEC();
          auto spJoin2 = makeSpatialJoin(qec, parameters, addDist, pv);
          auto spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());
          auto vc = spatialJoin->computeVariableToColumnMap();
          ASSERT_THAT(
              vc, ::testing::UnorderedElementsAreArray(expectedVarToColMap));
          if (matchWarning) {
            ASSERT_THAT(spatialJoin->collectWarnings(),
                        ::testing::Contains(
                            ::testing::HasSubstr(matchWarning.value())));
          }
        };

    // Expected variables
    V distVar{"?distOfTheTwoObjectsAddedInternally"};
    V obj{"?obj"};
    V geo{"?geo"};
    V name{"?name"};
    std::vector<V> smallChild{obj, geo};
    std::vector<V> bigChild{obj, geo, name};

    auto makeExpected = [&](bool leftSideBigChild, bool rightSideBigChild,
                            bool addDist, bool withObj, bool withName,
                            bool withGeo) -> qlever::VariableToColumnMap {
      size_t currentColumn = 0;
      qlever::VariableToColumnMap expected;

      auto addV = [&](std::vector<V> vec, bool right = false) {
        for (auto v : vec) {
          if (right && ((v == obj && !withObj) || (v == name && !withName) ||
                        (v == geo && !withGeo))) {
            continue;
          }
          V expectedVar{absl::StrCat(v.name(), right ? "2" : "1")};
          expected[expectedVar] = qlever::ColumnIndexAndTypeInfo{
              currentColumn,
              qlever::ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined};
          currentColumn++;
        }
      };

      addV(leftSideBigChild ? bigChild : smallChild);
      addV({V{"?point"}});
      addV(rightSideBigChild ? bigChild : smallChild, true);
      addV({V{"?point"}}, true);
      if (addDist) {
        expected[distVar] = qlever::ColumnIndexAndTypeInfo{
            currentColumn,
            qlever::ColumnIndexAndTypeInfo::UndefStatus::PossiblyUndefined};
      }
      return expected;
    };

    std::vector<bool> bools{true, false};
    for (bool withObj : bools) {
      for (bool withGeo : bools) {
        for (bool withName : bools) {
          for (bool addDist : bools) {
            std::vector<qlever::Variable> payloadVars;
            if (withObj) {
              payloadVars.push_back(qlever::Variable{"?obj2"});
            }
            if (withGeo) {
              payloadVars.push_back(qlever::Variable{"?geo2"});
            }
            if (withName && rightSideBigChild) {
              payloadVars.push_back(qlever::Variable{"?name2"});
            }

            // Test the regular and valid payloadVars
            auto exp = makeExpected(leftSideBigChild, rightSideBigChild,
                                    addDist, withObj, withName, withGeo);
            computeAndCompareVarToColMaps(addDist, payloadVars, exp);

            // Also test the PayloadAllVariables version
            if (withGeo && withObj && (withName || !rightSideBigChild)) {
              computeAndCompareVarToColMaps(addDist, PayloadVariables::all(),
                                            exp);
            }

            // Test multiple occurrences of variables
            if (withObj) {
              payloadVars.push_back(qlever::Variable{"?obj2"});
              // Variable ?obj2 is now contained twice
              computeAndCompareVarToColMaps(addDist, payloadVars, exp);
              payloadVars.pop_back();
            }

            // Test contained right variable in payloadVars
            payloadVars.push_back(qlever::Variable{"?point2"});
            computeAndCompareVarToColMaps(addDist, payloadVars, exp);
            payloadVars.pop_back();

            // Test warnings for unbound variables
            payloadVars.push_back(qlever::Variable{"?point1"});
            computeAndCompareVarToColMaps(
                addDist, payloadVars, exp,
                "Variable '?point1' selected as payload to "
                "spatial join but not present in right child");
            payloadVars.pop_back();

            payloadVars.push_back(qlever::Variable{"?obj1"});
            computeAndCompareVarToColMaps(
                addDist, payloadVars, exp,
                "Variable '?obj1' selected as payload to "
                "spatial join but not present in right child");
            payloadVars.pop_back();

            payloadVars.push_back(qlever::Variable{"?isThereSomebodyHere"});
            computeAndCompareVarToColMaps(
                addDist, payloadVars, exp,
                "Variable '?isThereSomebodyHere' selected as payload to "
                "spatial join but not present in right child");
            payloadVars.pop_back();

            // Test if distance variable contained
            if (addDist) {
              payloadVars.push_back(distVar);
              computeAndCompareVarToColMaps(
                  addDist, payloadVars, exp,
                  "Variable '?distOfTheTwoObjectsAddedInternally' selected "
                  "as payload to spatial join but not present in right "
                  "child");
              payloadVars.pop_back();
            }
          }
        }
      }
    }
  }
};

TEST_P(SpatialJoinVarColParamTest, variableToColumnMap) {
  testGetResultWidthOrVariableToColumnMap(GetParam());
}

// Test `libspatialjoin` with `within-dist` spatial join type. This is
// essentially a self-join (but the payload may be different for the two
// sides).
TEST_P(SpatialJoinVarColParamTest, variableToColumnMapLibspatialjoin) {
  testGetResultWidthOrVariableToColumnMap(GetParam(),
                                          SpatialJoinAlgorithm::LIBSPATIALJOIN);
}

// Test `libspatialjoin` with `contains` spatial join type. Here the two sides
// have different sets of objects,
TEST_P(SpatialJoinVarColParamTest, variableToColumnMapLibspatialjoinContains) {
  testGetResultWidthOrVariableToColumnMapSpatialJoinContains(GetParam());
}

// Test a spatial join with VALUES as both children
TEST(SpatialJoinVarColParamTest, testLibspatialjoinFromvalues) {
  auto qec = buildNonSelfJoinDataset();

  auto spJoin2 = makeSpatialJoinFromValues(
      qec, PayloadVariables::all(), SpatialJoinAlgorithm::LIBSPATIALJOIN);
  auto spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());

  auto resultTable = spatialJoin->computeResult(false);
  ASSERT_EQ(resultTable.idTableView().numRows(), 1);
}

TEST_P(SpatialJoinVarColParamTest, payloadVariables) {
  testPayloadVariablesVarToColMap(GetParam());
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
    auto checkEmptyResult = [](qlever::SpatialJoin* spatialJoin_,
                               bool shouldBeEmpty) {
      ASSERT_EQ(spatialJoin_->knownEmptyResult(), shouldBeEmpty);
    };

    auto getChild = [](qlever::QueryExecutionContext* qec, bool emptyChild) {
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

    std::shared_ptr<qlever::QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<qlever::SpatialJoin>(
            qec,
            SpatialJoinConfiguration{MaxDistanceConfig{0},
                                     qlever::Variable{"?point1"},
                                     qlever::Variable{"?point2"}},
            std::nullopt, std::nullopt);
    std::shared_ptr<qlever::Operation> op =
        spatialJoinOperation->getRootOperation();
    qlever::SpatialJoin* spatialJoin =
        static_cast<qlever::SpatialJoin*>(op.get());
    auto firstChild = addLeftChildFirst ? leftChild : rightChild;
    auto secondChild = addLeftChildFirst ? rightChild : leftChild;
    qlever::Variable firstVariable = addLeftChildFirst
                                         ? qlever::Variable{"?point1"}
                                         : qlever::Variable{"?point2"};
    qlever::Variable secondVariable = addLeftChildFirst
                                          ? qlever::Variable{"?point2"}
                                          : qlever::Variable{"?point1"};
    bool firstChildEmpty =
        addLeftChildFirst ? leftSideEmptyChild : rightSideEmptyChild;
    bool secondChildEmpty =
        addLeftChildFirst ? rightSideEmptyChild : leftSideEmptyChild;

    checkEmptyResult(spatialJoin, false);

    auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
    spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin1.get());
    checkEmptyResult(spatialJoin, firstChildEmpty);

    auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
    spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());
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
  std::string kg = createSmallDataset();

  auto qec = buildQec(kg);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);

  qlever::TripleComponent obj1{qlever::Variable{"?point1"}};
  qlever::TripleComponent obj2{qlever::Variable{"?point2"}};
  auto leftChild = buildIndexScan(qec, {"?geometry1", "<asWKT>", "?point1"});
  auto rightChild = buildIndexScan(qec, {"?geometry2", "<asWKT>", "?point2"});

  std::shared_ptr<qlever::QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<qlever::SpatialJoin>(
          qec,
          SpatialJoinConfiguration{MaxDistanceConfig{10000000},
                                   qlever::Variable{"?point1"},
                                   qlever::Variable{"?point2"}},
          std::nullopt, std::nullopt);

  // add children and test, that multiplicity is a dummy return before all
  // children are added
  std::shared_ptr<qlever::Operation> op =
      spatialJoinOperation->getRootOperation();
  qlever::SpatialJoin* spatialJoin =
      static_cast<qlever::SpatialJoin*>(op.get());
  ASSERT_EQ(spatialJoin->getResultSortedOn().size(), 0);
  auto spJoin1 = spatialJoin->addChild(leftChild, obj1.getVariable());
  spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin1.get());
  ASSERT_EQ(spatialJoin->getResultSortedOn().size(), 0);
  auto spJoin2 = spatialJoin->addChild(rightChild, obj2.getVariable());
  spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());
  ASSERT_EQ(spatialJoin->getResultSortedOn().size(), 0);
}

}  // namespace resultSortedOn

namespace stringRepresentation {

TEST(SpatialJoin, getDescriptor) {
  auto qec = getQec();
  qlever::TripleComponent subject{qlever::Variable{"?subject"}};
  qlever::TripleComponent object{qlever::Variable{"?object"}};

  std::shared_ptr<qlever::QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<qlever::SpatialJoin>(
          qec,
          SpatialJoinConfiguration{MaxDistanceConfig{1000},
                                   subject.getVariable(), object.getVariable()},
          std::nullopt, std::nullopt);
  std::shared_ptr<qlever::Operation> op =
      spatialJoinOperation->getRootOperation();
  qlever::SpatialJoin* spatialJoin =
      static_cast<qlever::SpatialJoin*>(op.get());

  auto description = spatialJoin->getDescriptor();
  ASSERT_THAT(description, ::testing::HasSubstr(absl::StrCat(
                               spatialJoin->getMaxDist().value_or(-1))));
  ASSERT_TRUE(description.find("?subject") != std::string::npos);
  ASSERT_TRUE(description.find("?object") != std::string::npos);
}

// _____________________________________________________________________________
TEST(SpatialJoin, getDescriptorLibSJWithJoinType) {
  // The `SpatialJoin`'s descriptor should contain a readable representation of
  // the join type
  std::shared_ptr<qlever::QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<qlever::SpatialJoin>(
          getQec(),
          SpatialJoinConfiguration{
              LibSpatialJoinConfig{SpatialJoinType::INTERSECTS},
              qlever::Variable{"?subject"}, qlever::Variable{"?object"}},
          std::nullopt, std::nullopt);
  auto description = spatialJoinOperation->getRootOperation()->getDescriptor();
  ASSERT_THAT(description, ::testing::HasSubstr("?subject"));
  ASSERT_THAT(description, ::testing::HasSubstr("?object"));
  ASSERT_THAT(description, ::testing::HasSubstr("intersects"));
}

// _____________________________________________________________________________
TEST(SpatialJoin, getCacheKeyImpl) {
  auto qec = buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  qlever::Variable subj{"?point1"};
  qlever::Variable obj{"?point2"};
  auto leftChild =
      buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
  auto rightChild =
      buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

  std::shared_ptr<qlever::QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<qlever::SpatialJoin>(
          qec, SpatialJoinConfiguration{MaxDistanceConfig{1000}, subj, obj},
          std::nullopt, std::nullopt);
  std::shared_ptr<qlever::Operation> op =
      spatialJoinOperation->getRootOperation();
  qlever::SpatialJoin* spatialJoin =
      static_cast<qlever::SpatialJoin*>(op.get());

  ASSERT_EQ(spatialJoin->getCacheKeyImpl(), "incomplete SpatialJoin class");

  auto spJoin1 = spatialJoin->addChild(leftChild, subj);
  spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin1.get());

  ASSERT_EQ(spatialJoin->getCacheKeyImpl(), "incomplete SpatialJoin class");

  auto spJoin2 = spatialJoin->addChild(rightChild, obj);
  spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());

  auto cacheKeyString = spatialJoin->getCacheKeyImpl();
  auto leftCacheKeyString =
      spatialJoin->onlyForTestingGetLeftChild()->getCacheKey();
  auto rightCacheKeyString =
      spatialJoin->onlyForTestingGetRightChild()->getCacheKey();

  ASSERT_TRUE(cacheKeyString.find(
                  absl::StrCat(spatialJoin->getMaxDist().value_or(-1))) !=
              std::string::npos);
  ASSERT_TRUE(cacheKeyString.find(leftCacheKeyString) != std::string::npos);
  ASSERT_TRUE(cacheKeyString.find(rightCacheKeyString) != std::string::npos);
}

// _____________________________________________________________________________
TEST(SpatialJoin, clone) {
  auto qec = buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  auto leftChild =
      buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
  auto rightChild =
      buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

  {
    qlever::SpatialJoin spatialJoin{
        qec,
        SpatialJoinConfiguration{MaxDistanceConfig{1000},
                                 qlever::Variable{"?point1"},
                                 qlever::Variable{"?point2"}},
        std::nullopt, std::nullopt};

    auto clone = spatialJoin.clone();
    ASSERT_TRUE(clone);
    const auto& cloneReference = *clone;
    EXPECT_EQ(typeid(spatialJoin), typeid(cloneReference));
    EXPECT_EQ(cloneReference.getDescriptor(), spatialJoin.getDescriptor());

    EXPECT_EQ(spatialJoin.getChildren().empty(),
              cloneReference.getChildren().empty());
  }

  {
    qlever::SpatialJoin spatialJoin{
        qec,
        SpatialJoinConfiguration{MaxDistanceConfig{1000},
                                 qlever::Variable{"?point1"},
                                 qlever::Variable{"?point2"}},
        leftChild, std::nullopt};

    auto clone = spatialJoin.clone();
    ASSERT_TRUE(clone);
    const auto& cloneReference = *clone;
    EXPECT_EQ(typeid(spatialJoin), typeid(cloneReference));
    EXPECT_EQ(cloneReference.getDescriptor(), spatialJoin.getDescriptor());

    EXPECT_NE(spatialJoin.getChildren().at(0),
              cloneReference.getChildren().at(0));
  }

  {
    qlever::SpatialJoin spatialJoin{
        qec,
        SpatialJoinConfiguration{MaxDistanceConfig{1000},
                                 qlever::Variable{"?point1"},
                                 qlever::Variable{"?point2"}},
        std::nullopt, rightChild};

    auto clone = spatialJoin.clone();
    ASSERT_TRUE(clone);
    const auto& cloneReference = *clone;
    EXPECT_EQ(typeid(spatialJoin), typeid(cloneReference));
    EXPECT_EQ(cloneReference.getDescriptor(), spatialJoin.getDescriptor());

    EXPECT_NE(spatialJoin.getChildren().at(0),
              cloneReference.getChildren().at(0));
  }

  {
    qlever::SpatialJoin spatialJoin{
        qec,
        SpatialJoinConfiguration{MaxDistanceConfig{1000},
                                 qlever::Variable{"?point1"},
                                 qlever::Variable{"?point2"}},
        leftChild, rightChild};

    auto clone = spatialJoin.clone();
    ASSERT_TRUE(clone);
    const auto& cloneReference = *clone;
    EXPECT_EQ(typeid(spatialJoin), typeid(cloneReference));
    EXPECT_EQ(cloneReference.getDescriptor(), spatialJoin.getDescriptor());

    EXPECT_NE(spatialJoin.getChildren().at(0),
              cloneReference.getChildren().at(0));
    EXPECT_NE(spatialJoin.getChildren().at(1),
              cloneReference.getChildren().at(1));
  }
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
    auto multiplicitiesBeforeAllChildrenAdded =
        [&](qlever::SpatialJoin* spatialJoin) {
          for (size_t i = 0; i < spatialJoin->getResultWidth(); i++) {
            ASSERT_EQ(spatialJoin->getMultiplicity(i), 1);
          }
        };

    const double doubleBound = 0.00001;
    std::string kg = createSmallDataset();

    // add multiplicities to test knowledge graph
    kg += "<node_1> <name> \"testing multiplicity\" .";
    kg += "<node_1> <name> \"testing multiplicity 2\" .";

    auto qec = buildQec(kg);
    auto numTriples = qec->getIndex().numTriples().normal;
    const unsigned int nrTriplesInput = 17;
    ASSERT_EQ(numTriples, nrTriplesInput);

    qlever::Variable subj{"?point1"};
    qlever::Variable obj{"?point2"};

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

    std::shared_ptr<qlever::QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<qlever::SpatialJoin>(
            qec,
            SpatialJoinConfiguration{MaxDistanceConfig{10000000}, subj, obj},
            std::nullopt, std::nullopt);

    // add children and test, that multiplicity is a dummy return before all
    // children are added
    std::shared_ptr<qlever::Operation> op =
        spatialJoinOperation->getRootOperation();
    qlever::SpatialJoin* spatialJoin =
        static_cast<qlever::SpatialJoin*>(op.get());
    auto firstChild = addLeftChildFirst ? leftChild : rightChild;
    auto secondChild = addLeftChildFirst ? rightChild : leftChild;
    qlever::Variable firstVariable = addLeftChildFirst ? subj : obj;
    qlever::Variable secondVariable = addLeftChildFirst ? obj : subj;

    if (testMultiplicities == MultiplicityOrSizeEst::Multiplicity) {
      // expected behavior:
      // assert (result table at column i has the same distinctness as the
      // corresponding input table (via varToColMap))
      // assert, that distinctness * multiplicity = sizeEstimate

      multiplicitiesBeforeAllChildrenAdded(spatialJoin);
      auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
      spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin1.get());
      multiplicitiesBeforeAllChildrenAdded(spatialJoin);
      auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
      spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());
      auto varColsMap = spatialJoin->getExternallyVisibleVariableColumns();
      auto varColsVec = copySortedByColumnIndex(varColsMap);
      auto leftVarColMap = leftChild->getVariableColumns();
      auto rightVarColMap = rightChild->getVariableColumns();
      for (size_t i = 0; i < spatialJoin->getResultWidth(); i++) {
        // get variable at column 0 of the result table
        qlever::Variable var = varColsVec.at(i).first;
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
          qlever::ColumnIndex colIndex;
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
      spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin1.get());
      ASSERT_EQ(spatialJoin->getSizeEstimate(), 1);
      auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
      spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());
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
      std::string kg = createSmallDataset();

      // add multiplicities to test knowledge graph
      kg += "<geometry1> <asWKT> \"POINT(7.12345 48.12345)\".";
      kg += "<geometry1> <asWKT> \"POINT(7.54321 48.54321)\".";

      auto qec = buildQec(kg);
      auto numTriples = qec->getIndex().numTriples().normal;
      const unsigned int nrTriplesInput = 17;
      ASSERT_EQ(numTriples, nrTriplesInput);

      qlever::Variable subj{"?point1"};
      qlever::Variable obj{"?point2"};

      qlever::TripleComponent subj1{qlever::Variable{"?geometry1"}};
      qlever::TripleComponent obj1{qlever::Variable{"?point1"}};
      qlever::TripleComponent subj2{qlever::Variable{"?geometry2"}};
      qlever::TripleComponent obj2{qlever::Variable{"?point2"}};
      auto leftChild =
          buildIndexScan(qec, {"?geometry1", "<asWKT>", "?point1"});
      auto rightChild =
          buildIndexScan(qec, {"?geometry2", "<asWKT>", "?point2"});

      std::shared_ptr<qlever::QueryExecutionTree> spatialJoinOperation =
          ad_utility::makeExecutionTree<qlever::SpatialJoin>(
              qec,
              SpatialJoinConfiguration{MaxDistanceConfig{10000000}, subj, obj,
                                       qlever::Variable{"?distanceForTesting"}},
              std::nullopt, std::nullopt);

      // add children and test, that multiplicity is a dummy return before all
      // children are added
      std::shared_ptr<qlever::Operation> op =
          spatialJoinOperation->getRootOperation();
      qlever::SpatialJoin* spatialJoin =
          static_cast<qlever::SpatialJoin*>(op.get());
      auto firstChild = addLeftChildFirst ? leftChild : rightChild;
      auto secondChild = addLeftChildFirst ? rightChild : leftChild;
      qlever::Variable firstVariable = addLeftChildFirst ? subj : obj;
      qlever::Variable secondVariable = addLeftChildFirst ? obj : subj;

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
        auto assertMultiplicity = [&](qlever::Variable var, double multiplicity,
                                      qlever::SpatialJoin* spatialJoin,
                                      qlever::VariableToColumnMap& varColsMap) {
          ASSERT_NEAR(
              spatialJoin->getMultiplicity(varColsMap[var].columnIndex_),
              multiplicity, doubleBound);
        };
        multiplicitiesBeforeAllChildrenAdded(spatialJoin);
        auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
        spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin1.get());
        multiplicitiesBeforeAllChildrenAdded(spatialJoin);
        auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
        spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());
        auto varColsMap = spatialJoin->getExternallyVisibleVariableColumns();

        assertMultiplicity(subj1.getVariable(), 4.2, spatialJoin, varColsMap);
        assertMultiplicity(obj1.getVariable(), 3.0, spatialJoin, varColsMap);
        assertMultiplicity(subj2.getVariable(), 4.2, spatialJoin, varColsMap);
        assertMultiplicity(obj2.getVariable(), 3.0, spatialJoin, varColsMap);
        ASSERT_TRUE(
            spatialJoin->onlyForTestingGetDistanceVariable().has_value());
        assertMultiplicity(qlever::Variable{"?distanceForTesting"}, 1,
                           spatialJoin, varColsMap);
      } else {
        auto leftEstimate = leftChild->getSizeEstimate();
        auto rightEstimate = rightChild->getSizeEstimate();
        auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
        spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin1.get());
        auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
        spatialJoin = static_cast<qlever::SpatialJoin*>(spJoin2.get());
        ASSERT_LE(spatialJoin->getSizeEstimate(), leftEstimate * rightEstimate);
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
