// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gmock/gmock.h>

#include <cstdio>

#include "./IndexTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "engine/GroupBy.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/QueryPlanner.h"
#include "engine/Sort.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "gtest/gtest.h"
#include "index/ConstantsIndexBuilding.h"
#include "parser/SparqlParser.h"

using namespace ad_utility::testing;

namespace {
auto I = IntId;
}

// This fixture is used to create an Index for the tests.
// The full index creation is required for initialization of the vocabularies.
class GroupByTest : public ::testing::Test {
 public:
  GroupByTest() {
    FILE_BUFFER_SIZE = 1000;
    // Create the index. The full index creation is run here to allow for
    // loading a docsDb file, which is not otherwise accessible
    std::string docsFileContent = "0\tExert 1\n1\tExert 2\n2\tExert3";
    std::string wordsFileContent =
        "Exert\t0\t0\t0\n"
        "1\t0\t0\t0\n"
        "Exert\t1\t0\t0\n"
        "2\t1\t0\t0\n"
        "Exert\t2\t0\t0\n"
        "3\t2\t0\t0\n";
    std::string ntFileContent = "<a>\t<b>\t<c>\t.";
    ad_utility::File docsFile("group_by_test.documents", "w");
    ad_utility::File wordsFile("group_by_test.words", "w");
    ad_utility::File ntFile("group_by_test.nt", "w");
    docsFile.write(docsFileContent.c_str(), docsFileContent.size());
    wordsFile.write(wordsFileContent.c_str(), wordsFileContent.size());
    ntFile.write(ntFileContent.c_str(), ntFileContent.size());
    docsFile.close();
    wordsFile.close();
    ntFile.close();
    _index.setKbName("group_by_test");
    _index.setTextName("group_by_test");
    _index.setOnDiskBase("group_ty_test");
    _index.createFromFile("group_by_test.nt");
    _index.addTextFromContextFile("group_by_test.words", false);
    _index.buildDocsDB("group_by_test.documents");

    _index.addTextFromOnDiskIndex();
  }

  virtual ~GroupByTest() {
    // delete all files created during index creation
    std::remove("group_by_test.documents");
    std::remove("group_by_test.words");
    std::remove("group_by_test.text.vocabulary");
    std::remove("group_by_test.vocabulary");
    std::remove("group_by_test.text.index");
    std::remove("group_by_test.text.docsDB");
    std::remove("group_by_test.index.pso");
    std::remove("group_by_test.index.pos");
    std::remove("group_by_test.nt");
  }

  Index _index = makeIndexWithTestSettings();
};

TEST_F(GroupByTest, doGroupBy) {
  using std::string;
  using std::vector;

  // There are 7 different aggregates, of which 5 (all apart from SAMPLE and
  // COUNT) react different to the 5 different ResultTypes.

  Id floatBuffers[3]{Id::makeUndefined(), Id::makeUndefined(),
                     Id::makeUndefined()};
  float floatValues[3] = {-3, 2, 1231};
  for (int i = 0; i < 3; i++) {
    floatBuffers[i] = Id::makeFromDouble(floatValues[i]);
  }

  // Add some words to the index's vocabulary.
  auto& vocab = const_cast<RdfsVocabulary&>(_index.getVocab());
  ad_utility::HashSet<std::string> s;
  s.insert("<entity1>");
  s.insert("<entity2>");
  s.insert("<entity3>");
  vocab.createFromSet(s);

  // Create an input result table with a local vocabulary.
  auto localVocab = std::make_shared<LocalVocab>();
  localVocab->getIndexAndAddIfNotContained("<local1>");
  localVocab->getIndexAndAddIfNotContained("<local2>");
  localVocab->getIndexAndAddIfNotContained("<local3>");

  IdTable inputData(6, makeAllocator());
  // The input data types are KB, KB, VERBATIM, TEXT, FLOAT, STRING.
  inputData.push_back({I(1), I(4), I(123), I(0), floatBuffers[0], I(0)});
  inputData.push_back({I(1), I(5), I(0), I(1), floatBuffers[1], I(1)});

  inputData.push_back({I(2), I(6), I(41223), I(2), floatBuffers[2], I(2)});
  inputData.push_back({I(2), I(7), I(123), I(0), floatBuffers[0], I(0)});
  inputData.push_back({I(2), I(7), I(123), I(0), floatBuffers[0], I(0)});

  inputData.push_back({I(3), I(8), I(0), I(1), floatBuffers[1], I(1)});
  inputData.push_back({I(3), I(9), I(41223), I(2), floatBuffers[2], I(2)});

  /*
    COUNT,
    GROUP_CONCAT,
    FIRST,
    LAST,
    SAMPLE,
    MIN,
    MAX,
    SUM,
    AVG
   */

  /*
  std::vector<size_t> groupByCols = {0};
  std::string delim1(", ");
  std::vector<GroupBy::Aggregate> aggregates = {
      // type                                in out userdata
      {ParsedQuery::AggregateType::COUNT, 1, 1, nullptr},

      {ParsedQuery::AggregateType::GROUP_CONCAT, 1, 2, &delim1},
      {ParsedQuery::AggregateType::GROUP_CONCAT, 2, 3, &delim1},
      {ParsedQuery::AggregateType::GROUP_CONCAT, 3, 4, &delim1},
      {ParsedQuery::AggregateType::GROUP_CONCAT, 4, 5, &delim1},
      {ParsedQuery::AggregateType::GROUP_CONCAT, 5, 6, &delim1},

      {ParsedQuery::AggregateType::SAMPLE, 1, 7, nullptr},

      {ParsedQuery::AggregateType::MIN, 1, 8, nullptr},
      {ParsedQuery::AggregateType::MIN, 2, 9, nullptr},
      {ParsedQuery::AggregateType::MIN, 3, 10, nullptr},
      {ParsedQuery::AggregateType::MIN, 4, 11, nullptr},

      {ParsedQuery::AggregateType::MAX, 1, 12, nullptr},
      {ParsedQuery::AggregateType::MAX, 2, 13, nullptr},
      {ParsedQuery::AggregateType::MAX, 3, 14, nullptr},
      {ParsedQuery::AggregateType::MAX, 4, 15, nullptr},

      {ParsedQuery::AggregateType::SUM, 1, 16, nullptr},
      {ParsedQuery::AggregateType::SUM, 2, 17, nullptr},
      {ParsedQuery::AggregateType::SUM, 3, 18, nullptr},
      {ParsedQuery::AggregateType::SUM, 4, 19, nullptr},

      {ParsedQuery::AggregateType::AVG, 1, 20, nullptr},
      {ParsedQuery::AggregateType::AVG, 2, 21, nullptr},
      {ParsedQuery::AggregateType::AVG, 3, 22, nullptr},
      {ParsedQuery::AggregateType::AVG, 4, 23, nullptr}};

  ResultTable outTable{allocator()};

  // This is normally done when calling computeResult in the GroupBy
  // operation.
  outTable._data.setNumColumns(24);

  int inWidth = inputData.cols();
  int outWidth = outTable._data.cols();
  GroupBy G{nullptr, {}, {}};
  CALL_FIXED_SIZE_2(inWidth, outWidth, G.doGroupBy, inputData, inputTypes,
                    groupByCols, aggregates, &outTable._data, &inTable,
                    &outTable, this->_index);

  ASSERT_EQ(3u, outTable._data.size());

  ASSERT_EQ(24u, outTable._data[0].size());
  ASSERT_EQ(24u, outTable._data[1].size());
  ASSERT_EQ(24u, outTable._data[2].size());

  // COUNT CHECKS
  ASSERT_EQ(2u, outTable._data[0][1]);
  ASSERT_EQ(3u, outTable._data[1][1]);
  ASSERT_EQ(2u, outTable._data[2][1]);

  // GROUP CONCAT CHECKS
  // check that the local vocab ids are ascending
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(0u + i, outTable._data[0][2 + i]);
    ASSERT_EQ(0u + i + 5, outTable._data[1][2 + i]);
    ASSERT_EQ(0u + i + 10, outTable._data[2][2 + i]);
  }
  // check for a local vocab entry for each of the 5 input numColumns
  ASSERT_EQ(std::string("<entity1>, <entity2>"), (*outTable._localVocab)[0]);
  ASSERT_EQ(std::string("123, 0"), (*outTable._localVocab)[1]);
  ASSERT_EQ(std::string("Exert 1, Exert 2"), (*outTable._localVocab)[2]);
  std::ostringstream groupConcatFloatString;
  groupConcatFloatString << floatValues[0] << ", " << floatValues[1];
  ASSERT_EQ(groupConcatFloatString.str(), (*outTable._localVocab)[3]);
  ASSERT_EQ(std::string("<local1>, <local2>"), (*outTable._localVocab)[4]);

  // SAMPLE CHECKS
  ASSERT_EQ(5u, outTable._data[0][7]);
  ASSERT_EQ(7u, outTable._data[1][7]);
  ASSERT_EQ(9u, outTable._data[2][7]);

  // MIN CHECKS
  float buffer;
  ASSERT_EQ(4u, outTable._data[0][8]);
  ASSERT_EQ(6u, outTable._data[1][8]);
  ASSERT_EQ(8u, outTable._data[2][8]);

  ASSERT_EQ(0u, outTable._data[0][9]);
  ASSERT_EQ(123u, outTable._data[1][9]);
  ASSERT_EQ(0u, outTable._data[2][9]);

  ASSERT_EQ(ID_NO_VALUE, outTable._data[0][10]);
  ASSERT_EQ(ID_NO_VALUE, outTable._data[1][10]);
  ASSERT_EQ(ID_NO_VALUE, outTable._data[2][10]);

  std::memcpy(&buffer, &outTable._data[0][11], sizeof(float));
  ASSERT_FLOAT_EQ(-3, buffer);
  std::memcpy(&buffer, &outTable._data[1][11], sizeof(float));
  ASSERT_FLOAT_EQ(-3, buffer);
  std::memcpy(&buffer, &outTable._data[2][11], sizeof(float));
  ASSERT_FLOAT_EQ(2, buffer);

  // MAX CHECKS
  ASSERT_EQ(5u, outTable._data[0][12]);
  ASSERT_EQ(7u, outTable._data[1][12]);
  ASSERT_EQ(9u, outTable._data[2][12]);

  ASSERT_EQ(123u, outTable._data[0][13]);
  ASSERT_EQ(41223u, outTable._data[1][13]);
  ASSERT_EQ(41223u, outTable._data[2][13]);

  ASSERT_EQ(ID_NO_VALUE, outTable._data[0][14]);
  ASSERT_EQ(ID_NO_VALUE, outTable._data[1][14]);
  ASSERT_EQ(ID_NO_VALUE, outTable._data[2][14]);

  std::memcpy(&buffer, &outTable._data[0][15], sizeof(float));
  ASSERT_FLOAT_EQ(2, buffer);
  std::memcpy(&buffer, &outTable._data[1][15], sizeof(float));
  ASSERT_FLOAT_EQ(1231, buffer);
  std::memcpy(&buffer, &outTable._data[2][15], sizeof(float));
  ASSERT_FLOAT_EQ(1231, buffer);

  // SUM CHECKS
  std::memcpy(&buffer, &outTable._data[0][16], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._data[1][16], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._data[2][16], sizeof(float));
  ASSERT_FLOAT_EQ(12, buffer);

  std::memcpy(&buffer, &outTable._data[0][17], sizeof(float));
  ASSERT_FLOAT_EQ(123, buffer);
  std::memcpy(&buffer, &outTable._data[1][17], sizeof(float));
  ASSERT_FLOAT_EQ(41469, buffer);
  std::memcpy(&buffer, &outTable._data[2][17], sizeof(float));
  ASSERT_FLOAT_EQ(41223, buffer);

  std::memcpy(&buffer, &outTable._data[0][18], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._data[1][18], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._data[2][18], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));

  std::memcpy(&buffer, &outTable._data[0][19], sizeof(float));
  ASSERT_FLOAT_EQ(-1, buffer);
  std::memcpy(&buffer, &outTable._data[1][19], sizeof(float));
  ASSERT_FLOAT_EQ(1225, buffer);
  std::memcpy(&buffer, &outTable._data[2][19], sizeof(float));
  ASSERT_FLOAT_EQ(1233, buffer);

  // AVG CHECKS
  std::memcpy(&buffer, &outTable._data[0][20], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._data[1][20], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._data[2][20], sizeof(float));
  ASSERT_FLOAT_EQ(6, buffer);

  std::memcpy(&buffer, &outTable._data[0][21], sizeof(float));
  ASSERT_FLOAT_EQ(61.5, buffer);
  std::memcpy(&buffer, &outTable._data[1][21], sizeof(float));
  ASSERT_FLOAT_EQ(13823, buffer);
  std::memcpy(&buffer, &outTable._data[2][21], sizeof(float));
  ASSERT_FLOAT_EQ(20611.5, buffer);

  std::memcpy(&buffer, &outTable._data[0][22], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._data[1][22], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._data[2][22], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));

  std::memcpy(&buffer, &outTable._data[0][23], sizeof(float));
  ASSERT_FLOAT_EQ(-0.5, buffer);
  std::memcpy(&buffer, &outTable._data[1][23], sizeof(float));
  ASSERT_FLOAT_EQ(408.3333333333333, buffer);
  std::memcpy(&buffer, &outTable._data[2][23], sizeof(float));
  ASSERT_FLOAT_EQ(616.5, buffer);
   */
}

namespace {
// All the operations take a `QueryExecutionContext` as a first argument.
// Todo: Continue the comment.
template <typename Operation>
std::shared_ptr<QueryExecutionTree> makeExecutionTree(
    QueryExecutionContext* qec, auto&&... args) {
  return std::make_shared<QueryExecutionTree>(
      qec, std::make_shared<Operation>(qec, AD_FWD(args)...));
}

using namespace sparqlExpression;
struct GroupByOptimizations : ::testing::Test {
  using Tree = std::shared_ptr<QueryExecutionTree>;
  Variable varX{"?x"};
  Variable varY{"?y"};
  Variable varZ{"?z"};
  Variable varA{"?a"};

  std::string turtleInput =
      "<x> <label> \"alpha\" . <x> <label> \"älpha\" . <x> <label> \"A\" . "
      "<a> <is-a> <f> . <a> <is> 20 . <b> <is-a> <f> . <b> <is> 40.0 . <c> "
      "<is-a> <g> . <c> <is> 100 . <x> <is-a> <f> . <x> <is> \"A\" . "
      "<x> "
      "<label> \"Beta\". <x> <is-a> <y>. <y> <is-a> <x>. <z> <label> "
      "\"zz\"@en";

  QueryExecutionContext* qec = getQec(turtleInput);
  SparqlTriple xyzTriple{Variable{"?x"}, "?y", Variable{"?z"}};
  Tree xyzScanSortedByX =
      makeExecutionTree<IndexScan>(qec, Permutation::Enum::SOP, xyzTriple);
  Tree xyzScanSortedByY =
      makeExecutionTree<IndexScan>(qec, Permutation::Enum::POS, xyzTriple);
  Tree xScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTriple{{"<x>"}, {"<label>"}, Variable{"?x"}});
  Tree xyScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTriple{Variable{"?x"}, {"<label>"}, Variable{"?y"}});
  Tree xScanEmptyResult = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTriple{{"<x>"}, {"<notInKg>"}, Variable{"?x"}});

  Tree invalidJoin = makeExecutionTree<Join>(qec, xScan, xScan, 0, 0);
  Tree validJoinWhenGroupingByX =
      makeExecutionTree<Join>(qec, xScan, xyzScanSortedByX, 0, 0);

  std::vector<Variable> emptyVariables{};
  std::vector<Variable> variablesOnlyX{varX};
  std::vector<Variable> variablesOnlyY{varY};

  std::vector<Alias> emptyAliases{};

  static SparqlExpression::Ptr makeLiteralDoubleExpr(double constant) {
    return std::make_unique<IdExpression>(DoubleId(constant));
  }

  static SparqlExpressionPimpl makeLiteralDoublePimpl(double constant) {
    return SparqlExpressionPimpl{makeLiteralDoubleExpr(constant), "constant"};
  }

  static SparqlExpression::Ptr makeVariableExpression(const Variable& var) {
    return std::make_unique<VariableExpression>(var);
  }
  static SparqlExpressionPimpl makeVariablePimpl(const Variable& var) {
    return SparqlExpressionPimpl{makeVariableExpression(var), var.name()};
  }

  static SparqlExpressionPimpl makeCountPimpl(const Variable& var,
                                              bool distinct = false) {
    return SparqlExpressionPimpl{std::make_unique<CountExpression>(
                                     distinct, makeVariableExpression(var)),
                                 "COUNT(?someVariable)"};
  }

  static SparqlExpressionPimpl makeAvgPimpl(const Variable& var,
                                            bool distinct = false) {
    return SparqlExpressionPimpl{
        std::make_unique<AvgExpression>(distinct, makeVariableExpression(var)),
        "AVG(?someVariable)"};
  }

  static SparqlExpressionPimpl makeMinPimpl(const Variable& var) {
    return SparqlExpressionPimpl{
        std::make_unique<MinExpression>(false, makeVariableExpression(var)),
        "MIN(?someVariable)"};
  }

  static SparqlExpressionPimpl makeAvgCountPimpl(const Variable& var) {
    auto countExpression =
        std::make_unique<CountExpression>(false, makeVariableExpression(var));
    return SparqlExpressionPimpl{
        std::make_unique<AvgExpression>(false, std::move(countExpression)),
        "AVG(COUNT(?someVariable))"};
  }

  SparqlExpressionPimpl varxExpressionPimpl = makeVariablePimpl(varX);
  SparqlExpression::Ptr varXExpression2 =
      std::make_unique<VariableExpression>(varX);
  SparqlExpressionPimpl countXPimpl = makeCountPimpl(varX, false);
  SparqlExpressionPimpl countDistinctXPimpl = makeCountPimpl(varX, true);
  std::vector<Alias> aliasesXAsV{Alias{varxExpressionPimpl, Variable{"?v"}}};
  std::vector<Alias> aliasesCountDistinctX{
      Alias{countDistinctXPimpl, Variable{"?count"}}};
  std::vector<Alias> aliasesCountX{Alias{countXPimpl, Variable{"?count"}}};

  std::vector<Alias> aliasesCountXTwice{
      Alias{makeCountPimpl(varX, false), Variable{"?count"}},
      Alias{makeCountPimpl(varX, false), Variable{"?count2"}}};

  const Join* getJoinPtr(const Tree& tree) {
    auto join = dynamic_cast<const Join*>(tree->getRootOperation().get());
    AD_CONTRACT_CHECK(join);
    return join;
  }
  const IndexScan* getScanPtr(const Tree& tree) {
    auto scan = dynamic_cast<const IndexScan*>(tree->getRootOperation().get());
    AD_CONTRACT_CHECK(scan);
    return scan;
  }
};

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, getPermutationForThreeVariableTriple) {
  using enum Permutation::Enum;
  const QueryExecutionTree& xyzScan = *xyzScanSortedByX;

  // Valid inputs.
  ASSERT_EQ(SPO,
            GroupBy::getPermutationForThreeVariableTriple(xyzScan, varX, varX));
  ASSERT_EQ(POS,
            GroupBy::getPermutationForThreeVariableTriple(xyzScan, varY, varZ));
  ASSERT_EQ(OSP,
            GroupBy::getPermutationForThreeVariableTriple(xyzScan, varZ, varY));

  // First variable not contained in triple.
  ASSERT_EQ(std::nullopt,
            GroupBy::getPermutationForThreeVariableTriple(xyzScan, varA, varX));

  // Second variable not contained in triple.
  ASSERT_EQ(std::nullopt,
            GroupBy::getPermutationForThreeVariableTriple(xyzScan, varX, varA));

  // Not a three variable triple.
  ASSERT_EQ(std::nullopt,
            GroupBy::getPermutationForThreeVariableTriple(*xScan, varX, varX));
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, findAggregates) {
  // ((2 * AVG(?y)) * AVG(4 * ?y))
  auto fourTimesYExpr = makeMultiplyExpression(makeLiteralDoubleExpr(4.0),
                                               makeVariableExpression(varY));
  auto avgFourTimesYExpr =
      std::make_unique<AvgExpression>(false, std::move(fourTimesYExpr));
  auto avgYExpr =
      std::make_unique<AvgExpression>(false, makeVariableExpression(varY));
  auto twoTimesAvgYExpr =
      makeMultiplyExpression(makeLiteralDoubleExpr(2.0), std::move(avgYExpr));
  auto twoTimesAvgY_times_avgFourTimesYExpr = makeMultiplyExpression(
      std::move(twoTimesAvgYExpr), std::move(avgFourTimesYExpr));

  auto foundAggregates =
      GroupBy::findAggregates(twoTimesAvgY_times_avgFourTimesYExpr.get());
  ASSERT_TRUE(foundAggregates.has_value());
  auto value = foundAggregates.value();
  ASSERT_EQ(value.size(), 2);
  ASSERT_EQ(value.at(0).parentAndIndex_.value().nThChild_, 1);
  ASSERT_EQ(value.at(1).parentAndIndex_.value().nThChild_, 1);
  ASSERT_EQ(value.at(0).parentAndIndex_.value().parent_,
            twoTimesAvgY_times_avgFourTimesYExpr->children()[0].get());
  ASSERT_EQ(value.at(1).parentAndIndex_.value().parent_,
            twoTimesAvgY_times_avgFourTimesYExpr.get());

  // (MIN(?y) + AVG(?x))
  auto minYExpr =
      std::make_unique<MinExpression>(false, makeVariableExpression(varY));
  auto avgXExpr =
      std::make_unique<AvgExpression>(false, makeVariableExpression(varX));
  auto minYPlusAvgXExpr =
      makeAddExpression(std::move(minYExpr), std::move(avgXExpr));
  auto unsupportedAggregates = GroupBy::findAggregates(minYPlusAvgXExpr.get());
  ASSERT_FALSE(unsupportedAggregates.has_value());
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, findGroupedVariable) {
  Variable varA = Variable{"?a"};
  Variable varX = Variable{"?x"};
  Variable varB = Variable{"?b"};

  using namespace sparqlExpression;
  using TC = TripleComponent;

  // `(?a as ?x)`.
  auto expr1 = makeVariableExpression(varA);

  // `(?a + COUNT(?b) AS ?y)`.
  auto expr2 = makeAddExpression(
      makeVariableExpression(varA),
      std::make_unique<CountExpression>(false, makeVariableExpression(varB)));

  // `(?x + AVG(?b) as ?z)`.
  auto expr3 = makeAddExpression(
      makeVariableExpression(varX),
      std::make_unique<AvgExpression>(false, makeVariableExpression(varB)));

  // Set up the Group By object.
  parsedQuery::SparqlValues input;
  input._variables = std::vector{varA, varB};
  input._values.push_back(std::vector{TC(1.0), TC(3.0)});
  auto values = ad_utility::makeExecutionTree<Values>(
      ad_utility::testing::getQec(), input);
  GroupBy groupBy{ad_utility::testing::getQec(), {Variable{"?a"}}, {}, values};

  auto variableAtTop = groupBy.findGroupedVariable(expr1.get());
  ASSERT_TRUE(std::get_if<GroupBy::OccurenceAsRoot>(&variableAtTop));

  auto variableInExpression = groupBy.findGroupedVariable(expr2.get());
  auto variableInExpressionOccurrences =
      std::get_if<std::vector<GroupBy::ParentAndChildIndex>>(
          &variableInExpression);
  ASSERT_TRUE(variableInExpressionOccurrences);
  ASSERT_EQ(variableInExpressionOccurrences->size(), 1);
  auto parentAndChildIndex = variableInExpressionOccurrences->at(0);
  ASSERT_EQ(parentAndChildIndex.nThChild_, 0);
  ASSERT_EQ(parentAndChildIndex.parent_, expr2.get());

  auto variableNotFound = groupBy.findGroupedVariable(expr3.get());
  auto variableNotFoundOccurrences =
      std::get_if<std::vector<GroupBy::ParentAndChildIndex>>(&variableNotFound);
  ASSERT_EQ(variableNotFoundOccurrences->size(), 0);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, checkIfHashMapOptimizationPossible) {
  auto testFailure = [this](const auto& groupByVariables, const auto& aliases,
                            const auto& join, auto& aggregates) {
    auto groupBy = GroupBy{qec, groupByVariables, aliases, join};
    ASSERT_FALSE(groupBy.checkIfHashMapOptimizationPossible(aggregates));
  };

  std::vector<Variable> variablesXAndY{varX, varY};

  std::vector<ColumnIndex> sortedColumns = {0};
  Tree subtreeWithSort =
      makeExecutionTree<Sort>(qec, validJoinWhenGroupingByX, sortedColumns);

  SparqlExpressionPimpl avgXPimpl = makeAvgPimpl(varX);
  SparqlExpressionPimpl avgDistinctXPimpl = makeAvgPimpl(varX, true);
  SparqlExpressionPimpl avgCountXPimpl = makeAvgCountPimpl(varX);
  SparqlExpressionPimpl minXPimpl = makeMinPimpl(varX);

  std::vector<Alias> aliasesAvgX{Alias{avgXPimpl, Variable{"?avg"}}};
  std::vector<Alias> aliasesAvgDistinctX{
      Alias{avgDistinctXPimpl, Variable{"?avgDistinct"}}};
  std::vector<Alias> aliasesAvgCountX{
      Alias{avgCountXPimpl, Variable("?avgcount")}};
  std::vector<Alias> aliasesMinX{Alias{minXPimpl, Variable{"?minX"}}};

  std::vector<GroupBy::Aggregate> countAggregate = {{countXPimpl, 1}};
  std::vector<GroupBy::Aggregate> avgAggregate = {{avgXPimpl, 1}};
  std::vector<GroupBy::Aggregate> avgDistinctAggregate = {
      {avgDistinctXPimpl, 1}};
  std::vector<GroupBy::Aggregate> avgCountAggregate = {{avgCountXPimpl, 1}};
  std::vector<GroupBy::Aggregate> minAggregate = {{minXPimpl, 1}};

  // Enable optimization
  RuntimeParameters().set<"use-group-by-hash-map-optimization">(true);

  // Must have exactly one variable to group by.
  testFailure(emptyVariables, aliasesAvgX, subtreeWithSort, avgAggregate);
  testFailure(variablesXAndY, aliasesAvgX, subtreeWithSort, avgAggregate);
  // No support for min at the moment
  testFailure(variablesOnlyX, aliasesMinX, subtreeWithSort, minAggregate);
  // Top operation must be SORT
  testFailure(variablesOnlyX, aliasesAvgX, validJoinWhenGroupingByX,
              avgAggregate);
  // Can not be a nested aggregate
  testFailure(variablesOnlyX, aliasesAvgCountX, subtreeWithSort,
              avgCountAggregate);
  // Do not support distinct aggregates
  testFailure(variablesOnlyX, aliasesAvgDistinctX, subtreeWithSort,
              avgDistinctAggregate);
  // Optimization has to be enabled
  RuntimeParameters().set<"use-group-by-hash-map-optimization">(false);
  testFailure(variablesOnlyX, aliasesAvgX, subtreeWithSort, avgAggregate);

  // Everything is valid for the following example.
  RuntimeParameters().set<"use-group-by-hash-map-optimization">(true);
  GroupBy groupBy{qec, variablesOnlyX, aliasesAvgX, subtreeWithSort};
  auto optimizedAggregateData =
      groupBy.checkIfHashMapOptimizationPossible(avgAggregate);
  ASSERT_TRUE(optimizedAggregateData.has_value());
  ASSERT_EQ(optimizedAggregateData->subtreeColumnIndex_, 0);
  // Check aggregate alias is correct
  auto aggregateAlias = optimizedAggregateData->aggregateAliases_[0];
  ASSERT_EQ(aggregateAlias.expr_.getPimpl(), avgXPimpl.getPimpl());
  // Check aggregate info is correct
  auto aggregateInfo = aggregateAlias.aggregateInfo_[0];
  ASSERT_EQ(aggregateInfo.aggregateDataIndex_, 0);
  ASSERT_FALSE(aggregateInfo.parentAndIndex_.has_value());
  ASSERT_EQ(aggregateInfo.expr_, avgXPimpl.getPimpl());

  // Disable optimization for following tests
  RuntimeParameters().set<"use-group-by-hash-map-optimization">(false);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, correctResultForHashMapOptimization) {
  /* Setup query:
  SELECT ?x (AVG(?y) as ?avg) WHERE {
    ?z <is-a> ?x .
    ?z <is> ?y
  } GROUP BY ?x
 */
  Tree zxScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTriple{Variable{"?z"}, {"<is-a>"}, Variable{"?x"}});
  Tree zyScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTriple{Variable{"?z"}, {"<is>"}, Variable{"?y"}});
  Tree join = makeExecutionTree<Join>(qec, zxScan, zyScan, 0, 0);
  std::vector<ColumnIndex> sortedColumns = {1};
  Tree sortedJoin = makeExecutionTree<Sort>(qec, join, sortedColumns);

  SparqlExpressionPimpl avgYPimpl = makeAvgPimpl(varY);
  std::vector<Alias> aliasesAvgY{Alias{avgYPimpl, Variable{"?avg"}}};

  // Calculate result with optimization
  RuntimeParameters().set<"use-group-by-hash-map-optimization">(true);
  GroupBy groupByWithOptimization{qec, variablesOnlyX, aliasesAvgY, sortedJoin};
  auto resultWithOptimization = groupByWithOptimization.getResult();

  // Clear cache, calculate result without optimization
  qec->clearCacheUnpinnedOnly();
  RuntimeParameters().set<"use-group-by-hash-map-optimization">(false);
  GroupBy groupByWithoutOptimization{qec, variablesOnlyX, aliasesAvgY,
                                     sortedJoin};
  auto resultWithoutOptimization = groupByWithoutOptimization.getResult();

  // Compare results, using debugString as the result only contains 2 rows
  ASSERT_EQ(resultWithOptimization->asDebugString(),
            resultWithoutOptimization->asDebugString());
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, hashMapOptimizationGroupedVariable) {
  // Make sure we are calculating the correct result when a grouped variable
  // occurs in an expression.
  RuntimeParameters().set<"use-group-by-hash-map-optimization">(true);

  parsedQuery::SparqlValues input;
  using TC = TripleComponent;

  // SELECT (?a AS ?x) (?a + COUNT(?b) AS ?y) (?x + AVG(?b) as ?z) WHERE {
  //   VALUES (?a ?b) { (1.0 3.0) (1.0 7.0) (5.0 4.0)}
  // } GROUP BY ?a
  Variable varA = Variable{"?a"};
  Variable varX = Variable{"?x"};
  Variable varB = Variable{"?b"};

  input._variables = std::vector{varA, varB};
  input._values.push_back(std::vector{TC(1.0), TC(3.0)});
  input._values.push_back(std::vector{TC(1.0), TC(7.0)});
  input._values.push_back(std::vector{TC(5.0), TC(4.0)});
  auto values = ad_utility::makeExecutionTree<Values>(
      ad_utility::testing::getQec(), input);

  using namespace sparqlExpression;

  // Create `Alias` object for `(?a as ?x)`.
  auto expr1 = makeVariableExpression(varA);
  auto alias1 =
      Alias{SparqlExpressionPimpl{std::move(expr1), "?a"}, Variable{"?x"}};

  // Create `Alias` object for `(?a + COUNT(?b) AS ?y)`.
  auto expr2 = makeAddExpression(
      makeVariableExpression(varA),
      std::make_unique<CountExpression>(false, makeVariableExpression(varB)));
  auto alias2 = Alias{SparqlExpressionPimpl{std::move(expr2), "?a + COUNT(?b)"},
                      Variable{"?y"}};

  // Create `Alias` object for `(?x + AVG(?b) as ?z)`.
  auto expr3 = makeAddExpression(
      makeVariableExpression(varX),
      std::make_unique<AvgExpression>(false, makeVariableExpression(varB)));
  auto alias3 = Alias{SparqlExpressionPimpl{std::move(expr3), "?x + AVG(?b)"},
                      Variable{"?z"}};

  // Set up and evaluate the GROUP BY clause.
  GroupBy groupBy{ad_utility::testing::getQec(),
                  {Variable{"?a"}},
                  {std::move(alias1), std::move(alias2), std::move(alias3)},
                  std::move(values)};
  auto result = groupBy.getResult();
  const auto& table = result->idTable();

  // Check the result.
  auto d = DoubleId;
  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?a"}, {0, AlwaysDefined}},
      {Variable{"?x"}, {1, PossiblyUndefined}},
      {Variable{"?y"}, {2, PossiblyUndefined}},
      {Variable{"?z"}, {3, PossiblyUndefined}}};
  EXPECT_THAT(groupBy.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
  auto expected = makeIdTableFromVector(
      {{d(1), d(1), d(3), d(6)}, {d(5), d(5), d(6), d(9)}});
  EXPECT_EQ(table, expected);

  // Disable optimization for following tests
  RuntimeParameters().set<"use-group-by-hash-map-optimization">(false);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, hashMapOptimizationNonTrivial) {
  /* Setup query:
  SELECT ?x (AVG(?y) as ?avg)
            (?avg + ((2 * COUNT(?y)) * AVG(4 * ?y)) as ?complexAvg)
            (5.0 as ?const) (42.0 as ?const2) (13.37 as ?const3)
            (?const + ?const2 + ?const3 + AVG(?y) + AVG(?y) + AVG(?y) as ?sth)
            WHERE {
    ?z <is-a> ?x .
    ?z <is> ?y
  } GROUP BY ?x
  */

  Tree zxScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTriple{Variable{"?z"}, {"<is-a>"}, Variable{"?x"}});
  Tree zyScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTriple{Variable{"?z"}, {"<is>"}, Variable{"?y"}});
  Tree join = makeExecutionTree<Join>(qec, zxScan, zyScan, 0, 0);
  std::vector<ColumnIndex> sortedColumns = {1};
  Tree sortedJoin = makeExecutionTree<Sort>(qec, join, sortedColumns);

  // (AVG(?y) as ?avg)
  Variable varAvg{"?avg"};
  SparqlExpressionPimpl avgYPimpl = makeAvgPimpl(varY);

  // (?avg + ((2 * COUNT(?y)) * AVG(4 * ?y)) as ?complexAvg)
  auto fourTimesYExpr = makeMultiplyExpression(makeLiteralDoubleExpr(4.0),
                                               makeVariableExpression(varY));
  auto avgFourTimesYExpr =
      std::make_unique<AvgExpression>(false, std::move(fourTimesYExpr));
  auto countYExpr =
      std::make_unique<CountExpression>(false, makeVariableExpression(varY));
  auto twoTimesCountYExpr =
      makeMultiplyExpression(makeLiteralDoubleExpr(2.0), std::move(countYExpr));
  auto twoTimesCountY_times_avgFourTimesYExpr = makeMultiplyExpression(
      std::move(twoTimesCountYExpr), std::move(avgFourTimesYExpr));
  auto avgY_plus_twoTimesCountY_times_avgFourTimesYExpr =
      makeAddExpression(makeVariableExpression(varAvg),
                        std::move(twoTimesCountY_times_avgFourTimesYExpr));
  SparqlExpressionPimpl avgY_plus_twoTimesCountY_times_avgFourTimesYPimpl(
      std::move(avgY_plus_twoTimesCountY_times_avgFourTimesYExpr),
      "(?avg + ((2 * AVG(?y)) * AVG(4 * ?y)) as ?complexAvg)");

  // (5.0 as ?const) (42.0 as ?const2) (13.37 as ?const3)
  Variable varConst = Variable{"?const"};
  SparqlExpressionPimpl constantFive = makeLiteralDoublePimpl(5.0);
  Variable varConst2 = Variable{"?const2"};
  SparqlExpressionPimpl constantFortyTwo = makeLiteralDoublePimpl(42.0);
  Variable varConst3 = Variable{"?const3"};
  SparqlExpressionPimpl constantLeet = makeLiteralDoublePimpl(13.37);

  // (?const + ?const2 + ?const3 + AVG(?y) + AVG(?y) + AVG(?y) as ?sth)
  auto constPlusConst2 = makeAddExpression(makeVariableExpression(varConst),
                                           makeVariableExpression(varConst2));
  auto constPlusConst2PlusConst3 = makeAddExpression(
      std::move(constPlusConst2), makeVariableExpression(varConst3));
  auto avgY1 =
      std::make_unique<AvgExpression>(false, makeVariableExpression(varY));
  auto constPusConst2PlusConst3PlusAvgY =
      makeAddExpression(std::move(constPlusConst2PlusConst3), std::move(avgY1));
  auto avgY2 =
      std::make_unique<AvgExpression>(false, makeVariableExpression(varY));
  auto constPlusConst2PlusConst3PlusAvgYPlusAvgY = makeAddExpression(
      std::move(constPusConst2PlusConst3PlusAvgY), std::move(avgY2));
  auto avgY3 =
      std::make_unique<AvgExpression>(false, makeVariableExpression(varY));
  auto constPlusEtc = makeAddExpression(
      std::move(constPlusConst2PlusConst3PlusAvgYPlusAvgY), std::move(avgY3));
  SparqlExpressionPimpl constPlusEtcPimpl(
      std::move(constPlusEtc),
      "?const + ?const2 + ?const3 + AVG(?y) + AVG(?y) + AVG(?y)");

  std::vector<Alias> aliasesAvgY{
      Alias{avgYPimpl, varAvg},
      Alias{avgY_plus_twoTimesCountY_times_avgFourTimesYPimpl,
            Variable{"?complexAvg"}},
      Alias{constantFive, varConst},
      Alias{constantFortyTwo, varConst2},
      Alias{constantLeet, varConst3},
      Alias{constPlusEtcPimpl, Variable{"?sth"}}};

  // Clear cache, calculate result without optimization
  RuntimeParameters().set<"use-group-by-hash-map-optimization">(false);
  GroupBy groupByWithoutOptimization{qec, variablesOnlyX, aliasesAvgY,
                                     sortedJoin};
  auto resultWithoutOptimization = groupByWithoutOptimization.getResult();

  // Calculate result with optimization, after calculating it without,
  // since optimization changes tree
  qec->clearCacheUnpinnedOnly();
  RuntimeParameters().set<"use-group-by-hash-map-optimization">(true);
  GroupBy groupByWithOptimization{qec, variablesOnlyX, aliasesAvgY, sortedJoin};
  auto resultWithOptimization = groupByWithOptimization.getResult();

  // Compare results, using debugString as the result only contains 2 rows
  ASSERT_EQ(resultWithOptimization->asDebugString(),
            resultWithoutOptimization->asDebugString());

  // Disable optimization for following tests
  RuntimeParameters().set<"use-group-by-hash-map-optimization">(false);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, checkIfJoinWithFullScan) {
  // Assert that a Group by, that is constructed from the given arguments,
  // can not perform the `OptimizedAggregateOnJoinChild` optimization.
  auto testFailure = [this](const auto& groupByVariables, const auto& aliases,
                            const auto& join) {
    auto groupBy = GroupBy{qec, groupByVariables, aliases, join};
    ASSERT_FALSE(groupBy.checkIfJoinWithFullScan(getJoinPtr(join)));
  };

  // Must have exactly one variable to group by.
  testFailure(emptyVariables, aliasesCountX, validJoinWhenGroupingByX);
  // Must have exactly one alias.
  testFailure(variablesOnlyX, emptyAliases, validJoinWhenGroupingByX);
  // The single alias must be a `COUNT`.
  testFailure(variablesOnlyX, aliasesXAsV, validJoinWhenGroupingByX);
  // The count must not be distinct.
  testFailure(variablesOnlyX, aliasesCountDistinctX, validJoinWhenGroupingByX);

  // Neither of the join children is a three variable triple
  testFailure(variablesOnlyX, aliasesCountX, invalidJoin);

  // The join is not on the GROUPED Variable.
  testFailure(variablesOnlyY, aliasesCountX, validJoinWhenGroupingByX);

  // Everything is valid for the following example.
  GroupBy groupBy{qec, variablesOnlyX, aliasesCountX, validJoinWhenGroupingByX};
  auto optimizedAggregateData =
      groupBy.checkIfJoinWithFullScan(getJoinPtr(validJoinWhenGroupingByX));
  ASSERT_TRUE(optimizedAggregateData.has_value());
  ASSERT_EQ(&optimizedAggregateData->otherSubtree_, xScan.get());
  ASSERT_EQ(optimizedAggregateData->permutation_, Permutation::SPO);
  ASSERT_EQ(optimizedAggregateData->subtreeColumnIndex_, 0);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, computeGroupByForJoinWithFullScan) {
  {
    // One of the invalid cases from the previous test.
    GroupBy invalidForOptimization{qec, emptyVariables, aliasesCountX,
                                   validJoinWhenGroupingByX};
    IdTable result{qec->getAllocator()};
    ASSERT_FALSE(
        invalidForOptimization.computeGroupByForJoinWithFullScan(&result));
    // No optimization was applied, so the result is untouched.
    AD_CONTRACT_CHECK(result.empty());

    // The child of the GROUP BY is not a join, so this is also
    // invalid.
    GroupBy invalidGroupBy2{qec, variablesOnlyX, emptyAliases, xScan};
    ASSERT_FALSE(invalidGroupBy2.computeGroupByForJoinWithFullScan(&result));
    AD_CONTRACT_CHECK(result.empty());
    ;
  }

  // `chooseInterface == true` means "use the dedicated
  // `computeGroupByForJoinWithFullScan` method", `chooseInterface == false`
  // means use the general `computeOptimizedGroupByIfPossible` function.
  using ad_utility::source_location;
  auto testWithBothInterfaces = [&](bool chooseInterface,
                                    source_location l =
                                        source_location::current()) {
    auto trace = generateLocationTrace(l);
    // Set up a `VALUES` clause with three values for `?x`, two of which
    // (`<x>` and `<y>`) actually appear in the test knowledge graph.
    parsedQuery::SparqlValues sparqlValues;
    sparqlValues._variables.push_back(varX);
    sparqlValues._values.emplace_back(std::vector{TripleComponent{"<x>"}});
    sparqlValues._values.emplace_back(std::vector{TripleComponent{"<xa>"}});
    sparqlValues._values.emplace_back(std::vector{TripleComponent{"<y>"}});
    auto values = makeExecutionTree<Values>(qec, sparqlValues);
    // Set up a GROUP BY operation for which the optimization can be applied.
    // The last two arguments of the `Join` constructor are the indices of the
    // join columns.
    IdTable result(qec->getAllocator());
    auto join = makeExecutionTree<Join>(qec, values, xyzScanSortedByX, 0, 0);
    GroupBy validForOptimization{qec, variablesOnlyX, aliasesCountX, join};
    if (chooseInterface) {
      ASSERT_TRUE(
          validForOptimization.computeGroupByForJoinWithFullScan(&result));
    } else {
      ASSERT_TRUE(
          validForOptimization.computeOptimizedGroupByIfPossible(&result));
    }

    // There are 7 triples with `<x>` as a subject, 0 triples with `<xa>` as a
    // subject, and 1 triple with `y` as a subject.
    ASSERT_EQ(result.numColumns(), 2u);
    ASSERT_EQ(result.size(), 2u);
    Id idOfX;
    Id idOfY;
    qec->getIndex().getId("<x>", &idOfX);
    qec->getIndex().getId("<y>", &idOfY);

    ASSERT_EQ(result(0, 0), idOfX);
    ASSERT_EQ(result(0, 1), Id::makeFromInt(7));
    ASSERT_EQ(result(1, 0), idOfY);
    ASSERT_EQ(result(1, 1), Id::makeFromInt(1));
  };
  testWithBothInterfaces(true);
  testWithBothInterfaces(false);

  // Test the case that the input is empty.
  {
    auto join =
        makeExecutionTree<Join>(qec, xScanEmptyResult, xyzScanSortedByX, 0, 0);
    IdTable result{qec->getAllocator()};
    GroupBy groupBy{qec, variablesOnlyX, aliasesCountX, join};
    ASSERT_TRUE(groupBy.computeGroupByForJoinWithFullScan(&result));
    ASSERT_EQ(result.numColumns(), 2u);
    ASSERT_EQ(result.size(), 0u);
  }
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, computeGroupByForSingleIndexScan) {
  // Assert that a GROUP BY, that is constructed from the given arguments,
  // can not perform the `OptimizedAggregateOnIndexScanChild` optimization.
  auto testFailure = [this](const auto& groupByVariables, const auto& aliases,
                            const auto& indexScan) {
    auto groupBy = GroupBy{qec, groupByVariables, aliases, indexScan};
    IdTable result{qec->getAllocator()};
    ASSERT_FALSE(groupBy.computeGroupByForSingleIndexScan(&result));
    ASSERT_EQ(result.size(), 0u);
  };
  // The IndexScan has only one variable, this is currently not supported.
  testFailure(emptyVariables, aliasesCountX, xScan);

  // Must have zero groupByVariables.
  testFailure(variablesOnlyX, aliasesCountX, xyzScanSortedByX);

  // Must (currently) have exactly one alias that is a count.
  // A distinct count is only supporte if the triple has three variables.
  testFailure(emptyVariables, emptyAliases, xyzScanSortedByX);
  testFailure(emptyVariables, aliasesCountDistinctX, xyScan);
  testFailure(emptyVariables, aliasesXAsV, xyzScanSortedByX);

  // `chooseInterface == true` means "use the dedicated
  // `computeGroupByForJoinWithFullScan` method", `chooseInterface == false`
  // means use the general `computeOptimizedGroupByIfPossible` function.
  auto testWithBothInterfaces = [&](bool chooseInterface) {
    IdTable result{qec->getAllocator()};
    auto groupBy =
        GroupBy{qec, emptyVariables, aliasesCountX, xyzScanSortedByX};
    if (chooseInterface) {
      ASSERT_TRUE(groupBy.computeGroupByForSingleIndexScan(&result));
    } else {
      ASSERT_TRUE(groupBy.computeOptimizedGroupByIfPossible(&result));
    }

    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.numColumns(), 1);
    // The test index currently consists of 15 triples.
    ASSERT_EQ(result(0, 0), Id::makeFromInt(15));
  };
  testWithBothInterfaces(true);
  testWithBothInterfaces(false);

  {
    IdTable result{qec->getAllocator()};
    auto groupBy = GroupBy{qec, emptyVariables, aliasesCountX, xyScan};
    ASSERT_TRUE(groupBy.computeGroupByForSingleIndexScan(&result));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.numColumns(), 1);
    // The test index currently consists of 5 triples that have the predicate
    // `<label>`
    ASSERT_EQ(result(0, 0), Id::makeFromInt(5));
  }
  {
    IdTable result{qec->getAllocator()};
    auto groupBy =
        GroupBy{qec, emptyVariables, aliasesCountDistinctX, xyzScanSortedByX};
    ASSERT_TRUE(groupBy.computeGroupByForSingleIndexScan(&result));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.numColumns(), 1);
    // The test index currently consists of six distinct subjects:
    // <x>, <y>, <z>, <a>, <b> and <c>.
    ASSERT_EQ(result(0, 0), Id::makeFromInt(6));
  }
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, computeGroupByForFullIndexScan) {
  // Assert that a GROUP BY which is constructed from the given arguments
  // can not perform the `GroupByForSingleIndexScan2` optimization.
  auto testFailure = [this](const auto& groupByVariables, const auto& aliases,
                            const auto& indexScan) {
    auto groupBy = GroupBy{qec, groupByVariables, aliases, indexScan};
    IdTable result{qec->getAllocator()};
    ASSERT_FALSE(groupBy.computeGroupByForFullIndexScan(&result));
    ASSERT_EQ(result.size(), 0u);
  };
  // The IndexScan doesn't have three variables.
  testFailure(variablesOnlyX, aliasesCountX, xScan);

  // Must have one groupByVariables.
  testFailure(emptyVariables, aliasesCountX, xyzScanSortedByX);

  // Must (currently) have zero aliases or one alias that is a non-distinct
  // count.
  testFailure(variablesOnlyX, aliasesCountDistinctX, xyzScanSortedByX);
  testFailure(variablesOnlyX, aliasesXAsV, xyzScanSortedByX);

  // This is the case that throws, because it can almost be optimized.
  ASSERT_THROW(
      testFailure(variablesOnlyX, aliasesCountXTwice, xyzScanSortedByX),
      std::runtime_error);

  // `chooseInterface == true` means "use the dedicated
  // `computeGroupByForJoinWithFullScan` method", `chooseInterface == false`
  // means use the general `computeOptimizedGroupByIfPossible` function.
  auto testWithBothInterfaces = [&](bool chooseInterface, bool includeCount) {
    IdTable result{qec->getAllocator()};
    auto groupBy =
        includeCount
            ? GroupBy{qec, variablesOnlyX, aliasesCountX, xyzScanSortedByX}
            : GroupBy{qec, variablesOnlyX, emptyAliases, xyzScanSortedByX};
    if (chooseInterface) {
      ASSERT_TRUE(groupBy.computeGroupByForFullIndexScan(&result));
    } else {
      ASSERT_TRUE(groupBy.computeOptimizedGroupByIfPossible(&result));
    }

    // Six distinct subjects.
    ASSERT_EQ(result.size(), 6);
    if (includeCount) {
      ASSERT_EQ(result.numColumns(), 2);
    } else {
      ASSERT_EQ(result.numColumns(), 1);
    }

    auto getId = makeGetId(qec->getIndex());
    EXPECT_THAT(
        result.getColumn(0),
        ::testing::ElementsAre(getId("<a>"), getId("<b>"), getId("<c>"),
                               getId("<x>"), getId("<y>"), getId("<z>")));
    if (includeCount) {
      EXPECT_THAT(
          result.getColumn(1),
          ::testing::ElementsAre(I(2), I(2), I(2), I(7), I(1),
                                 // TODO<joka921> This should be 1.
                                 // There is one triple added <z> @en@<label>
                                 // "zz"@en which is currently not filtered out.
                                 I(2)));
    }
  };
  testWithBothInterfaces(true, true);
  testWithBothInterfaces(true, false);
  testWithBothInterfaces(false, true);

  // TODO<joka921> Add a test with only one column
}

namespace {
// A helper function to set up expression trees in the following test.
template <typename ExprT>
auto make = [](auto&&... args) -> SparqlExpression::Ptr {
  return std::make_unique<ExprT>(AD_FWD(args)...);
};
}  // namespace
// _____________________________________________________________________________
TEST(GroupBy, GroupedVariableInExpressions) {
  parsedQuery::SparqlValues input;
  using TC = TripleComponent;
  // Test the following SPARQL query:
  //
  // SELECT (AVG(?a + ?b) as ?x) (?a + COUNT(?b) AS ?y) WHERE {
  //   VALUES (?a ?b) { (1.0 3.0) (1.0 7.0) (5.0 4.0)}
  // } GROUP BY ?a
  //
  // Note: The values are chosen such that the results are all integers.
  // Otherwise we would get into trouble with floating point comparisons. A
  // check with a similar query but with non-integral inputs and results can
  // be found in the E2E tests.

  Variable varA = Variable{"?a"};
  Variable varB = Variable{"?b"};

  input._variables = std::vector{varA, varB};
  input._values.push_back(std::vector{TC(1.0), TC(3.0)});
  input._values.push_back(std::vector{TC(1.0), TC(7.0)});
  input._values.push_back(std::vector{TC(5.0), TC(4.0)});
  auto values = ad_utility::makeExecutionTree<Values>(
      ad_utility::testing::getQec(), input);

  using namespace sparqlExpression;

  // Create `Alias` object for `(AVG(?a + ?b) AS ?x)`.
  auto sum = makeAddExpression(make<VariableExpression>(varA),
                               make<VariableExpression>(varB));
  auto avg = make<AvgExpression>(false, std::move(sum));
  auto alias1 = Alias{SparqlExpressionPimpl{std::move(avg), "avg(?a + ?b"},
                      Variable{"?x"}};

  // Create `Alias` object for `(?a + COUNT(?b) AS ?y)`.
  auto expr2 = makeAddExpression(
      make<VariableExpression>(varA),
      make<CountExpression>(false, make<VariableExpression>(varB)));
  auto alias2 = Alias{SparqlExpressionPimpl{std::move(expr2), "?a + COUNT(?b)"},
                      Variable{"?y"}};

  // Set up and evaluate the GROUP BY clause.
  GroupBy groupBy{ad_utility::testing::getQec(),
                  {Variable{"?a"}},
                  {std::move(alias1), std::move(alias2)},
                  std::move(values)};
  auto result = groupBy.getResult();
  const auto& table = result->idTable();

  // Check the result.
  auto d = DoubleId;
  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?a"}, {0, AlwaysDefined}},
      {Variable{"?x"}, {1, PossiblyUndefined}},
      {Variable{"?y"}, {2, PossiblyUndefined}}};
  EXPECT_THAT(groupBy.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
  auto expected =
      makeIdTableFromVector({{d(1), d(6), d(3)}, {d(5), d(9), d(6)}});
  EXPECT_EQ(table, expected);
}

// _____________________________________________________________________________
TEST(GroupBy, AliasResultReused) {
  parsedQuery::SparqlValues input;
  using TC = TripleComponent;
  // Test the following SPARQL query:
  //
  // SELECT (AVG(?a + ?b) as ?x) (?x + COUNT(?b) AS ?y) WHERE {
  //   VALUES (?a ?b) { (1.0 3.0) (1.0 7.0) (5.0 4.0)}
  // } GROUP BY ?a
  //
  // Note: The values are chosen such that the results are all integers.
  // Otherwise we would get into trouble with floating point comparisons. A
  // check with a similar query but with non-integral inputs and results can
  // be found in the E2E tests.

  Variable varA = Variable{"?a"};
  Variable varB = Variable{"?b"};

  input._variables = std::vector{varA, varB};
  input._values.push_back(std::vector{TC(1.0), TC(3.0)});
  input._values.push_back(std::vector{TC(1.0), TC(7.0)});
  input._values.push_back(std::vector{TC(5.0), TC(4.0)});
  auto values = ad_utility::makeExecutionTree<Values>(
      ad_utility::testing::getQec(), input);

  using namespace sparqlExpression;

  // Create `Alias` object for `(AVG(?a + ?b) AS ?x)`.
  auto sum = makeAddExpression(make<VariableExpression>(varA),
                               make<VariableExpression>(varB));
  auto avg = make<AvgExpression>(false, std::move(sum));
  auto alias1 = Alias{SparqlExpressionPimpl{std::move(avg), "avg(?a + ?b"},
                      Variable{"?x"}};

  // Create `Alias` object for `(?a + COUNT(?b) AS ?y)`.
  auto expr2 = makeAddExpression(
      make<VariableExpression>(Variable{"?x"}),
      make<CountExpression>(false, make<VariableExpression>(varB)));
  auto alias2 = Alias{SparqlExpressionPimpl{std::move(expr2), "?x + COUNT(?b)"},
                      Variable{"?y"}};

  // Set up and evaluate the GROUP BY clause.
  GroupBy groupBy{ad_utility::testing::getQec(),
                  {Variable{"?a"}},
                  {std::move(alias1), std::move(alias2)},
                  std::move(values)};
  auto result = groupBy.getResult();
  const auto& table = result->idTable();

  // Check the result.
  auto d = DoubleId;
  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?a"}, {0, AlwaysDefined}},
      {Variable{"?x"}, {1, PossiblyUndefined}},
      {Variable{"?y"}, {2, PossiblyUndefined}}};
  EXPECT_THAT(groupBy.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
  auto expected =
      makeIdTableFromVector({{d(1), d(6), d(8)}, {d(5), d(9), d(10)}});
  EXPECT_EQ(table, expected);
}

}  // namespace

// _____________________________________________________________________________
TEST(GroupBy, AddedHavingRows) {
  // Expressions in HAVING clauses are converted to special internal aliases.
  // Test the combination of parsing and evaluating such queries.
  auto query =
      "SELECT ?x (COUNT(?y) as ?count) WHERE {"
      " VALUES (?x ?y) {(0 1) (0 3) (0 5) (1 4) (1 3) } }"
      "GROUP BY ?x HAVING (?count > 2)";
  auto pq = SparqlParser::parseQuery(query);
  QueryPlanner qp{ad_utility::testing::getQec()};
  auto tree = qp.createExecutionTree(pq);

  auto res = tree.getResult();

  // The HAVING is implemented as an alias that creates an internal variable
  // which becomes part of the result, but is not selected by the query.
  EXPECT_THAT(pq.selectClause().getSelectedVariables(),
              ::testing::ElementsAre(Variable{"?x"}, Variable{"?count"}));
  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?x"}, {0, AlwaysDefined}},
      {Variable{"?count"}, {1, PossiblyUndefined}},
      {Variable{"?_QLever_internal_variable_0"}, {2, PossiblyUndefined}}};
  EXPECT_THAT(tree.getVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
  const auto& table = res->idTable();
  auto i = IntId;
  auto expected = makeIdTableFromVector({{i(0), i(3), Id::makeFromBool(true)}});
  EXPECT_EQ(table, expected);
}
