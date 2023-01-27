// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <cstdio>

#include "./IndexTestHelpers.h"
#include "engine/GroupBy.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "gtest/gtest.h"
#include "index/ConstantsIndexBuilding.h"

using namespace ad_utility::testing;

auto I = [](const auto& id) { return Id::makeFromInt(id); };

// This fixture is used to create an Index for the tests.
// The full index creation is required for initialization of the vocabularies.
class GroupByTest : public ::testing::Test {
 public:
  GroupByTest() {
    FILE_BUFFER_SIZE() = 1000;
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
    _index.createFromFile<TurtleParserAuto>("group_by_test.nt");
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

  // add some words to the index's vocabulary
  auto& vocab = const_cast<RdfsVocabulary&>(_index.getVocab());
  ad_utility::HashSet<std::string> s;
  s.insert("<entity1>");
  s.insert("<entity2>");
  s.insert("<entity3>");
  s.insert(ad_utility::convertFloatStringToIndexWord("1.1231"));
  s.insert(ad_utility::convertFloatStringToIndexWord("-5"));
  s.insert(ad_utility::convertFloatStringToIndexWord("17"));
  vocab.createFromSet(s);

  // create an input result table with a local vocabulary
  ResultTable inTable{makeAllocator()};
  inTable._localVocab->getIndexAndAddIfNotContained("<local1>");
  inTable._localVocab->getIndexAndAddIfNotContained("<local2>");
  inTable._localVocab->getIndexAndAddIfNotContained("<local3>");

  IdTable inputData(6, makeAllocator());
  // The input data types are
  //                   KB, KB, VERBATIM, TEXT, FLOAT,           STRING
  inputData.push_back({I(1), I(4), I(123), I(0), floatBuffers[0], I(0)});
  inputData.push_back({I(1), I(5), I(0), I(1), floatBuffers[1], I(1)});

  inputData.push_back({I(2), I(6), I(41223), I(2), floatBuffers[2], I(2)});
  inputData.push_back({I(2), I(7), I(123), I(0), floatBuffers[0], I(0)});
  inputData.push_back({I(2), I(7), I(123), I(0), floatBuffers[0], I(0)});

  inputData.push_back({I(3), I(8), I(0), I(1), floatBuffers[1], I(1)});
  inputData.push_back({I(3), I(9), I(41223), I(2), floatBuffers[2], I(2)});

  std::vector<ResultTable::ResultType> inputTypes = {
      ResultTable::ResultType::KB,       ResultTable::ResultType::KB,
      ResultTable::ResultType::VERBATIM, ResultTable::ResultType::TEXT,
      ResultTable::ResultType::FLOAT,    ResultTable::ResultType::LOCAL_VOCAB};

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
struct GroupBySpecialCount : ::testing::Test {
  using Tree = std::shared_ptr<QueryExecutionTree>;
  Variable varX{"?x"};
  Variable varY{"?y"};
  Variable varZ{"?z"};
  Variable varA{"?a"};
  QueryExecutionContext* qec = getQec();
  SparqlTriple xyzTriple{Variable{"?x"}, "?y", Variable{"?z"}};
  Tree xyzScanSortedByX = makeExecutionTree<IndexScan>(
      qec, IndexScan::FULL_INDEX_SCAN_SOP, xyzTriple);
  Tree xyzScanSortedByY = makeExecutionTree<IndexScan>(
      qec, IndexScan::FULL_INDEX_SCAN_POS, xyzTriple);
  Tree xScan = makeExecutionTree<IndexScan>(
      qec, IndexScan::PSO_BOUND_S,
      SparqlTriple{{"<x>"}, {"<label>"}, Variable{"?x"}});
  Tree xyScan = makeExecutionTree<IndexScan>(
      qec, IndexScan::PSO_FREE_S,
      SparqlTriple{Variable{"?x"}, {"<label>"}, Variable{"?y"}});
  Tree xScanEmptyResult = makeExecutionTree<IndexScan>(
      qec, IndexScan::PSO_BOUND_S,
      SparqlTriple{{"<x>"}, {"<notInKg>"}, Variable{"?x"}});

  Tree invalidJoin = makeExecutionTree<Join>(qec, xScan, xScan, 0, 0);
  Tree validJoinWhenGroupingByX =
      makeExecutionTree<Join>(qec, xScan, xyzScanSortedByX, 0, 0);

  std::vector<Variable> emptyVariables{};
  std::vector<Variable> variablesOnlyX{varX};
  std::vector<Variable> variablesOnlyY{varY};

  std::vector<Alias> emptyAliases{};

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
                                 "COUNT(?someVariable}"};
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
TEST_F(GroupBySpecialCount, getPermutationForThreeVariableTriple) {
  using enum Index::Permutation;
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
TEST_F(GroupBySpecialCount, checkIfJoinWithFullScan) {
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
  ASSERT_EQ(optimizedAggregateData->permutation_, Index::Permutation::SPO);
  ASSERT_EQ(optimizedAggregateData->subtreeColumnIndex_, 0);
}

TEST_F(GroupBySpecialCount, computeGroupByForJoinWithFullScan) {
  {
    // One of the invalid cases from the previous test.
    GroupBy invalidForOptimization{qec, emptyVariables, aliasesCountX,
                                   validJoinWhenGroupingByX};
    ResultTable result{qec->getAllocator()};
    ASSERT_FALSE(
        invalidForOptimization.computeGroupByForJoinWithFullScan(&result));
    // No optimization was applied, so the result is untouched.
    AD_CONTRACT_CHECK(result._idTable.size() == 0);

    // The child of the GROUP BY is not a join, so this is also
    // invalid.
    GroupBy invalidGroupBy2{qec, variablesOnlyX, emptyAliases, xScan};
    ASSERT_FALSE(invalidGroupBy2.computeGroupByForJoinWithFullScan(&result));
    AD_CONTRACT_CHECK(result._idTable.size() == 0);
    ;
  }

  // `chooseInterface == true` means "use the dedicated
  // `computeGroupByForJoinWithFullScan` method", `chooseInterface == false`
  // means use the general `computeOptimizedGroupByIfPossible` function.
  auto testWithBothInterfaces = [&](bool chooseInterface) {
    // Set up a `VALUES` clause with three values for `?x`, two of which (`<x>`
    // and `<y>`) actually appear in the test knowledge graph.
    parsedQuery::SparqlValues sparqlValues;
    sparqlValues._variables.push_back(varX);
    sparqlValues._values.emplace_back(std::vector{TripleComponent{"<x>"}});
    sparqlValues._values.emplace_back(std::vector{TripleComponent{"<xa>"}});
    sparqlValues._values.emplace_back(std::vector{TripleComponent{"<y>"}});
    auto values = makeExecutionTree<Values>(qec, sparqlValues);
    // Set up a GROUP BY operation for which the optimization can be applied.
    // The last two arguments of the `Join` constructor are the indices of the
    // join columns.
    ResultTable result(qec->getAllocator());
    auto join = makeExecutionTree<Join>(qec, values, xyzScanSortedByX, 0, 0);
    GroupBy validForOptimization{qec, variablesOnlyX, aliasesCountX, join};
    if (chooseInterface) {
      ASSERT_TRUE(
          validForOptimization.computeGroupByForJoinWithFullScan(&result));
    } else {
      ASSERT_TRUE(
          validForOptimization.computeOptimizedGroupByIfPossible(&result));
    }

    // There are 5 triples with `<x>` as a subject, 0 triples with `<xa>` as a
    // subject, and 1 triple with `y` as a subject.
    const auto& table = result._idTable;
    ASSERT_EQ(table.numColumns(), 2u);
    ASSERT_EQ(table.size(), 2u);
    Id idOfX;
    Id idOfY;
    qec->getIndex().getId("<x>", &idOfX);
    qec->getIndex().getId("<y>", &idOfY);

    ASSERT_EQ(table(0, 0), idOfX);
    ASSERT_EQ(table(0, 1), Id::makeFromInt(5));
    ASSERT_EQ(table(1, 0), idOfY);
    ASSERT_EQ(table(1, 1), Id::makeFromInt(1));
  };
  testWithBothInterfaces(true);
  testWithBothInterfaces(false);

  // Test the case that the input is empty.
  {
    auto join =
        makeExecutionTree<Join>(qec, xScanEmptyResult, xyzScanSortedByX, 0, 0);
    ResultTable result{qec->getAllocator()};
    GroupBy groupBy{qec, variablesOnlyX, aliasesCountX, join};
    ASSERT_TRUE(groupBy.computeGroupByForJoinWithFullScan(&result));
    ASSERT_EQ(result._idTable.numColumns(), 2u);
    ASSERT_EQ(result._idTable.size(), 0u);
  }
}

TEST_F(GroupBySpecialCount, computeGroupByForSingleIndexScan) {
  // Assert that a GROUP BY, that is constructed from the given arguments,
  // can not perform the `OptimizedAggregateOnIndexScanChild` optimization.
  auto testFailure = [this](const auto& groupByVariables, const auto& aliases,
                            const auto& indexScan) {
    auto groupBy = GroupBy{qec, groupByVariables, aliases, indexScan};
    ResultTable result{qec->getAllocator()};
    ASSERT_FALSE(groupBy.computeGroupByForSingleIndexScan(&result));
    ASSERT_EQ(result._idTable.size(), 0u);
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
    ResultTable result{qec->getAllocator()};
    auto groupBy =
        GroupBy{qec, emptyVariables, aliasesCountX, xyzScanSortedByX};
    if (chooseInterface) {
      ASSERT_TRUE(groupBy.computeGroupByForSingleIndexScan(&result));
    } else {
      ASSERT_TRUE(groupBy.computeOptimizedGroupByIfPossible(&result));
    }

    ASSERT_EQ(result._idTable.size(), 1);
    ASSERT_EQ(result._idTable.numColumns(), 1);
    // The test index currently consists of 7 triples.
    ASSERT_EQ(result._idTable(0, 0), Id::makeFromInt(7));
  };
  testWithBothInterfaces(true);
  testWithBothInterfaces(false);

  {
    ResultTable result{qec->getAllocator()};
    auto groupBy = GroupBy{qec, emptyVariables, aliasesCountX, xyScan};
    ASSERT_TRUE(groupBy.computeGroupByForSingleIndexScan(&result));
    ASSERT_EQ(result._idTable.size(), 1);
    ASSERT_EQ(result._idTable.numColumns(), 1);
    // The test index currently consists of 5 triples that have the predicate
    // `<label>`
    ASSERT_EQ(result._idTable(0, 0), Id::makeFromInt(5));
  }
  {
    ResultTable result{qec->getAllocator()};
    auto groupBy =
        GroupBy{qec, emptyVariables, aliasesCountDistinctX, xyzScanSortedByX};
    ASSERT_TRUE(groupBy.computeGroupByForSingleIndexScan(&result));
    ASSERT_EQ(result._idTable.size(), 1);
    ASSERT_EQ(result._idTable.numColumns(), 1);
    // The test index currently consists of three distinct subjects:
    // <x>, <y>, and <z>.
    ASSERT_EQ(result._idTable(0, 0), Id::makeFromInt(3));
  }
}

TEST_F(GroupBySpecialCount, computeGroupByForFullIndexScan) {
  // Assert that a GROUP BY which is constructed from the given arguments
  // can not perform the `GroupByForSingleIndexScan2` optimization.
  auto testFailure = [this](const auto& groupByVariables, const auto& aliases,
                            const auto& indexScan) {
    auto groupBy = GroupBy{qec, groupByVariables, aliases, indexScan};
    ResultTable result{qec->getAllocator()};
    ASSERT_FALSE(groupBy.computeGroupByForFullIndexScan(&result));
    ASSERT_EQ(result._idTable.size(), 0u);
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
    ResultTable result{qec->getAllocator()};
    auto groupBy =
        includeCount
            ? GroupBy{qec, variablesOnlyX, aliasesCountX, xyzScanSortedByX}
            : GroupBy{qec, variablesOnlyX, emptyAliases, xyzScanSortedByX};
    if (chooseInterface) {
      ASSERT_TRUE(groupBy.computeGroupByForFullIndexScan(&result));
    } else {
      ASSERT_TRUE(groupBy.computeOptimizedGroupByIfPossible(&result));
    }
    Id idOfX;
    Id idOfY;
    Id idOfZ;
    qec->getIndex().getId("<x>", &idOfX);
    qec->getIndex().getId("<y>", &idOfY);
    qec->getIndex().getId("<z>", &idOfZ);

    // Three distinct subjects.
    ASSERT_EQ(result._idTable.size(), 3);
    if (includeCount) {
      ASSERT_EQ(result._idTable.numColumns(), 2);
    } else {
      ASSERT_EQ(result._idTable.numColumns(), 1);
    }
    // The test index currently consists of 6 triples.
    EXPECT_EQ(result._idTable(0, 0), idOfX);
    EXPECT_EQ(result._idTable(1, 0), idOfY);
    EXPECT_EQ(result._idTable(2, 0), idOfZ);

    if (includeCount) {
      EXPECT_EQ(result._idTable(0, 1), Id::makeFromInt(5));
      EXPECT_EQ(result._idTable(1, 1), Id::makeFromInt(1));
      // TODO<joka921> This should be 1.
      // There is one triple added <z> @en@<label> "zz"@en which is
      // currently not filtered out.
      EXPECT_EQ(result._idTable(2, 1), Id::makeFromInt(2));
    }
  };
  testWithBothInterfaces(true, true);
  testWithBothInterfaces(true, false);
  testWithBothInterfaces(false, true);

  // TODO<joka921> Add a test with only one column
}

}  // namespace
