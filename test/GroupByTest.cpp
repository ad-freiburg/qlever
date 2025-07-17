// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//          Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include <absl/strings/str_join.h>
#include <gmock/gmock.h>

#include <cstdio>

#include "./util/GTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/RuntimeParametersTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/GroupBy.h"
#include "engine/GroupByImpl.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/QueryPlanner.h"
#include "engine/Sort.h"
#include "engine/SpatialJoinAlgorithms.h"
#include "engine/Values.h"
#include "engine/ValuesForTesting.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/CountStarExpression.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/SampleExpression.h"
#include "engine/sparqlExpressions/StdevExpression.h"
#include "global/RuntimeParameters.h"
#include "index/TextIndexBuilder.h"
#include "parser/SparqlParser.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

using namespace ad_utility::testing;
using ::testing::Eq;
using ::testing::Optional;

namespace {
auto I = IntId;
auto D = DoubleId;

// Return a matcher that checks, whether a given `std::optional<IdTable` has a
// value and that value is equal to `makeIdTableFromVector(table)`.
auto optionalHasTable = [](const VectorTable& table) {
  return Optional(matchesIdTableFromVector(table));
};

// _____________________________________________________________________________
auto lit(std::string_view s) {
  return ad_utility::triple_component::Literal::literalWithoutQuotes(s);
}

// Helper function to get the local vocab ID for a given word.
Id getLocalVocabIdFromVocab(const LocalVocab& localVocab,
                            const std::string& word) {
  auto lit =
      ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes(word);
  auto value = localVocab.getIndexOrNullopt(lit);
  if (value.has_value()) {
    return ValueId::makeFromLocalVocabIndex(value.value());
  }
  auto values = localVocab.getAllWordsForTesting();
  AD_THROW(absl::StrCat(
      "Local vocab does not contain: ", word, "\n existing words: ",
      absl::StrJoin(
          values | ql::views::transform([](const LocalVocabEntry& entry) {
            return entry.toStringRepresentation();
          }),
          ", ")));
};
}  // namespace

// This fixture is used to create an Index for the tests.
// The full index creation is required for initialization of the vocabularies.
class GroupByTest : public ::testing::Test {
 public:
  GroupByTest() {
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
    _index.createFromFiles(
        {{"group_by_test.nt", qlever::Filetype::Turtle, std::nullopt}});
    TextIndexBuilder textIndexBuilder{ad_utility::makeUnlimitedAllocator<Id>(),
                                      _index.getOnDiskBase()};
    textIndexBuilder.buildTextIndexFile(
        std::pair<std::string, std::string>{"group_by_test.words",
                                            "group_by_test.documents"},
        false);
    textIndexBuilder.buildDocsDB("group_by_test.documents");

    _index.addTextFromOnDiskIndex();
    _index.parserBufferSize() = 1_kB;
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

TEST_F(GroupByTest, getDescriptor) {
  auto expr =
      std::make_unique<sparqlExpression::VariableExpression>(Variable{"?a"});
  auto alias =
      Alias{sparqlExpression::SparqlExpressionPimpl{std::move(expr), "?a"},
            Variable{"?a"}};

  parsedQuery::SparqlValues input;
  input._variables = {Variable{"?a"}};
  auto values = ad_utility::makeExecutionTree<Values>(
      ad_utility::testing::getQec(), input);

  GroupBy groupBy{
      ad_utility::testing::getQec(), {Variable{"?a"}}, {alias}, values};
  ASSERT_EQ(groupBy.getDescriptor(), "GroupBy on ?a");
}

// _____________________________________________________________________________
TEST_F(GroupByTest, clone) {
  auto expr =
      std::make_unique<sparqlExpression::VariableExpression>(Variable{"?a"});
  auto alias =
      Alias{sparqlExpression::SparqlExpressionPimpl{std::move(expr), "?a"},
            Variable{"?a"}};

  parsedQuery::SparqlValues input;
  input._variables = {Variable{"?a"}};
  auto values = ad_utility::makeExecutionTree<Values>(getQec(), input);

  GroupBy groupBy{getQec(), {Variable{"?a"}}, {alias}, values};

  auto clone = groupBy.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(groupBy, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), groupBy.getDescriptor());
}

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
  auto filename = "groupByTestVocab.dat";
  vocab.createFromSet(s, filename);
  ad_utility::deleteFile(filename);

  // Create an input result table with a local vocabulary.
  auto localVocab = std::make_shared<LocalVocab>();
  constexpr auto iriref = [](const std::string& s) {
    return ad_utility::triple_component::LiteralOrIri::iriref(s);
  };
  localVocab->getIndexAndAddIfNotContained(iriref("<local1>"));
  localVocab->getIndexAndAddIfNotContained(iriref("<local2>"));
  localVocab->getIndexAndAddIfNotContained(iriref("<local3>"));

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

  Result outTable{allocator()};

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

  ASSERT_EQ(Id::makeUndefined(), outTable._data[0][10]);
  ASSERT_EQ(Id::makeUndefined(), outTable._data[1][10]);
  ASSERT_EQ(Id::makeUndefined(), outTable._data[2][10]);

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

  ASSERT_EQ(Id::makeUndefined(), outTable._data[0][14]);
  ASSERT_EQ(Id::makeUndefined(), outTable._data[1][14]);
  ASSERT_EQ(Id::makeUndefined(), outTable._data[2][14]);

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
      "<x> <label> \"alpha\" . "
      "<x> <label> \"älpha\" . "
      "<x> <label> \"A\" . "
      "<a> <is-a> <f> . "
      "<a> <is> 20 . "
      "<b> <is-a> <f> . "
      "<b> <is> 40.0 . "
      "<c> <is-a> <g> . "
      "<c> <is> 100 . "
      "<x> <is-a> <f> . "
      "<x> <is> \"A\" . "
      "<x> <label> \"Beta\" . "
      "<x> <is-a> <y> . "
      "<y> <is-a> <x> . "
      "<z> <label> \"zz\"@en .";

  QueryExecutionContext* qec = getQec(turtleInput);
  SparqlTripleSimple xyzTriple{Variable{"?x"}, Variable{"?y"}, Variable{"?z"}};
  Tree xyzScanSortedByX =
      makeExecutionTree<IndexScan>(qec, Permutation::Enum::SOP, xyzTriple);
  Tree xyzScanSortedByY =
      makeExecutionTree<IndexScan>(qec, Permutation::Enum::POS, xyzTriple);
  Tree xScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{iri("<x>"), iri("<label>"), Variable{"?x"}});
  Tree xyScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{Variable{"?x"}, iri("<label>"), Variable{"?y"}});
  Tree yxScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::POS,
      SparqlTripleSimple{Variable{"?x"}, iri("<label>"), Variable{"?y"}});
  Tree xScanIriNotInVocab = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{{iri("<x>")}, iri("<notInVocab>"), Variable{"?x"}});
  Tree xyScanIriNotInVocab = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{Variable{"?x"}, iri("<notInVocab>"), Variable{"?y"}});

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

  static SparqlExpressionPimpl makeMaxPimpl(const Variable& var) {
    return SparqlExpressionPimpl{
        std::make_unique<MaxExpression>(false, makeVariableExpression(var)),
        "MAX(?someVariable)"};
  }

  static SparqlExpressionPimpl makeSumPimpl(const Variable& var) {
    return SparqlExpressionPimpl{
        std::make_unique<SumExpression>(false, makeVariableExpression(var)),
        "SUM(?someVariable)"};
  }

  static SparqlExpressionPimpl makeGroupConcatPimpl(
      const Variable& var, const std::string& separator = " ") {
    return SparqlExpressionPimpl{
        std::make_unique<GroupConcatExpression>(
            false, makeVariableExpression(var), separator),
        "GROUP_CONCAT(?someVariable)"};
  }

  static SparqlExpressionPimpl makeSamplePimpl(const Variable& var) {
    return SparqlExpressionPimpl{
        std::make_unique<SampleExpression>(false, makeVariableExpression(var)),
        "SAMPLE(?someVariable)"};
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
  SparqlExpressionPimpl countYPimpl = makeCountPimpl(varY, false);
  SparqlExpressionPimpl countNotExistingPimpl =
      makeCountPimpl(Variable{"?notExistingVar"}, false);
  SparqlExpressionPimpl countDistinctXPimpl = makeCountPimpl(varX, true);
  std::vector<Alias> aliasesXAsV{Alias{varxExpressionPimpl, Variable{"?v"}}};
  std::vector<Alias> aliasesCountDistinctX{
      Alias{countDistinctXPimpl, Variable{"?count"}}};
  std::vector<Alias> aliasesCountX{Alias{countXPimpl, Variable{"?count"}}};
  std::vector<Alias> aliasesCountY{Alias{countYPimpl, Variable{"?count"}}};
  std::vector<Alias> aliasesCountNotExisting{
      Alias{countNotExistingPimpl, Variable{"?count"}}};

  std::vector<Alias> aliasesCountXTwice{
      Alias{makeCountPimpl(varX, false), Variable{"?count"}},
      Alias{makeCountPimpl(varX, false), Variable{"?count2"}}};

  const Join& getOperation(const Tree& tree) {
    auto join = std::dynamic_pointer_cast<const Join>(tree->getRootOperation());
    AD_CONTRACT_CHECK(join);
    return *join;
  }
};

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, getPermutationForThreeVariableTriple) {
  using enum Permutation::Enum;
  const QueryExecutionTree& xyzScan = *xyzScanSortedByX;

  // Valid inputs.
  ASSERT_EQ(SPO, GroupByImpl::getPermutationForThreeVariableTriple(xyzScan,
                                                                   varX, varX));
  ASSERT_EQ(POS, GroupByImpl::getPermutationForThreeVariableTriple(xyzScan,
                                                                   varY, varZ));
  ASSERT_EQ(OSP, GroupByImpl::getPermutationForThreeVariableTriple(xyzScan,
                                                                   varZ, varY));

  // First variable not contained in triple.
  AD_EXPECT_NULLOPT(
      GroupByImpl::getPermutationForThreeVariableTriple(xyzScan, varA, varX));

  // Second variable not contained in triple.
  AD_EXPECT_NULLOPT(
      GroupByImpl::getPermutationForThreeVariableTriple(xyzScan, varX, varA));

  // Not a three variable triple.
  AD_EXPECT_NULLOPT(
      GroupByImpl::getPermutationForThreeVariableTriple(*xScan, varX, varX));
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
      GroupByImpl::findAggregates(twoTimesAvgY_times_avgFourTimesYExpr.get());
  ASSERT_TRUE(foundAggregates.has_value());
  auto value = foundAggregates.value();
  ASSERT_EQ(value.size(), 2);
  ASSERT_EQ(value.at(0).parentAndIndex_.value().nThChild_, 1);
  ASSERT_EQ(value.at(1).parentAndIndex_.value().nThChild_, 1);
  ASSERT_EQ(value.at(0).parentAndIndex_.value().parent_,
            twoTimesAvgY_times_avgFourTimesYExpr->children()[0].get());
  ASSERT_EQ(value.at(1).parentAndIndex_.value().parent_,
            twoTimesAvgY_times_avgFourTimesYExpr.get());
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
  GroupByImpl groupBy{
      ad_utility::testing::getQec(), {Variable{"?a"}}, {}, values};

  auto variableAtTop = groupBy.findGroupedVariable(expr1.get(), Variable{"?a"});
  ASSERT_TRUE(std::get_if<GroupByImpl::OccurAsRoot>(&variableAtTop));

  auto variableInExpression =
      groupBy.findGroupedVariable(expr2.get(), Variable{"?a"});
  auto variableInExpressionOccurrences =
      std::get_if<std::vector<GroupByImpl::ParentAndChildIndex>>(
          &variableInExpression);
  ASSERT_TRUE(variableInExpressionOccurrences);
  ASSERT_EQ(variableInExpressionOccurrences->size(), 1);
  auto parentAndChildIndex = variableInExpressionOccurrences->at(0);
  ASSERT_EQ(parentAndChildIndex.nThChild_, 0);
  ASSERT_EQ(parentAndChildIndex.parent_, expr2.get());

  auto variableNotFound =
      groupBy.findGroupedVariable(expr3.get(), Variable{"?a"});
  auto variableNotFoundOccurrences =
      std::get_if<std::vector<GroupByImpl::ParentAndChildIndex>>(
          &variableNotFound);
  ASSERT_EQ(variableNotFoundOccurrences->size(), 0);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, checkIfHashMapOptimizationPossible) {
  auto testFailure = [this](const auto& groupByVariables, const auto& aliases,
                            const auto& join, auto& aggregates) {
    auto groupBy = GroupByImpl{qec, groupByVariables, aliases, join};
    ASSERT_EQ(std::nullopt,
              groupBy.checkIfHashMapOptimizationPossible(aggregates));
  };

  auto testSuccess = [this](const auto& groupByVariables, const auto& aliases,
                            const auto& join, auto& aggregates) {
    auto groupBy = GroupByImpl{qec, groupByVariables, aliases, join};
    auto optimizedAggregateData =
        groupBy.checkIfHashMapOptimizationPossible(aggregates);
    ASSERT_TRUE(optimizedAggregateData.has_value());
  };

  std::vector<Variable> variablesXAndY{varX, varY};

  std::vector<ColumnIndex> sortedColumns = {0};
  Tree subtreeWithSort =
      makeExecutionTree<Sort>(qec, validJoinWhenGroupingByX, sortedColumns);

  SparqlExpressionPimpl avgXPimpl = makeAvgPimpl(varX);
  SparqlExpressionPimpl avgDistinctXPimpl = makeAvgPimpl(varX, true);
  SparqlExpressionPimpl avgCountXPimpl = makeAvgCountPimpl(varX);
  SparqlExpressionPimpl minXPimpl = makeMinPimpl(varX);
  SparqlExpressionPimpl maxXPimpl = makeMaxPimpl(varX);
  SparqlExpressionPimpl sumXPimpl = makeSumPimpl(varX);
  SparqlExpressionPimpl sampleXPimpl = makeSamplePimpl(varX);

  std::vector<Alias> aliasesAvgX{Alias{avgXPimpl, Variable{"?avg"}}};
  std::vector<Alias> aliasesAvgDistinctX{
      Alias{avgDistinctXPimpl, Variable{"?avgDistinct"}}};
  std::vector<Alias> aliasesAvgCountX{
      Alias{avgCountXPimpl, Variable("?avgcount")}};
  std::vector<Alias> aliasesMinX{Alias{minXPimpl, Variable{"?minX"}}};
  std::vector<Alias> aliasesMaxX{Alias{maxXPimpl, Variable{"?maxX"}}};
  std::vector<Alias> aliasesSumX{Alias{sumXPimpl, Variable{"?sumX"}}};
  std::vector<Alias> aliasesSampleX{Alias{sampleXPimpl, Variable{"?sampleX"}}};

  std::vector<GroupByImpl::Aggregate> countAggregate = {{countXPimpl, 1}};
  std::vector<GroupByImpl::Aggregate> avgAggregate = {{avgXPimpl, 1}};
  std::vector<GroupByImpl::Aggregate> avgDistinctAggregate = {
      {avgDistinctXPimpl, 1}};
  std::vector<GroupByImpl::Aggregate> avgCountAggregate = {{avgCountXPimpl, 1}};
  std::vector<GroupByImpl::Aggregate> minAggregate = {{minXPimpl, 1}};
  std::vector<GroupByImpl::Aggregate> maxAggregate = {{maxXPimpl, 1}};
  std::vector<GroupByImpl::Aggregate> sumAggregate = {{sumXPimpl, 1}};
  std::vector<GroupByImpl::Aggregate> sampleAggregate = {{sampleXPimpl, 1}};

  // Enable optimization
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);

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
  RuntimeParameters().set<"group-by-hash-map-enabled">(false);
  testFailure(variablesOnlyX, aliasesAvgX, subtreeWithSort, avgAggregate);

  // Support for MIN & MAX & SUM
  RuntimeParameters().set<"group-by-hash-map-enabled">(true);
  testSuccess(variablesOnlyX, aliasesMaxX, subtreeWithSort, maxAggregate);
  testSuccess(variablesOnlyX, aliasesMinX, subtreeWithSort, minAggregate);
  testSuccess(variablesOnlyX, aliasesSumX, subtreeWithSort, sumAggregate);
  testSuccess(variablesOnlyX, aliasesSampleX, subtreeWithSort, sampleAggregate);

  // Check details of data structure are correct.
  GroupByImpl groupBy{qec, variablesOnlyX, aliasesAvgX, subtreeWithSort};
  auto optimizedAggregateData =
      groupBy.checkIfHashMapOptimizationPossible(avgAggregate);
  ASSERT_TRUE(optimizedAggregateData.has_value());
  // Check aggregate alias is correct
  auto aggregateAlias = optimizedAggregateData->aggregateAliases_[0];
  ASSERT_EQ(aggregateAlias.expr_.getPimpl(), avgXPimpl.getPimpl());
  // Check aggregate info is correct
  auto aggregateInfo = aggregateAlias.aggregateInfo_[0];
  ASSERT_EQ(aggregateInfo.aggregateDataIndex_, 0);
  ASSERT_FALSE(aggregateInfo.parentAndIndex_.has_value());
  ASSERT_EQ(aggregateInfo.expr_, avgXPimpl.getPimpl());
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
      SparqlTripleSimple{Variable{"?z"}, iri("<is-a>"), Variable{"?x"}});
  Tree zyScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{Variable{"?z"}, iri("<is>"), Variable{"?y"}});
  Tree join = makeExecutionTree<Join>(qec, zxScan, zyScan, 0, 0);
  std::vector<ColumnIndex> sortedColumns = {1};

  SparqlExpressionPimpl avgYPimpl = makeAvgPimpl(varY);
  std::vector<Alias> aliasesAvgY{Alias{avgYPimpl, Variable{"?avg"}}};

  // Calculate result with optimization
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);
  GroupBy groupByWithOptimization{qec, variablesOnlyX, aliasesAvgY, join};
  auto resultWithOptimization = groupByWithOptimization.getResult();

  // Clear cache, calculate result without optimization
  qec->clearCacheUnpinnedOnly();
  RuntimeParameters().set<"group-by-hash-map-enabled">(false);
  GroupBy groupByWithoutOptimization{qec, variablesOnlyX, aliasesAvgY, join};
  auto resultWithoutOptimization = groupByWithoutOptimization.getResult();

  // Compare results, using debugString as the result only contains 2 rows
  ASSERT_EQ(resultWithOptimization->asDebugString(),
            resultWithoutOptimization->asDebugString());
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, hashMapOptimizationLazyAndMaterializedInputs) {
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);
  /* Setup query:
  SELECT ?x (AVG(?y) as ?avg) WHERE {
    # explicitly defined subresult.
  } GROUP BY ?x
 */
  // Setup three unsorted input blocks. The first column will be the grouped
  // `?x`, and the second column the variable `?y` of which we compute the
  // average.
  auto runTest = [this](bool inputIsLazy) {
    std::vector<IdTable> tables;
    tables.push_back(makeIdTableFromVector({{3, 6}, {8, 27}, {5, 7}}, I));
    tables.push_back(makeIdTableFromVector({{8, 27}, {5, 9}}, I));
    tables.push_back(makeIdTableFromVector({{5, 2}, {3, 4}}, I));
    // The expected averages are as follows: (3 -> 5.0), (5 -> 6.0), (8
    // -> 27.0).
    auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(tables),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}});
    auto& values =
        dynamic_cast<ValuesForTesting&>(*subtree->getRootOperation());
    values.forceFullyMaterialized() = !inputIsLazy;

    SparqlExpressionPimpl avgYPimpl = makeAvgPimpl(varY);
    std::vector<Alias> aliasesAvgY{Alias{avgYPimpl, Variable{"?avg"}}};

    // Calculate result with optimization
    qec->getQueryTreeCache().clearAll();
    GroupBy groupBy{qec, variablesOnlyX, aliasesAvgY, std::move(subtree)};
    auto result = groupBy.computeResultOnlyForTesting();
    ASSERT_TRUE(result.isFullyMaterialized());
    EXPECT_THAT(
        result.idTable(),
        matchesIdTableFromVector({{I(3), D(5)}, {I(5), D(6)}, {I(8), D(27)}}));
  };
  runTest(true);
  runTest(false);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, correctResultForHashMapOptimizationForCountStar) {
  /* Setup query:
  SELECT ?x (COUNT(*) as ?c) WHERE {
    ?z <is-a> ?x .
    ?z <is> ?y
  } GROUP BY ?x
 */
  Tree zxScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{Variable{"?z"}, iri("<is-a>"), Variable{"?x"}});
  Tree zyScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{Variable{"?z"}, iri("<is>"), Variable{"?y"}});
  Tree join = makeExecutionTree<Join>(qec, zxScan, zyScan, 0, 0);
  std::vector<ColumnIndex> sortedColumns = {1};

  SparqlExpressionPimpl countStarPimpl{makeCountStarExpression(false),
                                       "COUNT(*) as ?c"};
  std::vector<Alias> aliasesCountStar{Alias{countStarPimpl, Variable{"?c"}}};

  // Calculate result with optimization
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);
  GroupBy groupByWithOptimization{qec, variablesOnlyX, aliasesCountStar, join};
  auto resultWithOptimization = groupByWithOptimization.getResult();

  // Clear cache, calculate result without optimization
  qec->clearCacheUnpinnedOnly();
  RuntimeParameters().set<"group-by-hash-map-enabled">(false);
  GroupBy groupByWithoutOptimization{qec, variablesOnlyX, aliasesCountStar,
                                     join};
  auto resultWithoutOptimization = groupByWithoutOptimization.getResult();

  // Compare results, using debugString as the result only contains 2 rows
  ASSERT_EQ(resultWithOptimization->asDebugString(),
            resultWithoutOptimization->asDebugString());
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations,
       correctResultForHashMapOptimizationMultipleVariablesInExpression) {
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);

  parsedQuery::SparqlValues input;
  using TC = TripleComponent;

  // SELECT (?b + AVG(?c) as ?x) (?a AS ?y) WHERE {
  //   VALUES (?a ?b ?c) { (1.0 2.0 3.0) (1.0 2.0 4.0) (2.0 2.0 5.0)}
  // } GROUP BY ?a ?b
  Variable varA = Variable{"?a"};
  Variable varB = Variable{"?b"};
  Variable varC = Variable{"?c"};
  Variable varY = Variable{"?y"};

  input._variables = std::vector{varA, varB, varC};
  input._values.push_back(std::vector{TC(1.0), TC(2.0), TC(3.0)});
  input._values.push_back(std::vector{TC(1.0), TC(2.0), TC(4.0)});
  input._values.push_back(std::vector{TC(2.0), TC(2.0), TC(5.0)});
  auto values = ad_utility::makeExecutionTree<Values>(
      ad_utility::testing::getQec(), input);

  using namespace sparqlExpression;

  // Create `Alias` object for `(?b + AVG(?c) as ?x)`.
  auto expr = makeAddExpression(
      makeVariableExpression(varB),
      std::make_unique<AvgExpression>(false, makeVariableExpression(varC)));
  auto alias =
      Alias{SparqlExpressionPimpl{std::move(expr), "AVG(?c)"}, Variable{"?x"}};

  // Create `Alias` object for (?a as ?y)
  auto alias2 = Alias{makeVariablePimpl(varA), Variable{"?y"}};

  // Set up and evaluate the GROUP BY clause.
  GroupBy groupBy{ad_utility::testing::getQec(),
                  {Variable{"?a"}, Variable{"?b"}},
                  {std::move(alias), std::move(alias2)},
                  std::move(values)};
  auto result = groupBy.getResult();
  const auto& table = result->idTable();

  // Check the result.
  auto d = DoubleId;
  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?a"}, {0, AlwaysDefined}},
      {Variable{"?b"}, {1, AlwaysDefined}},
      {Variable{"?x"}, {2, PossiblyUndefined}},
      {Variable{"?y"}, {3, PossiblyUndefined}}};
  EXPECT_THAT(groupBy.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
  auto expected = makeIdTableFromVector(
      {{d(1), d(2), d(5.5), d(1)}, {d(2), d(2), d(7.0), d(2.0)}});
  EXPECT_EQ(table, expected);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations,
       correctResultForHashMapOptimizationMultipleVariables) {
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);

  parsedQuery::SparqlValues input;
  using TC = TripleComponent;

  // SELECT (AVG(?c) as ?x) WHERE {
  //   VALUES (?a ?b ?c) { (1.0 2.0 3.0) (1.0 2.0 4.0) (2.0 2.0 5.0)}
  // } GROUP BY ?a ?b
  Variable varA = Variable{"?a"};
  Variable varB = Variable{"?b"};
  Variable varC = Variable{"?c"};

  input._variables = std::vector{varA, varB, varC};
  input._values.push_back(std::vector{TC(2.0), TC(2.0), TC(5.0)});
  input._values.push_back(std::vector{TC(1.0), TC(2.0), TC(3.0)});
  input._values.push_back(std::vector{TC(1.0), TC(2.0), TC(4.0)});
  input._values.push_back(std::vector{TC(4.0), TC(1.0), TC(42.0)});

  auto values = ad_utility::makeExecutionTree<Values>(
      ad_utility::testing::getQec(), input);

  using namespace sparqlExpression;

  // Create `Alias` object for `(AVG(?c) as ?x)`.
  auto expr =
      std::make_unique<AvgExpression>(false, makeVariableExpression(varC));
  auto alias =
      Alias{SparqlExpressionPimpl{std::move(expr), "AVG(?c)"}, Variable{"?x"}};

  // Set up and evaluate the GROUP BY clause.
  GroupBy groupBy{ad_utility::testing::getQec(),
                  {Variable{"?a"}, Variable{"?b"}},
                  {std::move(alias)},
                  std::move(values)};
  auto result = groupBy.getResult();
  const auto& table = result->idTable();

  // Check the result.
  auto d = DoubleId;
  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?a"}, {0, AlwaysDefined}},
      {Variable{"?b"}, {1, AlwaysDefined}},
      {Variable{"?x"}, {2, PossiblyUndefined}}};
  EXPECT_THAT(groupBy.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
  auto expected = makeIdTableFromVector(
      {{d(1), d(2), d(3.5)}, {d(2), d(2), d(5.0)}, {d(4), d(1), d(42.0)}});
  EXPECT_EQ(table, expected);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations,
       correctResultForHashMapOptimizationMultipleVariablesOutOfOrder) {
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);

  parsedQuery::SparqlValues input;
  using TC = TripleComponent;

  // SELECT (AVG(?b) as ?x) WHERE {
  //   VALUES (?a ?b ?c) { ... }
  // } GROUP BY ?a ?c
  Variable varA = Variable{"?a"};
  Variable varB = Variable{"?b"};
  Variable varC = Variable{"?c"};

  input._variables = std::vector{varA, varB, varC};
  input._values.push_back(std::vector{TC(2.0), TC(5.0), TC(2.0)});
  input._values.push_back(std::vector{TC(1.0), TC(3.0), TC(2.0)});
  input._values.push_back(std::vector{TC(1.0), TC(4.0), TC(2.0)});
  input._values.push_back(std::vector{TC(4.0), TC(42.0), TC(1.0)});

  auto values = ad_utility::makeExecutionTree<Values>(
      ad_utility::testing::getQec(), input);

  using namespace sparqlExpression;

  // Create `Alias` object for `(AVG(?b) as ?x)`.
  auto expr =
      std::make_unique<AvgExpression>(false, makeVariableExpression(varB));
  auto alias =
      Alias{SparqlExpressionPimpl{std::move(expr), "AVG(?b)"}, Variable{"?x"}};

  // Set up and evaluate the GROUP BY clause.
  GroupBy groupBy{ad_utility::testing::getQec(),
                  {Variable{"?a"}, Variable{"?c"}},
                  {std::move(alias)},
                  std::move(values)};
  auto result = groupBy.getResult();
  const auto& table = result->idTable();

  // Check the result.
  auto d = DoubleId;
  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?a"}, {0, AlwaysDefined}},
      {Variable{"?c"}, {1, AlwaysDefined}},
      {Variable{"?x"}, {2, PossiblyUndefined}}};
  EXPECT_THAT(groupBy.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
  auto expected = makeIdTableFromVector(
      {{d(1), d(2), d(3.5)}, {d(2), d(2), d(5.0)}, {d(4), d(1), d(42.0)}});
  EXPECT_EQ(table, expected);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, correctResultForHashMapOptimizationManyVariables) {
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);

  parsedQuery::SparqlValues input;
  using TC = TripleComponent;

  // SELECT (AVG(?g) as ?x) WHERE {
  //   VALUES (?a ?b ?c ?d ?e ?f ?g) { ... }
  // } GROUP BY ?a ?b ?c ?d ?e ?f
  Variable varA = Variable{"?a"};
  Variable varB = Variable{"?b"};
  Variable varC = Variable{"?c"};
  Variable varD = Variable{"?d"};
  Variable varE = Variable{"?e"};
  Variable varF = Variable{"?f"};
  Variable varG = Variable{"?g"};

  input._variables = std::vector{varA, varB, varC, varD, varE, varF, varG};
  input._values.push_back(std::vector{TC(2.0), TC(2.0), TC(2.0), TC(2.0),
                                      TC(2.0), TC(5.0), TC(5.0)});
  input._values.push_back(std::vector{TC(1.0), TC(2.0), TC(2.0), TC(2.0),
                                      TC(2.0), TC(5.0), TC(5.0)});
  input._values.push_back(std::vector{TC(1.0), TC(2.0), TC(2.0), TC(2.0),
                                      TC(2.0), TC(5.0), TC(3.0)});
  input._values.push_back(std::vector{TC(4.0), TC(1.0), TC(2.0), TC(2.0),
                                      TC(2.0), TC(5.0), TC(2.0)});

  auto values = ad_utility::makeExecutionTree<Values>(
      ad_utility::testing::getQec(), input);

  using namespace sparqlExpression;

  // Create `Alias` object for `(AVG(?c) as ?x)`.
  auto expr =
      std::make_unique<AvgExpression>(false, makeVariableExpression(varG));
  auto alias =
      Alias{SparqlExpressionPimpl{std::move(expr), "AVG(?g)"}, Variable{"?x"}};

  // Set up and evaluate the GROUP BY clause.
  GroupBy groupBy{ad_utility::testing::getQec(),
                  {Variable{"?a"}, Variable{"?b"}, Variable{"?c"},
                   Variable{"?d"}, Variable{"?e"}, Variable{"?f"}},
                  {std::move(alias)},
                  std::move(values)};
  auto result = groupBy.getResult();
  const auto& table = result->idTable();

  // Check the result.
  auto d = DoubleId;
  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?a"}, {0, AlwaysDefined}},
      {Variable{"?b"}, {1, AlwaysDefined}},
      {Variable{"?c"}, {2, AlwaysDefined}},
      {Variable{"?d"}, {3, AlwaysDefined}},
      {Variable{"?e"}, {4, AlwaysDefined}},
      {Variable{"?f"}, {5, AlwaysDefined}},
      {Variable{"?x"}, {6, PossiblyUndefined}}};
  EXPECT_THAT(groupBy.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
  auto expected =
      makeIdTableFromVector({{d(1), d(2), d(2), d(2), d(2), d(5), d(4)},
                             {d(2), d(2), d(2), d(2), d(2), d(5), d(5)},
                             {d(4), d(1), d(2), d(2), d(2), d(5), d(2)}});
  EXPECT_EQ(table, expected);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, hashMapOptimizationGroupedVariable) {
  // Make sure we are calculating the correct result when a grouped variable
  // occurs in an expression.
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);

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
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, hashMapOptimizationMinMaxSum) {
  // Test for support of min, max and sum when using the HashMap optimization.
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);

  parsedQuery::SparqlValues input;
  using TC = TripleComponent;

  // SELECT (MIN(?b) as ?x) (MAX(?b) as ?z) (SUM(?b) as ?w) WHERE {
  //   VALUES (?a ?b) { (1.0 3.0) (1.0 7.0) (5.0 4.0)}
  // } GROUP BY ?a
  Variable varA = Variable{"?a"};
  Variable varX = Variable{"?x"};
  Variable varB = Variable{"?b"};
  Variable varZ = Variable{"?z"};
  Variable varW = Variable{"?w"};

  input._variables = std::vector{varA, varB};
  input._values.push_back(std::vector{TC(1.0), TC(42)});
  input._values.push_back(std::vector{TC(1.0), TC(9.0)});
  input._values.push_back(std::vector{TC(1.0), TC(3)});
  input._values.push_back(std::vector{TC(3.0), TC(13.37)});
  input._values.push_back(std::vector{TC(3.0), TC(1.0)});
  input._values.push_back(std::vector{TC(3.0), TC(4.0)});
  input._values.push_back(std::vector<TripleComponent>{TC(4.0), TC::UNDEF{}});
  auto qec = ad_utility::testing::getQec();
  auto values = ad_utility::makeExecutionTree<Values>(qec, input);

  using namespace sparqlExpression;

  // Create `Alias` object for `(MIN(?b) as ?x)`.
  auto expr1 =
      std::make_unique<MinExpression>(false, makeVariableExpression(varB));
  auto alias1 =
      Alias{SparqlExpressionPimpl{std::move(expr1), "MIN(?b)"}, Variable{"?x"}};

  // Create `Alias` object for `(MAX(?b) as ?z)`.
  auto expr2 =
      std::make_unique<MaxExpression>(false, makeVariableExpression(varB));
  auto alias2 =
      Alias{SparqlExpressionPimpl{std::move(expr2), "MAX(?b)"}, Variable{"?z"}};

  // Create `Alias` object for `(SUM(?b) as ?w)`.
  auto expr3 =
      std::make_unique<SumExpression>(false, makeVariableExpression(varB));
  auto alias3 =
      Alias{SparqlExpressionPimpl{std::move(expr3), "SUM(?b)"}, Variable{"?w"}};

  // Set up and evaluate the GROUP BY clause.
  GroupBy groupBy{ad_utility::testing::getQec(),
                  {Variable{"?a"}},
                  {std::move(alias1), std::move(alias2), std::move(alias3)},
                  std::move(values)};
  auto result = groupBy.getResult();
  const auto& table = result->idTable();

  // Check the result.
  auto d = DoubleId;
  auto i = IntId;
  auto undef = ValueId::makeUndefined();
  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?a"}, {0, AlwaysDefined}},
      {Variable{"?x"}, {1, PossiblyUndefined}},
      {Variable{"?z"}, {2, PossiblyUndefined}},
      {Variable{"?w"}, {3, PossiblyUndefined}}};
  EXPECT_THAT(groupBy.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
  auto expected = makeIdTableFromVector({{d(1), i(3), i(42), d(54)},
                                         {d(3), d(1), d(13.37), d(18.37)},
                                         {d(4), undef, undef, undef}});
  EXPECT_EQ(table, expected);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, hashMapOptimizationMinMaxSumIntegers) {
  // Test for support of min, max and sum when using the HashMap optimization.
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);

  // SELECT (MIN(?b) as ?x) (MAX(?b) as ?z) (SUM(?b) as ?w) WHERE {
  //   VALUES (?a ?b) { (1.0 3.0) (1.0 7.0) (5.0 4.0)}
  // } GROUP BY ?a
  Variable varA = Variable{"?a"};
  Variable varX = Variable{"?x"};
  Variable varB = Variable{"?b"};
  Variable varZ = Variable{"?z"};
  Variable varW = Variable{"?w"};

  auto qec = ad_utility::testing::getQec();
  IdTable testTable{qec->getAllocator()};
  testTable.setNumColumns(2);
  testTable.resize(6);
  std::vector<unsigned long> firstColumn{1, 1, 1, 3, 3, 3};
  std::vector<unsigned long> secondColumn{42, 9, 3, 13, 1, 4};
  std::vector<std::optional<Variable>> variables = {Variable{"?a"},
                                                    Variable{"?b"}};

  auto firstTableColumn = testTable.getColumn(0);
  auto secondTableColumn = testTable.getColumn(1);

  auto unsignedLongToValueId = [](unsigned long value) {
    return ValueId::makeFromInt(static_cast<int64_t>(value));
  };
  ql::ranges::transform(firstColumn.begin(), firstColumn.end(),
                        firstTableColumn.begin(), unsignedLongToValueId);
  ql::ranges::transform(secondColumn.begin(), secondColumn.end(),
                        secondTableColumn.begin(), unsignedLongToValueId);

  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(testTable), variables, false);

  using namespace sparqlExpression;

  // Create `Alias` object for `(MIN(?b) as ?x)`.
  auto expr1 =
      std::make_unique<MinExpression>(false, makeVariableExpression(varB));
  auto alias1 =
      Alias{SparqlExpressionPimpl{std::move(expr1), "MIN(?b)"}, Variable{"?x"}};

  // Create `Alias` object for `(MAX(?b) as ?z)`.
  auto expr2 =
      std::make_unique<MaxExpression>(false, makeVariableExpression(varB));
  auto alias2 =
      Alias{SparqlExpressionPimpl{std::move(expr2), "MAX(?b)"}, Variable{"?z"}};

  // Create `Alias` object for `(SUM(?b) as ?w)`.
  auto expr3 =
      std::make_unique<SumExpression>(false, makeVariableExpression(varB));
  auto alias3 =
      Alias{SparqlExpressionPimpl{std::move(expr3), "SUM(?b)"}, Variable{"?w"}};

  // Set up and evaluate the GROUP BY clause.
  GroupBy groupBy{ad_utility::testing::getQec(),
                  {Variable{"?a"}},
                  {std::move(alias1), std::move(alias2), std::move(alias3)},
                  std::move(values)};
  auto result = groupBy.getResult();
  const auto& table = result->idTable();

  // Check the result.
  auto i = IntId;
  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?a"}, {0, AlwaysDefined}},
      {Variable{"?x"}, {1, PossiblyUndefined}},
      {Variable{"?z"}, {2, PossiblyUndefined}},
      {Variable{"?w"}, {3, PossiblyUndefined}}};
  EXPECT_THAT(groupBy.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
  auto expected = makeIdTableFromVector(
      {{i(1), i(3), i(42), i(54)}, {i(3), i(1), i(13), i(18)}});
  EXPECT_EQ(table, expected);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, hashMapOptimizationGroupConcatIndex) {
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);

  std::string turtleInput =
      "<x> <label> \"C\" . <x> <label> \"B\" . <x> <label> \"A\" . "
      "<y> <label> \"g\" . <y> <label> \"f\" . <y> <label> \"h\"";

  QueryExecutionContext* qec = getQec(turtleInput);

  Tree xyScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{varX, iri("<label>"), varY});

  // Optimization will not be used if subtree is not sort
  std::vector<ColumnIndex> sortedColumns = {0};
  Tree subtreeWithSort = makeExecutionTree<Sort>(qec, xyScan, sortedColumns);

  auto groupConcatExpression1 = makeGroupConcatPimpl(varY);
  auto aliasGC1 = Alias{groupConcatExpression1, varZ};

  auto varW = Variable{"?w"};
  auto groupConcatExpression2 = makeGroupConcatPimpl(varY, ",");
  auto aliasGC2 = Alias{groupConcatExpression2, varW};

  // SELECT (GROUP_CONCAT(?y) as ?z) (GROUP_CONCAT(?y;separator=",") as ?w)
  // WHERE {...} GROUP BY ?x
  GroupBy groupBy{qec, variablesOnlyX, {aliasGC1, aliasGC2}, subtreeWithSort};
  auto result = groupBy.getResult();
  const auto& table = result->idTable();

  auto getId = makeGetId(qec->getIndex());
  auto getLocalVocabId = [&result](const std::string& word) {
    return getLocalVocabIdFromVocab(result->localVocab(), word);
  };

  auto expected = makeIdTableFromVector(
      {{getId("<x>"), getLocalVocabId("A B C"), getLocalVocabId("A,B,C")},
       {getId("<y>"), getLocalVocabId("f g h"), getLocalVocabId("f,g,h")}});
  EXPECT_EQ(table, expected);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, hashMapOptimizationGroupConcatLocalVocab) {
  // Test for support of min, max and sum when using the HashMap optimization.
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);

  parsedQuery::SparqlValues input;
  using TC = TripleComponent;

  input._variables = std::vector{varX, varY};
  input._values.push_back(std::vector{TC(1.0), TC{lit("B")}});
  input._values.push_back(std::vector{TC(1.0), TC{lit("A")}});
  input._values.push_back(std::vector{TC(1.0), TC{lit("C")}});
  input._values.push_back(std::vector{TC(3.0), TC{lit("g")}});
  input._values.push_back(std::vector{TC(3.0), TC{lit("h")}});
  input._values.push_back(std::vector{TC(3.0), TC{lit("f")}});
  auto qec = ad_utility::testing::getQec();
  auto values = ad_utility::makeExecutionTree<Values>(qec, input);

  auto groupConcatExpression1 = makeGroupConcatPimpl(varY);
  auto aliasGC1 = Alias{groupConcatExpression1, varZ};

  auto varW = Variable{"?w"};
  auto groupConcatExpression2 = makeGroupConcatPimpl(varY, ",");
  auto aliasGC2 = Alias{groupConcatExpression2, varW};

  GroupBy groupBy{qec, variablesOnlyX, {aliasGC1, aliasGC2}, std::move(values)};
  auto result = groupBy.getResult();
  const auto& table = result->idTable();

  auto getId = makeGetId(qec->getIndex());
  auto d = DoubleId;
  auto getLocalVocabId = [&result](const std::string& word) {
    return getLocalVocabIdFromVocab(result->localVocab(), word);
  };

  auto expected = makeIdTableFromVector(
      {{d(1), getLocalVocabId("B A C"), getLocalVocabId("B,A,C")},
       {d(3), getLocalVocabId("g h f"), getLocalVocabId("g,h,f")}});
  EXPECT_EQ(table, expected);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, hashMapOptimizationMinMaxIndex) {
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(true);

  std::string turtleInput =
      "<x> <label> \"C\" . <x> <label> \"B\" . <x> <label> \"A\" . "
      "<y> <label> \"g\" . <y> <label> \"f\" . <y> <label> \"h\"";

  QueryExecutionContext* qec = getQec(turtleInput);

  Tree xyScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{varX, iri("<label>"), varY});

  // Optimization will not be used if subtree is not sort
  std::vector<ColumnIndex> sortedColumns = {0};
  Tree subtreeWithSort = makeExecutionTree<Sort>(qec, xyScan, sortedColumns);

  auto minExpression = makeMinPimpl(varY);
  auto aliasMin = Alias{minExpression, varZ};

  auto varW = Variable{"?w"};
  auto maxExpression = makeMaxPimpl(varY);
  auto aliasMax = Alias{maxExpression, varW};

  // SELECT (MIN(?y) as ?z) (MAX(?y) as ?w) WHERE {...} GROUP BY ?x
  GroupBy groupBy{qec, variablesOnlyX, {aliasMin, aliasMax}, subtreeWithSort};
  auto result = groupBy.getResult();
  const auto& table = result->idTable();

  auto getId = makeGetId(qec->getIndex());

  auto expected =
      makeIdTableFromVector({{getId("<x>"), getId("\"A\""), getId("\"C\"")},
                             {getId("<y>"), getId("\"f\""), getId("\"h\"")}});
  EXPECT_EQ(table, expected);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, hashMapOptimizationNonTrivial) {
  // Test to make sure that non-trivial nested expressions are supported.
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
      SparqlTripleSimple{Variable{"?z"}, iri("<is-a>"), Variable{"?x"}});
  Tree zyScan = makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{Variable{"?z"}, iri("<is>"), Variable{"?y"}});
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
  auto cleanup = setRuntimeParameterForTest<"group-by-hash-map-enabled">(false);
  GroupBy groupByWithoutOptimization{qec, variablesOnlyX, aliasesAvgY,
                                     sortedJoin};
  auto resultWithoutOptimization = groupByWithoutOptimization.getResult();

  // Calculate result with optimization, after calculating it without,
  // since optimization changes tree
  qec->clearCacheUnpinnedOnly();
  RuntimeParameters().set<"group-by-hash-map-enabled">(true);
  GroupBy groupByWithOptimization{qec, variablesOnlyX, aliasesAvgY, sortedJoin};
  auto resultWithOptimization = groupByWithOptimization.getResult();

  // Compare results, using debugString as the result only contains 2 rows
  ASSERT_EQ(resultWithOptimization->asDebugString(),
            resultWithoutOptimization->asDebugString());
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, checkIfJoinWithFullScan) {
  // Assert that a Group by, that is constructed from the given arguments,
  // can not perform the `OptimizedAggregateOnJoinChild` optimization.
  auto testFailure = [this](const auto& groupByVariables, const auto& aliases,
                            const auto& join) {
    auto groupBy = GroupByImpl{qec, groupByVariables, aliases, join};
    ASSERT_EQ(std::nullopt,
              groupBy.checkIfJoinWithFullScan(getOperation(join)));
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
  GroupByImpl groupBy{qec, variablesOnlyX, aliasesCountX,
                      validJoinWhenGroupingByX};
  auto optimizedAggregateData =
      groupBy.checkIfJoinWithFullScan(getOperation(validJoinWhenGroupingByX));
  ASSERT_TRUE(optimizedAggregateData.has_value());
  ASSERT_EQ(&optimizedAggregateData->otherSubtree_, xScan.get());
  ASSERT_EQ(optimizedAggregateData->permutation_, Permutation::SPO);
  ASSERT_EQ(optimizedAggregateData->subtreeColumnIndex_, 0);
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, computeGroupByForJoinWithFullScan) {
  {
    // One of the invalid cases from the previous test.
    GroupBy invalidForOptimizationPimpl{qec, emptyVariables, aliasesCountX,
                                        validJoinWhenGroupingByX};
    auto& invalidForOptimization = invalidForOptimizationPimpl.getImpl();
    ASSERT_EQ(std::nullopt,
              invalidForOptimization.computeGroupByForJoinWithFullScan());

    // The child of the GROUP BY is not a join, so this is also
    // invalid.
    GroupBy invalidGroupBy2Pimpl{qec, variablesOnlyX, emptyAliases, xScan};
    auto& invalidGroupBy2 = invalidGroupBy2Pimpl.getImpl();
    ASSERT_EQ(std::nullopt,
              invalidGroupBy2.computeGroupByForJoinWithFullScan());
  }

  // `chooseInterface == true` means "use the dedicated
  // `computeGroupByForJoinWithFullScan` method", `chooseInterface == false`
  // means use the general `computeOptimizedGroupByIfPossible` function.
  using ad_utility::source_location;
  auto testWithBothInterfaces = [&](bool chooseInterface,
                                    source_location l =
                                        source_location::current()) {
    auto trace = generateLocationTrace(l);
    auto getId = makeGetId(qec->getIndex());
    Id idOfX = getId("<x>");
    Id idOfY = getId("<y>");
    // Set up a `VALUES` clause with three values for `?x`, two of which
    // (`<x>` and `<y>`) actually appear in the test knowledge graph.
    parsedQuery::SparqlValues sparqlValues;
    sparqlValues._variables.push_back(varX);
    sparqlValues._values.emplace_back(std::vector{TripleComponent{iri("<x>")}});
    sparqlValues._values.emplace_back(
        std::vector{TripleComponent{iri("<xa>")}});
    sparqlValues._values.emplace_back(std::vector{TripleComponent{iri("<y>")}});
    auto values = makeExecutionTree<Values>(qec, sparqlValues);
    // Set up a GROUP BY operation for which the optimization can be applied.
    // The last two arguments of the `Join` constructor are the indices of the
    // join columns.
    auto join = makeExecutionTree<Join>(qec, values, xyzScanSortedByX, 0, 0);
    GroupByImpl validForOptimization{qec, variablesOnlyX, aliasesCountX, join};
    auto optional =
        chooseInterface
            ? validForOptimization.computeGroupByForJoinWithFullScan()
            : validForOptimization.computeOptimizedGroupByIfPossible();
    EXPECT_THAT(optional, optionalHasTable({{idOfX, I(7)}, {idOfY, I(1)}}));
  };
  testWithBothInterfaces(true);
  testWithBothInterfaces(false);

  // Test the case that the input is empty.
  {
    auto join = makeExecutionTree<Join>(qec, xScanIriNotInVocab,
                                        xyzScanSortedByX, 0, 0);
    GroupByImpl groupBy{qec, variablesOnlyX, aliasesCountX, join};
    auto result = groupBy.computeGroupByForJoinWithFullScan();
    EXPECT_THAT(result, Optional(matchesIdTable(2u, qec->getAllocator())));
  }
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, computeGroupByForSingleIndexScan) {
  // Assert that a GROUP BY, that is constructed from the given arguments,
  // can not perform the `OptimizedAggregateOnIndexScanChild` optimization.
  auto testFailure = [this](const auto& groupByVariables, const auto& aliases,
                            const auto& indexScan) {
    auto groupBy = GroupByImpl{qec, groupByVariables, aliases, indexScan};
    ASSERT_EQ(std::nullopt, groupBy.computeGroupByForSingleIndexScan());
  };
  // The IndexScan has only one variable, this is currently not supported.
  testFailure(emptyVariables, aliasesCountX, xScan);

  // Must have zero groupByVariables.
  testFailure(variablesOnlyX, aliasesCountX, xyzScanSortedByX);

  // Must (currently) have exactly one alias that is a count.
  // A distinct count is only supported if the triple has three variables.
  testFailure(emptyVariables, emptyAliases, xyzScanSortedByX);
  testFailure(emptyVariables, aliasesCountDistinctX, xyScan);
  testFailure(emptyVariables, aliasesXAsV, xyzScanSortedByX);

  // `chooseInterface == true` means "use the dedicated
  // `computeGroupByForJoinWithFullScan` method", `chooseInterface == false`
  // means use the general `computeOptimizedGroupByIfPossible` function.
  // `countVarIsUndef == true` means that the COUNT alias is on a variable
  // that doesn't exist, and therefore always has a count of zero.
  auto testWithBothInterfaces = [&](bool chooseInterface,
                                    bool countVarIsUndef) {
    auto groupBy =
        GroupByImpl{qec, emptyVariables,
                    countVarIsUndef ? aliasesCountNotExisting : aliasesCountX,
                    xyzScanSortedByX};
    auto optional = chooseInterface
                        ? groupBy.computeGroupByForSingleIndexScan()
                        : groupBy.computeOptimizedGroupByIfPossible();

    // The test index currently consists of 15 triples.
    if (countVarIsUndef) {
      EXPECT_THAT(optional, optionalHasTable({{I(0)}}));
    } else {
      EXPECT_THAT(optional, optionalHasTable({{I(15)}}));
    }
  };
  testWithBothInterfaces(true, true);
  testWithBothInterfaces(true, false);
  testWithBothInterfaces(false, true);
  testWithBothInterfaces(false, false);

  {
    auto groupBy = GroupByImpl{qec, emptyVariables, aliasesCountX, xyScan};
    auto optional = groupBy.computeGroupByForSingleIndexScan();
    // The test index currently consists of 5 triples that have the predicate
    // `<label>`
    ASSERT_THAT(optional, optionalHasTable({{I(5)}}));
  }
  {
    auto groupBy = GroupByImpl{qec, emptyVariables, aliasesCountDistinctX,
                               xyzScanSortedByX};
    auto optional = groupBy.computeGroupByForSingleIndexScan();
    // The test index currently consists of six distinct subjects:
    // <x>, <y>, <z>, <a>, <b> and <c>.
    ASSERT_THAT(optional, optionalHasTable({{I(6)}}));
  }
}
// _____________________________________________________________________________
TEST_F(GroupByOptimizations, computeGroupByObjectWithCount) {
  // Construct a GROUP BY operation from the given GROUP BY variables, aliases,
  // and index scan. Return `true` if and only if the optimization from
  // `computeGroupByForSingleIndexScan` is suited or this operation.
  //
  // TODO: This appears similarly in each TEST_F(GroupByOptimizations, ...)
  // separately. Define it once and use it in all tests. Also note the
  // `callSpecializedMethod`, which serves the same purpose as the
  // `testWithBothInterfaces` in the other tests, but with much less code.
  auto isSuited = [this](const auto& groupByVariables, const auto& aliases,
                         const auto& indexScan,
                         bool callSpecializedMethod = true) {
    auto groupBy = GroupByImpl{qec, groupByVariables, aliases, indexScan};
    return callSpecializedMethod
               ? groupBy.computeGroupByObjectWithCount().has_value()
               : groupBy.computeOptimizedGroupByIfPossible().has_value();
  };

  // The index scan must have exactly two variables, the IRI must be in the
  // vocabulary, there must be exactly one GROUP BY variable, and there must be
  // exactly one alias that is a non-DISTINCT count.
  ASSERT_TRUE(isSuited(variablesOnlyX, aliasesCountX, xyScan, true));
  ASSERT_TRUE(isSuited(variablesOnlyX, aliasesCountX, xyScan, false));
  ASSERT_FALSE(isSuited(variablesOnlyX, aliasesCountX, xScan));
  ASSERT_FALSE(isSuited(variablesOnlyX, aliasesCountX, xyzScanSortedByX));
  ASSERT_FALSE(isSuited(variablesOnlyX, aliasesCountX, xyScanIriNotInVocab));
  ASSERT_FALSE(isSuited(emptyVariables, aliasesCountX, xyScan));
  ASSERT_FALSE(isSuited(variablesOnlyX, emptyAliases, xyScan));
  ASSERT_FALSE(isSuited(variablesOnlyX, aliasesXAsV, xyScan));
  ASSERT_FALSE(isSuited(variablesOnlyX, aliasesCountDistinctX, xyScan));
  ASSERT_FALSE(isSuited(variablesOnlyX, aliasesCountXTwice, xyScan));

  // Unbound count variable.
  ASSERT_FALSE(isSuited(variablesOnlyX, aliasesCountNotExisting, xyScan, true));
  ASSERT_FALSE(
      isSuited(variablesOnlyX, aliasesCountNotExisting, xyScan, false));

  // The following two checks use a scan of the `<label>` predicate from the
  // test index; see `turtleInput` above. There are five triples, four with
  // subject `<x>` and one with subject `<z>`. The objects are all different.
  //
  // NOTE: The method we are testing here always produces its result sorted by
  // the first column (although the SPARQL standard does not require this).

  // TODO: When constructing the `GroupBy` operations below with a scan that
  // does not macht the group by variables (e.g., `variablesOnlyY` with
  // `xyScan`), the child of the `GroupBy` operation is not even an `IndexScan`.
  // Why is that?.

  // Group by subject.
  auto getId = makeGetId(qec->getIndex());
  {
    auto groupBy = GroupByImpl{qec, variablesOnlyX, aliasesCountX, xyScan};
    ASSERT_THAT(groupBy.computeGroupByObjectWithCount(),
                optionalHasTable({{getId("<x>"), I(4)}, {getId("<z>"), I(1)}}));
  }

  // Group by object.
  {
    auto groupBy = GroupByImpl{qec, variablesOnlyY, aliasesCountY, yxScan};
    ASSERT_THAT(groupBy.computeGroupByObjectWithCount(),
                optionalHasTable({{getId("\"A\""), I(1)},
                                  {getId("\"alpha\""), I(1)},
                                  {getId("\"älpha\""), I(1)},
                                  {getId("\"Beta\""), I(1)},
                                  {getId("\"zz\"@en"), I(1)}}));
  }
}

// _____________________________________________________________________________
TEST_F(GroupByOptimizations, computeGroupByForFullIndexScan) {
  // Assert that a GROUP BY which is constructed from the given arguments
  // can not perform the `GroupByForSingleIndexScan2` optimization.
  auto testFailure = [this](const auto& groupByVariables, const auto& aliases,
                            const auto& indexScan) {
    auto groupBy = GroupByImpl{qec, groupByVariables, aliases, indexScan};
    ASSERT_EQ(std::nullopt, groupBy.computeGroupByForFullIndexScan());
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
    // `countVarIsUnbound == true` means that the variable inside the count is
    // not part of the query and therefore the count always returns zero.
    auto testWithBoundAndUnboundCount = [&](bool countVarIsUnbound) {
      IdTable result{qec->getAllocator()};
      const auto& aliases = [&]() -> const std::vector<Alias>& {
        if (!includeCount) {
          return emptyAliases;
        }
        return countVarIsUnbound ? aliasesCountNotExisting : aliasesCountX;
      }();
      auto groupBy =
          GroupByImpl{qec, variablesOnlyX, aliases, xyzScanSortedByX};
      auto optional = chooseInterface
                          ? groupBy.computeGroupByForFullIndexScan()
                          : groupBy.computeOptimizedGroupByIfPossible();
      ASSERT_TRUE(optional.has_value());
      result = std::move(optional).value();

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
        if (countVarIsUnbound) {
          EXPECT_THAT(
              result.getColumn(1),
              ::testing::ElementsAre(I(0), I(0), I(0), I(0), I(0), I(0)));
        } else {
          EXPECT_THAT(
              result.getColumn(1),
              ::testing::ElementsAre(I(2), I(2), I(2), I(7), I(1), I(1)));
        }
      }
    };
    testWithBoundAndUnboundCount(true);
    testWithBoundAndUnboundCount(false);
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
  QueryPlanner qp{ad_utility::testing::getQec(),
                  std::make_shared<ad_utility::CancellationHandle<>>()};
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

TEST(GroupBy, Descriptor) {
  // Group by with variables
  auto* qec = ad_utility::testing::getQec();
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{3}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}});
  GroupBy groupBy{qec, {Variable{"?a"}}, {}, subtree};
  EXPECT_EQ(groupBy.getDescriptor(), "GroupBy on ?a");
  GroupBy groupBy2{qec, {}, {}, subtree};
  EXPECT_EQ(groupBy2.getDescriptor(), "GroupBy (implicit)");
}

// _____________________________________________________________________________
TEST(GroupBy, knownEmptyResult) {
  auto* qec = ad_utility::testing::getQec();
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{1, qec->getAllocator()},
      std::vector<std::optional<Variable>>{Variable{"?a"}});

  // Explicit group by should propagate knownEmptyResult() from child operation.
  {
    GroupBy groupBy{qec, {Variable{"?a"}}, {}, subtree};
    EXPECT_TRUE(groupBy.knownEmptyResult());
  }
  // Implicit group by always returns a result
  {
    GroupBy groupBy{qec, {}, {}, subtree};
    EXPECT_FALSE(groupBy.knownEmptyResult());
  }
}

// Helper expressions to test specific behaviour of the `GroupBy` class.
namespace {
class SetOfIntervalsExpression : public SparqlExpression {
 public:
  ExpressionResult evaluate(EvaluationContext*) const override {
    using SOI = ad_utility::SetOfIntervals;
    return SOI{SOI::Vec{}};
  }

  std::string getCacheKey(const VariableToColumnMap&) const override {
    return "SetOfIntervalsExpression";
  }

 private:
  ql::span<Ptr> childrenImpl() override { return {}; }
};

// _____________________________________________________________________________
class AggregationFunctionWithVector : public SparqlExpression {
  VectorWithMemoryLimit<ValueId> vector_;

 public:
  explicit AggregationFunctionWithVector(VectorWithMemoryLimit<ValueId> vector)
      : vector_{std::move(vector)} {}
  ExpressionResult evaluate(EvaluationContext*) const override {
    return vector_.clone();
  }

  std::string getCacheKey(const VariableToColumnMap&) const override {
    return "AggregationFunctionWithVector";
  }

 private:
  ql::span<Ptr> childrenImpl() override { return {}; }
};
}  // namespace

// _____________________________________________________________________________
TEST(GroupBy, nonConstantAggregationFunctions) {
  auto* qec = ad_utility::testing::getQec();
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{1, qec->getAllocator()},
      std::vector<std::optional<Variable>>{Variable{"?a"}});

  {
    auto expr0 = std::make_unique<VariableExpression>(Variable{"?a"});
    GroupBy groupBy{qec,
                    {},
                    {{SparqlExpressionPimpl{std::move(expr0), "expr0"},
                      Variable{"?result"}}},
                    subtree};
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        groupBy.computeResultOnlyForTesting(false),
        ::testing::HasSubstr("An expression returned an invalid type"),
        ad_utility::Exception);
  }
  {
    auto expr1 = std::make_unique<SetOfIntervalsExpression>();
    GroupBy groupBy{qec,
                    {},
                    {{SparqlExpressionPimpl{std::move(expr1), "expr1"},
                      Variable{"?result"}}},
                    subtree};
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        groupBy.computeResultOnlyForTesting(false),
        ::testing::HasSubstr("An expression returned an invalid type"),
        ad_utility::Exception);
  }
  {
    // Return empty vector
    auto expr2 = std::make_unique<AggregationFunctionWithVector>(
        VectorWithMemoryLimit<ValueId>{qec->getAllocator()});
    GroupBy groupBy{qec,
                    {},
                    {{SparqlExpressionPimpl{std::move(expr2), "expr2"},
                      Variable{"?result"}}},
                    subtree};
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        groupBy.computeResultOnlyForTesting(false),
        ::testing::HasSubstr(
            "An expression returned a vector expression result "
            "that contained an unexpected amount of entries."),
        ad_utility::Exception);
  }
  {
    // Return vector with 2 elements
    auto expr3 = std::make_unique<AggregationFunctionWithVector>(
        VectorWithMemoryLimit{{Id::makeFromBool(true), Id::makeFromBool(false)},
                              qec->getAllocator()});
    GroupBy groupBy{qec,
                    {},
                    {{SparqlExpressionPimpl{std::move(expr3), "expr3"},
                      Variable{"?result"}}},
                    subtree};
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        groupBy.computeResultOnlyForTesting(false),
        ::testing::HasSubstr(
            "An expression returned a vector expression result "
            "that contained an unexpected amount of entries."),
        ad_utility::Exception);
  }
  {
    auto expr4 = std::make_unique<AggregationFunctionWithVector>(
        VectorWithMemoryLimit{{Id::makeFromBool(true)}, qec->getAllocator()});
    GroupBy groupBy{qec,
                    {},
                    {{SparqlExpressionPimpl{std::move(expr4), "expr4"},
                      Variable{"?result"}}},
                    subtree};
    EXPECT_NO_THROW(groupBy.computeResultOnlyForTesting(false));
  }
}

namespace {
class GroupByLazyFixture : public ::testing::TestWithParam<bool> {
 protected:
  using V = Variable;
  QueryExecutionContext* qec_ = getQec();

  // ___________________________________________________________________________
  static std::vector<std::optional<Variable>> vars(
      std::convertible_to<std::string> auto&&... strings) {
    std::vector<std::optional<Variable>> result;
    result.reserve(sizeof...(strings));
    (result.emplace_back(V{AD_FWD(strings)}), ...);
    return result;
  }

  // ___________________________________________________________________________
  static std::unique_ptr<SumExpression> makeSum(std::string name) {
    return std::make_unique<SumExpression>(
        false, std::make_unique<VariableExpression>(V{std::move(name)}));
  }

  // ___________________________________________________________________________
  template <size_t N>
  static void expectReturningIdTables(
      GroupBy& groupBy, const std::array<IdTable, N>& idTables,
      ad_utility::source_location sourceLocation =
          ad_utility::source_location::current()) {
    auto l = generateLocationTrace(sourceLocation);
    bool lazyResult = GetParam();
    auto result = groupBy.computeResultOnlyForTesting(lazyResult);
    ASSERT_NE(result.isFullyMaterialized(), lazyResult);
    if (lazyResult) {
      size_t counter = 0;
      for (const Result::IdTableVocabPair& pair : result.idTables()) {
        ASSERT_LT(counter, idTables.size())
            << "Too many idTables yielded. Expected: " << idTables.size();
        EXPECT_EQ(idTables.at(counter), pair.idTable_);
        ++counter;
      }
      EXPECT_EQ(counter, idTables.size())
          << "Not enough idTables yielded. Expected: " << idTables.size();
    } else {
      IdTable aggregatedTable{groupBy.getResultWidth(), groupBy.allocator()};
      for (const IdTable& idTable : idTables) {
        aggregatedTable.insertAtEnd(idTable);
      }
      EXPECT_EQ(result.idTable(), aggregatedTable);
    }
  }

  static IdTable makeIntTable(const std::vector<std::vector<IntOrId>>& data) {
    return makeIdTableFromVector(data, IntId);
  }

  GroupBy makeSumXGroupByY(std::vector<IdTable> idTables) {
    auto subtree = makeExecutionTree<ValuesForTesting>(
        qec_, std::move(idTables), vars("?x", "?y"), true,
        std::vector<ColumnIndex>{1});
    Alias alias{SparqlExpressionPimpl{makeSum("?x"), "SUM(?x)"}, V{"?sum"}};
    return {qec_, {V{"?y"}}, {std::move(alias)}, std::move(subtree)};
  }
};
}  // namespace

INSTANTIATE_TEST_SUITE_P(FalseTrue, GroupByLazyFixture, testing::Bool());

// _____________________________________________________________________________
TEST_P(GroupByLazyFixture, emptyGeneratorYieldsEmptyResult) {
  auto subtree = makeExecutionTree<ValuesForTesting>(
      qec_, std::vector<IdTable>{}, vars("?x"), true,
      std::vector<ColumnIndex>{0});
  GroupBy groupBy{qec_, {V{"?x"}}, {}, std::move(subtree)};

  expectReturningIdTables<0>(groupBy, {});
}

// _____________________________________________________________________________
TEST_P(GroupByLazyFixture, generatorWithEmptyTablesYieldsEmptyResult) {
  std::vector<IdTable> idTables;
  // Arbitrary count, could be anything greater than 0
  for (size_t i = 0; i < 7; ++i) {
    idTables.emplace_back(1, qec_->getAllocator());
  }

  auto subtree = makeExecutionTree<ValuesForTesting>(
      qec_, std::move(idTables), vars("?x"), true, std::vector<ColumnIndex>{0});
  GroupBy groupBy{qec_, {V{"?x"}}, {}, std::move(subtree)};

  expectReturningIdTables<0>(groupBy, {});
}

// _____________________________________________________________________________
TEST_P(GroupByLazyFixture, emptyIdTableIsSkipped) {
  // SELECT (SUM(?x) as ?sum) WHERE { ... } GROUP BY ?y
  // The first column is ?x, the second is ?y
  std::vector<IdTable> idTables;
  idTables.emplace_back(2, qec_->getAllocator());
  idTables.push_back(makeIntTable({{1, 0}}));
  idTables.emplace_back(2, qec_->getAllocator());
  idTables.emplace_back(2, qec_->getAllocator());
  idTables.push_back(makeIntTable({{2, 0}}));
  idTables.emplace_back(2, qec_->getAllocator());
  idTables.push_back(makeIntTable({{4, 1}}));
  idTables.emplace_back(2, qec_->getAllocator());

  GroupBy groupBy = makeSumXGroupByY(std::move(idTables));

  expectReturningIdTables<2>(groupBy,
                             {makeIntTable({{0, 3}}), makeIntTable({{1, 4}})});
}

// _____________________________________________________________________________
TEST_P(GroupByLazyFixture, implicitGroupByWithNonEmptyInput) {
  std::vector<IdTable> idTables;
  idTables.push_back(makeIntTable({{1}}));
  idTables.push_back(makeIntTable({{2}}));
  idTables.push_back(makeIntTable({{4}}));

  auto subtree = makeExecutionTree<ValuesForTesting>(
      qec_, std::move(idTables), vars("?x"), true, std::vector<ColumnIndex>{0});
  Alias alias{SparqlExpressionPimpl{makeSum("?x"), "SUM(?x)"}, V{"?sum"}};
  GroupBy groupBy{qec_, {}, {std::move(alias)}, std::move(subtree)};

  expectReturningIdTables<1>(groupBy, {makeIntTable({{7}})});
}

// _____________________________________________________________________________
TEST_P(GroupByLazyFixture, groupsSpanningOverMultipleIdTables) {
  std::vector<IdTable> idTables;
  idTables.push_back(makeIntTable({{1, 0}, {2, 1}}));
  idTables.push_back(makeIntTable({{4, 1}, {8, 1}}));
  idTables.push_back(makeIntTable({{16, 1}, {32, 2}}));
  idTables.push_back(makeIntTable({{64, 2}, {128, 2}}));
  idTables.push_back(makeIntTable({{256, 3}}));
  idTables.push_back(makeIntTable({{512, 3}}));

  GroupBy groupBy = makeSumXGroupByY(std::move(idTables));

  expectReturningIdTables<4>(
      groupBy, {makeIntTable({{0, 1}}), makeIntTable({{1, 30}}),
                makeIntTable({{2, 224}}), makeIntTable({{3, 768}})});
}

// _____________________________________________________________________________
TEST_P(GroupByLazyFixture, groupsAtTableBoundariesAreAggregatedCorrectly) {
  std::vector<IdTable> idTables;
  idTables.push_back(makeIntTable({{1, 0}}));
  idTables.push_back(makeIntTable({{2, 1}, {4, 1}}));
  idTables.push_back(makeIntTable({{8, 2}, {16, 2}}));

  GroupBy groupBy = makeSumXGroupByY(std::move(idTables));

  expectReturningIdTables<3>(groupBy,
                             {makeIntTable({{0, 1}}), makeIntTable({{1, 6}}),
                              makeIntTable({{2, 24}})});
}

// _____________________________________________________________________________
TEST_P(GroupByLazyFixture, implicitGroupByWithEmptyInput) {
  std::vector<IdTable> idTables;
  idTables.emplace_back(1, qec_->getAllocator());

  auto subtree = makeExecutionTree<ValuesForTesting>(
      qec_, std::move(idTables), vars("?x"), true, std::vector<ColumnIndex>{0});
  Alias alias{SparqlExpressionPimpl{makeSum("?x"), "SUM(?x)"}, V{"?sum"}};
  GroupBy groupBy{qec_, {}, {std::move(alias)}, std::move(subtree)};

  expectReturningIdTables<1>(groupBy, {makeIntTable({{0}})});
}

// _____________________________________________________________________________
TEST_P(GroupByLazyFixture, groupsOfSize1AreAggregatedCorrectly) {
  std::vector<IdTable> idTables;
  idTables.push_back(makeIntTable({{1, 0}}));
  idTables.push_back(makeIntTable({{2, 1}, {4, 2}, {8, 3}}));
  idTables.push_back(makeIntTable({{16, 4}}));
  idTables.push_back(makeIntTable({{32, 5}, {64, 6}}));

  GroupBy groupBy = makeSumXGroupByY(std::move(idTables));

  expectReturningIdTables<4>(
      groupBy, {makeIntTable({{0, 1}, {1, 2}, {2, 4}}), makeIntTable({{3, 8}}),
                makeIntTable({{4, 16}, {5, 32}}), makeIntTable({{6, 64}})});
}

// _____________________________________________________________________________
TEST_P(GroupByLazyFixture, nestedAggregateFunctionsWork) {
  // Test queries of the class `SELECT (CONCAT(STR(SUM(?x)), "---") as ?result)
  // ...` where the aggregate function is not on top of the expression tree.
  using L = TripleComponent::Literal;
  std::vector<IdTable> idTables;
  idTables.push_back(makeIntTable({{1, 0}}));
  idTables.push_back(makeIntTable({{2, 1}, {4, 1}, {8, 2}}));
  auto subtree = makeExecutionTree<ValuesForTesting>(
      qec_, std::move(idTables), vars("?x", "?y"), true,
      std::vector<ColumnIndex>{1});

  std::vector<std::unique_ptr<SparqlExpression>> children;
  children.push_back(makeStrExpression(makeSum("?x")));
  children.push_back(std::make_unique<StringLiteralExpression>(
      L::fromStringRepresentation("\"---\"")));
  Alias alias{SparqlExpressionPimpl{makeConcatExpression(std::move(children)),
                                    "CONCAT(SUM(?x), \"---\")"},
              V{"?result"}};
  GroupBy groupBy{qec_, {V{"?y"}}, {std::move(alias)}, std::move(subtree)};
  // From here the code is similar to `expectReturningIdTables`, but checks the
  // local vocab state during consumption of the generator and uses it to
  // extract the required local vocab ids to match the result table against.
  auto result = groupBy.computeResultOnlyForTesting(GetParam());

  // Acquire the local vocab index for a given string representation if present.
  auto makeEntry = [](std::string string, const LocalVocab& localVocab) {
    return localVocab.getIndexOrNullopt(sparqlExpression::detail::LiteralOrIri{
        L::fromStringRepresentation(std::move(string))});
  };

  auto entryToId = [](std::optional<LocalVocabIndex> entry) {
    return Id::makeFromLocalVocabIndex(entry.value());
  };

  ASSERT_NE(result.isFullyMaterialized(), GetParam());
  auto i = IntId;

  if (GetParam()) {
    auto generator = result.idTables();

    auto iterator = generator.begin();
    ASSERT_NE(iterator, generator.end());
    EXPECT_EQ(iterator->localVocab_.size(), 2);
    auto entry1 = makeEntry("\"1---\"", iterator->localVocab_);
    auto entry2 = makeEntry("\"6---\"", iterator->localVocab_);
    EXPECT_EQ(iterator->idTable_,
              makeIdTableFromVector(
                  {{i(0), entryToId(entry1)}, {i(1), entryToId(entry2)}}));
    ++iterator;
    ASSERT_NE(iterator, generator.end());
    EXPECT_EQ(iterator->localVocab_.size(), 1);
    auto entry3 = makeEntry("\"8---\"", iterator->localVocab_);
    EXPECT_EQ(iterator->idTable_,
              makeIdTableFromVector({{i(2), entryToId(entry3)}}));

    EXPECT_EQ(++iterator, generator.end());

  } else {
    EXPECT_EQ(result.localVocab().size(), 3);
    auto entry1 = makeEntry("\"1---\"", result.localVocab());
    auto entry2 = makeEntry("\"6---\"", result.localVocab());
    auto entry3 = makeEntry("\"8---\"", result.localVocab());

    ASSERT_TRUE(entry1.has_value());
    ASSERT_TRUE(entry2.has_value());
    ASSERT_TRUE(entry3.has_value());

    EXPECT_EQ(result.idTable(),
              makeIdTableFromVector({{i(0), entryToId(entry1)},
                                     {i(1), entryToId(entry2)},
                                     {i(2), entryToId(entry3)}}));
  }
}

// _____________________________________________________________________________
TEST_P(GroupByLazyFixture, aliasRenameWorks) {
  std::vector<IdTable> idTables;
  idTables.push_back(makeIntTable({{1}}));
  idTables.push_back(makeIntTable({{2}, {4}, {8}}));
  auto subtree = makeExecutionTree<ValuesForTesting>(
      qec_, std::move(idTables), vars("?x"), true, std::vector<ColumnIndex>{0});
  Alias alias{SparqlExpressionPimpl{
                  std::make_unique<VariableExpression>(V{"?x"}), "?x as ?y"},
              V{"?y"}};
  GroupBy groupBy{qec_, {V{"?x"}}, {std::move(alias)}, std::move(subtree)};

  expectReturningIdTables<2>(groupBy, {makeIntTable({{1, 1}, {2, 2}, {4, 4}}),
                                       makeIntTable({{8, 8}})});
}

// _____________________________________________________________________________
TEST_P(GroupByLazyFixture, countStarWorks) {
  std::vector<IdTable> idTables;
  idTables.push_back(makeIntTable({{1}}));
  idTables.push_back(makeIntTable({{2}, {4}, {8}}));
  auto subtree = makeExecutionTree<ValuesForTesting>(
      qec_, std::move(idTables), vars("?x"), true, std::vector<ColumnIndex>{0});

  Alias alias{
      SparqlExpressionPimpl{makeCountStarExpression(false), "COUNT(*) as ?y"},
      V{"?y"}};
  GroupBy groupBy{qec_, {}, {std::move(alias)}, std::move(subtree)};

  expectReturningIdTables<1>(groupBy, {makeIntTable({{4}})});
}
