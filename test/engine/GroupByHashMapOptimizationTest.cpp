//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/AllocatorTestHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/GroupByHashMapOptimization.h"
#include "engine/GroupByImpl.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"
// For building subtree ops in tests
#include "./ValuesForTesting.h"
// For CountExpression and VariableExpression helpers
#include "engine/sparqlExpressions/AggregateExpression.h"

namespace {
auto I = ad_utility::testing::IntId;
auto D = ad_utility::testing::DoubleId;
using ad_utility::AllocatorWithLimit;
using ad_utility::testing::IntId;
}  // namespace

class GroupByHashMapOptimizationTest : public ::testing::Test {
 protected:
  AllocatorWithLimit<Id> alloc_ = ad_utility::testing::makeAllocator();
  QueryExecutionContext* qec_ = ad_utility::testing::getQec();
  VariableToColumnMap varToColMap_;
  LocalVocab localVocab_;
  IdTable table_{qec_->getAllocator()};
  sparqlExpression::EvaluationContext context_{
      *qec_,
      varToColMap_,
      table_,
      qec_->getAllocator(),
      localVocab_,
      std::make_shared<ad_utility::CancellationHandle<>>(),
      sparqlExpression::EvaluationContext::TimePoint::max()};

  Id calculate(const auto& data) { return data.calculateResult(&localVocab_); }

  template <typename T>
  auto makeCalcAndAddValue(T& data) {
    auto calc = [this, &data]() { return calculate(data); };
    auto addValue = [this, &data](auto&& x) {
      data.addValue(AD_FWD(x), &context_);
    };
    return std::tuple{std::move(calc), std::move(addValue)};
  }

  std::string idToString(Id result, bool stripQuotes = true) const {
    auto index = result.getLocalVocabIndex();
    auto resultString = localVocab_.getWord(index).toStringRepresentation();
    if (!stripQuotes) {
      return resultString;
    }
    // Strip leading and trailing quotes.
    AD_CORRECTNESS_CHECK(resultString.size() >= 2);
    size_t endIndex = resultString.size() - 1;
    return std::move(resultString).substr(1, endIndex - 1);
  };

  Id idFromString(std::string_view string) {
    using ad_utility::triple_component::LiteralOrIri;
    auto literal = LiteralOrIri::literalWithoutQuotes(string);
    return Id::makeFromLocalVocabIndex(
        localVocab_.getIndexAndAddIfNotContained(std::move(literal)));
  };

  // Helper: Build a minimal GroupByImpl and related optimization data for
  // grouping on column 0 and COUNT over column 1. The `exampleTable` is only
  // used to construct a subtree with the correct schema; the actual
  // aggregation uses the tables passed to updateHashMapWithTable.
  auto getGroupByCountSetup(const IdTable& exampleTable)
      -> std::tuple<GroupByImpl, GroupByImpl::HashMapOptimizationData,
                    GroupByImpl::HashMapAggregationData<1>, LocalVocab> {
    // Build a trivial subtree for GroupByImpl using the provided schema.
    std::vector<std::optional<Variable>> vars{Variable{"?g"}, Variable{"?v"}};
    auto values = std::make_shared<ValuesForTesting>(
        qec_, exampleTable.clone(), vars, false, std::vector<ColumnIndex>{});
    auto subtree = std::make_shared<QueryExecutionTree>(qec_, values);
    using namespace sparqlExpression;
    SparqlExpressionPimpl pimpl{
        std::make_unique<CountExpression>(
            false, std::make_unique<VariableExpression>(Variable{"?v"})),
        "COUNT(?v)"};
    std::vector<Alias> ctorAliases{Alias{std::move(pimpl), Variable{"?cnt"}}};
    auto gb = GroupByImpl(qec_, std::vector<Variable>{Variable{"?g"}},
                          std::move(ctorAliases), subtree);
    // Build optimization data equivalent to COUNT(?v) over grouped ?g
    SparqlExpressionPimpl pimpl2{
        std::make_unique<CountExpression>(
            false, std::make_unique<VariableExpression>(Variable{"?v"})),
        "COUNT(?v)"};
    using OptData = GroupByImpl::HashMapOptimizationData;
    using AggInfo = GroupByImpl::HashMapAggregateInformation;
    using AliasInfo = GroupByImpl::HashMapAliasInformation;
    using enum GroupByImpl::HashMapAggregateType;
    AggInfo aggInfo{pimpl2.getPimpl(), 0,
                    GroupByImpl::HashMapAggregateTypeWithData{COUNT}};
    AliasInfo alias{
        std::move(pimpl2), 1, std::vector<AggInfo>{std::move(aggInfo)}, {}};
    OptData data{std::vector<AliasInfo>{std::move(alias)}};
    data.columnIndices_ = std::vector<size_t>{0};

    LocalVocab localVocab;
    data.localVocabRef_ = localVocab;
    GroupByImpl::HashMapAggregationData<1> aggr{alloc_, data.aggregateAliases_,
                                                1};
    return {std::move(gb), std::move(data), std::move(aggr),
            std::move(localVocab)};
  }
};

// Small helper to build a two-column IdTable from pairs.
static IdTable make2(const std::vector<std::pair<int64_t, int64_t>>& rows,
                     AllocatorWithLimit<Id> alloc) {
  IdTable table{2, alloc};
  table.resize(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    table(i, 0) = IntId(rows[i].first);
    table(i, 1) = IntId(rows[i].second);
  }
  return table;
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, AvgAggregationDataAggregatesCorrectly) {
  AvgAggregationData data;
  auto [calc, addValue] = makeCalcAndAddValue(data);

  EXPECT_EQ(calc(), I(0));
  addValue(I(1));
  EXPECT_DOUBLE_EQ(calc().getDouble(), 1);
  addValue(I(3));
  EXPECT_DOUBLE_EQ(calc().getDouble(), 2);
  addValue(D(3));
  EXPECT_NEAR(calc().getDouble(), 7 / 3.0, 0.00001);

  data.reset();
  EXPECT_EQ(calc(), I(0));
  addValue(I(2));
  EXPECT_EQ(calc(), D(2));

  addValue(Id::makeUndefined());
  EXPECT_TRUE(calc().isUndefined());

  data.reset();
  EXPECT_EQ(calc(), I(0));
  using ad_utility::triple_component::LiteralOrIri;
  auto literal = LiteralOrIri::literalWithoutQuotes("non-numeric value");
  auto id = Id::makeFromLocalVocabIndex(
      localVocab_.getIndexAndAddIfNotContained(std::move(literal)));
  addValue(id);
  EXPECT_TRUE(calc().isUndefined());
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest,
       CountAggregationDataAggregatesCorrectly) {
  CountAggregationData data;
  auto [calc, addValue] = makeCalcAndAddValue(data);

  EXPECT_EQ(calc(), I(0));
  addValue(I(1));
  EXPECT_EQ(calc(), I(1));
  addValue(I(3));
  EXPECT_EQ(calc(), I(2));
  addValue(D(3));
  EXPECT_EQ(calc(), I(3));

  data.reset();
  EXPECT_EQ(calc(), I(0));
  addValue(Id::makeFromBool(false));
  EXPECT_EQ(calc(), I(1));

  addValue(Id::makeUndefined());
  EXPECT_EQ(calc(), I(1));
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, MinAggregationDataAggregatesCorrectly) {
  MinAggregationData data;
  auto [calc, addValue] = makeCalcAndAddValue(data);

  EXPECT_TRUE(calc().isUndefined());
  addValue(I(1));
  EXPECT_EQ(calc(), I(1));
  addValue(I(3));
  EXPECT_EQ(calc(), I(1));
  addValue(D(1));
  EXPECT_EQ(calc(), I(1));
  addValue(D(0));
  EXPECT_EQ(calc(), D(0));

  data.reset();
  EXPECT_TRUE(calc().isUndefined());
  addValue(Id::makeFromBool(true));
  EXPECT_EQ(calc(), Id::makeFromBool(true));

  // undefined < everything
  addValue(Id::makeUndefined());
  EXPECT_TRUE(calc().isUndefined());
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, MaxAggregationDataAggregatesCorrectly) {
  MaxAggregationData data;
  auto [calc, addValue] = makeCalcAndAddValue(data);

  EXPECT_TRUE(calc().isUndefined());
  addValue(I(1));
  EXPECT_EQ(calc(), I(1));
  addValue(I(3));
  EXPECT_EQ(calc(), I(3));
  addValue(D(0));
  EXPECT_EQ(calc(), I(3));
  addValue(D(4));
  EXPECT_EQ(calc(), D(4));

  data.reset();
  EXPECT_TRUE(calc().isUndefined());
  addValue(Id::makeFromBool(false));
  EXPECT_EQ(calc(), Id::makeFromBool(false));

  // undefined < everything
  addValue(Id::makeUndefined());
  EXPECT_EQ(calc(), Id::makeFromBool(false));
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, SumAggregationDataAggregatesCorrectly) {
  SumAggregationData data;
  auto [calc, addValue] = makeCalcAndAddValue(data);

  EXPECT_EQ(calc(), I(0));
  addValue(I(1));
  EXPECT_EQ(calc(), I(1));
  addValue(I(3));
  EXPECT_EQ(calc(), I(4));
  addValue(D(1));
  EXPECT_DOUBLE_EQ(calc().getDouble(), 5);
  addValue(D(0));
  EXPECT_DOUBLE_EQ(calc().getDouble(), 5);

  data.reset();
  EXPECT_EQ(calc(), I(0));
  addValue(Id::makeFromBool(true));
  EXPECT_EQ(calc(), I(1));
  addValue(Id::makeFromBool(false));
  EXPECT_EQ(calc(), I(1));

  addValue(Id::makeUndefined());
  EXPECT_TRUE(calc().isUndefined());
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest,
       GroupConcatAggregationDataAggregatesCorrectly) {
  GroupConcatAggregationData data{";"};
  auto [calc, addValue] = makeCalcAndAddValue(data);

  auto getResultString = [&](bool stripQuotes = true) {
    return idToString(calc(), stripQuotes);
  };

  auto addString = [&](std::string_view string) {
    addValue(idFromString(string));
  };

  EXPECT_EQ(getResultString(), "");
  addString("a");
  EXPECT_EQ(getResultString(), "a");
  addString("b");
  EXPECT_EQ(getResultString(), "a;b");
  addString("c");
  EXPECT_EQ(getResultString(), "a;b;c");
  addString("");
  EXPECT_EQ(getResultString(), "a;b;c;");
  addString("");
  EXPECT_EQ(getResultString(), "a;b;c;;");
  addString("d");
  EXPECT_EQ(getResultString(), "a;b;c;;;d");

  data.reset();
  EXPECT_EQ(getResultString(), "");
  addString("a");
  EXPECT_EQ(getResultString(), "a");
  addValue(Id::makeUndefined());
  EXPECT_EQ(calc(), Id::makeUndefined());
  addString("a");
  EXPECT_EQ(calc(), Id::makeUndefined());

  auto addStringWithLangTag = [&](std::string_view string,
                                  std::string langTag) {
    using ad_utility::triple_component::LiteralOrIri;
    auto literal =
        LiteralOrIri::literalWithoutQuotes(string, std::move(langTag));
    addValue(Id::makeFromLocalVocabIndex(
        localVocab_.getIndexAndAddIfNotContained(std::move(literal))));
  };

  data.reset();
  EXPECT_EQ(getResultString(), "");
  addString("a");
  EXPECT_EQ(getResultString(), "a");
  addStringWithLangTag("b", "en");
  EXPECT_EQ(getResultString(false), "\"a;b\"");

  data.reset();
  EXPECT_EQ(getResultString(), "");
  addStringWithLangTag("a", "en");
  EXPECT_EQ(getResultString(false), "\"a\"@en");
  addStringWithLangTag("b", "en");
  EXPECT_EQ(getResultString(false), "\"a;b\"@en");
  addStringWithLangTag("c", "de");
  EXPECT_EQ(getResultString(false), "\"a;b;c\"");

  data.reset();
  EXPECT_EQ(getResultString(), "");
  addStringWithLangTag("a", "en");
  EXPECT_EQ(getResultString(false), "\"a\"@en");
  addString("b");
  EXPECT_EQ(getResultString(false), "\"a;b\"");
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest,
       SampleAggregationDataAggregatesCorrectly) {
  SampleAggregationData data;
  auto [calc, addValue] = makeCalcAndAddValue(data);

  EXPECT_TRUE(calc().isUndefined());
  addValue(I(1));
  EXPECT_EQ(calc(), I(1));
  addValue(I(3));
  EXPECT_EQ(calc(), I(1));
  addValue(D(1));
  EXPECT_EQ(calc(), I(1));

  data.reset();
  EXPECT_TRUE(calc().isUndefined());
  addValue(D(0));
  EXPECT_EQ(calc(), D(0));

  data.reset();
  EXPECT_TRUE(calc().isUndefined());
  addValue(Id::makeFromBool(true));
  EXPECT_EQ(calc(), Id::makeFromBool(true));
  addValue(Id::makeUndefined());
  addValue(Id::makeFromBool(true));

  data.reset();
  addValue(Id::makeUndefined());
  EXPECT_TRUE(calc().isUndefined());

  data.reset();
  addValue(localVocab_.getWord(idFromString("Abc").getLocalVocabIndex()));
  EXPECT_EQ(calc(), idFromString("Abc"));
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, MakeKeyForHashMap_SingleAndMulti) {
  // Single-column table: [5, 6, 7]
  IdTable table1{1, alloc_};
  table1.resize(3);
  table1(0, 0) = IntId(5);
  table1(1, 0) = IntId(6);
  table1(2, 0) = IntId(7);

  using AggrDataOneCol = GroupByImpl::HashMapAggregationData<1>;
  AggrDataOneCol aggrData1{alloc_, {}, 1};
  auto col1_1 = table1.getColumn(0).subspan(0, 3);
  AggrDataOneCol::ArrayOrVector<ql::span<const Id>> spans1;
  spans1[0] = col1_1;

  auto key1_0 = aggrData1.makeKeyForHashMap(spans1, 0);
  auto key1_1 = aggrData1.makeKeyForHashMap(spans1, 1);
  auto key1_2 = aggrData1.makeKeyForHashMap(spans1, 2);
  EXPECT_EQ(key1_0[0], IntId(5));
  EXPECT_EQ(key1_1[0], IntId(6));
  EXPECT_EQ(key1_2[0], IntId(7));

  // Two-column table: [(1,4), (2,5), (3,6)]
  using AggrDataTwoCol = GroupByImpl::HashMapAggregationData<2>;
  auto table2 = make2({{1, 4}, {2, 5}, {3, 6}}, alloc_);
  AggrDataTwoCol aggrData2{alloc_, {}, 2};
  auto col2_1 = table2.getColumn(0).subspan(0, 3);
  auto col2_2 = table2.getColumn(1).subspan(0, 3);
  AggrDataTwoCol::ArrayOrVector<ql::span<const Id>> spans2;
  spans2[0] = col2_1;
  spans2[1] = col2_2;

  auto key2_0 = aggrData2.makeKeyForHashMap(spans2, 0);
  auto key2_1 = aggrData2.makeKeyForHashMap(spans2, 1);
  auto key2_2 = aggrData2.makeKeyForHashMap(spans2, 2);
  EXPECT_EQ(key2_0[0], IntId(1));
  EXPECT_EQ(key2_0[1], IntId(4));
  EXPECT_EQ(key2_1[0], IntId(2));
  EXPECT_EQ(key2_1[1], IntId(5));
  EXPECT_EQ(key2_2[0], IntId(3));
  EXPECT_EQ(key2_2[1], IntId(6));
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, GetHashEntries_InsertAndOnlyMatching) {
  using AggrDataTwoCol = GroupByImpl::HashMapAggregationData<2>;
  AggrDataTwoCol aggrData{alloc_, {}, 2};

  // First block, expect insertion and indices [0,1,0,2]
  auto table_A = make2({{1, 1}, {1, 2}, {1, 1}, {2, 2}}, alloc_);
  AggrDataTwoCol::ArrayOrVector<ql::span<const Id>> spansA;
  spansA[0] = table_A.getColumn(0).subspan(0, table_A.size());
  spansA[1] = table_A.getColumn(1).subspan(0, table_A.size());
  auto [entriesA, nonmatchA] = aggrData.getHashEntries(spansA, false);
  ASSERT_TRUE(nonmatchA.empty());
  std::vector<GroupByImpl::RowToGroup> expectedA{
      {0, 0}, {1, 1}, {2, 0}, {3, 2}};
  ASSERT_EQ(entriesA, expectedA);
  EXPECT_EQ(aggrData.numGroups(), 3u);

  // Second block, onlyMatching: new keys should be reported as non-matching
  auto table_B = make2({{1, 1}, {3, 3}, {2, 2}, {4, 4}}, alloc_);
  AggrDataTwoCol::ArrayOrVector<ql::span<const Id>> spansB;
  spansB[0] = table_B.getColumn(0).subspan(0, table_B.size());
  spansB[1] = table_B.getColumn(1).subspan(0, table_B.size());
  auto [entriesB, nonmatchB] = aggrData.getHashEntries(spansB, true);
  // Expect two matches at positions 0 -> key (1,1) and 2 -> key (2,2)
  // Their indices correspond to the ones assigned before: (1,1)->0, (2,2)->2
  std::vector<GroupByImpl::RowToGroup> expectedEntriesB{{0, 0}, {2, 2}};
  ASSERT_EQ(entriesB, expectedEntriesB);
  std::vector<size_t> expectedNonmatchB{1, 3};
  ASSERT_EQ(nonmatchB, expectedNonmatchB);

  // Verify getSortedGroupColumns returns sorted unique pairs
  auto sortedCols = aggrData.getSortedGroupColumns();
  ASSERT_EQ(sortedCols[0].size(), 3u);
  ASSERT_EQ(sortedCols[1].size(), 3u);
  // Expected order: (1,1), (1,2), (2,2)
  EXPECT_EQ(sortedCols[0][0], IntId(1));
  EXPECT_EQ(sortedCols[1][0], IntId(1));
  EXPECT_EQ(sortedCols[0][1], IntId(1));
  EXPECT_EQ(sortedCols[1][1], IntId(2));
  EXPECT_EQ(sortedCols[0][2], IntId(2));
  EXPECT_EQ(sortedCols[1][2], IntId(2));
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, MakeGroupValueSpans_SelectColumns) {
  // Build a 4x4 table with known values.
  IdTable inputTable{4, alloc_};
  inputTable.resize(4);
  // Rows: (10,11,12,13), (20,21,22,23), (30,31,32,33), (40,41,42,43)
  for (size_t rowIndex = 0; rowIndex < 4; ++rowIndex)
    for (size_t colIndex = 0; colIndex < 4; ++colIndex)
      inputTable(rowIndex, colIndex) = IntId((rowIndex + 1) * 10 + colIndex);

  // Build a trivial subtree for GroupByImpl. Provide variable metadata that
  // matches the table width (4 columns), but content is irrelevant here.
  std::vector<std::optional<Variable>> vars{Variable{"?a"}, Variable{"?b"},
                                            Variable{"?c"}, Variable{"?d"}};
  auto values = std::make_shared<ValuesForTesting>(
      qec_, inputTable.clone(), vars, false, std::vector<ColumnIndex>{});
  auto subtree = std::make_shared<QueryExecutionTree>(qec_, values);
  GroupByImpl gb{qec_, {}, {}, subtree};

  // Request spans for columns [1, 3].
  std::vector<ColumnIndex> cols{1, 3};
  auto spans =
      gb.makeGroupValueSpans<2>(inputTable, 0, inputTable.size(), cols);

  // Assertions: two spans, each of size 4, matching columns 1 and 3.
  ASSERT_EQ(spans.size(), 2u);
  ASSERT_EQ(spans[0].size(), inputTable.size());
  ASSERT_EQ(spans[1].size(), inputTable.size());
  for (size_t rowIndex = 0; rowIndex < inputTable.size(); ++rowIndex) {
    EXPECT_EQ(spans[0][rowIndex], inputTable(rowIndex, 1));
    EXPECT_EQ(spans[1][rowIndex], inputTable(rowIndex, 3));
  }
}

// Helper: Build HashMapOptimizationData with one COUNT aggregate over column 1
// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest,
       UpdateHashMapWithTable_CountSingleBlock) {
  auto inputTable =
      make2({{1, 100}, {1, 200}, {2, 300}, {2, 400}, {2, 500}}, alloc_);

  // Build a minimal GroupByImpl and optimization data: group on col0, COUNT
  // over col1.
  auto [gb, data, aggr, localVocab] = getGroupByCountSetup(inputTable);
  GroupByImpl::HashMapTimers timers{ad_utility::Timer::Stopped,
                                    ad_utility::Timer::Stopped};

  // Update the map with the single block.
  auto nonmatching =
      gb.updateHashMapWithTable<1>(inputTable, data, aggr, timers, false);
  ASSERT_TRUE(nonmatching.nonMatchingRows_.empty());
  EXPECT_FALSE(nonmatching.thresholdExceeded_);
  // Now finalize: create result sorted by group and check counts.
  auto result =
      gb.createResultFromHashMap<1>(aggr, data.aggregateAliases_, &localVocab);
  ASSERT_EQ(result.numColumns(), 2u);  // group + COUNT
  ASSERT_EQ(result.size(), 2u);
  // Expected: (1,2), (2,3)
  EXPECT_EQ(result(0, 0), IntId(1));
  EXPECT_EQ(result(0, 1).getInt(), 2);
  EXPECT_EQ(result(1, 0), IntId(2));
  EXPECT_EQ(result(1, 1).getInt(), 3);
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest,
       UpdateHashMapWithTable_CountTwoBlocksOverlap) {
  auto firstBlockTable = make2({{1, 100}, {1, 200}, {2, 300}}, alloc_);
  auto secondBlockTable = make2({{2, 400}, {3, 500}}, alloc_);
  auto [gb, data, aggr, localVocab] = getGroupByCountSetup(firstBlockTable);
  GroupByImpl::HashMapTimers timers{ad_utility::Timer::Stopped,
                                    ad_utility::Timer::Stopped};

  // First block
  auto nonmatchingIndicesFirstBlock =
      gb.updateHashMapWithTable<1>(firstBlockTable, data, aggr, timers, false);
  ASSERT_TRUE(nonmatchingIndicesFirstBlock.nonMatchingRows_.empty());
  EXPECT_FALSE(nonmatchingIndicesFirstBlock.thresholdExceeded_);
  // Second block
  auto nonmatchingIndicesSecondBlock =
      gb.updateHashMapWithTable<1>(secondBlockTable, data, aggr, timers, false);
  ASSERT_TRUE(nonmatchingIndicesSecondBlock.nonMatchingRows_.empty());
  EXPECT_FALSE(nonmatchingIndicesSecondBlock.thresholdExceeded_);

  auto result =
      gb.createResultFromHashMap<1>(aggr, data.aggregateAliases_, &localVocab);
  ASSERT_EQ(result.size(), 3u);
  // Expected groups: 1->2, 2->2, 3->1
  EXPECT_EQ(result(0, 0), IntId(1));
  EXPECT_EQ(result(0, 1).getInt(), 2);
  EXPECT_EQ(result(1, 0), IntId(2));
  EXPECT_EQ(result(1, 1).getInt(), 2);
  EXPECT_EQ(result(2, 0), IntId(3));
  EXPECT_EQ(result(2, 1).getInt(), 1);
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest,
       UpdateHashMapWithTable_OnlyMatchingNonmatchingList) {
  auto firstBlockTable = make2({{1, 10}, {2, 20}}, alloc_);
  auto secondBlockTable = make2({{1, 11}, {3, 30}, {2, 21}, {4, 40}}, alloc_);
  auto [gb, data, aggr, localVocab] = getGroupByCountSetup(firstBlockTable);
  GroupByImpl::HashMapTimers timers{ad_utility::Timer::Stopped,
                                    ad_utility::Timer::Stopped};

  // Seed map with first block.
  gb.updateHashMapWithTable<1>(firstBlockTable, data, aggr, timers, false);
  // Only match existing keys for second block.
  auto nonmatchingIndicesSecondBlock =
      gb.updateHashMapWithTable<1>(secondBlockTable, data, aggr, timers, true);
  std::vector<size_t> expected{1, 3};
  ASSERT_EQ(nonmatchingIndicesSecondBlock.nonMatchingRows_, expected);
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, ProcessAggregateAliasesForBlock) {
  // ================= SETUP ===================================================
  auto inputTable = make2({{42, 100}, {42, 200}, {84, 300}}, alloc_);
  auto [gb, data, aggr, localVocab] = getGroupByCountSetup(inputTable);
  VariableToColumnMap variableMap{
      {Variable{"?g"}, {0, ColumnIndexAndTypeInfo::AlwaysDefined}},
      {Variable{"?v"}, {1, ColumnIndexAndTypeInfo::AlwaysDefined}}};
  sparqlExpression::EvaluationContext evaluationContext(
      *qec_, variableMap, inputTable, qec_->getAllocator(), localVocab,
      std::make_shared<ad_utility::CancellationHandle<>>(),
      sparqlExpression::EvaluationContext::TimePoint::max());
  evaluationContext._beginIndex = 0;
  evaluationContext._endIndex = inputTable.size();

  using AggData = GroupByImpl::HashMapAggregationData<1>;
  AggData::ArrayOrVector<ql::span<const Id>> spans;
  spans[0] = inputTable.getColumn(0).subspan(0, inputTable.size());
  auto lookupResult = aggr.getHashEntries(spans, false);

  // ================= TEST ====================================================
  gb.processAggregateAliasesForBlock<1>(lookupResult, data, aggr,
                                        evaluationContext);

  // Verify that the aggregation data has been updated correctly.
  // Group with key=42 should have count=2 (rows 0,1),
  // group with key=84 should have count=1 (row 2)
  auto result =
      gb.createResultFromHashMap<1>(aggr, data.aggregateAliases_, &localVocab);
  ASSERT_EQ(result.size(), 2u);
  ASSERT_EQ(result.numColumns(), 2u);  // group column + count column

  // Results should be sorted by group key
  EXPECT_EQ(result(0, 0), IntId(42));   // Group key 42
  EXPECT_EQ(result(0, 1).getInt(), 2);  // Count for group 42: 2 values
  EXPECT_EQ(result(1, 0), IntId(84));   // Group key 84
  EXPECT_EQ(result(1, 1).getInt(), 1);  // Count for group 84: 1 value
}
