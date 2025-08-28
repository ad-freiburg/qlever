//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/AllocatorTestHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/GroupByHashMapOptimization.h"
#include "engine/GroupByImpl.h"
#include "engine/idTable/IdTable.h"

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
};

// Small helper to build a two-column IdTable from pairs.
static IdTable make2(const std::vector<std::pair<int64_t, int64_t>>& rows,
                     AllocatorWithLimit<Id> alloc) {
  IdTable t{2, alloc};
  t.resize(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    t(i, 0) = IntId(rows[i].first);
    t(i, 1) = IntId(rows[i].second);
  }
  return t;
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
  std::vector<size_t> expectedA{0, 1, 0, 2};
  ASSERT_EQ(entriesA, expectedA);
  EXPECT_EQ(aggrData.getNumberOfGroups(), 3u);

  // Second block, onlyMatching: new keys should be reported as non-matching
  auto table_B = make2({{1, 1}, {3, 3}, {2, 2}, {4, 4}}, alloc_);
  AggrDataTwoCol::ArrayOrVector<ql::span<const Id>> spansB;
  spansB[0] = table_B.getColumn(0).subspan(0, table_B.size());
  spansB[1] = table_B.getColumn(1).subspan(0, table_B.size());
  auto [entriesB, nonmatchB] = aggrData.getHashEntries(spansB, true);
  // Expect two matches at positions 0 -> key (1,1) and 2 -> key (2,2)
  // Their indices correspond to the ones assigned before: (1,1)->0, (2,2)->2
  std::vector<size_t> expectedEntriesB{0, 2};
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
