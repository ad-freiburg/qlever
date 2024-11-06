//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/GroupByHashMapOptimization.h"

namespace {
auto I = ad_utility::testing::IntId;
auto D = ad_utility::testing::DoubleId;
}  // namespace

class GroupByHashMapOptimizationTest : public ::testing::Test {
 protected:
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

  auto makeCalcAndAddValue(auto& data) {
    auto calc = [this, &data]() { return calculate(data); };
    auto addValue = [this, &data](auto&& x) {
      data.addValue(AD_FWD(x), &context_);
    };
    return std::tuple{std::move(calc), std::move(addValue)};
  }

  std::string idToString(Id result) const {
    auto index = result.getLocalVocabIndex();
    auto resultString = localVocab_.getWord(index).toStringRepresentation();
    // Strip leading and trailing quotes.
    AD_CORRECTNESS_CHECK(resultString.size() >= 2);
    size_t endIndex = resultString.size() - 1;
    return std::move(resultString).substr(1, endIndex - 1);
  };

  Id idFromString(std::string string) {
    using ad_utility::triple_component::LiteralOrIri;
    auto literal = LiteralOrIri::literalWithoutQuotes(std::move(string));
    return Id::makeFromLocalVocabIndex(
        localVocab_.getIndexAndAddIfNotContained(std::move(literal)));
  };
};

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

  auto getResultString = [&]() { return idToString(calc()); };

  auto addString = [&](std::string string) {
    addValue(idFromString(std::move(string)));
  };

  EXPECT_EQ(getResultString(), "");
  addString("a");
  EXPECT_EQ(getResultString(), "a");
  addString("b");
  EXPECT_EQ(getResultString(), "a;b");
  addString("c");
  EXPECT_EQ(getResultString(), "a;b;c");

  data.reset();
  EXPECT_EQ(getResultString(), "");
  addString("a");
  EXPECT_EQ(getResultString(), "a");
  addValue(Id::makeUndefined());
  EXPECT_EQ(getResultString(), "a");
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
