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
};

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, AvgAggregationDataAggregatesCorrectly) {
  AvgAggregationData data;

  EXPECT_EQ(calculate(data), I(0));
  data.addValue(I(1), &context_);
  EXPECT_DOUBLE_EQ(calculate(data).getDouble(), 1);
  data.addValue(I(3), &context_);
  EXPECT_DOUBLE_EQ(calculate(data).getDouble(), 2);
  data.addValue(D(3), &context_);
  EXPECT_NEAR(calculate(data).getDouble(), 7 / 3.0, 0.00001);

  data.reset();
  EXPECT_EQ(calculate(data), I(0));
  data.addValue(I(2), &context_);
  EXPECT_EQ(calculate(data), D(2));

  data.addValue(Id::makeUndefined(), &context_);
  EXPECT_TRUE(calculate(data).isUndefined());

  data.reset();
  EXPECT_EQ(calculate(data), I(0));
  using ad_utility::triple_component::LiteralOrIri;
  auto literal = LiteralOrIri::literalWithoutQuotes("non-numeric value");
  auto id = Id::makeFromLocalVocabIndex(
      localVocab_.getIndexAndAddIfNotContained(std::move(literal)));
  data.addValue(id, &context_);
  EXPECT_TRUE(calculate(data).isUndefined());
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest,
       CountAggregationDataAggregatesCorrectly) {
  CountAggregationData data;

  EXPECT_EQ(calculate(data), I(0));
  data.addValue(I(1), &context_);
  EXPECT_EQ(calculate(data), I(1));
  data.addValue(I(3), &context_);
  EXPECT_EQ(calculate(data), I(2));
  data.addValue(D(3), &context_);
  EXPECT_EQ(calculate(data), I(3));

  data.reset();
  EXPECT_EQ(calculate(data), I(0));
  data.addValue(Id::makeFromBool(false), &context_);
  EXPECT_EQ(calculate(data), I(1));

  data.addValue(Id::makeUndefined(), &context_);
  EXPECT_EQ(calculate(data), I(1));
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, MinAggregationDataAggregatesCorrectly) {
  MinAggregationData data;

  EXPECT_TRUE(calculate(data).isUndefined());
  data.addValue(I(1), &context_);
  EXPECT_EQ(calculate(data), I(1));
  data.addValue(I(3), &context_);
  EXPECT_EQ(calculate(data), I(1));
  data.addValue(D(1), &context_);
  EXPECT_EQ(calculate(data), I(1));
  data.addValue(D(0), &context_);
  EXPECT_EQ(calculate(data), D(0));

  data.reset();
  EXPECT_TRUE(calculate(data).isUndefined());
  data.addValue(Id::makeFromBool(true), &context_);
  EXPECT_EQ(calculate(data), Id::makeFromBool(true));

  // undefined < everything
  data.addValue(Id::makeUndefined(), &context_);
  EXPECT_TRUE(calculate(data).isUndefined());
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, MaxAggregationDataAggregatesCorrectly) {
  MaxAggregationData data;

  EXPECT_TRUE(calculate(data).isUndefined());
  data.addValue(I(1), &context_);
  EXPECT_EQ(calculate(data), I(1));
  data.addValue(I(3), &context_);
  EXPECT_EQ(calculate(data), I(3));
  data.addValue(D(0), &context_);
  EXPECT_EQ(calculate(data), I(3));
  data.addValue(D(4), &context_);
  EXPECT_EQ(calculate(data), D(4));

  data.reset();
  EXPECT_TRUE(calculate(data).isUndefined());
  data.addValue(Id::makeFromBool(false), &context_);
  EXPECT_EQ(calculate(data), Id::makeFromBool(false));

  // undefined < everything
  data.addValue(Id::makeUndefined(), &context_);
  EXPECT_EQ(calculate(data), Id::makeFromBool(false));
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, SumAggregationDataAggregatesCorrectly) {
  SumAggregationData data;

  EXPECT_EQ(calculate(data), I(0));
  data.addValue(I(1), &context_);
  EXPECT_EQ(calculate(data), I(1));
  data.addValue(I(3), &context_);
  EXPECT_EQ(calculate(data), I(4));
  data.addValue(D(1), &context_);
  EXPECT_DOUBLE_EQ(calculate(data).getDouble(), 5);
  data.addValue(D(0), &context_);
  EXPECT_DOUBLE_EQ(calculate(data).getDouble(), 5);

  data.reset();
  EXPECT_EQ(calculate(data), I(0));
  data.addValue(Id::makeFromBool(true), &context_);
  EXPECT_EQ(calculate(data), I(1));
  data.addValue(Id::makeFromBool(false), &context_);
  EXPECT_EQ(calculate(data), I(1));

  data.addValue(Id::makeUndefined(), &context_);
  EXPECT_TRUE(calculate(data).isUndefined());
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest,
       GroupConcatAggregationDataAggregatesCorrectly) {
  GroupConcatAggregationData data{";"};

  auto getResultString = [&]() {
    auto result = calculate(data);
    auto index = result.getLocalVocabIndex();
    auto resultString = localVocab_.getWord(index).toStringRepresentation();
    // Strip leading and trailing quotes.
    AD_CORRECTNESS_CHECK(resultString.size() >= 2);
    size_t endIndex = resultString.size() - 1;
    return std::move(resultString).substr(1, endIndex - 1);
  };

  auto addString = [&](std::string string) {
    using ad_utility::triple_component::LiteralOrIri;
    auto literal = LiteralOrIri::literalWithoutQuotes(std::move(string));
    auto id = Id::makeFromLocalVocabIndex(
        localVocab_.getIndexAndAddIfNotContained(std::move(literal)));
    data.addValue(id, &context_);
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
  data.addValue(Id::makeUndefined(), &context_);
  EXPECT_EQ(getResultString(), "a");
}
