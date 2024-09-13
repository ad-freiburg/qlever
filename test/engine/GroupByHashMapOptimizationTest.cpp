//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/IndexTestHelpers.h"
#include "engine/GroupByHashMapOptimization.h"

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
};

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, AvgAggregationDataAggregatesCorrectly) {
  AvgAggregationData data;

  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(0));
  data.addValue(Id::makeFromInt(1), &context_);
  EXPECT_DOUBLE_EQ(data.calculateResult(&localVocab_).getDouble(), 1);
  data.addValue(Id::makeFromInt(3), &context_);
  EXPECT_DOUBLE_EQ(data.calculateResult(&localVocab_).getDouble(), 2);
  data.addValue(Id::makeFromDouble(3), &context_);
  EXPECT_NEAR(data.calculateResult(&localVocab_).getDouble(), 7 / 3.0, 0.00001);

  data.reset();
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(0));
  data.addValue(Id::makeFromInt(2), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), ValueId::makeFromDouble(2));

  data.addValue(Id::makeUndefined(), &context_);
  EXPECT_TRUE(data.calculateResult(&localVocab_).isUndefined());
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest,
       CountAggregationDataAggregatesCorrectly) {
  CountAggregationData data;

  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(0));
  data.addValue(Id::makeFromInt(1), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(1));
  data.addValue(Id::makeFromInt(3), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(2));
  data.addValue(Id::makeFromDouble(3), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(3));

  data.reset();
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(0));
  data.addValue(Id::makeFromBool(false), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(1));

  data.addValue(Id::makeUndefined(), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(1));
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, MinAggregationDataAggregatesCorrectly) {
  MinAggregationData data;

  EXPECT_TRUE(data.calculateResult(&localVocab_).isUndefined());
  data.addValue(Id::makeFromInt(1), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(1));
  data.addValue(Id::makeFromInt(3), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(1));
  data.addValue(Id::makeFromDouble(1), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(1));
  data.addValue(Id::makeFromDouble(0), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromDouble(0));

  data.reset();
  EXPECT_TRUE(data.calculateResult(&localVocab_).isUndefined());
  data.addValue(Id::makeFromBool(true), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromBool(true));

  // undefined < everything
  data.addValue(Id::makeUndefined(), &context_);
  EXPECT_TRUE(data.calculateResult(&localVocab_).isUndefined());
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, MaxAggregationDataAggregatesCorrectly) {
  MaxAggregationData data;

  EXPECT_TRUE(data.calculateResult(&localVocab_).isUndefined());
  data.addValue(Id::makeFromInt(1), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(1));
  data.addValue(Id::makeFromInt(3), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(3));
  data.addValue(Id::makeFromDouble(0), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(3));
  data.addValue(Id::makeFromDouble(4), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromDouble(4));

  data.reset();
  EXPECT_TRUE(data.calculateResult(&localVocab_).isUndefined());
  data.addValue(Id::makeFromBool(false), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromBool(false));

  // undefined < everything
  data.addValue(Id::makeUndefined(), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromBool(false));
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest, SumAggregationDataAggregatesCorrectly) {
  SumAggregationData data;

  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(0));
  data.addValue(Id::makeFromInt(1), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(1));
  data.addValue(Id::makeFromInt(3), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(4));
  data.addValue(Id::makeFromDouble(1), &context_);
  EXPECT_DOUBLE_EQ(data.calculateResult(&localVocab_).getDouble(), 5);
  data.addValue(Id::makeFromDouble(0), &context_);
  EXPECT_DOUBLE_EQ(data.calculateResult(&localVocab_).getDouble(), 5);

  data.reset();
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(0));
  data.addValue(Id::makeFromBool(true), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(1));
  data.addValue(Id::makeFromBool(false), &context_);
  EXPECT_EQ(data.calculateResult(&localVocab_), Id::makeFromInt(1));

  data.addValue(Id::makeUndefined(), &context_);
  EXPECT_TRUE(data.calculateResult(&localVocab_).isUndefined());
}

// _____________________________________________________________________________
TEST_F(GroupByHashMapOptimizationTest,
       GroupConcatAggregationDataAggregatesCorrectly) {
  GroupConcatAggregationData data{";"};

  auto getResultString = [&]() {
    auto result = data.calculateResult(&localVocab_);
    auto index = result.getLocalVocabIndex();
    auto resultString = localVocab_.getWord(index).toStringRepresentation();
    // Strip leading and trailing quotes.
    AD_CORRECTNESS_CHECK(resultString.size() >= 2);
    size_t endIndex = resultString.size() - 1;
    return std::move(resultString).substr(1, endIndex - 1);
  };

  auto makeString = [&](std::string string) {
    using ad_utility::triple_component::LiteralOrIri;
    auto literal = LiteralOrIri::literalWithoutQuotes(std::move(string));
    return Id::makeFromLocalVocabIndex(
        localVocab_.getIndexAndAddIfNotContained(std::move(literal)));
  };

  EXPECT_EQ(getResultString(), "");
  data.addValue(makeString("a"), &context_);
  EXPECT_EQ(getResultString(), "a");
  data.addValue(makeString("b"), &context_);
  EXPECT_EQ(getResultString(), "a;b");
  data.addValue(makeString("c"), &context_);
  EXPECT_EQ(getResultString(), "a;b;c");

  data.reset();
  EXPECT_EQ(getResultString(), "");
  data.addValue(makeString("a"), &context_);
  EXPECT_EQ(getResultString(), "a");
  data.addValue(Id::makeUndefined(), &context_);
  EXPECT_EQ(getResultString(), "a");
}
