//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <vector>

#include "./SparqlExpressionTestHelpers.h"
#include "index/CompressedBlockPrefiltering.h"
#include "util/IdTestHelpers.h"

using ad_utility::testing::DoubleId;
using ad_utility::testing::IntId;
using ad_utility::testing::LocalVocabId;
using ad_utility::testing::UndefId;
using ad_utility::testing::VocabId;
using sparqlExpression::TestContext;
using namespace prefilterExpressions;

//______________________________________________________________________________
struct MetadataBlocks {
  /*
  Our pre-filtering procedure expects blocks that are in correct (ascending)
  order w.r.t. their contained ValueIds given the first and last triple.

  The correct order of the ValueIds is dependent on their type and underlying
  representation.

  Short overview on the ascending order logic for the underlying values:
  Order ValueIds for (signed) integer values - [0... max, -max... -1]
  Order ValueIds for (signed) doubles values - [0.0... max, -0.0... -max]
  Order ValueIds for Vocab and LocalVocab values given the vocabulary with
  indices (up to N) - [VocabId(0), .... VocabId(N)]

  COLUMN 0 and COLUMN 1 contain fixed values, this is a necessary condition
  that is also checked during the pre-filtering procedure. The actual evaluation
  column (we filter w.r.t. values of COLUMN 2) contains mixed types.
  */

  // Values for fixed columns
  const Id VocabId10 = VocabId(10);
  const Id DoubleId33 = DoubleId(33);

  auto makeBlock(const ValueId& firstId, const ValueId& lastId)
      -> BlockMetadata {
    return {{},
            0,
            // COLUMN 0  |  COLUMN 1  |  COLUMN 2
            {VocabId10, DoubleId33, firstId},  // firstTriple
            {VocabId10, DoubleId33, lastId},   // lastTriple
            {},
            false};
  };

  BlockMetadata b1 = makeBlock(IntId(0), IntId(0));
  BlockMetadata b2 = makeBlock(IntId(0), IntId(5));
  BlockMetadata b3 = makeBlock(IntId(5), IntId(6));
  BlockMetadata b4 = makeBlock(IntId(8), IntId(9));
  BlockMetadata b5 = makeBlock(IntId(-10), IntId(-8));
  BlockMetadata b6 = makeBlock(IntId(-4), IntId(-4));
  // b7 contains mixed datatypes in COLUMN 2
  BlockMetadata b7 = makeBlock(IntId(-4), DoubleId(2));
  BlockMetadata b8 = makeBlock(DoubleId(2), DoubleId(2));
  BlockMetadata b9 = makeBlock(DoubleId(4), DoubleId(4));
  BlockMetadata b10 = makeBlock(DoubleId(4), DoubleId(10));
  BlockMetadata b11 = makeBlock(DoubleId(-1.23), DoubleId(-6.25));
  BlockMetadata b12 = makeBlock(DoubleId(-6.25), DoubleId(-6.25));
  BlockMetadata b13 = makeBlock(DoubleId(-10.42), DoubleId(-12.00));
  // b14 contains mixed datatypes
  BlockMetadata b14 = makeBlock(DoubleId(-14.01), VocabId(0));
  BlockMetadata b15 = makeBlock(VocabId(10), VocabId(14));
  BlockMetadata b16 = makeBlock(VocabId(14), VocabId(14));
  BlockMetadata b17 = makeBlock(VocabId(14), VocabId(17));
  std::vector<BlockMetadata> blocks = {b1,  b2,  b3,  b4,  b5,  b6,
                                       b7,  b8,  b9,  b10, b11, b12,
                                       b13, b14, b15, b16, b17};

  // The following blocks will be swapped with their respective correct block,
  // to test if the method evaluate checks the pre-conditions properly.
  BlockMetadata b1_1{{},
                     0,
                     {VocabId10, DoubleId33, IntId(0)},
                     {VocabId10, DoubleId(22), IntId(0)},
                     {},
                     false};
  std::vector<BlockMetadata> blocksInvalidCol1 = {b1_1, b2,  b3,  b4,  b5,  b6,
                                                  b7,   b8,  b9,  b10, b11, b12,
                                                  b13,  b14, b15, b16, b17};
  BlockMetadata b5_1{{},
                     0,
                     {VocabId(11), DoubleId33, IntId(-10)},
                     {VocabId10, DoubleId33, IntId(-8)},
                     {},
                     false};
  std::vector<BlockMetadata> blocksInvalidCol02 = {
      b1,  b2,  b3,  b4,  b5_1, b6,  b7,  b8, b9,
      b10, b11, b12, b13, b14,  b15, b16, b17};
  std::vector<BlockMetadata> blocksInvalidOrder1 = {
      b2,  b1,  b3,  b4,  b5,  b6,  b7,  b8, b9,
      b10, b11, b12, b13, b14, b15, b16, b17};
  std::vector<BlockMetadata> blocksInvalidOrder2 = {
      b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8, b9,
      b10, b11, b12, b14, b13, b15, b16, b17};
  std::vector<BlockMetadata> blocksWithDuplicate1 = {
      b1, b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,
      b9, b10, b11, b12, b13, b14, b15, b16, b17};
  std::vector<BlockMetadata> blocksWithDuplicate2 = {
      b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,
      b10, b11, b12, b13, b14, b15, b16, b17, b17};
};

// Static tests, they focus on corner case values for the given block triples.
//______________________________________________________________________________
//______________________________________________________________________________
// Test PrefilterExpressions

template <typename T>
struct check_is : std::false_type {};

template <LogicalOperators Op>
struct check_is<LogicalExpression<Op>> : std::true_type {};
template <CompOp Op>
struct check_is<RelationalExpression<Op>> : std::true_type {};

template <typename T>
constexpr bool check_is_logical_v = check_is<T>::value;
template <typename T>
constexpr bool check_is_relational_v = check_is<T>::value;

//______________________________________________________________________________
static const auto testThrowError =
    [](std::unique_ptr<PrefilterExpression> expression, size_t evaluationColumn,
       const std::vector<BlockMetadata>& input,
       const std::string& expectedErrorMessage) {
      try {
        expression->evaluate(input, evaluationColumn);
        FAIL() << "Expected thrown error message.";
      } catch (const std::runtime_error& error) {
        EXPECT_EQ(std::string(error.what()), expectedErrorMessage);
      } catch (...) {
        FAIL() << "Expected an error of type std::runtime_error, but a "
                  "different error was thrown.";
      }
    };

//______________________________________________________________________________
template <typename RelExpr, typename ResT>
struct TestRelationalExpression {
  void operator()(size_t evaluationColumn, ValueId referenceId,
                  const std::vector<BlockMetadata>& input, const ResT& expected)
      requires check_is_relational_v<RelExpr> {
    auto expression = std::make_unique<RelExpr>(referenceId);
    if constexpr (std::is_same_v<ResT, std::string>) {
      testThrowError(std::move(expression), evaluationColumn, input, expected);
    } else {
      static_assert(std::is_same_v<ResT, std::vector<BlockMetadata>>);
      EXPECT_EQ(expression->evaluate(input, evaluationColumn), expected);
    }
  }
};

//______________________________________________________________________________
template <typename LogExpr, typename ResT>
struct TestLogicalExpression {
  template <typename RelExpr1, typename RelExpr2>
  void test(size_t evaluationColumn, ValueId referenceIdChild1,
            ValueId referenceIdChild2, const std::vector<BlockMetadata>& input,
            const ResT& expected)
      requires(check_is_logical_v<LogExpr> && check_is_relational_v<RelExpr1> &&
               check_is_relational_v<RelExpr2>) {
    auto expression = std::make_unique<LogExpr>(
        std::make_unique<RelExpr1>(referenceIdChild1),
        std::make_unique<RelExpr2>(referenceIdChild2));
    if constexpr (std::is_same_v<ResT, std::string>) {
      testThrowError(std::move(expression), evaluationColumn, input, expected);
    } else {
      static_assert(std::is_same_v<ResT, std::vector<BlockMetadata>>);
      EXPECT_EQ(expression->evaluate(input, evaluationColumn), expected);
    }
  }
};

//______________________________________________________________________________
template <typename ResT>
struct TestNotExpression {
  template <typename RelExpr>
  void test(size_t evaluationColumn, ValueId referenceIdChild,
            const std::vector<BlockMetadata>& input, const ResT& expected)
      requires check_is_relational_v<RelExpr> {
    auto expression = std::make_unique<NotExpression>(
        std::make_unique<RelExpr>(referenceIdChild));
    if constexpr (std::is_same_v<ResT, std::string>) {
      testThrowError(std::move(expression), evaluationColumn, input, expected);
    } else {
      static_assert(std::is_same_v<ResT, std::vector<BlockMetadata>>);
      EXPECT_EQ(expression->evaluate(input, evaluationColumn), expected);
    }
  }
};

//______________________________________________________________________________
// Test LessThanExpression
TEST(RelationalExpression, testLessThanExpressions) {
  MetadataBlocks blocks{};
  TestRelationalExpression<LessThanExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression(
      2, IntId(5), blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b5, blocks.b6, blocks.b7, blocks.b8,
       blocks.b9, blocks.b10, blocks.b11, blocks.b12, blocks.b13, blocks.b14});

  testExpression(2, IntId(-12), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression(2, IntId(0), blocks.blocks,
                 {blocks.b5, blocks.b6, blocks.b7, blocks.b11, blocks.b12,
                  blocks.b13, blocks.b14});
  testExpression(2, IntId(100), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5,
                  blocks.b6, blocks.b7, blocks.b8, blocks.b9, blocks.b10,
                  blocks.b11, blocks.b12, blocks.b13, blocks.b14});
  testExpression(2, DoubleId(-3), blocks.blocks,
                 {blocks.b5, blocks.b6, blocks.b7, blocks.b11, blocks.b12,
                  blocks.b13, blocks.b14});
  testExpression(2, DoubleId(-14.01), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression(2, DoubleId(-11.22), blocks.blocks,
                 {blocks.b7, blocks.b13, blocks.b14});
  testExpression(
      2, DoubleId(-4.121), blocks.blocks,
      {blocks.b5, blocks.b7, blocks.b11, blocks.b12, blocks.b13, blocks.b14});
  testExpression(2, VocabId(0), blocks.blocks, {blocks.b14});
  testExpression(2, VocabId(12), blocks.blocks, {blocks.b14, blocks.b15});
  testExpression(2, VocabId(14), blocks.blocks, {blocks.b14, blocks.b15});
  testExpression(2, VocabId(16), blocks.blocks,
                 {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
}

//______________________________________________________________________________
// Test LessEqualExpression
TEST(RelationalExpression, testLessEqualExpressions) {
  MetadataBlocks blocks{};
  TestRelationalExpression<LessEqualExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression(2, IntId(0), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b5, blocks.b6, blocks.b7,
                  blocks.b11, blocks.b12, blocks.b13, blocks.b14});
  testExpression(
      2, IntId(-6), blocks.blocks,
      {blocks.b5, blocks.b7, blocks.b11, blocks.b12, blocks.b13, blocks.b14});
  testExpression(2, IntId(7), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b5, blocks.b6,
                  blocks.b7, blocks.b8, blocks.b9, blocks.b10, blocks.b11,
                  blocks.b12, blocks.b13, blocks.b14});
  testExpression(2, IntId(-9), blocks.blocks,
                 {blocks.b5, blocks.b7, blocks.b13, blocks.b14});
  testExpression(2, DoubleId(-9.131), blocks.blocks,
                 {blocks.b5, blocks.b7, blocks.b13, blocks.b14});
  testExpression(2, DoubleId(1.1415), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b5, blocks.b6, blocks.b7,
                  blocks.b11, blocks.b12, blocks.b13, blocks.b14});
  testExpression(2, DoubleId(3.1415), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b5, blocks.b6, blocks.b7,
                  blocks.b8, blocks.b11, blocks.b12, blocks.b13, blocks.b14});
  testExpression(2, DoubleId(-11.99999999999999), blocks.blocks,
                 {blocks.b7, blocks.b13, blocks.b14});
  testExpression(2, DoubleId(-14.03), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression(2, VocabId(0), blocks.blocks, {blocks.b14});
  testExpression(2, VocabId(11), blocks.blocks, {blocks.b14, blocks.b15});
  testExpression(2, VocabId(14), blocks.blocks,
                 {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
}

//______________________________________________________________________________
// Test GreaterThanExpression
TEST(RelationalExpression, testGreaterThanExpression) {
  MetadataBlocks blocks{};
  TestRelationalExpression<GreaterThanExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression(2, DoubleId(5.5375), blocks.blocks,
                 {blocks.b3, blocks.b4, blocks.b7, blocks.b10, blocks.b14});
  testExpression(2, DoubleId(9.9994), blocks.blocks,
                 {blocks.b7, blocks.b10, blocks.b14});
  testExpression(
      2, IntId(-5), blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b6, blocks.b7,
       blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b14});
  testExpression(
      2, DoubleId(-5.5375), blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b6, blocks.b7,
       blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b14});
  testExpression(
      2, DoubleId(-6.2499999), blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b6, blocks.b7,
       blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b14});
  testExpression(2, IntId(1), blocks.blocks,
                 {blocks.b2, blocks.b3, blocks.b4, blocks.b7, blocks.b8,
                  blocks.b9, blocks.b10, blocks.b14});
  testExpression(2, IntId(3), blocks.blocks,
                 {blocks.b2, blocks.b3, blocks.b4, blocks.b7, blocks.b9,
                  blocks.b10, blocks.b14});
  testExpression(
      2, IntId(4), blocks.blocks,
      {blocks.b2, blocks.b3, blocks.b4, blocks.b7, blocks.b10, blocks.b14});
  testExpression(2, IntId(-4), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b7,
                  blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b14});
  testExpression(2, IntId(33), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression(2, VocabId(22), blocks.blocks, {blocks.b14});
  testExpression(2, VocabId(14), blocks.blocks, {blocks.b14, blocks.b17});
  testExpression(2, VocabId(12), blocks.blocks,
                 {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
}

//______________________________________________________________________________
// Test GreaterEqualExpression
TEST(RelationalExpression, testGreaterEqualExpression) {
  MetadataBlocks blocks{};
  TestRelationalExpression<GreaterEqualExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression(2, IntId(0), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b7,
                  blocks.b8, blocks.b9, blocks.b10, blocks.b14});
  testExpression(2, IntId(8), blocks.blocks,
                 {blocks.b4, blocks.b7, blocks.b10, blocks.b14});
  testExpression(2, DoubleId(9.98), blocks.blocks,
                 {blocks.b7, blocks.b10, blocks.b14});
  testExpression(2, IntId(-3), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b7,
                  blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b14});
  testExpression(2, IntId(-10), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5,
                  blocks.b6, blocks.b7, blocks.b8, blocks.b9, blocks.b10,
                  blocks.b11, blocks.b12, blocks.b14});
  testExpression(2, DoubleId(-3.1415), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b7,
                  blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b14});
  testExpression(
      2, DoubleId(-4.000001), blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b6, blocks.b7,
       blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b14});
  testExpression(2, DoubleId(10.000), blocks.blocks,
                 {blocks.b7, blocks.b10, blocks.b14});
  testExpression(2, DoubleId(-15.22), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5,
                  blocks.b6, blocks.b7, blocks.b8, blocks.b9, blocks.b10,
                  blocks.b11, blocks.b12, blocks.b13, blocks.b14});
  testExpression(2, DoubleId(7.999999), blocks.blocks,
                 {blocks.b4, blocks.b7, blocks.b10, blocks.b14});
  testExpression(2, DoubleId(10.0001), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression(2, VocabId(14), blocks.blocks,
                 {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression(2, VocabId(10), blocks.blocks,
                 {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression(2, VocabId(17), blocks.blocks, {blocks.b14, blocks.b17});
}

//______________________________________________________________________________
// Test EqualExpression
TEST(RelationalExpression, testEqualExpression) {
  MetadataBlocks blocks{};
  TestRelationalExpression<EqualExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression(2, IntId(0), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b7, blocks.b14});
  testExpression(2, IntId(5), blocks.blocks,
                 {blocks.b2, blocks.b3, blocks.b7, blocks.b10, blocks.b14});
  testExpression(2, IntId(22), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression(2, IntId(-10), blocks.blocks,
                 {blocks.b5, blocks.b7, blocks.b14});
  testExpression(2, DoubleId(-6.25), blocks.blocks,
                 {blocks.b7, blocks.b11, blocks.b12, blocks.b14});
  testExpression(2, IntId(-11), blocks.blocks,
                 {blocks.b7, blocks.b13, blocks.b14});
  testExpression(2, DoubleId(-14.02), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression(2, DoubleId(-0.001), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression(2, DoubleId(0), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b7, blocks.b14});
  testExpression(2, IntId(2), blocks.blocks,
                 {blocks.b2, blocks.b7, blocks.b8, blocks.b14});
  testExpression(2, DoubleId(5.5), blocks.blocks,
                 {blocks.b3, blocks.b7, blocks.b10, blocks.b14});
  testExpression(2, DoubleId(1.5), blocks.blocks,
                 {blocks.b2, blocks.b7, blocks.b14});
  testExpression(2, VocabId(1), blocks.blocks, {blocks.b14});
  testExpression(2, VocabId(14), blocks.blocks,
                 {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression(2, VocabId(11), blocks.blocks, {blocks.b14, blocks.b15});
  testExpression(2, VocabId(17), blocks.blocks, {blocks.b14, blocks.b17});
  testExpression(2, IntId(-4), blocks.blocks,
                 {blocks.b6, blocks.b7, blocks.b11, blocks.b14});
}

//______________________________________________________________________________
// Test NotEqualExpression
TEST(RelationalExpression, testNotEqualExpression) {
  MetadataBlocks blocks{};
  TestRelationalExpression<NotEqualExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression(2, DoubleId(0.00), blocks.blocks,
                 {blocks.b2, blocks.b3, blocks.b4, blocks.b5, blocks.b6,
                  blocks.b7, blocks.b8, blocks.b9, blocks.b10, blocks.b11,
                  blocks.b12, blocks.b13, blocks.b14});
  testExpression(2, IntId(-4), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5,
                  blocks.b7, blocks.b8, blocks.b9, blocks.b10, blocks.b11,
                  blocks.b12, blocks.b13, blocks.b14});
  testExpression(2, DoubleId(0.001), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5,
                  blocks.b6, blocks.b7, blocks.b8, blocks.b9, blocks.b10,
                  blocks.b11, blocks.b12, blocks.b13, blocks.b14});
  testExpression(2, IntId(2), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5,
                  blocks.b6, blocks.b7, blocks.b9, blocks.b10, blocks.b11,
                  blocks.b12, blocks.b13, blocks.b14});
  testExpression(2, DoubleId(-6.2500), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5,
                  blocks.b6, blocks.b7, blocks.b8, blocks.b9, blocks.b10,
                  blocks.b11, blocks.b13, blocks.b14});
  testExpression(2, IntId(5), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5,
                  blocks.b6, blocks.b7, blocks.b8, blocks.b9, blocks.b10,
                  blocks.b11, blocks.b12, blocks.b13, blocks.b14});
  testExpression(2, DoubleId(-101.23), blocks.blocks,
                 {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5,
                  blocks.b6, blocks.b7, blocks.b8, blocks.b9, blocks.b10,
                  blocks.b11, blocks.b12, blocks.b13, blocks.b14});
  testExpression(2, VocabId(0), blocks.blocks,
                 {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression(2, VocabId(7), blocks.blocks,
                 {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression(2, VocabId(14), blocks.blocks,
                 {blocks.b14, blocks.b15, blocks.b17});
  testExpression(2, VocabId(17), blocks.blocks,
                 {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
}

//______________________________________________________________________________
//______________________________________________________________________________
// Test Logical Expressions

// Test AndExpression
TEST(LogicalExpression, testAndExpression) {
  MetadataBlocks blocks{};
  TestLogicalExpression<AndExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression.test<GreaterEqualExpression, GreaterThanExpression>(
      2, VocabId(10), VocabId(10), blocks.blocks,
      {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression.test<GreaterEqualExpression, GreaterEqualExpression>(
      2, VocabId(0), VocabId(17), blocks.blocks, {blocks.b14, blocks.b17});
  testExpression.test<GreaterEqualExpression, GreaterThanExpression>(
      2, VocabId(12), VocabId(17), blocks.blocks, {blocks.b14});
  testExpression.test<GreaterEqualExpression, LessThanExpression>(
      2, VocabId(10), VocabId(14), blocks.blocks, {blocks.b14, blocks.b15});
  testExpression.test<LessEqualExpression, LessThanExpression>(
      2, VocabId(0), VocabId(10), blocks.blocks, {blocks.b14});
  testExpression.test<LessEqualExpression, LessThanExpression>(
      2, VocabId(17), VocabId(17), blocks.blocks,
      {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression.test<GreaterEqualExpression, LessThanExpression>(
      2, DoubleId(-6.25), IntId(-7), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression.test<GreaterThanExpression, LessThanExpression>(
      2, DoubleId(-6.25), DoubleId(-6.25), blocks.blocks,
      {blocks.b7, blocks.b14});
  // Corner case: Logically it is impossible to satisfy (x > 0) and (x < 0) at
  // the same time. But given that we evaluate on block boundaries and their
  // possible values (Ids) in between, block b7 satisfies both conditions over
  // its range [IntId(-4)... DoubleId(2)] for column 2.
  testExpression.test<GreaterThanExpression, LessThanExpression>(
      2, IntId(0), IntId(0), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression.test<GreaterEqualExpression, LessThanExpression>(
      2, IntId(-10), DoubleId(0.00), blocks.blocks,
      {blocks.b5, blocks.b6, blocks.b7, blocks.b11, blocks.b12, blocks.b14});
  // Also a corner case.
  testExpression.test<GreaterThanExpression, EqualExpression>(
      2, IntId(0), DoubleId(0), blocks.blocks,
      {blocks.b2, blocks.b7, blocks.b14});
  testExpression.test<GreaterEqualExpression, EqualExpression>(
      2, IntId(0), IntId(0), blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b7, blocks.b14});
  testExpression.test<GreaterThanExpression, GreaterEqualExpression>(
      2, DoubleId(-34.23), DoubleId(15.1), blocks.blocks,
      {blocks.b7, blocks.b14});
  testExpression.test<LessThanExpression, LessEqualExpression>(
      2, IntId(0), DoubleId(-4), blocks.blocks,
      {blocks.b5, blocks.b6, blocks.b7, blocks.b11, blocks.b12, blocks.b13,
       blocks.b14});
  testExpression.test<NotEqualExpression, NotEqualExpression>(
      2, IntId(0), IntId(-4), blocks.blocks,
      {blocks.b2, blocks.b3, blocks.b4, blocks.b5, blocks.b7, blocks.b8,
       blocks.b9, blocks.b10, blocks.b11, blocks.b12, blocks.b13, blocks.b14});
  testExpression.test<NotEqualExpression, EqualExpression>(
      2, DoubleId(-3.1415), DoubleId(4.5), blocks.blocks,
      {blocks.b2, blocks.b7, blocks.b10, blocks.b14});
  testExpression.test<NotEqualExpression, LessThanExpression>(
      2, DoubleId(-6.25), IntId(0), blocks.blocks,
      {blocks.b5, blocks.b6, blocks.b7, blocks.b11, blocks.b13, blocks.b14});
  testExpression.test<LessEqualExpression, GreaterEqualExpression>(
      2, DoubleId(-4), DoubleId(1), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression.test<LessEqualExpression, EqualExpression>(
      2, DoubleId(-2), IntId(-3), blocks.blocks,
      {blocks.b7, blocks.b11, blocks.b14});
}

//______________________________________________________________________________
// Test OrExpression
TEST(LogicalExpression, testOrExpression) {
  MetadataBlocks blocks{};
  TestLogicalExpression<OrExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression.test<LessThanExpression, LessEqualExpression>(
      2, VocabId(22), VocabId(0), blocks.blocks,
      {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression.test<LessEqualExpression, GreaterEqualExpression>(
      2, VocabId(0), VocabId(16), blocks.blocks, {blocks.b14, blocks.b17});
  testExpression.test<GreaterThanExpression, GreaterEqualExpression>(
      2, VocabId(17), VocabId(242), blocks.blocks, {blocks.b14});
  testExpression.test<LessThanExpression, EqualExpression>(
      2, DoubleId(-5.95), VocabId(14), blocks.blocks,
      {blocks.b5, blocks.b7, blocks.b11, blocks.b12, blocks.b13, blocks.b14,
       blocks.b15, blocks.b16, blocks.b17});
  testExpression.test<EqualExpression, NotEqualExpression>(
      2, DoubleId(0), VocabId(14), blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b7, blocks.b14, blocks.b15, blocks.b17});
  testExpression.test<EqualExpression, EqualExpression>(
      2, DoubleId(0.0), DoubleId(-6.25), blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b7, blocks.b11, blocks.b12, blocks.b14});
  testExpression.test<EqualExpression, LessThanExpression>(
      2, DoubleId(-11.99), DoubleId(-15.22), blocks.blocks,
      {blocks.b7, blocks.b13, blocks.b14});
  testExpression.test<GreaterEqualExpression, LessThanExpression>(
      2, DoubleId(7.99), DoubleId(-7.99), blocks.blocks,
      {blocks.b4, blocks.b5, blocks.b7, blocks.b10, blocks.b13, blocks.b14});
  testExpression.test<GreaterThanExpression, EqualExpression>(
      2, IntId(-15), IntId(2), blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5, blocks.b6,
       blocks.b7, blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b12,
       blocks.b13, blocks.b14});
  testExpression.test<EqualExpression, EqualExpression>(
      2, IntId(0), IntId(-4), blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b6, blocks.b7, blocks.b11, blocks.b14});
  testExpression.test<NotEqualExpression, EqualExpression>(
      2, VocabId(14), IntId(2), blocks.blocks,
      {blocks.b2, blocks.b7, blocks.b8, blocks.b14, blocks.b15, blocks.b17});
  testExpression.test<LessThanExpression, GreaterEqualExpression>(
      2, DoubleId(-1), IntId(1), blocks.blocks,
      {blocks.b2, blocks.b3, blocks.b4, blocks.b5, blocks.b6, blocks.b7,
       blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b12, blocks.b13,
       blocks.b14});
  testExpression.test<LessEqualExpression, EqualExpression>(
      2, DoubleId(-4), IntId(-4), blocks.blocks,
      {blocks.b5, blocks.b6, blocks.b7, blocks.b11, blocks.b12, blocks.b13,
       blocks.b14});
}

//______________________________________________________________________________
// Test NotExpression
TEST(LogicalExpression, testNotExpression) {
  MetadataBlocks blocks{};
  TestNotExpression<std::vector<BlockMetadata>> testExpression{};
  testExpression.test<EqualExpression>(
      2, VocabId(2), blocks.blocks,
      {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression.test<EqualExpression>(2, VocabId(14), blocks.blocks,
                                       {blocks.b14, blocks.b15, blocks.b17});
  testExpression.test<NotEqualExpression>(
      2, VocabId(14), blocks.blocks,
      {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression.test<EqualExpression>(
      2, VocabId(0), blocks.blocks,
      {blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression.test<LessThanExpression>(
      2, DoubleId(-14.01), blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5, blocks.b6,
       blocks.b7, blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b12,
       blocks.b13, blocks.b14});
  testExpression.test<GreaterEqualExpression>(
      2, DoubleId(-14.01), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression.test<GreaterThanExpression>(
      2, DoubleId(-4.00), blocks.blocks,
      {blocks.b5, blocks.b6, blocks.b7, blocks.b11, blocks.b12, blocks.b13,
       blocks.b14});
  testExpression.test<GreaterEqualExpression>(2, DoubleId(-24.4), blocks.blocks,
                                              {blocks.b7, blocks.b14});
  testExpression.test<LessEqualExpression>(
      2, IntId(0), blocks.blocks,
      {blocks.b2, blocks.b3, blocks.b4, blocks.b7, blocks.b8, blocks.b9,
       blocks.b10, blocks.b14});
  testExpression.test<EqualExpression>(
      2, DoubleId(-6.25), blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5, blocks.b6,
       blocks.b7, blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b13,
       blocks.b14});
  testExpression.test<NotEqualExpression>(
      2, DoubleId(4), blocks.blocks,
      {blocks.b2, blocks.b7, blocks.b9, blocks.b10, blocks.b14});
  testExpression.test<GreaterThanExpression>(
      2, DoubleId(0), blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b5, blocks.b6, blocks.b7, blocks.b11,
       blocks.b12, blocks.b13, blocks.b14});
}

//______________________________________________________________________________
// Test that correct errors are thrown for invalid input (condition checking).
TEST(PrefilterExpression, testInputConditionCheck) {
  MetadataBlocks blocks{};
  TestRelationalExpression<LessThanExpression, std::string>
      testRelationalExpression{};
  testRelationalExpression(
      2, DoubleId(10), blocks.blocksInvalidCol1,
      "The columns up to the evaluation column must contain the same values.");
  testRelationalExpression(
      1, DoubleId(10), blocks.blocksInvalidCol1,
      "The data blocks must be provided in sorted order regarding the "
      "evaluation column.");
  testRelationalExpression(2, DoubleId(10), blocks.blocksWithDuplicate2,
                           "The provided data blocks must be unique.");

  TestNotExpression<std::string> testNotExpression{};
  testNotExpression.test<NotEqualExpression>(
      2, VocabId(2), blocks.blocksWithDuplicate1,
      "The provided data blocks must be unique.");
  testNotExpression.test<LessThanExpression>(
      2, DoubleId(-14.1), blocks.blocksInvalidOrder1,
      "The data blocks must be provided in sorted order regarding the "
      "evaluation column.");
  testNotExpression.test<EqualExpression>(
      0, IntId(0), blocks.blocksInvalidCol02,
      "The data blocks must be provided in sorted order regarding the "
      "evaluation column.");
  testNotExpression.test<EqualExpression>(
      1, IntId(0), blocks.blocksInvalidCol02,
      "The columns up to the evaluation column must contain the same values.");
  testNotExpression.test<EqualExpression>(
      2, IntId(0), blocks.blocksInvalidCol02,
      "The columns up to the evaluation column must contain the same values.");

  TestLogicalExpression<AndExpression, std::string> testAndExpression{};
  testAndExpression.test<GreaterThanExpression, LessThanExpression>(
      2, DoubleId(-4.24), IntId(5), blocks.blocksWithDuplicate2,
      "The provided data blocks must be unique.");
  testAndExpression.test<GreaterThanExpression, LessThanExpression>(
      2, DoubleId(-4.24), IntId(5), blocks.blocksInvalidOrder1,
      "The data blocks must be provided in sorted order regarding the "
      "evaluation column.");
  testAndExpression.test<GreaterThanExpression, LessThanExpression>(
      2, DoubleId(-4.24), IntId(5), blocks.blocksInvalidCol02,
      "The columns up to the evaluation column must contain the same values.");
}
