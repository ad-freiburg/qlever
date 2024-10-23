//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <vector>

#include "./SparqlExpressionTestHelpers.h"
#include "index/CompressedBlockPrefiltering.h"
#include "util/DateYearDuration.h"
#include "util/GTestHelpers.h"
#include "util/IdTestHelpers.h"

using ad_utility::testing::BlankNodeId;
using ad_utility::testing::BoolId;
using ad_utility::testing::DateId;
using ad_utility::testing::DoubleId;
using ad_utility::testing::IntId;
using ad_utility::testing::UndefId;
using ad_utility::testing::VocabId;
using sparqlExpression::TestContext;
using namespace prefilterExpressions;

//______________________________________________________________________________
struct TestPrefilterExprOnBlockMetadata : public ::testing::Test {
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
  // (Graph) Index that technically doesn't matter for this test
  const Id GraphId = VocabId(0);

  auto makeBlock(const ValueId& firstId, const ValueId& lastId)
      -> BlockMetadata {
    return {{{},
             0,
             // COLUMN 0  |  COLUMN 1  |  COLUMN 2
             {VocabId10, DoubleId33, firstId, GraphId},  // firstTriple
             {VocabId10, DoubleId33, lastId, GraphId},   // lastTriple
             {},
             false},
            0};
  };

  const BlockMetadata b1 = makeBlock(IntId(0), IntId(0));
  const BlockMetadata b2 = makeBlock(IntId(0), IntId(5));
  const BlockMetadata b3 = makeBlock(IntId(5), IntId(6));
  const BlockMetadata b4 = makeBlock(IntId(8), IntId(9));
  const BlockMetadata b5 = makeBlock(IntId(-10), IntId(-8));
  const BlockMetadata b6 = makeBlock(IntId(-4), IntId(-4));
  // b7 contains mixed datatypes (COLUMN 2)
  const BlockMetadata b7 = makeBlock(IntId(-4), DoubleId(2));
  const BlockMetadata b8 = makeBlock(DoubleId(2), DoubleId(2));
  const BlockMetadata b9 = makeBlock(DoubleId(4), DoubleId(4));
  const BlockMetadata b10 = makeBlock(DoubleId(4), DoubleId(10));
  const BlockMetadata b11 = makeBlock(DoubleId(-1.23), DoubleId(-6.25));
  const BlockMetadata b12 = makeBlock(DoubleId(-6.25), DoubleId(-6.25));
  const BlockMetadata b13 = makeBlock(DoubleId(-10.42), DoubleId(-12.00));
  // b14 contains mixed datatypes (COLUMN 2)
  const BlockMetadata b14 = makeBlock(DoubleId(-14.01), VocabId(0));
  const BlockMetadata b15 = makeBlock(VocabId(10), VocabId(14));
  const BlockMetadata b16 = makeBlock(VocabId(14), VocabId(14));
  const BlockMetadata b17 = makeBlock(VocabId(14), VocabId(17));
  std::vector<BlockMetadata> blocks = {b1,  b2,  b3,  b4,  b5,  b6,
                                       b7,  b8,  b9,  b10, b11, b12,
                                       b13, b14, b15, b16, b17};

  // The following blocks will be swapped with their respective correct block,
  // to test if the method evaluate checks the pre-conditions properly.
  const BlockMetadata b1_1{{{},
                            0,
                            {VocabId10, DoubleId33, IntId(0), GraphId},
                            {VocabId10, DoubleId(22), IntId(0), GraphId},
                            {},
                            false},
                           0};
  std::vector<BlockMetadata> blocksInvalidCol1 = {b1_1, b2,  b3,  b4,  b5,  b6,
                                                  b7,   b8,  b9,  b10, b11, b12,
                                                  b13,  b14, b15, b16, b17};
  const BlockMetadata b5_1{{{},
                            0,
                            {VocabId(11), DoubleId33, IntId(-10), GraphId},
                            {VocabId10, DoubleId33, IntId(-8), GraphId},
                            {},
                            false},
                           0};
  std::vector<BlockMetadata> blocksInvalidCol2 = {b1,  b2,  b3,  b4,  b5_1, b6,
                                                  b7,  b8,  b9,  b10, b11,  b12,
                                                  b13, b14, b15, b16, b17};
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

  //____________________________________________________________________________
  // Additional blocks for different datatype mix.
  const Id undef = Id::makeUndefined();
  const Id falseId = BoolId(false);
  const Id trueId = BoolId(true);
  const Id referenceDate1 =
      DateId(DateYearOrDuration::parseXsdDate, "1999-11-11");
  const Id referenceDate2 =
      DateId(DateYearOrDuration::parseXsdDate, "2005-02-27");
  const Id referenceDateEqual =
      DateId(DateYearOrDuration::parseXsdDate, "2000-01-01");
  const BlockMetadata bd1 = makeBlock(undef, undef);
  const BlockMetadata bd2 = makeBlock(undef, falseId);
  const BlockMetadata bd3 = makeBlock(falseId, falseId);
  const BlockMetadata bd4 = makeBlock(trueId, trueId);
  const BlockMetadata bd5 =
      makeBlock(trueId, DateId(DateYearOrDuration::parseXsdDate, "1999-12-12"));
  const BlockMetadata bd6 =
      makeBlock(DateId(DateYearOrDuration::parseXsdDate, "2000-01-01"),
                DateId(DateYearOrDuration::parseXsdDate, "2000-01-01"));
  const BlockMetadata bd7 = makeBlock(
      DateId(DateYearOrDuration::parseXsdDate, "2024-10-08"), BlankNodeId(10));
  std::vector<BlockMetadata> otherBlocks = {bd1, bd2, bd3, bd4, bd5, bd6, bd7};
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
      AD_EXPECT_THROW_WITH_MESSAGE(
          expression->evaluate(input, evaluationColumn), expectedErrorMessage);
    };

//______________________________________________________________________________
template <typename RelExpr1>
std::unique_ptr<PrefilterExpression> makeRelExpr(const ValueId& referenceId) {
  return std::make_unique<RelExpr1>(referenceId);
}

//______________________________________________________________________________
template <typename LogExpr, typename RelExpr1, typename RelExpr2>
std::unique_ptr<PrefilterExpression> makeLogExpr(const ValueId& referenceId1,
                                                 const ValueId& referenceId2) {
  return std::make_unique<LogExpr>(makeRelExpr<RelExpr1>(referenceId1),
                                   makeRelExpr<RelExpr2>(referenceId2));
}

//______________________________________________________________________________
template <typename RelExpr1, typename LogExpr = std::nullptr_t,
          typename RelExpr2 = std::nullptr_t>
std::unique_ptr<PrefilterExpression> makeNotExpression(
    const ValueId& referenceId1, const std::optional<ValueId> optId2) {
  if constexpr (check_is_relational_v<RelExpr2> &&
                check_is_logical_v<LogExpr>) {
    assert(optId2.has_value() && "Logical Expressions require two ValueIds");
    return std::make_unique<NotExpression>(
        makeLogExpr<LogExpr, RelExpr1, RelExpr2>(referenceId1, optId2.value()));
  } else if constexpr (std::is_same_v<LogExpr, NotExpression>) {
    return std::make_unique<NotExpression>(
        makeNotExpression<RelExpr1>(referenceId1, optId2));
  } else {
    return std::make_unique<NotExpression>(makeRelExpr<RelExpr1>(referenceId1));
  }
}

//______________________________________________________________________________
template <typename RelExpr1, typename ResT>
struct TestRelationalExpression {
  void operator()(size_t evaluationColumn, const ValueId& referenceId,
                  const std::vector<BlockMetadata>& input, const ResT& expected)
      requires check_is_relational_v<RelExpr1> {
    auto expression = makeRelExpr<RelExpr1>(referenceId);
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
  void test(size_t evaluationColumn, const ValueId& referenceIdChild1,
            ValueId referenceIdChild2, const std::vector<BlockMetadata>& input,
            const ResT& expected)
      requires(check_is_logical_v<LogExpr> && check_is_relational_v<RelExpr1> &&
               check_is_relational_v<RelExpr2>) {
    auto expression = makeLogExpr<LogExpr, RelExpr1, RelExpr2>(
        referenceIdChild1, referenceIdChild2);
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
  template <typename RelExpr1, typename LogExpr = std::nullptr_t,
            typename RelExpr2 = std::nullptr_t>
  void test(size_t evaluationColumn, const std::vector<BlockMetadata>& input,
            const ResT& expected, const ValueId& referenceId1,
            std::optional<ValueId> optId2 = std::nullopt)
      requires check_is_relational_v<RelExpr1> {
    auto expression =
        makeNotExpression<RelExpr1, LogExpr, RelExpr2>(referenceId1, optId2);
    if constexpr (std::is_same_v<ResT, std::string>) {
      testThrowError(std::move(expression), evaluationColumn, input, expected);
    } else {
      static_assert(std::is_same_v<ResT, std::vector<BlockMetadata>>);
      EXPECT_EQ(expression->evaluate(input, evaluationColumn), expected);
    }
  }
};

//______________________________________________________________________________
TEST_F(TestPrefilterExprOnBlockMetadata, testBlockFormatForDebugging) {
  EXPECT_EQ(
      "#BlockMetadata\n(first) Triple: V:10 D:33.000000 I:0 V:0\n(last) "
      "Triple: V:10 D:33.000000 I:0 V:0\nnum. rows: 0.\n",
      (std::stringstream() << b1).str());
  EXPECT_EQ(
      "#BlockMetadata\n(first) Triple: V:10 D:33.000000 I:-4 V:0\n(last) "
      "Triple: V:10 D:33.000000 D:2.000000 V:0\nnum. rows: 0.\n",
      (std::stringstream() << b7).str());
  EXPECT_EQ(
      "#BlockMetadata\n(first) Triple: V:10 D:33.000000 V:14 V:0\n(last) "
      "Triple: V:10 D:33.000000 V:17 V:0\nnum. rows: 0.\n",
      (std::stringstream() << b17).str());
}

//______________________________________________________________________________
// Test LessThanExpression
TEST_F(TestPrefilterExprOnBlockMetadata, testLessThanExpressions) {
  TestRelationalExpression<LessThanExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression(2, IntId(5), blocks,
                 {b1, b2, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14});

  testExpression(2, IntId(-12), blocks, {b7, b14});
  testExpression(2, IntId(0), blocks, {b5, b6, b7, b11, b12, b13, b14});
  testExpression(2, IntId(100), blocks,
                 {b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14});
  testExpression(2, DoubleId(-3), blocks, {b5, b6, b7, b11, b12, b13, b14});
  testExpression(2, DoubleId(-14.01), blocks, {b7, b14});
  testExpression(2, DoubleId(-11.22), blocks, {b7, b13, b14});
  testExpression(2, DoubleId(-4.121), blocks, {b5, b7, b11, b12, b13, b14});
  testExpression(2, VocabId(0), blocks, {b7, b14});
  testExpression(2, VocabId(12), blocks, {b7, b14, b15});
  testExpression(2, VocabId(14), blocks, {b7, b14, b15});
  testExpression(2, VocabId(16), blocks, {b7, b14, b15, b16, b17});
  // test blocks.otherBlocks
  testExpression(2, undef, otherBlocks, {bd2, bd5, bd7});
  testExpression(2, falseId, otherBlocks, {bd2, bd5, bd7});
  testExpression(2, trueId, otherBlocks, {bd2, bd3, bd5, bd7});
  testExpression(2, referenceDate1, otherBlocks, {bd2, bd5, bd7});
  testExpression(2, referenceDateEqual, otherBlocks, {bd2, bd5, bd7});
  testExpression(2, referenceDate2, otherBlocks, {bd2, bd5, bd6, bd7});
  testExpression(2, BlankNodeId(11), otherBlocks, {bd2, bd5, bd7});
}

//______________________________________________________________________________
// Test LessEqualExpression
TEST_F(TestPrefilterExprOnBlockMetadata, testLessEqualExpressions) {
  TestRelationalExpression<LessEqualExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression(2, IntId(0), blocks, {b1, b2, b5, b6, b7, b11, b12, b13, b14});
  testExpression(2, IntId(-6), blocks, {b5, b7, b11, b12, b13, b14});
  testExpression(2, IntId(7), blocks,
                 {b1, b2, b3, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14});
  testExpression(2, IntId(-9), blocks, {b5, b7, b13, b14});
  testExpression(2, DoubleId(-9.131), blocks, {b5, b7, b13, b14});
  testExpression(2, DoubleId(1.1415), blocks,
                 {b1, b2, b5, b6, b7, b11, b12, b13, b14});
  testExpression(2, DoubleId(3.1415), blocks,
                 {b1, b2, b5, b6, b7, b8, b11, b12, b13, b14});
  testExpression(2, DoubleId(-11.99999999999999), blocks, {b7, b13, b14});
  testExpression(2, DoubleId(-14.03), blocks, {b7, b14});
  testExpression(2, VocabId(0), blocks, {b7, b14});
  testExpression(2, VocabId(11), blocks, {b7, b14, b15});
  testExpression(2, VocabId(14), blocks, {b7, b14, b15, b16, b17});
  // test block.otherBlocks
  testExpression(2, undef, otherBlocks, {bd2, bd5, bd7});
  testExpression(2, falseId, otherBlocks, {bd2, bd3, bd5, bd7});
  testExpression(2, trueId, otherBlocks, {bd2, bd3, bd4, bd5, bd7});
  testExpression(2, referenceDateEqual, otherBlocks, {bd2, bd5, bd6, bd7});
  testExpression(2, BlankNodeId(11), otherBlocks, {bd2, bd5, bd7});
}

//______________________________________________________________________________
// Test GreaterThanExpression
TEST_F(TestPrefilterExprOnBlockMetadata, testGreaterThanExpression) {
  TestRelationalExpression<GreaterThanExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression(2, DoubleId(5.5375), blocks, {b3, b4, b7, b10, b14});
  testExpression(2, DoubleId(9.9994), blocks, {b7, b10, b14});
  testExpression(2, IntId(-5), blocks,
                 {b1, b2, b3, b4, b6, b7, b8, b9, b10, b11, b14});
  testExpression(2, DoubleId(-5.5375), blocks,
                 {b1, b2, b3, b4, b6, b7, b8, b9, b10, b11, b14});
  testExpression(2, DoubleId(-6.2499999), blocks,
                 {b1, b2, b3, b4, b6, b7, b8, b9, b10, b11, b14});
  testExpression(2, IntId(1), blocks, {b2, b3, b4, b7, b8, b9, b10, b14});
  testExpression(2, IntId(3), blocks, {b2, b3, b4, b7, b9, b10, b14});
  testExpression(2, IntId(4), blocks, {b2, b3, b4, b7, b10, b14});
  testExpression(2, IntId(-4), blocks,
                 {b1, b2, b3, b4, b7, b8, b9, b10, b11, b14});
  testExpression(2, IntId(33), blocks, {b7, b14});
  testExpression(2, VocabId(22), blocks, {b7, b14});
  testExpression(2, VocabId(14), blocks, {b7, b14, b17});
  testExpression(2, VocabId(12), blocks, {b7, b14, b15, b16, b17});
  // test otherBlocks
  testExpression(2, undef, otherBlocks, {bd2, bd5, bd7});
  testExpression(2, falseId, otherBlocks, {bd2, bd4, bd5, bd7});
  testExpression(2, trueId, otherBlocks, {bd2, bd5, bd7});
  testExpression(2, referenceDateEqual, otherBlocks, {bd2, bd5, bd7});
  testExpression(2, referenceDate1, otherBlocks, {bd2, bd5, bd6, bd7});
  testExpression(2, IntId(5), otherBlocks, {bd2, bd5, bd7});
}

//______________________________________________________________________________
// Test GreaterEqualExpression
TEST_F(TestPrefilterExprOnBlockMetadata, testGreaterEqualExpression) {
  TestRelationalExpression<GreaterEqualExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression(2, IntId(0), blocks, {b1, b2, b3, b4, b7, b8, b9, b10, b14});
  testExpression(2, IntId(8), blocks, {b4, b7, b10, b14});
  testExpression(2, DoubleId(9.98), blocks, {b7, b10, b14});
  testExpression(2, IntId(-3), blocks,
                 {b1, b2, b3, b4, b7, b8, b9, b10, b11, b14});
  testExpression(2, IntId(-10), blocks,
                 {b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b14});
  testExpression(2, DoubleId(-3.1415), blocks,
                 {b1, b2, b3, b4, b7, b8, b9, b10, b11, b14});
  testExpression(2, DoubleId(-4.000001), blocks,
                 {b1, b2, b3, b4, b6, b7, b8, b9, b10, b11, b14});
  testExpression(2, DoubleId(10.000), blocks, {b7, b10, b14});
  testExpression(2, DoubleId(-15.22), blocks,
                 {b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14});
  testExpression(2, DoubleId(7.999999), blocks, {b4, b7, b10, b14});
  testExpression(2, DoubleId(10.0001), blocks, {b7, b14});
  testExpression(2, VocabId(14), blocks, {b7, b14, b15, b16, b17});
  testExpression(2, VocabId(10), blocks, {b7, b14, b15, b16, b17});
  testExpression(2, VocabId(17), blocks, {b7, b14, b17});
  // test otherBlocks
  testExpression(2, undef, otherBlocks, {bd2, bd5, bd7});
  testExpression(2, falseId, otherBlocks, {bd2, bd3, bd4, bd5, bd7});
  testExpression(2, trueId, otherBlocks, {bd2, bd4, bd5, bd7});
  testExpression(2, referenceDateEqual, otherBlocks, {bd2, bd5, bd6, bd7});
  testExpression(2, VocabId(0), otherBlocks, {bd2, bd5, bd7});
}

//______________________________________________________________________________
// Test EqualExpression
TEST_F(TestPrefilterExprOnBlockMetadata, testEqualExpression) {
  TestRelationalExpression<EqualExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression(2, IntId(0), blocks, {b1, b2, b7, b14});
  testExpression(2, IntId(5), blocks, {b2, b3, b7, b10, b14});
  testExpression(2, IntId(22), blocks, {b7, b14});
  testExpression(2, IntId(-10), blocks, {b5, b7, b14});
  testExpression(2, DoubleId(-6.25), blocks, {b7, b11, b12, b14});
  testExpression(2, IntId(-11), blocks, {b7, b13, b14});
  testExpression(2, DoubleId(-14.02), blocks, {b7, b14});
  testExpression(2, DoubleId(-0.001), blocks, {b7, b14});
  testExpression(2, DoubleId(0), blocks, {b1, b2, b7, b14});
  testExpression(2, IntId(2), blocks, {b2, b7, b8, b14});
  testExpression(2, DoubleId(5.5), blocks, {b3, b7, b10, b14});
  testExpression(2, DoubleId(1.5), blocks, {b2, b7, b14});
  testExpression(2, VocabId(1), blocks, {b7, b14});
  testExpression(2, VocabId(14), blocks, {b7, b14, b15, b16, b17});
  testExpression(2, VocabId(11), blocks, {b7, b14, b15});
  testExpression(2, VocabId(17), blocks, {b7, b14, b17});
  testExpression(2, IntId(-4), blocks, {b6, b7, b11, b14});
  // test otherBlocks
  testExpression(2, trueId, otherBlocks, {bd2, bd4, bd5, bd7});
  testExpression(2, referenceDate1, otherBlocks, {bd2, bd5, bd7});
  testExpression(2, referenceDateEqual, otherBlocks, {bd2, bd5, bd6, bd7});
  testExpression(2, referenceDate2, otherBlocks, {bd2, bd5, bd7});
}

//______________________________________________________________________________
// Test NotEqualExpression
TEST_F(TestPrefilterExprOnBlockMetadata, testNotEqualExpression) {
  TestRelationalExpression<NotEqualExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression(2, DoubleId(0.00), blocks,
                 {b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14});
  testExpression(2, IntId(-4), blocks,
                 {b1, b2, b3, b4, b5, b7, b8, b9, b10, b11, b12, b13, b14});
  testExpression(2, DoubleId(0.001), blocks,
                 {b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14});
  testExpression(2, IntId(2), blocks,
                 {b1, b2, b3, b4, b5, b6, b7, b9, b10, b11, b12, b13, b14});
  testExpression(2, DoubleId(-6.2500), blocks,
                 {b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b13, b14});
  testExpression(2, IntId(5), blocks,
                 {b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14});
  testExpression(2, DoubleId(-101.23), blocks,
                 {b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14});
  testExpression(2, VocabId(0), blocks, {b7, b14, b15, b16, b17});
  testExpression(2, VocabId(7), blocks, {b7, b14, b15, b16, b17});
  testExpression(2, VocabId(14), blocks, {b7, b14, b15, b17});
  testExpression(2, VocabId(17), blocks, {b7, b14, b15, b16, b17});
  // test otherBlocks
  testExpression(2, undef, otherBlocks, {bd2, bd5, bd7});
  testExpression(2, falseId, otherBlocks, {bd2, bd4, bd5, bd7});
  testExpression(2, referenceDateEqual, otherBlocks, {bd2, bd5, bd7});
  testExpression(2, referenceDate1, otherBlocks, {bd2, bd5, bd6, bd7});
}

//______________________________________________________________________________
//______________________________________________________________________________
// Test Logical Expressions

// Test AndExpression
TEST_F(TestPrefilterExprOnBlockMetadata, testAndExpression) {
  TestLogicalExpression<AndExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression.test<GreaterEqualExpression, GreaterThanExpression>(
      2, VocabId(10), VocabId(10), blocks, {b7, b14, b15, b16, b17});
  testExpression.test<GreaterEqualExpression, GreaterEqualExpression>(
      2, VocabId(0), VocabId(17), blocks, {b7, b14, b17});
  testExpression.test<GreaterEqualExpression, GreaterThanExpression>(
      2, VocabId(12), VocabId(17), blocks, {b7, b14});
  testExpression.test<GreaterEqualExpression, LessThanExpression>(
      2, VocabId(10), VocabId(14), blocks, {b7, b14, b15});
  testExpression.test<LessEqualExpression, LessThanExpression>(
      2, VocabId(0), VocabId(10), blocks, {b7, b14});
  testExpression.test<LessEqualExpression, LessThanExpression>(
      2, VocabId(17), VocabId(17), blocks, {b7, b14, b15, b16, b17});
  testExpression.test<GreaterEqualExpression, LessThanExpression>(
      2, DoubleId(-6.25), IntId(-7), blocks, {b7, b14});
  testExpression.test<GreaterThanExpression, LessThanExpression>(
      2, DoubleId(-6.25), DoubleId(-6.25), blocks, {b7, b14});
  testExpression.test<GreaterThanExpression, LessThanExpression>(
      2, IntId(0), IntId(0), blocks, {b7, b14});
  testExpression.test<GreaterEqualExpression, LessThanExpression>(
      2, IntId(-10), DoubleId(0.00), blocks, {b5, b6, b7, b11, b12, b14});
  // Also a corner case.
  testExpression.test<GreaterThanExpression, EqualExpression>(
      2, IntId(0), DoubleId(0), blocks, {b2, b7, b14});
  testExpression.test<GreaterEqualExpression, EqualExpression>(
      2, IntId(0), IntId(0), blocks, {b1, b2, b7, b14});
  testExpression.test<GreaterThanExpression, GreaterEqualExpression>(
      2, DoubleId(-34.23), DoubleId(15.1), blocks, {b7, b14});
  testExpression.test<LessThanExpression, LessEqualExpression>(
      2, IntId(0), DoubleId(-4), blocks, {b5, b6, b7, b11, b12, b13, b14});
  testExpression.test<NotEqualExpression, NotEqualExpression>(
      2, IntId(0), IntId(-4), blocks,
      {b2, b3, b4, b5, b7, b8, b9, b10, b11, b12, b13, b14});
  testExpression.test<NotEqualExpression, EqualExpression>(
      2, DoubleId(-3.1415), DoubleId(4.5), blocks, {b2, b7, b10, b14});
  testExpression.test<NotEqualExpression, LessThanExpression>(
      2, DoubleId(-6.25), IntId(0), blocks, {b5, b6, b7, b11, b13, b14});
  testExpression.test<LessEqualExpression, GreaterEqualExpression>(
      2, DoubleId(-4), DoubleId(1), blocks, {b7, b14});
  testExpression.test<LessEqualExpression, EqualExpression>(
      2, DoubleId(-2), IntId(-3), blocks, {b7, b11, b14});
}

//______________________________________________________________________________
// Test OrExpression
TEST_F(TestPrefilterExprOnBlockMetadata, testOrExpression) {
  TestLogicalExpression<OrExpression, std::vector<BlockMetadata>>
      testExpression;
  testExpression.test<LessThanExpression, LessEqualExpression>(
      2, VocabId(22), VocabId(0), blocks, {b7, b14, b15, b16, b17});
  testExpression.test<LessEqualExpression, GreaterEqualExpression>(
      2, VocabId(0), VocabId(16), blocks, {b7, b14, b17});
  testExpression.test<GreaterThanExpression, GreaterEqualExpression>(
      2, VocabId(17), VocabId(242), blocks, {b7, b14});
  testExpression.test<LessThanExpression, EqualExpression>(
      2, DoubleId(-5.95), VocabId(14), blocks,
      {b5, b7, b11, b12, b13, b14, b15, b16, b17});
  testExpression.test<EqualExpression, NotEqualExpression>(
      2, DoubleId(0), VocabId(14), blocks, {b1, b2, b7, b14, b15, b17});
  testExpression.test<EqualExpression, EqualExpression>(
      2, DoubleId(0.0), DoubleId(-6.25), blocks, {b1, b2, b7, b11, b12, b14});
  testExpression.test<EqualExpression, LessThanExpression>(
      2, DoubleId(-11.99), DoubleId(-15.22), blocks, {b7, b13, b14});
  testExpression.test<GreaterEqualExpression, LessThanExpression>(
      2, DoubleId(7.99), DoubleId(-7.99), blocks, {b4, b5, b7, b10, b13, b14});
  testExpression.test<GreaterThanExpression, EqualExpression>(
      2, IntId(-15), IntId(2), blocks,
      {b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14});
  testExpression.test<EqualExpression, EqualExpression>(
      2, IntId(0), IntId(-4), blocks, {b1, b2, b6, b7, b11, b14});
  testExpression.test<NotEqualExpression, EqualExpression>(
      2, VocabId(14), IntId(2), blocks, {b2, b7, b8, b14, b15, b17});
  testExpression.test<LessThanExpression, GreaterEqualExpression>(
      2, DoubleId(-1), IntId(1), blocks,
      {b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14});
  testExpression.test<LessEqualExpression, EqualExpression>(
      2, DoubleId(-4), IntId(-4), blocks, {b5, b6, b7, b11, b12, b13, b14});
}

//______________________________________________________________________________
// Test NotExpression
TEST_F(TestPrefilterExprOnBlockMetadata, testNotExpression) {
  TestNotExpression<std::vector<BlockMetadata>> testExpression{};
  testExpression.test<EqualExpression>(2, blocks, {b7, b14, b15, b16, b17},
                                       VocabId(2));
  testExpression.test<EqualExpression>(2, blocks, {b7, b14, b15, b17},
                                       VocabId(14));
  testExpression.test<NotEqualExpression>(2, blocks, {b7, b14, b15, b16, b17},
                                          VocabId(14));
  testExpression.test<EqualExpression>(2, blocks, {b7, b14, b15, b16, b17},
                                       VocabId(0));
  testExpression.test<LessThanExpression>(
      2, blocks, {b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14},
      DoubleId(-14.01));
  testExpression.test<GreaterEqualExpression>(2, blocks, {b7, b14},
                                              DoubleId(-14.01));
  testExpression.test<GreaterThanExpression>(
      2, blocks, {b5, b6, b7, b11, b12, b13, b14}, DoubleId(-4.00));
  testExpression.test<GreaterEqualExpression>(2, blocks, {b7, b14},
                                              DoubleId(-24.4));
  testExpression.test<LessEqualExpression>(
      2, blocks, {b2, b3, b4, b7, b8, b9, b10, b14}, IntId(0));
  testExpression.test<EqualExpression>(
      2, blocks, {b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b13, b14},
      DoubleId(-6.25));
  testExpression.test<NotEqualExpression>(2, blocks, {b2, b7, b9, b10, b14},
                                          DoubleId(4));
  testExpression.test<GreaterThanExpression>(
      2, blocks, {b1, b2, b5, b6, b7, b11, b12, b13, b14}, DoubleId(0));
  testExpression.test<EqualExpression>(0, blocks, {}, VocabId(10));
  testExpression.test<EqualExpression>(1, blocks, {}, DoubleId(33));
  testExpression.test<LessThanExpression>(0, blocks, blocks, VocabId(10));
  testExpression.test<GreaterEqualExpression>(1, blocks, {}, DoubleId(33));
  testExpression.test<EqualExpression, NotExpression>(
      2, blocks, {b1, b2, b7, b14}, IntId(0));
  testExpression.test<NotEqualExpression, NotExpression>(
      2, blocks, {b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b13, b14},
      DoubleId(-6.25));
  testExpression.test<LessThanExpression, NotExpression>(2, blocks, {b7, b14},
                                                         VocabId(10));
  testExpression.test<GreaterEqualExpression, NotExpression>(
      2, blocks, {b2, b3, b4, b7, b9, b10, b14}, DoubleId(3.99));
  testExpression
      .test<LessEqualExpression, AndExpression, GreaterEqualExpression>(
          2, blocks, {b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14},
          IntId(0), IntId(0));
  testExpression.test<NotEqualExpression, AndExpression, NotEqualExpression>(
      2, blocks, {b5, b7, b14}, IntId(-10), DoubleId(-14.02));
  testExpression
      .test<GreaterThanExpression, AndExpression, GreaterEqualExpression>(
          2, blocks,
          {b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14},
          IntId(10), DoubleId(-6.25));
  testExpression
      .test<GreaterThanExpression, AndExpression, GreaterEqualExpression>(
          2, blocks, {b5, b6, b7, b11, b12, b13, b14}, IntId(-4),
          DoubleId(-6.25));
  testExpression
      .test<LessThanExpression, AndExpression, GreaterEqualExpression>(
          2, blocks,
          {b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14},
          DoubleId(-7), IntId(6));
  testExpression
      .test<LessEqualExpression, OrExpression, GreaterEqualExpression>(
          2, blocks, {b2, b3, b7, b8, b9, b10, b14}, IntId(0), DoubleId(6));
  testExpression
      .test<GreaterEqualExpression, OrExpression, GreaterThanExpression>(
          2, blocks, {b5, b7, b13, b14}, DoubleId(0), IntId(-10));
  testExpression.test<LessThanExpression, OrExpression, GreaterThanExpression>(
      2, blocks, {b7, b14, b15}, VocabId(10), VocabId(10));
  testExpression.test<LessThanExpression, OrExpression, GreaterThanExpression>(
      2, blocks, {b6, b7, b11, b14}, DoubleId(-4), IntId(-4));
  testExpression
      .test<GreaterThanExpression, OrExpression, GreaterEqualExpression>(
          2, blocks, {b7, b14}, IntId(-42), VocabId(0));
  testExpression
      .test<GreaterEqualExpression, OrExpression, GreaterThanExpression>(
          2, blocks, {b7, b14, b15}, VocabId(14), VocabId(15));
  testExpression.test<LessThanExpression, OrExpression, NotEqualExpression>(
      2, blocks, {b7, b11, b12, b14}, DoubleId(-7.25), DoubleId(-6.25));
}

//______________________________________________________________________________
// Test that correct errors are thrown for invalid input (condition checking).
TEST_F(TestPrefilterExprOnBlockMetadata, testInputConditionCheck) {
  TestRelationalExpression<LessThanExpression, std::string>
      testRelationalExpression{};
  testRelationalExpression(
      2, DoubleId(10), blocksInvalidCol1,
      "The columns up to the evaluation column must contain the same values.");
  testRelationalExpression(
      1, DoubleId(10), blocksInvalidCol1,
      "The data blocks must be provided in sorted order regarding the "
      "evaluation column.");
  testRelationalExpression(2, DoubleId(10), blocksWithDuplicate2,
                           "The provided data blocks must be unique.");

  TestNotExpression<std::string> testNotExpression{};
  testNotExpression.test<NotEqualExpression>(
      2, blocksWithDuplicate1, "The provided data blocks must be unique.",
      VocabId(2));
  testNotExpression.test<LessThanExpression>(
      2, blocksInvalidOrder1,
      "The data blocks must be provided in sorted order regarding the "
      "evaluation column.",
      DoubleId(-14.1));
  testNotExpression.test<EqualExpression>(
      0, blocksInvalidCol2,
      "The data blocks must be provided in sorted order regarding the "
      "evaluation column.",
      IntId(0));
  testNotExpression.test<EqualExpression>(
      1, blocksInvalidCol2,
      "The columns up to the evaluation column must contain the same values.",
      IntId(0));
  testNotExpression.test<EqualExpression>(
      2, blocksInvalidCol2,
      "The columns up to the evaluation column must contain the same values.",
      IntId(0));

  TestLogicalExpression<AndExpression, std::string> testAndExpression{};
  testAndExpression.test<GreaterThanExpression, LessThanExpression>(
      2, DoubleId(-4.24), IntId(5), blocksWithDuplicate2,
      "The provided data blocks must be unique.");
  testAndExpression.test<GreaterThanExpression, LessThanExpression>(
      2, DoubleId(-4.24), IntId(5), blocksInvalidOrder1,
      "The data blocks must be provided in sorted order regarding the "
      "evaluation column.");
  testAndExpression.test<GreaterThanExpression, LessThanExpression>(
      2, DoubleId(-4.24), IntId(5), blocksInvalidCol2,
      "The columns up to the evaluation column must contain the same values.");
}
