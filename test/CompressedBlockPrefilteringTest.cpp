//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <vector>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "index/CompressedBlockPrefiltering.h"
#include "util/DateYearDuration.h"
#include "util/IdTestHelpers.h"

using ad_utility::source_location;
using ad_utility::testing::BlankNodeId;
using ad_utility::testing::BoolId;
using ad_utility::testing::DateId;
using ad_utility::testing::DoubleId;
using ad_utility::testing::IntId;
using ad_utility::testing::UndefId;
using ad_utility::testing::VocabId;
using namespace prefilterExpressions;

// TEST SECTION 1
//______________________________________________________________________________
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
            false,
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
  const BlockMetadata b1_1{{},
                           0,
                           {VocabId10, DoubleId33, IntId(0)},
                           {VocabId10, DoubleId(22), IntId(0)},
                           {},
                           false,
                           0};
  std::vector<BlockMetadata> blocksInvalidCol1 = {b1_1, b2,  b3,  b4,  b5,  b6,
                                                  b7,   b8,  b9,  b10, b11, b12,
                                                  b13,  b14, b15, b16, b17};
  const BlockMetadata b5_1{{},
                           0,
                           {VocabId(11), DoubleId33, IntId(-10)},
                           {VocabId10, DoubleId33, IntId(-8)},
                           {},
                           false,
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
TEST(BlockMetadata, testBlockFormatForDebugging) {
  MetadataBlocks blocks{};
  EXPECT_EQ(
      "#BlockMetadata\n(first) Triple: V:10 D:33.000000 I:0\n(last) Triple: "
      "V:10 D:33.000000 I:0\nnum. rows: 0.\n",
      (std::stringstream() << blocks.b1).str());
  EXPECT_EQ(
      "#BlockMetadata\n(first) Triple: V:10 D:33.000000 I:-4\n(last) Triple: "
      "V:10 D:33.000000 D:2.000000\nnum. rows: 0.\n",
      (std::stringstream() << blocks.b7).str());
  EXPECT_EQ(
      "#BlockMetadata\n(first) Triple: V:10 D:33.000000 V:14\n(last) Triple: "
      "V:10 D:33.000000 V:17\nnum. rows: 0.\n",
      (std::stringstream() << blocks.b17).str());
}

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
  testExpression(2, VocabId(0), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression(2, VocabId(12), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b15});
  testExpression(2, VocabId(14), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b15});
  testExpression(2, VocabId(16), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  // test blocks.otherBlocks
  testExpression(2, blocks.undef, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.falseId, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.trueId, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd3, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.referenceDate1, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.referenceDateEqual, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.referenceDate2, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd6, blocks.bd7});
  testExpression(2, BlankNodeId(11), blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
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
  testExpression(2, VocabId(0), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression(2, VocabId(11), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b15});
  testExpression(2, VocabId(14), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  // test block.otherBlocks
  testExpression(2, blocks.undef, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.falseId, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd3, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.trueId, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd3, blocks.bd4, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.referenceDateEqual, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd6, blocks.bd7});
  testExpression(2, BlankNodeId(11), blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
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
  testExpression(2, VocabId(22), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression(2, VocabId(14), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b17});
  testExpression(2, VocabId(12), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  // test blocks.otherBlocks
  testExpression(2, blocks.undef, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.falseId, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd4, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.trueId, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.referenceDateEqual, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.referenceDate1, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd6, blocks.bd7});
  testExpression(2, IntId(5), blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
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
                 {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression(2, VocabId(10), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression(2, VocabId(17), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b17});
  // test blocks.otherBlocks
  testExpression(2, blocks.undef, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.falseId, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd3, blocks.bd4, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.trueId, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd4, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.referenceDateEqual, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd6, blocks.bd7});
  testExpression(2, VocabId(0), blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
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
  testExpression(2, VocabId(1), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression(2, VocabId(14), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression(2, VocabId(11), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b15});
  testExpression(2, VocabId(17), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b17});
  testExpression(2, IntId(-4), blocks.blocks,
                 {blocks.b6, blocks.b7, blocks.b11, blocks.b14});
  // test blocks.otherBlocks
  testExpression(2, blocks.trueId, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd4, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.referenceDate1, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.referenceDateEqual, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd6, blocks.bd7});
  testExpression(2, blocks.referenceDate2, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
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
                 {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression(2, VocabId(7), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression(2, VocabId(14), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b15, blocks.b17});
  testExpression(2, VocabId(17), blocks.blocks,
                 {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  // test blocks.otherBlocks
  testExpression(2, blocks.undef, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.falseId, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd4, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.referenceDateEqual, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd7});
  testExpression(2, blocks.referenceDate1, blocks.otherBlocks,
                 {blocks.bd2, blocks.bd5, blocks.bd6, blocks.bd7});
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
      {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression.test<GreaterEqualExpression, GreaterEqualExpression>(
      2, VocabId(0), VocabId(17), blocks.blocks,
      {blocks.b7, blocks.b14, blocks.b17});
  testExpression.test<GreaterEqualExpression, GreaterThanExpression>(
      2, VocabId(12), VocabId(17), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression.test<GreaterEqualExpression, LessThanExpression>(
      2, VocabId(10), VocabId(14), blocks.blocks,
      {blocks.b7, blocks.b14, blocks.b15});
  testExpression.test<LessEqualExpression, LessThanExpression>(
      2, VocabId(0), VocabId(10), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression.test<LessEqualExpression, LessThanExpression>(
      2, VocabId(17), VocabId(17), blocks.blocks,
      {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression.test<GreaterEqualExpression, LessThanExpression>(
      2, DoubleId(-6.25), IntId(-7), blocks.blocks, {blocks.b7, blocks.b14});
  testExpression.test<GreaterThanExpression, LessThanExpression>(
      2, DoubleId(-6.25), DoubleId(-6.25), blocks.blocks,
      {blocks.b7, blocks.b14});
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
      {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17});
  testExpression.test<LessEqualExpression, GreaterEqualExpression>(
      2, VocabId(0), VocabId(16), blocks.blocks,
      {blocks.b7, blocks.b14, blocks.b17});
  testExpression.test<GreaterThanExpression, GreaterEqualExpression>(
      2, VocabId(17), VocabId(242), blocks.blocks, {blocks.b7, blocks.b14});
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
      2, blocks.blocks,
      {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17}, VocabId(2));
  testExpression.test<EqualExpression>(
      2, blocks.blocks, {blocks.b7, blocks.b14, blocks.b15, blocks.b17},
      VocabId(14));
  testExpression.test<NotEqualExpression>(
      2, blocks.blocks,
      {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17}, VocabId(14));
  testExpression.test<EqualExpression>(
      2, blocks.blocks,
      {blocks.b7, blocks.b14, blocks.b15, blocks.b16, blocks.b17}, VocabId(0));
  testExpression.test<LessThanExpression>(
      2, blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5, blocks.b6,
       blocks.b7, blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b12,
       blocks.b13, blocks.b14},
      DoubleId(-14.01));
  testExpression.test<GreaterEqualExpression>(
      2, blocks.blocks, {blocks.b7, blocks.b14}, DoubleId(-14.01));
  testExpression.test<GreaterThanExpression>(
      2, blocks.blocks,
      {blocks.b5, blocks.b6, blocks.b7, blocks.b11, blocks.b12, blocks.b13,
       blocks.b14},
      DoubleId(-4.00));
  testExpression.test<GreaterEqualExpression>(
      2, blocks.blocks, {blocks.b7, blocks.b14}, DoubleId(-24.4));
  testExpression.test<LessEqualExpression>(
      2, blocks.blocks,
      {blocks.b2, blocks.b3, blocks.b4, blocks.b7, blocks.b8, blocks.b9,
       blocks.b10, blocks.b14},
      IntId(0));
  testExpression.test<EqualExpression>(
      2, blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5, blocks.b6,
       blocks.b7, blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b13,
       blocks.b14},
      DoubleId(-6.25));
  testExpression.test<NotEqualExpression>(
      2, blocks.blocks,
      {blocks.b2, blocks.b7, blocks.b9, blocks.b10, blocks.b14}, DoubleId(4));
  testExpression.test<GreaterThanExpression>(
      2, blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b5, blocks.b6, blocks.b7, blocks.b11,
       blocks.b12, blocks.b13, blocks.b14},
      DoubleId(0));
  testExpression.test<EqualExpression>(0, blocks.blocks, {}, blocks.VocabId10);
  testExpression.test<EqualExpression>(1, blocks.blocks, {}, blocks.DoubleId33);
  testExpression.test<LessThanExpression>(0, blocks.blocks, blocks.blocks,
                                          blocks.VocabId10);
  testExpression.test<GreaterEqualExpression>(1, blocks.blocks, {},
                                              blocks.DoubleId33);
  testExpression.test<EqualExpression, NotExpression>(
      2, blocks.blocks, {blocks.b1, blocks.b2, blocks.b7, blocks.b14},
      IntId(0));
  testExpression.test<NotEqualExpression, NotExpression>(
      2, blocks.blocks,
      {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5, blocks.b6,
       blocks.b7, blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b13,
       blocks.b14},
      DoubleId(-6.25));
  testExpression.test<LessThanExpression, NotExpression>(
      2, blocks.blocks, {blocks.b7, blocks.b14}, VocabId(10));
  testExpression.test<GreaterEqualExpression, NotExpression>(
      2, blocks.blocks,
      {blocks.b2, blocks.b3, blocks.b4, blocks.b7, blocks.b9, blocks.b10,
       blocks.b14},
      DoubleId(3.99));
  testExpression
      .test<LessEqualExpression, AndExpression, GreaterEqualExpression>(
          2, blocks.blocks,
          {blocks.b2, blocks.b3, blocks.b4, blocks.b5, blocks.b6, blocks.b7,
           blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b12, blocks.b13,
           blocks.b14},
          IntId(0), IntId(0));
  testExpression.test<NotEqualExpression, AndExpression, NotEqualExpression>(
      2, blocks.blocks, {blocks.b5, blocks.b7, blocks.b14}, IntId(-10),
      DoubleId(-14.02));
  testExpression
      .test<GreaterThanExpression, AndExpression, GreaterEqualExpression>(
          2, blocks.blocks,
          {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5, blocks.b6,
           blocks.b7, blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b12,
           blocks.b13, blocks.b14},
          IntId(10), DoubleId(-6.25));
  testExpression
      .test<GreaterThanExpression, AndExpression, GreaterEqualExpression>(
          2, blocks.blocks,
          {blocks.b5, blocks.b6, blocks.b7, blocks.b11, blocks.b12, blocks.b13,
           blocks.b14},
          IntId(-4), DoubleId(-6.25));
  testExpression
      .test<LessThanExpression, AndExpression, GreaterEqualExpression>(
          2, blocks.blocks,
          {blocks.b1, blocks.b2, blocks.b3, blocks.b4, blocks.b5, blocks.b6,
           blocks.b7, blocks.b8, blocks.b9, blocks.b10, blocks.b11, blocks.b12,
           blocks.b13, blocks.b14},
          DoubleId(-7), IntId(6));
  testExpression
      .test<LessEqualExpression, OrExpression, GreaterEqualExpression>(
          2, blocks.blocks,
          {blocks.b2, blocks.b3, blocks.b7, blocks.b8, blocks.b9, blocks.b10,
           blocks.b14},
          IntId(0), DoubleId(6));
  testExpression
      .test<GreaterEqualExpression, OrExpression, GreaterThanExpression>(
          2, blocks.blocks, {blocks.b5, blocks.b7, blocks.b13, blocks.b14},
          DoubleId(0), IntId(-10));
  testExpression.test<LessThanExpression, OrExpression, GreaterThanExpression>(
      2, blocks.blocks, {blocks.b7, blocks.b14, blocks.b15}, VocabId(10),
      VocabId(10));
  testExpression.test<LessThanExpression, OrExpression, GreaterThanExpression>(
      2, blocks.blocks, {blocks.b6, blocks.b7, blocks.b11, blocks.b14},
      DoubleId(-4), IntId(-4));
  testExpression
      .test<GreaterThanExpression, OrExpression, GreaterEqualExpression>(
          2, blocks.blocks, {blocks.b7, blocks.b14}, IntId(-42), VocabId(0));
  testExpression
      .test<GreaterEqualExpression, OrExpression, GreaterThanExpression>(
          2, blocks.blocks, {blocks.b7, blocks.b14, blocks.b15}, VocabId(14),
          VocabId(15));
  testExpression.test<LessThanExpression, OrExpression, NotEqualExpression>(
      2, blocks.blocks, {blocks.b7, blocks.b11, blocks.b12, blocks.b14},
      DoubleId(-7.25), DoubleId(-6.25));
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
      2, blocks.blocksWithDuplicate1,
      "The provided data blocks must be unique.", VocabId(2));
  testNotExpression.test<LessThanExpression>(
      2, blocks.blocksInvalidOrder1,
      "The data blocks must be provided in sorted order regarding the "
      "evaluation column.",
      DoubleId(-14.1));
  testNotExpression.test<EqualExpression>(
      0, blocks.blocksInvalidCol2,
      "The data blocks must be provided in sorted order regarding the "
      "evaluation column.",
      IntId(0));
  testNotExpression.test<EqualExpression>(
      1, blocks.blocksInvalidCol2,
      "The columns up to the evaluation column must contain the same values.",
      IntId(0));
  testNotExpression.test<EqualExpression>(
      2, blocks.blocksInvalidCol2,
      "The columns up to the evaluation column must contain the same values.",
      IntId(0));

  TestLogicalExpression<AndExpression, std::string> testAndExpression{};
  testAndExpression.test<GreaterThanExpression, LessThanExpression>(
      2, DoubleId(-4.24), IntId(5), blocks.blocksWithDuplicate2,
      "The provided data blocks must be unique.");
  testAndExpression.test<GreaterThanExpression, LessThanExpression>(
      2, DoubleId(-4.24), IntId(5), blocks.blocksInvalidOrder1,
      "The data blocks must be provided in sorted order regarding the "
      "evaluation column.");
  testAndExpression.test<GreaterThanExpression, LessThanExpression>(
      2, DoubleId(-4.24), IntId(5), blocks.blocksInvalidCol2,
      "The columns up to the evaluation column must contain the same values.");
}

// TEST SECTION 2
//______________________________________________________________________________
//______________________________________________________________________________
using namespace sparqlExpression;
using optPrefilterVec = std::optional<std::vector<PrefilterExprVariablePair>>;
using Literal = ad_utility::triple_component::Literal;
using Iri = ad_utility::triple_component::Iri;
using MakeBinarySprqlExprFunc = std::function<SparqlExpression::Ptr(
    SparqlExpression::Ptr, SparqlExpression::Ptr)>;
using RelValues = std::variant<Variable, ValueId, Iri, Literal>;

// TEST HELPER SECTION
//______________________________________________________________________________
// make `Literal`
const auto L = [](const std::string& content) -> Literal {
  return Literal::fromStringRepresentation(content);
};

//______________________________________________________________________________
// make `Iri`
const auto I = [](const std::string& content) -> Iri {
  return Iri::fromIriref(content);
};

//______________________________________________________________________________
const auto makeLiteralSparqlExpr =
    [](const auto& child) -> std::unique_ptr<SparqlExpression> {
  using T = std::decay_t<decltype(child)>;
  if constexpr (std::is_same_v<T, ValueId>) {
    return std::make_unique<IdExpression>(child);
  } else if constexpr (std::is_same_v<T, Variable>) {
    return std::make_unique<VariableExpression>(child);
  } else if constexpr (std::is_same_v<T, Literal>) {
    return std::make_unique<StringLiteralExpression>(child);
  } else if constexpr (std::is_same_v<T, Iri>) {
    return std::make_unique<IriExpression>(child);
  } else {
    throw std::runtime_error(
        "Can't create a LiteralExpression from provided (input) type.");
  }
};

//______________________________________________________________________________
template <typename RelExpr>
std::unique_ptr<SparqlExpression> makeRelationalSparqlExprImpl(
    const RelValues& child0, const RelValues& child1) {
  auto getExpr =
      [](const auto& variantVal) -> std::unique_ptr<SparqlExpression> {
    return makeLiteralSparqlExpr(variantVal);
  };
  return std::make_unique<RelExpr>(std::array<SparqlExpression::Ptr, 2>{
      std::visit(getExpr, child0), std::visit(getExpr, child1)});
};

//______________________________________________________________________________
template <typename BinaryOp>
std::unique_ptr<PrefilterExpression> makeLogicalBinaryPrefilterExprImpl(
    std::unique_ptr<PrefilterExpression> child1,
    std::unique_ptr<PrefilterExpression> child2) {
  return std::make_unique<BinaryOp>(std::move(child1), std::move(child2));
};

//______________________________________________________________________________
std::unique_ptr<PrefilterExpression> makeNotPrefilterExprImpl(
    std::unique_ptr<PrefilterExpression> child) {
  return std::make_unique<prefilterExpressions::NotExpression>(
      std::move(child));
};

//______________________________________________________________________________
constexpr auto lessThanSparqlExpr =
    &makeRelationalSparqlExprImpl<sparqlExpression::LessThanExpression>;
constexpr auto lessThanFilterExpr =
    &makeRelExpr<prefilterExpressions::LessThanExpression>;
constexpr auto lessEqSparqlExpr =
    &makeRelationalSparqlExprImpl<sparqlExpression::LessEqualExpression>;
constexpr auto lessEqFilterExpr =
    &makeRelExpr<prefilterExpressions::LessEqualExpression>;
constexpr auto eqSparqlExpr =
    &makeRelationalSparqlExprImpl<sparqlExpression::EqualExpression>;
constexpr auto eqFilterExpr =
    &makeRelExpr<prefilterExpressions::EqualExpression>;
constexpr auto notEqSparqlExpr =
    &makeRelationalSparqlExprImpl<sparqlExpression::NotEqualExpression>;
constexpr auto notEqFilterExpr =
    &makeRelExpr<prefilterExpressions::NotEqualExpression>;
constexpr auto greaterEqSparqlExpr =
    &makeRelationalSparqlExprImpl<sparqlExpression::GreaterEqualExpression>;
constexpr auto greaterEqFilterExpr =
    &makeRelExpr<prefilterExpressions::GreaterEqualExpression>;
constexpr auto greaterThanSparqlExpr =
    &makeRelationalSparqlExprImpl<sparqlExpression::GreaterThanExpression>;
constexpr auto greaterThanFilterExpr =
    &makeRelExpr<prefilterExpressions::GreaterThanExpression>;
constexpr auto andSparqlExpr = &makeAndExpression;
constexpr auto orSparqlExpr = &makeOrExpression;
constexpr auto notSparqlExpr = &makeUnaryNegateExpression;
constexpr auto andFilterExpr =
    &makeLogicalBinaryPrefilterExprImpl<prefilterExpressions::AndExpression>;
constexpr auto orFilterExpr =
    &makeLogicalBinaryPrefilterExprImpl<prefilterExpressions::OrExpression>;
constexpr auto notFilterExpr = &makeNotPrefilterExprImpl;

//______________________________________________________________________________
const auto checkVectorPrefilterExprVariablePair =
    [](const std::vector<PrefilterExprVariablePair>& result,
       const std::vector<PrefilterExprVariablePair>& expected) -> bool {
  if (result.size() != expected.size()) {
    ADD_FAILURE() << "Expected vectors (result vs. expected) of equal length";
    return false;
  };
  const auto isEqualImpl = [](const PrefilterExprVariablePair& resPair,
                              const PrefilterExprVariablePair& expPair) {
    if (*resPair.first != *expPair.first || resPair.second != expPair.second) {
      std::stringstream stream;
      stream << "The following value pairs don't match:"
             << "\nRESULT: " << *resPair.first << "EXPECTED: " << *expPair.first
             << "RESULT: VARIABLE" << resPair.second.name()
             << "\nEXPECTED: VARIABLE" << expPair.second.name() << std::endl;
      ADD_FAILURE() << stream.str();
      return false;
    }
    return true;
  };
  return std::equal(result.begin(), result.end(), expected.begin(),
                    isEqualImpl);
};

//______________________________________________________________________________
const auto checkEqualityPrefilterMethodT =
    [](const optPrefilterVec& result, const optPrefilterVec& expected) {
      const auto testEquality =
          [](const optPrefilterVec& result, const optPrefilterVec& expected) {
            if (!result.has_value() && !expected.has_value()) {
              return true;
            } else if (result.has_value() && expected.has_value()) {
              return checkVectorPrefilterExprVariablePair(result.value(),
                                                          expected.value());
            } else {
              ADD_FAILURE()
                  << "Expected both values to either contain a value or to be "
                     "std::optional.";
              return false;
            }
          };
      ASSERT_TRUE(testEquality(result, expected));
    };

//______________________________________________________________________________
const auto evalToEmptyCheck = [](std::unique_ptr<SparqlExpression> sparqlExpr) {
  std::vector<PrefilterExprVariablePair> prefilterVarPair;
  checkEqualityPrefilterMethodT(sparqlExpr->getPrefilterExpressionForMetadata(),
                                std::nullopt);
};

//______________________________________________________________________________
const auto evalAndEqualityCheck =
    [](std::unique_ptr<SparqlExpression> sparqlExpr,
       std::convertible_to<PrefilterExprVariablePair> auto&&... prefilterArgs) {
      std::vector<PrefilterExprVariablePair> prefilterVarPair;
      (prefilterVarPair.emplace_back(
           std::forward<PrefilterExprVariablePair>(prefilterArgs)),
       ...);
      checkEqualityPrefilterMethodT(
          sparqlExpr->getPrefilterExpressionForMetadata(),
          std::move(prefilterVarPair));
    };

//______________________________________________________________________________
// Construct a pair with the given `PrefilterExpression` and `Variable` value.
auto pr = [](std::unique_ptr<PrefilterExpression> expr,
             const Variable& var) -> PrefilterExprVariablePair {
  return {std::move(expr), var};
};

//______________________________________________________________________________
// TEST SECTION 2

//______________________________________________________________________________
// Test PrefilterExpression equality operator.
TEST(PrefilterExpression, testEqualityOperator) {
  MetadataBlocks blocks{};
  // Relational PrefilterExpressions
  ASSERT_FALSE(*greaterEqFilterExpr(blocks.referenceDate1) ==
               *greaterEqFilterExpr(blocks.referenceDate2));
  ASSERT_FALSE(*notEqFilterExpr(BoolId(true)) == *eqFilterExpr(BoolId(true)));
  ASSERT_TRUE(*eqFilterExpr(IntId(1)) == *eqFilterExpr(IntId(1)));
  ASSERT_TRUE(*greaterEqFilterExpr(blocks.referenceDate1) ==
              *greaterEqFilterExpr(blocks.referenceDate1));
  // NotExpression
  ASSERT_TRUE(*notFilterExpr(eqFilterExpr(IntId(0))) ==
              *notFilterExpr(eqFilterExpr(IntId(0))));
  ASSERT_TRUE(*notFilterExpr(notFilterExpr(greaterEqFilterExpr(VocabId(0)))) ==
              *notFilterExpr(notFilterExpr(greaterEqFilterExpr(VocabId(0)))));
  ASSERT_FALSE(*notFilterExpr(greaterThanFilterExpr(IntId(0))) ==
               *eqFilterExpr(IntId(0)));
  ASSERT_FALSE(*notFilterExpr(andFilterExpr(eqFilterExpr(IntId(1)),
                                            eqFilterExpr(IntId(0)))) ==
               *notFilterExpr(greaterEqFilterExpr(VocabId(0))));
  // Binary PrefilterExpressions (AND and OR)
  ASSERT_TRUE(
      *orFilterExpr(eqFilterExpr(IntId(0)), lessEqFilterExpr(IntId(0))) ==
      *orFilterExpr(eqFilterExpr(IntId(0)), lessEqFilterExpr(IntId(0))));
  ASSERT_TRUE(
      *andFilterExpr(lessEqFilterExpr(VocabId(1)),
                     lessEqFilterExpr(IntId(0))) ==
      *andFilterExpr(lessEqFilterExpr(VocabId(1)), lessEqFilterExpr(IntId(0))));
  ASSERT_FALSE(
      *orFilterExpr(eqFilterExpr(IntId(0)), lessEqFilterExpr(IntId(0))) ==
      *andFilterExpr(lessEqFilterExpr(VocabId(1)), lessEqFilterExpr(IntId(0))));
  ASSERT_FALSE(
      *notFilterExpr(
          orFilterExpr(eqFilterExpr(IntId(0)), lessEqFilterExpr(IntId(0)))) ==
      *orFilterExpr(eqFilterExpr(IntId(0)), lessEqFilterExpr(IntId(0))));
}

//______________________________________________________________________________
// Test PrefilterExpression content formatting for debugging.
TEST(PrefilterExpression, checkPrintFormattedPrefilterExpression) {
  const auto& relExpr = lessThanFilterExpr(IntId(10));
  EXPECT_EQ((std::stringstream() << *relExpr).str(),
            "Prefilter RelationalExpression<0>\nValueId: I:10\n.\n");
  const auto& notExpr = notFilterExpr(eqFilterExpr(VocabId(0)));
  EXPECT_EQ((std::stringstream() << *notExpr).str(),
            "Prefilter NotExpression:\nchild {Prefilter "
            "RelationalExpression<3>\nValueId: V:0\n}\n.\n");
  const auto& orExpr =
      orFilterExpr(lessEqFilterExpr(IntId(0)), eqFilterExpr(IntId(5)));
  EXPECT_EQ((std::stringstream() << *orExpr).str(),
            "Prefilter LogicalExpression<1>\nchild1 {Prefilter "
            "RelationalExpression<1>\nValueId: I:0\n}child2 {Prefilter "
            "RelationalExpression<2>\nValueId: I:5\n}\n.\n");
}

//______________________________________________________________________________
// Test coverage for the default implementation of
// getPrefilterExpressionForMetadata.
TEST(SparqlExpression, testGetPrefilterExpressionDefault) {
  evalToEmptyCheck(makeUnaryMinusExpression(makeLiteralSparqlExpr(IntId(0))));
  evalToEmptyCheck(makeMultiplyExpression(makeLiteralSparqlExpr(DoubleId(11)),
                                          makeLiteralSparqlExpr(DoubleId(3))));
  evalToEmptyCheck(
      makeStrEndsExpression(makeLiteralSparqlExpr(L("\"Freiburg\"")),
                            makeLiteralSparqlExpr(L("\"burg\""))));
  evalToEmptyCheck(makeIsIriExpression(makeLiteralSparqlExpr(I("<IriIri>"))));
  evalToEmptyCheck(makeLogExpression(makeLiteralSparqlExpr(DoubleId(8))));
  evalToEmptyCheck(
      makeStrIriDtExpression(makeLiteralSparqlExpr(L("\"test\"")),
                             makeLiteralSparqlExpr(I("<test_iri>"))));
}

//______________________________________________________________________________
// Check that the (Sparql) RelationalExpression returns the expected
// PrefilterExpression.
TEST(SparqlExpression, getPrefilterExpressionFromSparqlRelational) {
  const Variable var = Variable{"?x"};
  MetadataBlocks blocks{};
  evalAndEqualityCheck(eqSparqlExpr(var, BoolId(true)),
                       pr(eqFilterExpr(BoolId(true)), var));
  evalAndEqualityCheck(greaterEqSparqlExpr(var, IntId(1)),
                       pr(greaterEqFilterExpr(IntId(1)), var));
  evalAndEqualityCheck(notEqSparqlExpr(VocabId(10), var),
                       pr(notEqFilterExpr(VocabId(10)), var));
  evalAndEqualityCheck(greaterEqSparqlExpr(BlankNodeId(1), var),
                       pr(greaterEqFilterExpr(BlankNodeId(1)), var));
  evalAndEqualityCheck(lessEqSparqlExpr(var, blocks.referenceDate1),
                       pr(lessEqFilterExpr(blocks.referenceDate1), var));
  evalAndEqualityCheck(lessThanSparqlExpr(DoubleId(10.2), var),
                       pr(lessThanFilterExpr(DoubleId(10.2)), var));
}

//______________________________________________________________________________
// More complex relational SparqlExpressions for which
// getPrefilterExpressionForMetadata should yield a vector containing the actual
// corresponding PrefilterExpression values.
TEST(SparqlExpression, getPrefilterExpressionsToComplexSparqlExpressions) {
  const Variable varX = Variable{"?x"};
  const Variable varY = Variable{"?y"};
  const Variable varZ = Variable{"?z"};
  // ?x >= 10 AND ?x != 20
  // expected prefilter pairs:
  // {<((>= 10) AND (!= 20)), ?x>}
  evalAndEqualityCheck(andSparqlExpr(greaterEqSparqlExpr(varX, IntId(10)),
                                     notEqSparqlExpr(varX, IntId(20))),
                       pr(andFilterExpr(greaterEqFilterExpr(IntId(10)),
                                        notEqFilterExpr(IntId(20))),
                          varX));
  // ?z > VocabId(0) AND ?y > 0 AND ?x < 30.00
  // expected prefilter pairs
  // {<(< 30.00), ?x>, <(> 0), ?y>, <(> VocabId(0)), ?z>}
  evalAndEqualityCheck(
      andSparqlExpr(andSparqlExpr(greaterThanSparqlExpr(varZ, VocabId(0)),
                                  greaterThanSparqlExpr(varY, IntId(0))),
                    lessThanSparqlExpr(varX, DoubleId(30.00))),
      pr(lessThanFilterExpr(DoubleId(30.00)), varX),
      pr(greaterThanFilterExpr(IntId(0)), varY),
      pr(greaterThanFilterExpr(VocabId(0)), varZ));

  // ?x == VocabId(10) AND ?y >= VocabId(10)
  // expected prefilter pairs:
  // {<(== VocabId(10)), ?x>, <(>= VocabId(10)), ?y>}
  evalAndEqualityCheck(andSparqlExpr(eqSparqlExpr(varX, VocabId(10)),
                                     greaterEqSparqlExpr(varY, VocabId(10))),
                       pr(eqFilterExpr(VocabId(10)), varX),
                       pr(greaterEqFilterExpr(VocabId(10)), varY));
  // !(?x >= 10 OR ?x <= 0)
  // expected prefilter pairs:
  // {<!(?x >= 10 OR ?x <= 0), ?x>}
  evalAndEqualityCheck(
      notSparqlExpr(orSparqlExpr(greaterEqSparqlExpr(varX, IntId(10)),
                                 lessEqSparqlExpr(varX, IntId(0)))),
      pr(notFilterExpr(orFilterExpr(greaterEqFilterExpr(IntId(10)),
                                    lessEqFilterExpr(IntId(0)))),
         varX));
  // !(?z == VocabId(10) AND ?z >= VocabId(20))
  // expected prefilter pairs:
  // {<!(?z == VocabId(10) AND ?z >= VocabId(20)) , ?z>}
  evalAndEqualityCheck(
      notSparqlExpr(andSparqlExpr(eqSparqlExpr(varZ, VocabId(10)),
                                  greaterEqSparqlExpr(varZ, VocabId(20)))),
      pr(notFilterExpr(andFilterExpr(eqFilterExpr(VocabId(10)),
                                     greaterEqFilterExpr(VocabId(20)))),
         varZ));
  // (?x == VocabId(10) AND ?z == VocabId(0)) AND ?y != DoubleId(22.1)
  // expected prefilter pairs:
  // {<(==VocabId(10)) , ?x>, <(!=DoubleId(22.1)), ?y>, <(==VocabId(0)), ?z>}
  evalAndEqualityCheck(
      andSparqlExpr(andSparqlExpr(eqSparqlExpr(VocabId(10), varX),
                                  eqSparqlExpr(varZ, VocabId(0))),
                    notEqSparqlExpr(DoubleId(22.1), varY)),
      pr(eqFilterExpr(VocabId(10)), varX),
      pr(notEqFilterExpr(DoubleId(22.1)), varY),
      pr(eqFilterExpr(VocabId(0)), varZ));
  // (?z >= 1000 AND ?x == VocabId(10)) OR ?z >= 10000
  // expected prefilter pairs:
  // {<((>=1000) OR (>= 10000)), ?z>}
  evalAndEqualityCheck(
      orSparqlExpr(andSparqlExpr(greaterEqSparqlExpr(varZ, IntId(1000)),
                                 eqSparqlExpr(varX, VocabId(10))),
                   greaterEqSparqlExpr(varZ, IntId(10000))),
      pr(orFilterExpr(greaterEqFilterExpr(IntId(1000)),
                      greaterEqFilterExpr(IntId(10000))),
         varZ));
  // !((?z <= VocabId(10) OR ?y <= VocabId(10)) OR ?x <= VocabId(10))
  // expected prefilter pairs:
  // {<!(<= VocabId(10)), ?x>, <!(<= VocabId(10)), ?y>, <!(<= VocabId(10)), ?z>}
  evalAndEqualityCheck(notSparqlExpr(orSparqlExpr(
                           orSparqlExpr(lessEqSparqlExpr(varZ, VocabId(10)),
                                        lessEqSparqlExpr(varY, VocabId(10))),
                           lessEqSparqlExpr(varX, VocabId(10)))),
                       pr(notFilterExpr(lessEqFilterExpr(VocabId(10))), varX),
                       pr(notFilterExpr(lessEqFilterExpr(VocabId(10))), varY),
                       pr(notFilterExpr(lessEqFilterExpr(VocabId(10))), varZ));
  // ?x >= 10 AND ?y >= 10
  // expected prefilter pairs:
  // {<(>= 10), ?x>, <(>= 10), ?y>}
  evalAndEqualityCheck(andSparqlExpr(greaterEqSparqlExpr(varX, IntId(10)),
                                     greaterEqSparqlExpr(varY, IntId(10))),
                       pr(greaterEqFilterExpr(IntId(10)), varX),
                       pr(greaterEqFilterExpr(IntId(10)), varY));
  // ?x <= 0 AND ?y <= 0
  // expected prefilter pairs:
  // {<(<= 0), ?x>, <(<= 0), ?y>}
  evalAndEqualityCheck(andSparqlExpr(lessEqSparqlExpr(varX, IntId(0)),
                                     lessEqSparqlExpr(varY, IntId(0))),
                       pr(lessEqFilterExpr(IntId(0)), varX),
                       pr(lessEqFilterExpr(IntId(0)), varY));
  // (?x >= 10 AND ?y >= 10) OR (?x <= 0 AND ?y <= 0)
  // expected prefilter pairs:
  // {<((>= 10) OR (<= 0)), ?x> <(>= 10) OR (<= 0)), ?y>}
  evalAndEqualityCheck(
      orSparqlExpr(andSparqlExpr(greaterEqSparqlExpr(varX, IntId(10)),
                                 greaterEqSparqlExpr(varY, IntId(10))),
                   andSparqlExpr(lessEqSparqlExpr(varX, IntId(0)),
                                 lessEqSparqlExpr(varY, IntId(0)))),
      pr(orFilterExpr(greaterEqFilterExpr(IntId(10)),
                      lessEqFilterExpr(IntId(0))),
         varX),
      pr(orFilterExpr(greaterEqFilterExpr(IntId(10)),
                      lessEqFilterExpr(IntId(0))),
         varY));
  // !(?x >= 10 OR ?y >= 10) OR !(?x <= 0 OR ?y <= 0)
  // expected prefilter pairs:
  // {<((!(>= 10) OR !(<= 0))), ?x> <(!(>= 10) OR !(<= 0))), ?y>}
  evalAndEqualityCheck(
      orSparqlExpr(
          notSparqlExpr(orSparqlExpr(greaterEqSparqlExpr(varX, IntId(10)),
                                     greaterEqSparqlExpr(varY, IntId(10)))),
          notSparqlExpr(orSparqlExpr(lessEqSparqlExpr(varX, IntId(0)),
                                     lessEqSparqlExpr(varY, IntId(0))))),
      pr(orFilterExpr(notFilterExpr(greaterEqFilterExpr(IntId(10))),
                      notFilterExpr(lessEqFilterExpr(IntId(0)))),
         varX),
      pr(orFilterExpr(notFilterExpr(greaterEqFilterExpr(IntId(10))),
                      notFilterExpr(lessEqFilterExpr(IntId(0)))),
         varY));
  // !(?x == VocabId(10) OR ?x == VocabId(20)) AND !(?z >= 10.00 OR ?y == false)
  // expected prefilter pairs:
  // {<!((== VocabId(10)) OR (== VocabId(20))), ?x>, <!(== false), ?y>,
  // <!(>= 10), ?z>}
  evalAndEqualityCheck(
      andSparqlExpr(
          notSparqlExpr(orSparqlExpr(eqSparqlExpr(varX, VocabId(10)),
                                     eqSparqlExpr(varX, VocabId(20)))),
          notSparqlExpr(orSparqlExpr(greaterEqSparqlExpr(varZ, DoubleId(10)),
                                     eqSparqlExpr(varY, BoolId(false))))),
      pr(notFilterExpr(orFilterExpr(eqFilterExpr(VocabId(10)),
                                    eqFilterExpr(VocabId(20)))),
         varX),
      pr(notFilterExpr(eqFilterExpr(BoolId(false))), varY),
      pr(notFilterExpr(greaterEqFilterExpr(DoubleId(10))), varZ));
  // !(!(?x >= 10 AND ?y >= 10)) OR !(!(?x <= 0 AND ?y <= 0))
  // expected prefilter pairs:
  // {<(!!(>= 10) OR !!(<= 0)), ?x>, <(!!(>= 10) OR !!(<= 0)) ,?y>}
  evalAndEqualityCheck(
      orSparqlExpr(notSparqlExpr(notSparqlExpr(
                       andSparqlExpr(greaterEqSparqlExpr(varX, IntId(10)),
                                     greaterEqSparqlExpr(varY, IntId(10))))),
                   notSparqlExpr(notSparqlExpr(
                       andSparqlExpr(lessEqSparqlExpr(varX, IntId(0)),
                                     lessEqSparqlExpr(varY, IntId(0)))))),
      pr(orFilterExpr(
             notFilterExpr(notFilterExpr(greaterEqFilterExpr(IntId(10)))),
             notFilterExpr(notFilterExpr(lessEqFilterExpr(IntId(0))))),
         varX),
      pr(orFilterExpr(
             notFilterExpr(notFilterExpr(greaterEqFilterExpr(IntId(10)))),
             notFilterExpr(notFilterExpr(lessEqFilterExpr(IntId(0))))),
         varY));
  // !((?x >= VocabId(0) AND ?x <= VocabId(10)) OR !(?x != VocabId(99)))
  // expected prefilter pairs:
  // {<!(((>= VocabId(0)) AND (<= VocabId(10))) OR !(!= VocabId(99))) , ?x>}
  evalAndEqualityCheck(notSparqlExpr(orSparqlExpr(
                           andSparqlExpr(greaterEqSparqlExpr(varX, VocabId(0)),
                                         lessEqSparqlExpr(varX, VocabId(10))),
                           notSparqlExpr(notEqSparqlExpr(varX, VocabId(99))))),
                       pr(notFilterExpr(orFilterExpr(
                              andFilterExpr(greaterEqFilterExpr(VocabId(0)),
                                            lessEqFilterExpr(VocabId(10))),
                              notFilterExpr(notEqFilterExpr(VocabId(99))))),
                          varX));
  // !((?y >= 10 AND ?y <= 100) OR !(?x >= VocabId(99)))
  // expected prefilter pairs:
  // {<!((>= VocabId(0)) AND (<= VocabId(10)), ?y>, <!(!(>= VocabId(99))), ?x>}
  evalAndEqualityCheck(
      notSparqlExpr(
          orSparqlExpr(andSparqlExpr(greaterEqSparqlExpr(varY, VocabId(0)),
                                     lessEqSparqlExpr(varY, VocabId(10))),
                       notSparqlExpr(greaterEqSparqlExpr(varX, VocabId(99))))),
      pr(notFilterExpr(notFilterExpr(greaterEqFilterExpr(VocabId(99)))), varX),
      pr(notFilterExpr(andFilterExpr(greaterEqFilterExpr(VocabId(0)),
                                     lessEqFilterExpr(VocabId(10)))),
         varY));
  // ?z >= 10 AND ?z <= 100 AND ?x >= 10 AND ?x != 50 AND !(?y <= 10) AND
  // !(?city <= VocabId(1000) OR ?city == VocabId(1005))
  // expected prefilter pairs:
  // {<!((<= VocabId(1000)) OR (== VocabId(1005))), ?city>, <((>= 10) AND (!=
  // 50)), ?x>, <!(<= 10), ?y>, <((>= 10) AND (<= 100)), ?z>}
  evalAndEqualityCheck(
      andSparqlExpr(
          andSparqlExpr(
              andSparqlExpr(greaterEqSparqlExpr(varZ, IntId(10)),
                            lessEqSparqlExpr(varZ, IntId(100))),
              andSparqlExpr(andSparqlExpr(greaterEqSparqlExpr(varX, IntId(10)),
                                          notEqSparqlExpr(varX, IntId(50))),
                            notSparqlExpr(lessEqSparqlExpr(varY, IntId(10))))),
          notSparqlExpr(
              orSparqlExpr(lessEqSparqlExpr(Variable{"?city"}, VocabId(1000)),
                           eqSparqlExpr(Variable{"?city"}, VocabId(1005))))),
      pr(notFilterExpr(orFilterExpr(lessEqFilterExpr(VocabId(1000)),
                                    eqFilterExpr(VocabId(1005)))),
         Variable{"?city"}),
      pr(andFilterExpr(greaterEqFilterExpr(IntId(10)),
                       notEqFilterExpr(IntId(50))),
         varX),
      pr(notFilterExpr(lessEqFilterExpr(IntId(10))), varY),
      pr(andFilterExpr(greaterEqFilterExpr(IntId(10)),
                       lessEqFilterExpr(IntId(100))),
         varZ));
  // ?x >= 10 OR (?x >= -10 AND ?x < 0.00)
  // expected prefilter pairs:
  // {<((>= 10) OR ((>= -10) AND (< 0.00))), ?x>}
  evalAndEqualityCheck(
      orSparqlExpr(greaterEqSparqlExpr(varX, IntId(10)),
                   andSparqlExpr(greaterEqSparqlExpr(varX, IntId(-10)),
                                 lessThanSparqlExpr(DoubleId(0.00), varX))),
      pr(orFilterExpr(greaterEqFilterExpr(IntId(10)),
                      andFilterExpr(greaterEqFilterExpr(IntId(-10)),
                                    lessThanFilterExpr(DoubleId(0.00)))),
         varX));
  // !(!(?x >= 10) OR !!(?x >= -10 AND ?x < 0.00))
  // expected prefilter pairs:
  // {<!(!(>= 10) OR !!((>= -10) AND (< 0.00))), ?x>}
  evalAndEqualityCheck(notSparqlExpr(orSparqlExpr(
                           notSparqlExpr(greaterEqSparqlExpr(varX, IntId(10))),
                           notSparqlExpr(notSparqlExpr(andSparqlExpr(
                               greaterEqSparqlExpr(varX, IntId(-10)),
                               lessThanSparqlExpr(DoubleId(0.00), varX)))))),
                       pr(notFilterExpr(orFilterExpr(
                              notFilterExpr(greaterEqFilterExpr(IntId(10))),
                              notFilterExpr(notFilterExpr(andFilterExpr(
                                  greaterEqFilterExpr(IntId(-10)),
                                  lessThanFilterExpr(DoubleId(0.00))))))),
                          varX));
  // ?y != ?x AND ?x >= 10
  // expected prefilter pairs:
  // {<(>= 10), ?x>}
  evalAndEqualityCheck(andSparqlExpr(notEqSparqlExpr(varY, varX),
                                     greaterEqSparqlExpr(varX, IntId(10))),
                       pr(greaterEqFilterExpr(IntId(10)), varX));
  evalAndEqualityCheck(andSparqlExpr(greaterEqSparqlExpr(varX, IntId(10)),
                                     notEqSparqlExpr(varY, varX)),
                       pr(greaterEqFilterExpr(IntId(10)), varX));
}

//______________________________________________________________________________
// For this test we expect that no PrefilterExpression is available.
TEST(SparqlExpression, getEmptyPrefilterFromSparqlRelational) {
  const Variable var = Variable{"?x"};
  const Iri iri = I("<Iri>");
  const Literal lit = L("\"lit\"");
  evalToEmptyCheck(lessEqSparqlExpr(var, var));
  evalToEmptyCheck(notEqSparqlExpr(iri, var));
  evalToEmptyCheck(eqSparqlExpr(var, iri));
  evalToEmptyCheck(notEqSparqlExpr(IntId(10), DoubleId(23.3)));
  evalToEmptyCheck(greaterThanSparqlExpr(DoubleId(10), lit));
  evalToEmptyCheck(lessThanSparqlExpr(VocabId(10), BoolId(10)));
  evalToEmptyCheck(greaterEqSparqlExpr(lit, lit));
  evalToEmptyCheck(eqSparqlExpr(iri, iri));
  evalToEmptyCheck(orSparqlExpr(eqSparqlExpr(var, var),
                                greaterThanSparqlExpr(var, IntId(0))));
  evalToEmptyCheck(
      orSparqlExpr(eqSparqlExpr(var, var), greaterThanSparqlExpr(var, var)));
  evalToEmptyCheck(
      andSparqlExpr(eqSparqlExpr(var, var), greaterThanSparqlExpr(var, var)));
}

//______________________________________________________________________________
// For the following more complex SparqlExpression trees, we also expect an
// empty PrefilterExpression vector.
TEST(SparqlExpression, getEmptyPrefilterForMoreComplexSparqlExpressions) {
  const Variable varX = Variable{"?x"};
  const Variable varY = Variable{"?y"};
  const Variable varZ = Variable{"?z"};
  // ?x <= 10.00 OR ?y > 10
  evalToEmptyCheck(orSparqlExpr(lessEqSparqlExpr(DoubleId(10), varX),
                                greaterThanSparqlExpr(IntId(10), varY)));
  // ?x >= VocabId(23) OR ?z == VocabId(1)
  evalToEmptyCheck(orSparqlExpr(greaterEqSparqlExpr(varX, VocabId(23)),
                                eqSparqlExpr(varZ, VocabId(1))));
  // (?x < VocabId(10) OR ?z <= VocabId(4)) OR ?z != 5.00
  evalToEmptyCheck(
      orSparqlExpr(orSparqlExpr(lessThanSparqlExpr(varX, VocabId(10)),
                                lessEqSparqlExpr(VocabId(4), varZ)),
                   notEqSparqlExpr(varZ, DoubleId(5))));
  // !(?z > 10.20 AND ?x < 0.001)
  // is equal to
  // ?z <= 10.20 OR ?x >= 0.001
  evalToEmptyCheck(
      notSparqlExpr(andSparqlExpr(greaterThanSparqlExpr(DoubleId(10.2), varZ),
                                  lessThanSparqlExpr(DoubleId(0.001), varX))));
  // !(?x > 10.20 AND ?z != VocabId(22))
  // is equal to
  // ?x <= 10.20 OR ?z == VocabId(22)
  evalToEmptyCheck(
      notSparqlExpr(andSparqlExpr(greaterThanSparqlExpr(DoubleId(10.2), varX),
                                  notEqSparqlExpr(VocabId(22), varZ))));
  // !(!((?x < VocabId(10) OR ?x <= VocabId(4)) OR ?z != 5.00))
  // is equal to
  // (?x < VocabId(10) OR ?x <= VocabId(4)) OR ?z != 5.00
  evalToEmptyCheck(notSparqlExpr(notSparqlExpr(
      orSparqlExpr(orSparqlExpr(lessThanSparqlExpr(varX, VocabId(10)),
                                lessEqSparqlExpr(VocabId(4), varX)),
                   notEqSparqlExpr(varZ, DoubleId(5))))));
  // !(?x != 10 AND !(?y >= 10.00 OR ?z <= 10))
  // is equal to
  // ?x == 10 OR ?y >= 10.00 OR ?z <= 10
  evalToEmptyCheck(notSparqlExpr(andSparqlExpr(
      notEqSparqlExpr(varX, IntId(10)),
      notSparqlExpr(orSparqlExpr(greaterEqSparqlExpr(varY, DoubleId(10.00)),
                                 lessEqSparqlExpr(varZ, IntId(10)))))));
  // !((?x != 10 AND ?z != 10) AND (?y == 10 AND ?x >= 20))
  // is equal to
  //?x == 10 OR ?z == 10 OR ?y != 10 OR ?x < 20
  evalToEmptyCheck(notSparqlExpr(
      andSparqlExpr(andSparqlExpr(notEqSparqlExpr(varX, IntId(10)),
                                  notEqSparqlExpr(varZ, IntId(10))),
                    andSparqlExpr(eqSparqlExpr(varY, IntId(10)),
                                  greaterEqSparqlExpr(varX, IntId(20))))));
  // !(?z >= 40 AND (?z != 10.00 AND ?y != VocabId(1)))
  // is equal to
  // ?z <= 40 OR ?z == 10.00 OR ?y == VocabId(1)
  evalToEmptyCheck(notSparqlExpr(
      andSparqlExpr(greaterEqSparqlExpr(varZ, IntId(40)),
                    andSparqlExpr(notEqSparqlExpr(varZ, DoubleId(10.00)),
                                  notEqSparqlExpr(varY, VocabId(1))))));
  // ?z <= true OR !(?x == 10 AND ?y == 10)
  // is equal to
  // ?z <= true OR ?x != 10 OR ?y != 10
  evalToEmptyCheck(orSparqlExpr(
      lessEqSparqlExpr(varZ, BoolId(true)),
      notSparqlExpr(andSparqlExpr(eqSparqlExpr(varX, IntId(10)),
                                  eqSparqlExpr(IntId(10), varY)))));
  // !(!(?z <= true OR !(?x == 10 AND ?y == 10)))
  // is equal to
  // ?z <= true OR ?x != 10 OR ?y != 10
  evalToEmptyCheck(notSparqlExpr(notSparqlExpr(orSparqlExpr(
      lessEqSparqlExpr(varZ, BoolId(true)),
      notSparqlExpr(andSparqlExpr(eqSparqlExpr(varX, IntId(10)),
                                  eqSparqlExpr(IntId(10), varY)))))));
  // !(!(?x != 10 OR !(?y >= 10.00 AND ?z <= 10)))
  // is equal to
  // ?x != 10 OR ?y < 10.00 OR ?z > 10
  evalToEmptyCheck(notSparqlExpr(notSparqlExpr(orSparqlExpr(
      notEqSparqlExpr(varX, IntId(10)),
      notSparqlExpr(andSparqlExpr(greaterEqSparqlExpr(varY, DoubleId(10.00)),
                                  lessEqSparqlExpr(varZ, IntId(10))))))));
  // !(!(?x == VocabId(10) OR ?y >= 25) AND !(!(?z == true AND ?country ==
  // VocabId(20))))
  // is equal to
  // ?x == VocabId(10) OR ?y >= 25 OR ?z == true AND ?country == VocabId(20)
  evalToEmptyCheck(notSparqlExpr(andSparqlExpr(
      notSparqlExpr(orSparqlExpr(eqSparqlExpr(varX, VocabId(10)),
                                 greaterEqSparqlExpr(varY, IntId(25)))),
      notSparqlExpr(notSparqlExpr(
          andSparqlExpr(eqSparqlExpr(varZ, BoolId(true)),
                        eqSparqlExpr(Variable{"?country"}, VocabId(20))))))));
}
