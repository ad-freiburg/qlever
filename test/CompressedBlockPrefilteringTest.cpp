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

namespace {
using ad_utility::testing::BlankNodeId;
using ad_utility::testing::BoolId;
using ad_utility::testing::DateId;
using ad_utility::testing::DoubleId;
using ad_utility::testing::IntId;
using ad_utility::testing::UndefId;
using ad_utility::testing::VocabId;
constexpr auto DateParser = &DateYearOrDuration::parseXsdDate;
using namespace prefilterExpressions;

namespace makeFilterExpr {
//______________________________________________________________________________
// Make RelationalExpression
template <typename RelExpr>
auto relExpr =
    [](const ValueId& referenceId) -> std::unique_ptr<PrefilterExpression> {
  return std::make_unique<RelExpr>(referenceId);
};

// Make AndExpression or OrExpression
template <typename LogExpr>
auto logExpr = [](std::unique_ptr<PrefilterExpression> child1,
                  std::unique_ptr<PrefilterExpression> child2)
    -> std::unique_ptr<PrefilterExpression> {
  return std::make_unique<LogExpr>(std::move(child1), std::move(child2));
};

// Make NotExpression
auto notExpr = [](std::unique_ptr<PrefilterExpression> child)
    -> std::unique_ptr<PrefilterExpression> {
  return std::make_unique<NotExpression>(std::move(child));
};

}  // namespace makeFilterExpr
//______________________________________________________________________________
// instantiation relational
// LESS THAN (`<`)
constexpr auto lt = makeFilterExpr::relExpr<LessThanExpression>;
// LESS EQUAL (`<=`)
constexpr auto le = makeFilterExpr::relExpr<LessEqualExpression>;
// GREATER EQUAL (`>=`)
constexpr auto ge = makeFilterExpr::relExpr<GreaterEqualExpression>;
// GREATER THAN (`>`)
constexpr auto gt = makeFilterExpr::relExpr<GreaterThanExpression>;
// EQUAL (`==`)
constexpr auto eq = makeFilterExpr::relExpr<EqualExpression>;
// NOT EQUAL (`!=`)
constexpr auto neq = makeFilterExpr::relExpr<NotEqualExpression>;
// AND (`&&`)
constexpr auto andExpr = makeFilterExpr::logExpr<AndExpression>;
// OR (`||`)
constexpr auto orExpr = makeFilterExpr::logExpr<OrExpression>;
// NOT (`!`)
constexpr auto notExpr = makeFilterExpr::notExpr;

//______________________________________________________________________________
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

COLUMN 1 and COLUMN 2 contain fixed values, this is a necessary condition
that is also checked during the pre-filtering procedure. The actual evaluation
column (we filter w.r.t. values of COLUMN 0) contains mixed types.
*/
//______________________________________________________________________________
class TestPrefilterExprOnBlockMetadata : public ::testing::Test {
 public:
  const Id referenceDate1 = DateId(DateParser, "1999-11-11");
  const Id referenceDate2 = DateId(DateParser, "2005-02-27");
  const Id undef = Id::makeUndefined();
  const Id falseId = BoolId(false);
  const Id trueId = BoolId(true);
  const Id referenceDateEqual = DateId(DateParser, "2000-01-01");

  // Fixed column ValueIds
  const Id VocabId10 = VocabId(10);
  const Id DoubleId33 = DoubleId(33);
  const Id GraphId = VocabId(0);

  // Define BlockMetadata
  const BlockMetadata b1 = makeBlock(undef, undef);
  const BlockMetadata b2 = makeBlock(undef, falseId);
  const BlockMetadata b3 = makeBlock(falseId, falseId);
  const BlockMetadata b4 = makeBlock(trueId, IntId(0));
  const BlockMetadata b5 = makeBlock(IntId(0), IntId(0));
  const BlockMetadata b6 = makeBlock(IntId(0), IntId(5));
  const BlockMetadata b7 = makeBlock(IntId(5), IntId(6));
  const BlockMetadata b8 = makeBlock(IntId(8), IntId(9));
  const BlockMetadata b9 = makeBlock(IntId(-10), IntId(-8));
  const BlockMetadata b10 = makeBlock(IntId(-4), IntId(-4));
  const BlockMetadata b11 = makeBlock(IntId(-4), DoubleId(2));
  const BlockMetadata b12 = makeBlock(DoubleId(2), DoubleId(2));
  const BlockMetadata b13 = makeBlock(DoubleId(4), DoubleId(4));
  const BlockMetadata b14 = makeBlock(DoubleId(4), DoubleId(10));
  const BlockMetadata b15 = makeBlock(DoubleId(-1.23), DoubleId(-6.25));
  const BlockMetadata b16 = makeBlock(DoubleId(-6.25), DoubleId(-6.25));
  const BlockMetadata b17 = makeBlock(DoubleId(-10.42), DoubleId(-12.00));
  const BlockMetadata b18 = makeBlock(DoubleId(-14.01), VocabId(0));
  const BlockMetadata b19 = makeBlock(VocabId(10), VocabId(14));
  const BlockMetadata b20 = makeBlock(VocabId(14), VocabId(14));
  const BlockMetadata b21 = makeBlock(VocabId(14), VocabId(17));
  const BlockMetadata b22 =
      makeBlock(VocabId(20), DateId(DateParser, "1999-12-12"));
  const BlockMetadata b23 = makeBlock(DateId(DateParser, "2000-01-01"),
                                      DateId(DateParser, "2000-01-01"));
  const BlockMetadata b24 =
      makeBlock(DateId(DateParser, "2024-10-08"), BlankNodeId(10));

  // All blocks that contain mixed (ValueId) types over column 0
  const std::vector<BlockMetadata> mixedBlocks = {b2, b4, b11, b18, b22, b24};

  // Ordered and unique vector with BlockMetadata
  const std::vector<BlockMetadata> blocks = {
      b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,  b10, b11, b12,
      b13, b14, b15, b16, b17, b18, b19, b20, b21, b22, b23, b24};

  const std::vector<BlockMetadata> blocksInvalidOrder1 = {
      b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,  b10, b11, b12,
      b13, b14, b15, b16, b17, b18, b19, b20, b21, b22, b24, b23};

  const std::vector<BlockMetadata> blocksInvalidOrder2 = {
      b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,  b10, b11, b12,
      b14, b10, b15, b16, b17, b18, b19, b20, b21, b22, b23, b24};

  const std::vector<BlockMetadata> blocksWithDuplicate1 = {
      b1,  b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,  b10, b11, b12,
      b13, b14, b15, b16, b17, b18, b19, b20, b21, b22, b23, b24};

  const std::vector<BlockMetadata> blocksWithDuplicate2 = {
      b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,  b10, b11, b12, b13,
      b14, b15, b16, b17, b18, b19, b20, b21, b22, b23, b24, b24};

  // Function to create BlockMetadata
  const BlockMetadata makeBlock(const ValueId& firstId, const ValueId& lastId) {
    assert(firstId <= lastId);
    static size_t blockIdx = 0;
    ++blockIdx;
    return {{{},
             0,
             // COLUMN 0  |  COLUMN 1  |  COLUMN 2
             {firstId, VocabId10, DoubleId33, GraphId},  // firstTriple
             {lastId, VocabId10, DoubleId33, GraphId},   // lastTriple
             {},
             false},
            blockIdx};
  }

  // Check if expected error is thrown.
  auto makeTestErrorCheck(std::unique_ptr<PrefilterExpression> expr,
                          const std::vector<BlockMetadata>& input,
                          const std::string& expected,
                          size_t evaluationColumn = 0) {
    AD_EXPECT_THROW_WITH_MESSAGE(expr->evaluate(input, evaluationColumn),
                                 ::testing::HasSubstr(expected));
  }

  // Check that the provided expression prefilters the correct blocks.
  auto makeTest(std::unique_ptr<PrefilterExpression> expr,
                std::vector<BlockMetadata>&& expected) {
    std::vector<BlockMetadata> expectedAdjusted;
    // This is for convenience, we automatically insert all mixed blocks
    // which must be always returned.
    std::ranges::set_union(
        expected, mixedBlocks, std::back_inserter(expectedAdjusted),
        [](const BlockMetadata& b1, const BlockMetadata& b2) {
          return b1.blockIndex_ < b2.blockIndex_;
        });
    ASSERT_EQ(expr->evaluate(blocks, 0), expectedAdjusted);
  }
};

}  // namespace

//______________________________________________________________________________
TEST_F(TestPrefilterExprOnBlockMetadata, testBlockFormatForDebugging) {
  EXPECT_EQ(
      "#BlockMetadata\n(first) Triple: I:0 V:10 D:33.000000 V:0\n(last) "
      "Triple: I:0 V:10 D:33.000000 V:0\nnum. rows: 0.\n",
      (std::stringstream() << b5).str());
  EXPECT_EQ(
      "#BlockMetadata\n(first) Triple: I:-4 V:10 D:33.000000 V:0\n(last) "
      "Triple: D:2.000000 V:10 D:33.000000 V:0\nnum. rows: 0.\n",
      (std::stringstream() << b11).str());
  EXPECT_EQ(
      "#BlockMetadata\n(first) Triple: V:14 V:10 D:33.000000 V:0\n(last) "
      "Triple: V:17 V:10 D:33.000000 V:0\nnum. rows: 0.\n",
      (std::stringstream() << b21).str());
}

// Test Relational Expressions
//______________________________________________________________________________
// Test LessThanExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(TestPrefilterExprOnBlockMetadata, testLessThanExpressions) {
  makeTest(lt(IntId(5)),
           {b5, b6, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(lt(IntId(-12)), {b18});
  makeTest(lt(IntId(0)), {b9, b10, b15, b16, b17, b18});
  makeTest(lt(DoubleId(-14.01)), {b18});
  makeTest(lt(DoubleId(-11.22)), {b17, b18});
  makeTest(lt(DoubleId(-4.121)), {b9, b15, b16, b17, b18});
  makeTest(lt(VocabId(0)), {b18});
  makeTest(lt(VocabId(12)), {b18, b19});
  makeTest(lt(VocabId(14)), {b18, b19});
  makeTest(lt(VocabId(16)), {b18, b19, b20, b21});
  makeTest(lt(IntId(100)),
           {b5, b6, b7, b8, b9, b10, b12, b13, b14, b15, b16, b17, b18});
  makeTest(lt(undef), {});
  makeTest(lt(falseId), {});
  makeTest(lt(trueId), {b2, b3});
  makeTest(lt(referenceDate1), {});
  makeTest(lt(referenceDateEqual), {b22});
  makeTest(lt(referenceDate2), {b22, b23, b24});
  makeTest(lt(BlankNodeId(11)), {b24});
}

//______________________________________________________________________________
// Test LessEqualExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(TestPrefilterExprOnBlockMetadata, testLessEqualExpressions) {
  makeTest(le(IntId(0)), {b5, b6, b9, b10, b11, b15, b16, b17, b18});
  makeTest(le(IntId(-6)), {b9, b11, b15, b16, b17, b18});
  makeTest(le(IntId(7)),
           {b5, b6, b7, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(le(IntId(-9)), {b9, b11, b17, b18});
  makeTest(le(DoubleId(-9.131)), {b9, b11, b17, b18});
  makeTest(le(DoubleId(1.1415)), {b5, b6, b9, b10, b11, b15, b16, b17, b18});
  makeTest(le(DoubleId(3.1415)),
           {b5, b6, b9, b10, b11, b12, b15, b16, b17, b18});
  makeTest(le(DoubleId(-11.99999999999999)), {b17, b18});
  makeTest(le(DoubleId(-14.03)), {b18});
  makeTest(le(VocabId(0)), {b18});
  makeTest(le(VocabId(11)), {b18, b19});
  makeTest(le(VocabId(14)), {b18, b19, b20, b21});
  makeTest(le(undef), {});
  makeTest(le(falseId), {b2, b3});
  makeTest(le(trueId), {b2, b3, b4});
  makeTest(le(referenceDateEqual), {b22, b23});
  makeTest(le(BlankNodeId(11)), {b24});
}

//______________________________________________________________________________
// Test GreaterThanExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(TestPrefilterExprOnBlockMetadata, testGreaterThanExpression) {
  makeTest(gt(DoubleId(5.5375)), {b7, b8, b11, b14, b18});
  makeTest(gt(DoubleId(9.9994)), {b14});
  makeTest(gt(IntId(-5)), {b5, b6, b7, b8, b10, b11, b12, b13, b14, b15});
  makeTest(gt(DoubleId(-5.5375)),
           {b5, b6, b7, b8, b10, b11, b12, b13, b14, b15});
  makeTest(gt(DoubleId(-6.2499999)),
           {b5, b6, b7, b8, b10, b11, b12, b13, b14, b15});
  makeTest(gt(IntId(1)), {b6, b7, b8, b11, b12, b13, b14});
  makeTest(gt(IntId(3)), {b6, b7, b8, b11, b13, b14});
  makeTest(gt(IntId(4)), {b6, b7, b8, b11, b14});
  makeTest(gt(IntId(-4)), {b5, b6, b7, b8, b11, b12, b13, b14, b15});
  makeTest(gt(IntId(33)), {});
  makeTest(gt(VocabId(22)), {b22});
  makeTest(gt(VocabId(14)), {b21, b22});
  makeTest(gt(VocabId(12)), {b19, b20, b21, b22});
  makeTest(gt(undef), {});
  makeTest(gt(falseId), {b4});
  makeTest(gt(trueId), {});
  makeTest(gt(referenceDateEqual), {b24});
  makeTest(gt(referenceDate1), {b22, b23, b24});
  makeTest(gt(referenceDate2), {b24});
}

//______________________________________________________________________________
// Test GreaterEqualExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(TestPrefilterExprOnBlockMetadata, testGreaterEqualExpression) {
  makeTest(ge(IntId(0)), {b5, b6, b7, b8, b11, b12, b13, b14});
  makeTest(ge(IntId(8)), {b8, b11, b14});
  makeTest(ge(DoubleId(9.98)), {b11, b14});
  makeTest(ge(IntId(-3)), {b5, b6, b7, b8, b11, b12, b13, b14, b15});
  makeTest(ge(IntId(-10)),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16});
  makeTest(ge(DoubleId(-3.1415)), {b5, b6, b7, b8, b11, b12, b13, b14, b15});
  makeTest(ge(DoubleId(-4.000001)),
           {b5, b6, b7, b8, b10, b11, b12, b13, b14, b15});
  makeTest(ge(DoubleId(10.000)), {b11, b14});
  makeTest(ge(DoubleId(-15.22)),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(ge(DoubleId(7.999999)), {b8, b11, b14});
  makeTest(ge(DoubleId(10.0001)), {});
  makeTest(ge(VocabId(14)), {b18, b19, b20, b21, b22});
  makeTest(ge(VocabId(10)), {b18, b19, b20, b21, b22});
  makeTest(ge(VocabId(17)), {b18, b21, b22});
  makeTest(ge(undef), {});
  makeTest(ge(falseId), {b2, b3, b4});
  makeTest(ge(trueId), {b4});
  makeTest(ge(referenceDateEqual), {b23, b24});
}

//______________________________________________________________________________
// Test EqualExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(TestPrefilterExprOnBlockMetadata, testEqualExpression) {
  makeTest(eq(IntId(0)), {b4, b5, b6, b11});
  makeTest(eq(IntId(5)), {b6, b7, b11, b14});
  makeTest(eq(IntId(22)), {});
  makeTest(eq(IntId(-10)), {b9, b11, b18});
  makeTest(eq(DoubleId(-6.25)), {b15, b16});
  makeTest(eq(IntId(-11)), {b17});
  makeTest(eq(DoubleId(-14.02)), {b18});
  makeTest(eq(DoubleId(-0.001)), {b11});
  makeTest(eq(DoubleId(0)), {b4, b5, b6, b11});
  makeTest(eq(IntId(2)), {b6, b11, b12});
  makeTest(eq(DoubleId(5.5)), {b7, b11, b14});
  makeTest(eq(DoubleId(1.5)), {b6, b11});
  makeTest(eq(VocabId(1)), {b18});
  makeTest(eq(VocabId(14)), {b18, b19, b20, b21});
  makeTest(eq(VocabId(11)), {b18, b19});
  makeTest(eq(VocabId(17)), {b18, b21});
  makeTest(eq(IntId(-4)), {b10, b11, b15});
  makeTest(eq(trueId), {b4});
  makeTest(eq(referenceDate1), {b22});
  makeTest(eq(referenceDateEqual), {b23});
  makeTest(eq(referenceDate2), {});
}

//______________________________________________________________________________
// Test NotEqualExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(TestPrefilterExprOnBlockMetadata, testNotEqualExpression) {
  makeTest(neq(DoubleId(0.00)),
           {b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(neq(IntId(-4)),
           {b5, b6, b7, b8, b9, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(neq(DoubleId(0.001)),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(neq(IntId(2)),
           {b5, b6, b7, b8, b9, b10, b11, b13, b14, b15, b16, b17, b18});
  makeTest(neq(DoubleId(-6.2500)),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b17, b18});
  makeTest(neq(IntId(5)),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(neq(DoubleId(-101.23)),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(neq(VocabId(0)), {b19, b20, b21, b22});
  makeTest(neq(VocabId(7)), {b18, b19, b20, b21, b22});
  makeTest(neq(VocabId(14)), {b18, b19, b21, b22});
  makeTest(neq(VocabId(17)), {b18, b19, b20, b21, b22});
  makeTest(neq(undef), {});
  makeTest(neq(falseId), {b4});
  makeTest(neq(referenceDateEqual), {b22, b24});
  makeTest(neq(referenceDate1), {b22, b23, b24});
}

// Test Logical Expressions
//______________________________________________________________________________
// Test AndExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(TestPrefilterExprOnBlockMetadata, testAndExpression) {
  makeTest(andExpr(ge(VocabId(10)), gt(VocabId(10))), {b19, b20, b21, b22});
  makeTest(andExpr(ge(VocabId(10)), ge(VocabId(10))), {b19, b20, b21, b22});
  makeTest(andExpr(ge(VocabId(12)), gt(VocabId(17))), {b22});
  makeTest(andExpr(ge(VocabId(12)), gt(VocabId(17))), {b22});
  makeTest(andExpr(ge(VocabId(10)), lt(VocabId(14))), {b19});
  makeTest(andExpr(le(VocabId(0)), lt(VocabId(10))), {b18});
  makeTest(andExpr(le(VocabId(17)), lt(VocabId(17))), {b18, b19, b20, b21});
  makeTest(andExpr(ge(DoubleId(-6.25)), lt(IntId(-7))), {});
  makeTest(andExpr(gt(DoubleId(-6.25)), lt(DoubleId(-6.25))), {});
  makeTest(andExpr(gt(IntId(0)), lt(IntId(0))), {});
  makeTest(andExpr(gt(IntId(-10)), lt(DoubleId(0))), {b9, b10, b11, b15, b16});
  makeTest(andExpr(gt(IntId(0)), eq(DoubleId(0))), {b6, b11});
  makeTest(andExpr(ge(IntId(0)), eq(IntId(0))), {b5, b6, b11});
  makeTest(andExpr(gt(DoubleId(-34.23)), ge(DoubleId(15.1))), {});
  makeTest(andExpr(lt(IntId(0)), le(DoubleId(-4))),
           {b9, b10, b11, b15, b16, b17, b18});
  makeTest(andExpr(neq(IntId(0)), neq(IntId(-4))),
           {b6, b7, b8, b9, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(andExpr(neq(DoubleId(-3.141)), eq(DoubleId(4.5))),
           {b6, b11, b14, b18});
  makeTest(andExpr(neq(DoubleId(-6.25)), lt(IntId(0))),
           {b9, b10, b11, b15, b17, b18});
  makeTest(andExpr(le(DoubleId(-4)), ge(DoubleId(1))), {});
  makeTest(andExpr(le(DoubleId(-2)), eq(IntId(-3))), {b11, b15});
  makeTest(andExpr(andExpr(le(IntId(10)), gt(DoubleId(0))), eq(undef)), {});
  makeTest(andExpr(gt(referenceDate1), le(IntId(10))), {});
  makeTest(andExpr(gt(IntId(4)), andExpr(gt(DoubleId(8)), lt(IntId(10)))),
           {b8, b14});
  makeTest(andExpr(eq(IntId(0)), andExpr(lt(IntId(-20)), gt(IntId(30)))), {});
  makeTest(andExpr(eq(IntId(0)), andExpr(le(IntId(0)), ge(IntId(0)))),
           {b4, b5, b6, b11});
}

//______________________________________________________________________________
// Test OrExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(TestPrefilterExprOnBlockMetadata, testOrExpression) {
  makeTest(orExpr(lt(VocabId(22)), le(VocabId(0))), {b18, b19, b20, b21});
  makeTest(orExpr(le(VocabId(0)), ge(VocabId(16))), {b18, b21, b22});
  makeTest(orExpr(gt(VocabId(17)), ge(VocabId(17))), {b21, b22});
  makeTest(orExpr(lt(DoubleId(-5.95)), eq(VocabId(14))),
           {b9, b15, b16, b17, b18, b19, b20, b21});
  makeTest(orExpr(eq(DoubleId(0)), neq(VocabId(14))),
           {b5, b6, b11, b18, b19, b21});
  makeTest(orExpr(eq(DoubleId(0)), eq(DoubleId(-6.25))),
           {b5, b6, b11, b15, b16, b18});
  makeTest(orExpr(gt(undef), le(IntId(-6))), {b9, b15, b16, b17, b18});
  makeTest(orExpr(le(trueId), gt(referenceDate1)), {b2, b3, b4, b22, b23, b24});
  makeTest(orExpr(eq(IntId(0)), orExpr(lt(IntId(-10)), gt(IntId(8)))),
           {b5, b6, b8, b11, b14, b17, b18});
  makeTest(orExpr(gt(referenceDate2), eq(trueId)), {b4});
  makeTest(orExpr(eq(VocabId(17)), orExpr(lt(VocabId(0)), gt(VocabId(20)))),
           {b21, b22});
  makeTest(orExpr(eq(undef), gt(referenceDateEqual)), {b24});
  makeTest(orExpr(gt(IntId(8)), gt(DoubleId(22.1))), {b8, b14});
  makeTest(orExpr(lt(DoubleId(-8.25)), le(IntId(-10))), {b9, b17, b18});
  makeTest(orExpr(eq(IntId(0)), neq(DoubleId(0.25))),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(orExpr(gt(referenceDate1), orExpr(gt(trueId), eq(IntId(0)))),
           {b4, b5, b6, b11, b22, b23, b24});
  makeTest(orExpr(gt(DoubleId(-6.25)), lt(DoubleId(-6.25))),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b17, b18});
  makeTest(orExpr(orExpr(eq(IntId(0)), eq(IntId(5))),
                  orExpr(eq(DoubleId(-6.25)), lt(DoubleId(-12)))),
           {b4, b5, b6, b7, b11, b14, b15, b16, b18});
  makeTest(orExpr(le(trueId), gt(falseId)), {b2, b3, b4});
  makeTest(orExpr(eq(VocabId(0)), eq(DoubleId(0.25))), {b6, b11, b18});
}

//______________________________________________________________________________
// Test NotExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(TestPrefilterExprOnBlockMetadata, testNotExpression) {
  makeTest(notExpr(eq(VocabId(2))), {b18, b19, b20, b21, b22});
  makeTest(notExpr(eq(VocabId(14))), {b18, b19, b21, b22});
  makeTest(notExpr(neq(VocabId(14))), {b19, b20, b21});
  makeTest(notExpr(gt(VocabId(2))), {b18});
  makeTest(notExpr(lt(DoubleId(-14.01))),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(notExpr(ge(DoubleId(-14.01))), {b18});
  makeTest(notExpr(gt(DoubleId(-4.00))), {b9, b10, b11, b15, b16, b17, b18});
  makeTest(notExpr(ge(DoubleId(-24.4))), {b18});
  makeTest(notExpr(gt(referenceDate2)), {b22, b23});
  makeTest(notExpr(le(trueId)), {});
  makeTest(notExpr(le(IntId(0))), {b6, b7, b8, b11, b12, b13, b14});
  makeTest(notExpr(gt(undef)), {});
  makeTest(notExpr(eq(DoubleId(-6.25))),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b17, b18});
  makeTest(notExpr(neq(DoubleId(4))), {b6, b11, b13, b14, b18});
  makeTest(notExpr(gt(DoubleId(0))),
           {b4, b5, b6, b9, b10, b11, b15, b16, b17, b18});
  makeTest(notExpr(notExpr(eq(IntId(0)))), {b4, b5, b6, b11});
  makeTest(notExpr(notExpr(neq(DoubleId(-6.25)))),
           {b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b17, b18});
  makeTest(notExpr(notExpr(lt(VocabId(10)))), {b18});
  makeTest(notExpr(notExpr(ge(DoubleId(3.99)))), {b6, b7, b8, b11, b13, b14});
  makeTest(notExpr(andExpr(le(IntId(0)), ge(IntId(0)))),
           {b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(notExpr(andExpr(neq(IntId(-10)), neq(DoubleId(-14.02)))), {b9, b18});
  makeTest(
      notExpr(andExpr(gt(IntId(10)), ge(DoubleId(-6.25)))),
      {b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(
      notExpr(andExpr(lt(DoubleId(-7)), ge(IntId(6)))),
      {b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(notExpr(orExpr(le(IntId(0)), ge(DoubleId(6)))),
           {b6, b7, b11, b12, b13, b14});
  makeTest(notExpr(orExpr(ge(DoubleId(0)), gt(IntId(-10)))),
           {b9, b11, b17, b18});
  makeTest(notExpr(orExpr(lt(VocabId(10)), gt(VocabId(10)))), {b19});
  makeTest(notExpr(orExpr(lt(DoubleId(-4)), gt(IntId(-4)))), {b10, b11, b15});
  makeTest(notExpr(orExpr(gt(IntId(-42)), ge(VocabId(0)))), {b11});
  makeTest(notExpr(orExpr(ge(VocabId(14)), gt(VocabId(15)))), {b18, b19});
}

//______________________________________________________________________________
// Test PrefilterExpressions mixed
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(TestPrefilterExprOnBlockMetadata, testGeneralPrefilterExprCombinations) {
  makeTest(andExpr(notExpr(gt(DoubleId(-14.01))), lt(IntId(0))), {b18});
  makeTest(
      orExpr(andExpr(gt(DoubleId(8.25)), le(IntId(10))), eq(DoubleId(-6.25))),
      {b8, b14, b15, b16});
  makeTest(
      orExpr(andExpr(gt(DoubleId(8.25)), le(IntId(10))), lt(DoubleId(-6.25))),
      {b8, b9, b14, b17, b18});
  makeTest(andExpr(orExpr(ge(trueId), le(falseId)), eq(referenceDate1)), {});
  makeTest(andExpr(eq(IntId(0)), orExpr(lt(IntId(-11)), le(IntId(-12)))), {});
  makeTest(
      andExpr(eq(DoubleId(-4)), orExpr(gt(IntId(-4)), lt(DoubleId(-1.25)))),
      {b10, b11, b15});
  makeTest(orExpr(notExpr(andExpr(lt(IntId(10)), gt(IntId(5)))), eq(IntId(0))),
           {b4, b5, b6, b7, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(andExpr(orExpr(gt(VocabId(16)), le(VocabId(5))), gt(DoubleId(7.25))),
           {});
  makeTest(andExpr(lt(falseId), orExpr(lt(IntId(10)), gt(DoubleId(17.25)))),
           {});
  makeTest(
      orExpr(andExpr(gt(VocabId(16)), ge(VocabId(17))), gt(DoubleId(7.25))),
      {b8, b14, b18, b21, b22});
  makeTest(orExpr(eq(trueId), andExpr(gt(referenceDate1), lt(referenceDate2))),
           {b4, b22, b23});
}

//______________________________________________________________________________
// Test that correct errors are thrown for invalid input (condition)
TEST_F(TestPrefilterExprOnBlockMetadata, testInputConditionCheck) {
  makeTestErrorCheck(le(IntId(5)), blocksWithDuplicate1,
                     "The provided data blocks must be unique.");
  makeTestErrorCheck(andExpr(gt(VocabId(10)), le(VocabId(20))),
                     blocksWithDuplicate2,
                     "The provided data blocks must be unique.");
  makeTestErrorCheck(gt(DoubleId(2)), blocksInvalidOrder1,
                     "The blocks must be provided in sorted order.");
  makeTestErrorCheck(andExpr(gt(VocabId(10)), le(VocabId(20))),
                     blocksInvalidOrder2,
                     "The blocks must be provided in sorted order.");
  makeTestErrorCheck(
      gt(DoubleId(2)), blocks,
      "The values in the columns up to the evaluation column must be "
      "consistent.",
      1);
  makeTestErrorCheck(
      gt(DoubleId(2)), blocks,
      "The values in the columns up to the evaluation column must be "
      "consistent.",
      2);
}

//______________________________________________________________________________
// Check for correctness given only one BlockMetadata value is provided.
TEST_F(TestPrefilterExprOnBlockMetadata, testWithOneBlockMetadataValue) {
  auto expr = orExpr(eq(DoubleId(-6.25)), eq(IntId(0)));
  std::vector<BlockMetadata> input = {b16};
  EXPECT_EQ(expr->evaluate(input, 0), input);
  EXPECT_EQ(expr->evaluate(input, 1), std::vector<BlockMetadata>{});
  EXPECT_EQ(expr->evaluate(input, 2), std::vector<BlockMetadata>{});
}
