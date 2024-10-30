//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <vector>

#include "./SparqlExpressionTestHelpers.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
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

//______________________________________________________________________________
//______________________________________________________________________________
// TEST SECTION 2
//______________________________________________________________________________
//______________________________________________________________________________
namespace {

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
struct TestDates {
  const Id referenceDate1 = DateId(DateParser, "1999-11-11");
  const Id referenceDate2 = DateId(DateParser, "2005-02-27");
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
// LESS THAN (`<`, `SparqlExpression`)
constexpr auto ltSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::LessThanExpression>;
// LESS EQUAL (`<=`, `SparqlExpression`)
constexpr auto leSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::LessEqualExpression>;
// EQUAL (`==`, `SparqlExpression`)
constexpr auto eqSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::EqualExpression>;
// NOT EQUAL (`!=`, `SparqlExpression`)
constexpr auto neqSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::NotEqualExpression>;
// GREATER EQUAL (`>=`, `SparqlExpression`)
constexpr auto geSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::GreaterEqualExpression>;
// GREATER THAN (`>`, `SparqlExpression`)
constexpr auto gtSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::GreaterThanExpression>;
// AND (`&&`, `SparqlExpression`)
constexpr auto andSprqlExpr = &makeAndExpression;
// OR (`||`, `SparqlExpression`)
constexpr auto orSprqlExpr = &makeOrExpression;
// NOT (`!`, `SparqlExpression`)
constexpr auto notSprqlExpr = &makeUnaryNegateExpression;

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
// Construct a `PAIR` with the given `PrefilterExpression` and `Variable` value.
auto pr = [](std::unique_ptr<PrefilterExpression> expr,
             const Variable& var) -> PrefilterExprVariablePair {
  return {std::move(expr), var};
};
}  // namespace

//______________________________________________________________________________
// Test PrefilterExpression equality operator.
TEST(PrefilterExpression, testEqualityOperator) {
  const TestDates dt{};
  // Relational PrefilterExpressions
  ASSERT_FALSE(*ge(dt.referenceDate1) == *ge(dt.referenceDate2));
  ASSERT_FALSE(*neq(BoolId(true)) == *eq(BoolId(true)));
  ASSERT_TRUE(*eq(IntId(1)) == *eq(IntId(1)));
  ASSERT_TRUE(*ge(dt.referenceDate1) == *ge(dt.referenceDate1));
  // NotExpression
  ASSERT_TRUE(*notExpr(eq(IntId(0))) == *notExpr(eq(IntId(0))));
  ASSERT_TRUE(*notExpr(notExpr(ge(VocabId(0)))) ==
              *notExpr(notExpr(ge(VocabId(0)))));
  ASSERT_FALSE(*notExpr(gt(IntId(0))) == *eq(IntId(0)));
  ASSERT_FALSE(*notExpr(andExpr(eq(IntId(1)), eq(IntId(0)))) ==
               *notExpr(ge(VocabId(0))));
  // Binary PrefilterExpressions (AND and OR)
  ASSERT_TRUE(*orExpr(eq(IntId(0)), le(IntId(0))) ==
              *orExpr(eq(IntId(0)), le(IntId(0))));
  ASSERT_TRUE(*andExpr(le(VocabId(1)), le(IntId(0))) ==
              *andExpr(le(VocabId(1)), le(IntId(0))));
  ASSERT_FALSE(*orExpr(eq(IntId(0)), le(IntId(0))) ==
               *andExpr(le(VocabId(1)), le(IntId(0))));
  ASSERT_FALSE(*notExpr(orExpr(eq(IntId(0)), le(IntId(0)))) ==
               *orExpr(eq(IntId(0)), le(IntId(0))));
}

//______________________________________________________________________________
// Test PrefilterExpression content formatting for debugging.
TEST(PrefilterExpression, checkPrintFormattedPrefilterExpression) {
  auto expr = lt(IntId(10));
  EXPECT_EQ((std::stringstream() << *expr).str(),
            "Prefilter RelationalExpression<0>\nValueId: I:10\n.\n");
  expr = notExpr(eq(VocabId(0)));
  EXPECT_EQ((std::stringstream() << *expr).str(),
            "Prefilter NotExpression:\nchild {Prefilter "
            "RelationalExpression<3>\nValueId: V:0\n}\n.\n");
  expr = orExpr(le(IntId(0)), eq(IntId(5)));
  EXPECT_EQ((std::stringstream() << *expr).str(),
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
  const TestDates dt{};
  const Variable var = Variable{"?x"};
  // ?x == BooldId(true) (RelationalExpression Sparql)
  // expected: <(== BoolId(true)), ?x> (PrefilterExpression, Variable)
  evalAndEqualityCheck(eqSprql(var, BoolId(true)), pr(eq(BoolId(true)), var));
  // For BoolId(true) == ?x we expect the same PrefilterExpression pair.
  evalAndEqualityCheck(eqSprql(BoolId(true), var), pr(eq(BoolId(true)), var));
  // ?x != BooldId(true) (RelationalExpression Sparql)
  // expected: <(!= BoolId(true)), ?x> (PrefilterExpression, Variable)
  evalAndEqualityCheck(neqSprql(var, BoolId(false)),
                       pr(neq(BoolId(false)), var));
  // Same expected value for BoolId(true) != ?x.
  evalAndEqualityCheck(neqSprql(BoolId(false), var),
                       pr(neq(BoolId(false)), var));
  // ?x >= IntId(1)
  // expected: <(>= IntId(1)), ?x>
  evalAndEqualityCheck(geSprql(var, IntId(1)), pr(ge(IntId(1)), var));
  // IntId(1) <= ?x
  // expected: <(>= IntId(1)), ?x>
  evalAndEqualityCheck(leSprql(IntId(1), var), pr(ge(IntId(1)), var));
  // ?x > IntId(1)
  // expected: <(> IntId(1)), ?x>
  evalAndEqualityCheck(gtSprql(var, IntId(1)), pr(gt(IntId(1)), var));
  // VocabId(10) != ?x
  // expected: <(!= VocabId(10)), ?x>
  evalAndEqualityCheck(neqSprql(VocabId(10), var), pr(neq(VocabId(10)), var));
  // BlankNodeId(1) > ?x
  // expected: <(< BlankNodeId(1)), ?x>
  evalAndEqualityCheck(geSprql(BlankNodeId(1), var),
                       pr(le(BlankNodeId(1)), var));
  // ?x < BlankNodeId(1)
  // expected: <(< BlankNodeId(1)), ?x>
  evalAndEqualityCheck(ltSprql(var, BlankNodeId(1)),
                       pr(lt(BlankNodeId(1)), var));
  // ?x <= referenceDate1
  // expected: <(<= referenceDate1), ?x>
  evalAndEqualityCheck(leSprql(var, dt.referenceDate1),
                       pr(le(dt.referenceDate1), var));
  // referenceDate1 >= ?x
  // expected: <(<= referenceDate1), ?x>
  evalAndEqualityCheck(geSprql(dt.referenceDate1, var),
                       pr(le(dt.referenceDate1), var));
  // DoubleId(10.2) < ?x
  // expected: <(> DoubleId(10.2)), ?x>
  evalAndEqualityCheck(ltSprql(DoubleId(10.2), var),
                       pr(gt(DoubleId(10.2)), var));
  // ?x > DoubleId(10.2)
  // expected: <(> DoubleId(10.2)), ?x>
  evalAndEqualityCheck(gtSprql(var, DoubleId(10.2)),
                       pr(gt(DoubleId(10.2)), var));
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
  evalAndEqualityCheck(
      andSprqlExpr(geSprql(varX, IntId(10)), neqSprql(varX, IntId(20))),
      pr(andExpr(ge(IntId(10)), neq(IntId(20))), varX));
  // ?z > VocabId(0) AND ?y > 0 AND ?x < 30.00
  // expected prefilter pairs
  // {<(< 30.00), ?x>, <(> 0), ?y>, <(> VocabId(0)), ?z>}
  evalAndEqualityCheck(andSprqlExpr(andSprqlExpr(gtSprql(varZ, VocabId(0)),
                                                 gtSprql(varY, IntId(0))),
                                    ltSprql(varX, DoubleId(30.00))),
                       pr(lt(DoubleId(30.00)), varX), pr(gt(IntId(0)), varY),
                       pr(gt(VocabId(0)), varZ));

  // ?x == VocabId(10) AND ?y >= VocabId(10)
  // expected prefilter pairs:
  // {<(== VocabId(10)), ?x>, <(>= VocabId(10)), ?y>}
  evalAndEqualityCheck(
      andSprqlExpr(eqSprql(varX, VocabId(10)), geSprql(varY, VocabId(10))),
      pr(eq(VocabId(10)), varX), pr(ge(VocabId(10)), varY));
  // !(?x >= 10 OR ?x <= 0)
  // expected prefilter pairs:
  // {<!(?x >= 10 OR ?x <= 0), ?x>}
  evalAndEqualityCheck(notSprqlExpr(orSprqlExpr(geSprql(varX, IntId(10)),
                                                leSprql(varX, IntId(0)))),
                       pr(notExpr(orExpr(ge(IntId(10)), le(IntId(0)))), varX));
  // !(?z == VocabId(10) AND ?z >= VocabId(20))
  // expected prefilter pairs:
  // {<!(?z == VocabId(10) AND ?z >= VocabId(20)) , ?z>}
  evalAndEqualityCheck(
      notSprqlExpr(
          andSprqlExpr(eqSprql(varZ, VocabId(10)), geSprql(varZ, VocabId(20)))),
      pr(notExpr(andExpr(eq(VocabId(10)), ge(VocabId(20)))), varZ));
  // (?x == VocabId(10) AND ?z == VocabId(0)) AND ?y != DoubleId(22.1)
  // expected prefilter pairs:
  // {<(==VocabId(10)) , ?x>, <(!=DoubleId(22.1)), ?y>, <(==VocabId(0)), ?z>}
  evalAndEqualityCheck(andSprqlExpr(andSprqlExpr(eqSprql(VocabId(10), varX),
                                                 eqSprql(varZ, VocabId(0))),
                                    neqSprql(DoubleId(22.1), varY)),
                       pr(eq(VocabId(10)), varX), pr(neq(DoubleId(22.1)), varY),
                       pr(eq(VocabId(0)), varZ));
  // (?z >= 1000 AND ?x == VocabId(10)) OR ?z >= 10000
  // expected prefilter pairs:
  // {<((>=1000) OR (>= 10000)), ?z>}
  evalAndEqualityCheck(orSprqlExpr(andSprqlExpr(geSprql(varZ, IntId(1000)),
                                                eqSprql(varX, VocabId(10))),
                                   geSprql(varZ, IntId(10000))),
                       pr(orExpr(ge(IntId(1000)), ge(IntId(10000))), varZ));
  // !((?z <= VocabId(10) OR ?y <= VocabId(10)) OR ?x <= VocabId(10))
  // expected prefilter pairs:
  // {<!(<= VocabId(10)), ?x>, <!(<= VocabId(10)), ?y>, <!(<= VocabId(10)), ?z>}
  evalAndEqualityCheck(
      notSprqlExpr(orSprqlExpr(
          orSprqlExpr(leSprql(varZ, VocabId(10)), leSprql(varY, VocabId(10))),
          leSprql(varX, VocabId(10)))),
      pr(notExpr(le(VocabId(10))), varX), pr(notExpr(le(VocabId(10))), varY),
      pr(notExpr(le(VocabId(10))), varZ));
  // ?x >= 10 AND ?y >= 10
  // expected prefilter pairs:
  // {<(>= 10), ?x>, <(>= 10), ?y>}
  evalAndEqualityCheck(
      andSprqlExpr(geSprql(varX, IntId(10)), geSprql(varY, IntId(10))),
      pr(ge(IntId(10)), varX), pr(ge(IntId(10)), varY));
  // ?x <= 0 AND ?y <= 0
  // expected prefilter pairs:
  // {<(<= 0), ?x>, <(<= 0), ?y>}
  evalAndEqualityCheck(
      andSprqlExpr(leSprql(varX, IntId(0)), leSprql(varY, IntId(0))),
      pr(le(IntId(0)), varX), pr(le(IntId(0)), varY));
  // (?x >= 10 AND ?y >= 10) OR (?x <= 0 AND ?y <= 0)
  // expected prefilter pairs:
  // {<((>= 10) OR (<= 0)), ?x> <(>= 10) OR (<= 0)), ?y>}
  evalAndEqualityCheck(
      orSprqlExpr(
          andSprqlExpr(geSprql(varX, IntId(10)), geSprql(varY, IntId(10))),
          andSprqlExpr(leSprql(varX, IntId(0)), leSprql(varY, IntId(0)))),
      pr(orExpr(ge(IntId(10)), le(IntId(0))), varX),
      pr(orExpr(ge(IntId(10)), le(IntId(0))), varY));
  // !(?x >= 10 OR ?y >= 10) OR !(?x <= 0 OR ?y <= 0)
  // expected prefilter pairs:
  // {<((!(>= 10) OR !(<= 0))), ?x> <(!(>= 10) OR !(<= 0))), ?y>}
  evalAndEqualityCheck(
      orSprqlExpr(notSprqlExpr(orSprqlExpr(geSprql(varX, IntId(10)),
                                           geSprql(varY, IntId(10)))),
                  notSprqlExpr(orSprqlExpr(leSprql(varX, IntId(0)),
                                           leSprql(varY, IntId(0))))),
      pr(orExpr(notExpr(ge(IntId(10))), notExpr(le(IntId(0)))), varX),
      pr(orExpr(notExpr(ge(IntId(10))), notExpr(le(IntId(0)))), varY));
  // !(?x == VocabId(10) OR ?x == VocabId(20)) AND !(?z >= 10.00 OR ?y == false)
  // expected prefilter pairs:
  // {<!((== VocabId(10)) OR (== VocabId(20))), ?x>, <!(== false), ?y>,
  // <!(>= 10), ?z>}
  evalAndEqualityCheck(
      andSprqlExpr(notSprqlExpr(orSprqlExpr(eqSprql(varX, VocabId(10)),
                                            eqSprql(varX, VocabId(20)))),
                   notSprqlExpr(orSprqlExpr(geSprql(varZ, DoubleId(10)),
                                            eqSprql(varY, BoolId(false))))),
      pr(notExpr(orExpr(eq(VocabId(10)), eq(VocabId(20)))), varX),
      pr(notExpr(eq(BoolId(false))), varY),
      pr(notExpr(ge(DoubleId(10))), varZ));
  // !(!(?x >= 10 AND ?y >= 10)) OR !(!(?x <= 0 AND ?y <= 0))
  // expected prefilter pairs:
  // {<(!!(>= 10) OR !!(<= 0)), ?x>, <(!!(>= 10) OR !!(<= 0)) ,?y>}
  evalAndEqualityCheck(
      orSprqlExpr(notSprqlExpr(notSprqlExpr(andSprqlExpr(
                      geSprql(varX, IntId(10)), geSprql(varY, IntId(10))))),
                  notSprqlExpr(notSprqlExpr(andSprqlExpr(
                      leSprql(varX, IntId(0)), leSprql(varY, IntId(0)))))),
      pr(orExpr(notExpr(notExpr(ge(IntId(10)))),
                notExpr(notExpr(le(IntId(0))))),
         varX),
      pr(orExpr(notExpr(notExpr(ge(IntId(10)))),
                notExpr(notExpr(le(IntId(0))))),
         varY));
  // !((?x >= VocabId(0) AND ?x <= VocabId(10)) OR !(?x != VocabId(99)))
  // expected prefilter pairs:
  // {<!(((>= VocabId(0)) AND (<= VocabId(10))) OR !(!= VocabId(99))) , ?x>}
  evalAndEqualityCheck(
      notSprqlExpr(orSprqlExpr(
          andSprqlExpr(geSprql(varX, VocabId(0)), leSprql(varX, VocabId(10))),
          notSprqlExpr(neqSprql(varX, VocabId(99))))),
      pr(notExpr(orExpr(andExpr(ge(VocabId(0)), le(VocabId(10))),
                        notExpr(neq(VocabId(99))))),
         varX));
  // !((?y >= 10 AND ?y <= 100) OR !(?x >= VocabId(99)))
  // expected prefilter pairs:
  // {<!((>= VocabId(0)) AND (<= VocabId(10)), ?y>, <!(!(>= VocabId(99))), ?x>}
  evalAndEqualityCheck(
      notSprqlExpr(orSprqlExpr(
          andSprqlExpr(geSprql(varY, VocabId(0)), leSprql(varY, VocabId(10))),
          notSprqlExpr(geSprql(varX, VocabId(99))))),
      pr(notExpr(notExpr(ge(VocabId(99)))), varX),
      pr(notExpr(andExpr(ge(VocabId(0)), le(VocabId(10)))), varY));
  // ?z >= 10 AND ?z <= 100 AND ?x >= 10 AND ?x != 50 AND !(?y <= 10) AND
  // !(?city <= VocabId(1000) OR ?city == VocabId(1005))
  // expected prefilter pairs:
  // {<!((<= VocabId(1000)) OR (== VocabId(1005))), ?city>, <((>= 10) AND (!=
  // 50)), ?x>, <!(<= 10), ?y>, <((>= 10) AND (<= 100)), ?z>}
  evalAndEqualityCheck(
      andSprqlExpr(
          andSprqlExpr(
              andSprqlExpr(geSprql(varZ, IntId(10)), leSprql(varZ, IntId(100))),
              andSprqlExpr(andSprqlExpr(geSprql(varX, IntId(10)),
                                        neqSprql(varX, IntId(50))),
                           notSprqlExpr(leSprql(varY, IntId(10))))),
          notSprqlExpr(orSprqlExpr(leSprql(Variable{"?city"}, VocabId(1000)),
                                   eqSprql(Variable{"?city"}, VocabId(1005))))),
      pr(notExpr(orExpr(le(VocabId(1000)), eq(VocabId(1005)))),
         Variable{"?city"}),
      pr(andExpr(ge(IntId(10)), neq(IntId(50))), varX),
      pr(notExpr(le(IntId(10))), varY),
      pr(andExpr(ge(IntId(10)), le(IntId(100))), varZ));
  // ?x >= 10 OR (?x >= -10 AND ?x < 0.00)
  // expected prefilter pairs:
  // {<((>= 10) OR ((>= -10) AND (< 0.00))), ?x>}
  evalAndEqualityCheck(
      orSprqlExpr(geSprql(varX, IntId(10)),
                  andSprqlExpr(geSprql(varX, IntId(-10)),
                               ltSprql(varX, DoubleId(0.00)))),
      pr(orExpr(ge(IntId(10)), andExpr(ge(IntId(-10)), lt(DoubleId(0.00)))),
         varX));
  // !(!(?x >= 10) OR !!(?x >= -10 AND ?x < 0.00))
  // expected prefilter pairs:
  // {<!(!(>= 10) OR !!((>= -10) AND (< 0.00))), ?x>}
  evalAndEqualityCheck(
      notSprqlExpr(orSprqlExpr(
          notSprqlExpr(geSprql(varX, IntId(10))),
          notSprqlExpr(notSprqlExpr(andSprqlExpr(
              geSprql(varX, IntId(-10)), ltSprql(varX, DoubleId(0.00))))))),
      pr(notExpr(orExpr(
             notExpr(ge(IntId(10))),
             notExpr(notExpr(andExpr(ge(IntId(-10)), lt(DoubleId(0.00))))))),
         varX));
  // ?y != ?x AND ?x >= 10
  // expected prefilter pairs:
  // {<(>= 10), ?x>}
  evalAndEqualityCheck(
      andSprqlExpr(neqSprql(varY, varX), geSprql(varX, IntId(10))),
      pr(ge(IntId(10)), varX));
  evalAndEqualityCheck(
      andSprqlExpr(geSprql(varX, IntId(10)), neqSprql(varY, varX)),
      pr(ge(IntId(10)), varX));
}

//______________________________________________________________________________
// For this test we expect that no PrefilterExpression is available.
TEST(SparqlExpression, getEmptyPrefilterFromSparqlRelational) {
  const Variable var = Variable{"?x"};
  const Iri iri = I("<Iri>");
  const Literal lit = L("\"lit\"");
  evalToEmptyCheck(leSprql(var, var));
  evalToEmptyCheck(neqSprql(iri, var));
  evalToEmptyCheck(eqSprql(var, iri));
  evalToEmptyCheck(neqSprql(IntId(10), DoubleId(23.3)));
  evalToEmptyCheck(gtSprql(DoubleId(10), lit));
  evalToEmptyCheck(ltSprql(VocabId(10), BoolId(10)));
  evalToEmptyCheck(geSprql(lit, lit));
  evalToEmptyCheck(eqSprql(iri, iri));
  evalToEmptyCheck(orSprqlExpr(eqSprql(var, var), gtSprql(var, IntId(0))));
  evalToEmptyCheck(orSprqlExpr(eqSprql(var, var), gtSprql(var, var)));
  evalToEmptyCheck(andSprqlExpr(eqSprql(var, var), gtSprql(var, var)));
}

//______________________________________________________________________________
// For the following more complex SparqlExpression trees, we also expect an
// empty PrefilterExpression vector.
TEST(SparqlExpression, getEmptyPrefilterForMoreComplexSparqlExpressions) {
  const Variable varX = Variable{"?x"};
  const Variable varY = Variable{"?y"};
  const Variable varZ = Variable{"?z"};
  // ?x <= 10.00 OR ?y > 10
  evalToEmptyCheck(
      orSprqlExpr(leSprql(DoubleId(10), varX), gtSprql(IntId(10), varY)));
  // ?x >= VocabId(23) OR ?z == VocabId(1)
  evalToEmptyCheck(
      orSprqlExpr(geSprql(varX, VocabId(23)), eqSprql(varZ, VocabId(1))));
  // (?x < VocabId(10) OR ?z <= VocabId(4)) OR ?z != 5.00
  evalToEmptyCheck(orSprqlExpr(
      orSprqlExpr(ltSprql(varX, VocabId(10)), leSprql(VocabId(4), varZ)),
      neqSprql(varZ, DoubleId(5))));
  // !(?z > 10.20 AND ?x < 0.001)
  // is equal to
  // ?z <= 10.20 OR ?x >= 0.001
  evalToEmptyCheck(notSprqlExpr(andSprqlExpr(gtSprql(DoubleId(10.2), varZ),
                                             ltSprql(DoubleId(0.001), varX))));
  // !(?x > 10.20 AND ?z != VocabId(22))
  // is equal to
  // ?x <= 10.20 OR ?z == VocabId(22)
  evalToEmptyCheck(notSprqlExpr(andSprqlExpr(gtSprql(DoubleId(10.2), varX),
                                             neqSprql(VocabId(22), varZ))));
  // !(!((?x < VocabId(10) OR ?x <= VocabId(4)) OR ?z != 5.00))
  // is equal to
  // (?x < VocabId(10) OR ?x <= VocabId(4)) OR ?z != 5.00
  evalToEmptyCheck(notSprqlExpr(notSprqlExpr(orSprqlExpr(
      orSprqlExpr(ltSprql(varX, VocabId(10)), leSprql(VocabId(4), varX)),
      neqSprql(varZ, DoubleId(5))))));
  // !(?x != 10 AND !(?y >= 10.00 OR ?z <= 10))
  // is equal to
  // ?x == 10 OR ?y >= 10.00 OR ?z <= 10
  evalToEmptyCheck(notSprqlExpr(
      andSprqlExpr(neqSprql(varX, IntId(10)),
                   notSprqlExpr(orSprqlExpr(geSprql(varY, DoubleId(10.00)),
                                            leSprql(varZ, IntId(10)))))));
  // !((?x != 10 AND ?z != 10) AND (?y == 10 AND ?x >= 20))
  // is equal to
  //?x == 10 OR ?z == 10 OR ?y != 10 OR ?x < 20
  evalToEmptyCheck(notSprqlExpr(andSprqlExpr(
      andSprqlExpr(neqSprql(varX, IntId(10)), neqSprql(varZ, IntId(10))),
      andSprqlExpr(eqSprql(varY, IntId(10)), geSprql(varX, IntId(20))))));
  // !(?z >= 40 AND (?z != 10.00 AND ?y != VocabId(1)))
  // is equal to
  // ?z <= 40 OR ?z == 10.00 OR ?y == VocabId(1)
  evalToEmptyCheck(notSprqlExpr(andSprqlExpr(
      geSprql(varZ, IntId(40)), andSprqlExpr(neqSprql(varZ, DoubleId(10.00)),
                                             neqSprql(varY, VocabId(1))))));
  // ?z <= true OR !(?x == 10 AND ?y == 10)
  // is equal to
  // ?z <= true OR ?x != 10 OR ?y != 10
  evalToEmptyCheck(
      orSprqlExpr(leSprql(varZ, BoolId(true)),
                  notSprqlExpr(andSprqlExpr(eqSprql(varX, IntId(10)),
                                            eqSprql(IntId(10), varY)))));
  // !(!(?z <= true OR !(?x == 10 AND ?y == 10)))
  // is equal to
  // ?z <= true OR ?x != 10 OR ?y != 10
  evalToEmptyCheck(notSprqlExpr(notSprqlExpr(
      orSprqlExpr(leSprql(varZ, BoolId(true)),
                  notSprqlExpr(andSprqlExpr(eqSprql(varX, IntId(10)),
                                            eqSprql(IntId(10), varY)))))));
  // !(!(?x != 10 OR !(?y >= 10.00 AND ?z <= 10)))
  // is equal to
  // ?x != 10 OR ?y < 10.00 OR ?z > 10
  evalToEmptyCheck(notSprqlExpr(notSprqlExpr(
      orSprqlExpr(neqSprql(varX, IntId(10)),
                  notSprqlExpr(andSprqlExpr(geSprql(varY, DoubleId(10.00)),
                                            leSprql(varZ, IntId(10))))))));
  // !(!(?x == VocabId(10) OR ?y >= 25) AND !(!(?z == true AND ?country ==
  // VocabId(20))))
  // is equal to
  // ?x == VocabId(10) OR ?y >= 25 OR ?z == true AND ?country == VocabId(20)
  evalToEmptyCheck(notSprqlExpr(andSprqlExpr(
      notSprqlExpr(
          orSprqlExpr(eqSprql(varX, VocabId(10)), geSprql(varY, IntId(25)))),
      notSprqlExpr(notSprqlExpr(
          andSprqlExpr(eqSprql(varZ, BoolId(true)),
                       eqSprql(Variable{"?country"}, VocabId(20))))))));
}

// Test that the conditions required for a correct merge of child
// PrefilterExpressions are properly checked during the PrefilterExpression
// construction procedure. This check is applied in the SparqlExpression (for
// NOT, AND and OR) counter-expressions, while constructing their corresponding
// PrefilterExpression.
//______________________________________________________________________________
TEST(SparqlExpression, checkPropertiesForPrefilterConstruction) {
  namespace pd = prefilterExpressions::detail;
  const Variable varX = Variable{"?x"};
  const Variable varY = Variable{"?y"};
  const Variable varZ = Variable{"?z"};
  const Variable varW = Variable{"?w"};
  std::vector<PrefilterExprVariablePair> vec{};
  vec.push_back(pr(andExpr(lt(IntId(5)), gt(DoubleId(-0.01))), varX));
  vec.push_back(pr(gt(VocabId(0)), varY));
  EXPECT_NO_THROW(pd::checkPropertiesForPrefilterConstruction(vec));
  vec.push_back(pr(eq(VocabId(33)), varZ));
  EXPECT_NO_THROW(pd::checkPropertiesForPrefilterConstruction(vec));
  // Add a pair <PrefilterExpression, Variable> with duplicate Variable.
  vec.push_back(pr(gt(VocabId(0)), varZ));
  AD_EXPECT_THROW_WITH_MESSAGE(
      pd::checkPropertiesForPrefilterConstruction(vec),
      ::testing::HasSubstr("For each relevant Variable must exist exactly "
                           "one <PrefilterExpression, Variable> pair."));
  // Remove the last two pairs and add a pair <PrefilterExpression, Variable>
  // which violates the order on Variable(s).
  vec.pop_back();
  vec.pop_back();
  vec.push_back(pr(eq(VocabId(0)), varW));
  AD_EXPECT_THROW_WITH_MESSAGE(
      pd::checkPropertiesForPrefilterConstruction(vec),
      ::testing::HasSubstr(
          "The vector must contain the <PrefilterExpression, Variable> "
          "pairs in sorted order w.r.t. Variable value."));
}
