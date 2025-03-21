//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include <vector>

#include "./PrefilterExpressionTestHelpers.h"
#include "./SparqlExpressionTestHelpers.h"
#include "util/GTestHelpers.h"

using ad_utility::testing::BlankNodeId;
using ad_utility::testing::BoolId;
using ad_utility::testing::DoubleId;
using ad_utility::testing::IntId;
using ad_utility::testing::UndefId;
using ad_utility::testing::VocabId;

namespace {

using namespace prefilterExpressions;
using namespace makeFilterExpression;
using namespace filterHelper;

constexpr auto getId = PrefilterExpression::getValueIdFromIdOrLocalVocabEntry;

//______________________________________________________________________________
// Helper to create Date-ValueIds for a date specified by its components.
const auto makeIdForDate = [](int year, int month = 0, int day = 0,
                              int hour = -1, int minute = 0,
                              double seconds = 0.00) {
  return Id::makeFromDate(
      DateYearOrDuration(Date(year, month, day, hour, minute, seconds)));
};

using DateT = DateYearOrDuration::Type;
// For (large) year values `> 9999` or `< 9999`.
const auto makeIdForLYearDate = [](int year, DateT type = DateT::Date) {
  return Id::makeFromDate(DateYearOrDuration(year, type));
};

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

COLUMN 0 and COLUMN 1 contain fixed values, this is a necessary condition
that is also checked during the pre-filtering procedure. The actual evaluation
column (we filter w.r.t. values of COLUMN 2) contains mixed types.
*/

// Fixed column ValueIds for our test-metadata.
const Id VocabId10 = VocabId(10);
const Id DoubleId33 = DoubleId(33);
const Id GraphId = VocabId(0);

//______________________________________________________________________________
class PrefilterExpressionOnMetadataTest : public ::testing::Test {
 public:
  // Not directly used. However, given that we depend on LocalVocab values
  // during evaluation an active Index + global vocabulary is required.
  QueryExecutionContext* qet = ad_utility::testing::getQec();
  LocalVocab vocab{};
  const Id referenceDate1 = DateId(DateParser, "1999-11-11");
  const Id referenceDate2 = DateId(DateParser, "2005-02-27");
  const Id undef = Id::makeUndefined();
  const Id falseId = BoolId(false);
  const Id trueId = BoolId(true);
  const Id referenceDateEqual = DateId(DateParser, "2000-01-01");
  const LocalVocabEntry augsburg = LVE("\"Augsburg\"");
  const LocalVocabEntry berlin = LVE("\"Berlin\"");
  const LocalVocabEntry düsseldorf = LVE("\"Düsseldorf\"");
  const LocalVocabEntry frankfurt = LVE("\"Frankfurt\"");
  const LocalVocabEntry hamburg = LVE("\"Hamburg\"");
  const LocalVocabEntry köln = LVE("\"Köln\"");
  const LocalVocabEntry münchen = LVE("\"München\"");
  const LocalVocabEntry stuttgart = LVE("\"Stuttgart\"");
  const Id idAugsburg = getId(augsburg, vocab);
  const Id idBerlin = getId(berlin, vocab);
  const Id idDüsseldorf = getId(düsseldorf, vocab);
  const Id idFrankfurt = getId(frankfurt, vocab);
  const Id idHamburg = getId(hamburg, vocab);
  const Id idKöln = getId(köln, vocab);
  const Id idMünchen = getId(münchen, vocab);
  const Id idStuttgart = getId(stuttgart, vocab);

  // Define BlockMetadata
  const BlockMetadata b1 = makeBlock(undef, undef);
  const BlockMetadata bFirstIncomplete =
      makeBlock(undef, undef, IntId(10), IntId(10), IntId(10), IntId(11));
  const BlockMetadata b2 = makeBlock(undef, falseId);
  const BlockMetadata b3 = makeBlock(falseId, falseId);
  const BlockMetadata b4 = makeBlock(trueId, IntId(0));
  const BlockMetadata b4Incomplete = makeBlock(
      trueId, IntId(0), VocabId(10), DoubleId(33), VocabId(11), DoubleId(33));
  const BlockMetadata b5 = makeBlock(IntId(0), IntId(0));
  const BlockMetadata b5Incomplete = makeBlock(
      IntId(0), IntId(0), VocabId(10), DoubleId(33), VocabId(10), DoubleId(34));
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
  const BlockMetadata b18 = makeBlock(DoubleId(-14.01), idAugsburg);
  const BlockMetadata b19 = makeBlock(idDüsseldorf, idHamburg);
  const BlockMetadata b20 = makeBlock(idHamburg, idHamburg);
  const BlockMetadata b21 = makeBlock(idHamburg, idMünchen);
  const BlockMetadata b22 =
      makeBlock(idStuttgart, DateId(DateParser, "1999-12-12"));
  const BlockMetadata b23 = makeBlock(DateId(DateParser, "2000-01-01"),
                                      DateId(DateParser, "2000-01-01"));
  const BlockMetadata b24 =
      makeBlock(DateId(DateParser, "2024-10-08"), BlankNodeId(10));
  const BlockMetadata bLastIncomplete = makeBlock(
      DateId(DateParser, "2024-10-08"), DateId(DateParser, "2025-10-08"),
      VocabId(0), VocabId(0), VocabId(1), VocabId(0));

  // Date related blocks.
  const BlockMetadata b1Date =
      makeBlock(makeIdForLYearDate(-17546), makeIdForLYearDate(-17545));
  const BlockMetadata b2Date =
      makeBlock(makeIdForLYearDate(-16300), makeIdForLYearDate(-16099));
  const BlockMetadata b3Date =
      makeBlock(makeIdForLYearDate(-15345), makeIdForLYearDate(-10001));
  const BlockMetadata b4Date =
      makeBlock(makeIdForLYearDate(-10001), makeIdForDate(2000, 1, 2));
  const BlockMetadata b5Date = makeBlock(
      makeIdForDate(2000, 8, 9), makeIdForDate(2010, 2, 2, 3, 5, 59.99));
  const BlockMetadata b6Date = makeBlock(
      makeIdForDate(2015, 5, 10), makeIdForDate(2020, 7, 25, 12, 30, 45));
  const BlockMetadata b7Date = makeBlock(makeIdForDate(2025, 3, 15, 8, 0, 0),
                                         makeIdForDate(2030, 6, 5, 14, 15, 30));
  const BlockMetadata b8Date =
      makeBlock(makeIdForDate(2040, 1, 1, 3, 33, 22.35),
                makeIdForDate(2040, 4, 18, 22, 45, 10.5));
  const BlockMetadata b9Date =
      makeBlock(makeIdForDate(2041, 9, 30, 6, 20, 0.001),
                makeIdForDate(2050, 12, 31, 23, 59, 59.99));
  const BlockMetadata b10Date =
      makeBlock(makeIdForLYearDate(10001), makeIdForLYearDate(10033));
  const BlockMetadata b11Date =
      makeBlock(makeIdForLYearDate(10033), makeIdForLYearDate(12000));
  const BlockMetadata b12Date =
      makeBlock(makeIdForLYearDate(14579), makeIdForLYearDate(38263));

  // All blocks that contain mixed (ValueId) types over column 2,
  // or possibly incomplete ones.
  const std::vector<BlockMetadata> mixedBlocks = {b2, b4, b11, b18, b22, b24};

  // All blocks that contain mixed types over column 2 + the first and last
  // incomplete block.
  const std::vector<BlockMetadata> mixedAndIncompleteBlocks = {
      bFirstIncomplete, b2, b4, b11, b18, b22, bLastIncomplete};

  // Ordered and unique vector with BlockMetadata
  const std::vector<BlockMetadata> blocks = {
      b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,  b10, b11, b12,
      b13, b14, b15, b16, b17, b18, b19, b20, b21, b22, b23, b24};

  // Selection of date related blocks.
  const std::vector<BlockMetadata> dateBlocks = {
      b1Date, b2Date, b3Date, b4Date,  b5Date,  b6Date,
      b7Date, b8Date, b9Date, b10Date, b11Date, b12Date};

  const std::vector<BlockMetadata> blocksIncomplete = {bFirstIncomplete,
                                                       b2,
                                                       b3,
                                                       b4,
                                                       b5,
                                                       b6,
                                                       b7,
                                                       b8,
                                                       b9,
                                                       b10,
                                                       b11,
                                                       b12,
                                                       b13,
                                                       b14,
                                                       b15,
                                                       b16,
                                                       b17,
                                                       b18,
                                                       b19,
                                                       b20,
                                                       b21,
                                                       b22,
                                                       b23,
                                                       bLastIncomplete};

  const std::vector<BlockMetadata> blocksInvalidOrder1 = {
      b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,  b10, b11, b12,
      b13, b14, b15, b16, b17, b18, b19, b20, b21, b22, b24, b23};

  const std::vector<BlockMetadata> blocksInvalidOrder2 = {
      b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,  b10, b11, b12,
      b14, b10, b15, b16, b17, b18, b19, b20, b21, b22, b23, b24};

  const std::vector<BlockMetadata> blocksWithDuplicate1 = {b1, b1, b2,
                                                           b3, b4, b5};

  const std::vector<BlockMetadata> blocksWithDuplicate2 = {b1,  b2,  b3,
                                                           b23, b24, b24};

  const std::vector<BlockMetadata> blocksInconsistent1 = {
      b1, b2, b3, b4Incomplete, b5, b6, b7};

  const std::vector<BlockMetadata> blocksInconsistent2 = {
      b1, b2, b3, b4, b5Incomplete, b6, b7};

  // Function to create BlockMetadata
  const BlockMetadata makeBlock(const ValueId& first2Id, const ValueId& last2Id,
                                const ValueId& first0Id = VocabId10,
                                const ValueId& first1Id = DoubleId33,
                                const ValueId& last0Id = VocabId10,
                                const ValueId& last1Id = DoubleId33) {
    assert(first2Id <= last2Id);
    static size_t blockIdx = 0;
    ++blockIdx;
    return {{{},
             0,
             // COLUMN 0  |  COLUMN 1  |  COLUMN 2
             {first0Id, first1Id, first2Id, GraphId},  // firstTriple
             {last0Id, last1Id, last2Id, GraphId},     // lastTriple
             {},
             false},
            blockIdx};
  }

  // Check if expected error is thrown.
  auto makeTestErrorCheck(std::unique_ptr<PrefilterExpression> expr,
                          const std::vector<BlockMetadata>& input,
                          const std::string& expected,
                          size_t evaluationColumn = 2) {
    std::vector<BlockMetadata> testBlocks = input;
    AD_EXPECT_THROW_WITH_MESSAGE(expr->evaluate(testBlocks, evaluationColumn),
                                 ::testing::HasSubstr(expected));
  }

  // Assert that the PrefilterExpression tree is properly copied when calling
  // method clone.
  auto makeTestClone(std::unique_ptr<PrefilterExpression> expr) {
    ASSERT_EQ(*expr, *expr->clone());
  }

  // Check that the provided expression prefilters the correct blocks.
  auto makeTest(std::unique_ptr<PrefilterExpression> expr,
                std::vector<BlockMetadata>&& expected,
                bool useBlocksIncomplete = false) {
    std::vector<BlockMetadata> expectedAdjusted;
    // This is for convenience, we automatically insert all mixed and possibly
    // incomplete blocks which must be always returned.
    ql::ranges::set_union(
        expected, useBlocksIncomplete ? mixedAndIncompleteBlocks : mixedBlocks,
        std::back_inserter(expectedAdjusted),
        [](const BlockMetadata& b1, const BlockMetadata& b2) {
          return b1.blockIndex_ < b2.blockIndex_;
        });
    std::vector<BlockMetadata> testBlocks =
        useBlocksIncomplete ? blocksIncomplete : blocks;
    ASSERT_EQ(expr->evaluate(testBlocks, 2), expectedAdjusted);
  }
  // Simple `ASSERT_EQ` on date blocks
  auto makeTestDate(std::unique_ptr<PrefilterExpression> expr,
                    std::vector<BlockMetadata>&& expected) {
    ASSERT_EQ(expr->evaluate(dateBlocks, 2), expected);
  }
};

}  // namespace

//______________________________________________________________________________
TEST_F(PrefilterExpressionOnMetadataTest, testBlockFormatForDebugging) {
  auto toString = [](const CompressedBlockMetadata& b) {
    return (std::stringstream{} << b).str();
  };

  auto matcher = [&toString](const std::string& substring) {
    return ::testing::ResultOf(toString, ::testing::HasSubstr(substring));
  };
  EXPECT_THAT(
      b5, matcher("#BlockMetadata\n(first) Triple: V:10 D:33.000000 I:0 "
                  "V:0\n(last) Triple: V:10 D:33.000000 I:0 V:0\nnum. rows: "
                  "0.\n"));
  EXPECT_THAT(
      b11,
      matcher("#BlockMetadata\n(first) Triple: V:10 D:33.000000 I:-4 "
              "V:0\n(last) Triple: V:10 D:33.000000 D:2.000000 V:0\nnum. rows: "
              "0.\n"));
  EXPECT_THAT(
      b21,
      matcher("#BlockMetadata\n(first) Triple: V:10 D:33.000000 L:\"Hamburg\" "
              "V:0\n(last) Triple: V:10 D:33.000000 L:\"M\xC3\xBCnchen\" "
              "V:0\nnum. rows: 0.\n"));

  auto blockWithGraphInfo = b21;
  blockWithGraphInfo.graphInfo_.emplace({IntId(12), IntId(13)});
  EXPECT_THAT(blockWithGraphInfo, matcher("Graphs: I:12, I:13\n"));
}

// Test Relational Expressions
//______________________________________________________________________________
// Test LessThanExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(PrefilterExpressionOnMetadataTest, testLessThanExpressions) {
  makeTest(lt(IntId(5)),
           {b5, b6, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18}, true);
  makeTest(lt(IntId(-12)), {b18});
  makeTest(lt(IntId(0)), {b9, b10, b15, b16, b17, b18}, true);
  makeTest(lt(DoubleId(-14.01)), {b18});
  makeTest(lt(DoubleId(-11.22)), {b17, b18}, true);
  makeTest(lt(DoubleId(-4.121)), {b9, b15, b16, b17, b18});
  makeTest(lt(augsburg), {b18});
  makeTest(lt(frankfurt), {b18, b19});
  makeTest(lt(hamburg), {b18, b19}, true);
  makeTest(lt(münchen), {b18, b19, b20, b21});
  makeTest(lt(IntId(100)),
           {b5, b6, b7, b8, b9, b10, b12, b13, b14, b15, b16, b17, b18});
  makeTest(lt(undef), {});
  makeTest(lt(falseId), {}, true);
  makeTest(lt(trueId), {b2, b3});
  makeTest(lt(referenceDate1), {});
  makeTest(lt(referenceDateEqual), {b22}, true);
  makeTest(lt(referenceDate2), {b22, b23, b24});
  makeTest(lt(BlankNodeId(11)), {b24});
}

//______________________________________________________________________________
// Test LessEqualExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(PrefilterExpressionOnMetadataTest, testLessEqualExpressions) {
  makeTest(le(IntId(0)), {b5, b6, b9, b10, b11, b15, b16, b17, b18});
  makeTest(le(IntId(-6)), {b9, b11, b15, b16, b17, b18});
  makeTest(le(IntId(7)),
           {b5, b6, b7, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18}, true);
  makeTest(le(IntId(-9)), {b9, b11, b17, b18});
  makeTest(le(DoubleId(-9.131)), {b9, b11, b17, b18});
  makeTest(le(DoubleId(1.1415)), {b5, b6, b9, b10, b11, b15, b16, b17, b18});
  makeTest(le(DoubleId(3.1415)),
           {b5, b6, b9, b10, b11, b12, b15, b16, b17, b18});
  makeTest(le(DoubleId(-11.99999999999999)), {b17, b18}, true);
  makeTest(le(DoubleId(-14.03)), {b18});
  makeTest(le(LVE("\"Aachen\"")), {b18});
  makeTest(le(frankfurt), {b18, b19});
  makeTest(le(hamburg), {b18, b19, b20, b21}, true);
  makeTest(le(undef), {});
  makeTest(le(falseId), {b2, b3});
  makeTest(le(trueId), {b2, b3, b4}, true);
  makeTest(le(referenceDateEqual), {b22, b23});
  makeTest(le(BlankNodeId(11)), {b24});
}

//______________________________________________________________________________
// Test GreaterThanExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(PrefilterExpressionOnMetadataTest, testGreaterThanExpression) {
  makeTest(gt(DoubleId(5.5375)), {b7, b8, b11, b14, b18});
  makeTest(gt(DoubleId(9.9994)), {b14}, true);
  makeTest(gt(IntId(-5)), {b5, b6, b7, b8, b10, b11, b12, b13, b14, b15});
  makeTest(gt(DoubleId(-5.5375)),
           {b5, b6, b7, b8, b10, b11, b12, b13, b14, b15}, true);
  makeTest(gt(DoubleId(-6.2499999)),
           {b5, b6, b7, b8, b10, b11, b12, b13, b14, b15}, true);
  makeTest(gt(IntId(1)), {b6, b7, b8, b11, b12, b13, b14});
  makeTest(gt(IntId(3)), {b6, b7, b8, b11, b13, b14}, true);
  makeTest(gt(IntId(4)), {b6, b7, b8, b11, b14});
  makeTest(gt(IntId(-4)), {b5, b6, b7, b8, b11, b12, b13, b14, b15});
  makeTest(gt(IntId(33)), {});
  makeTest(gt(stuttgart), {b22});
  makeTest(gt(hamburg), {b21, b22}, true);
  makeTest(gt(berlin), {b19, b20, b21, b22});
  makeTest(gt(undef), {}, true);
  makeTest(gt(falseId), {b4}, true);
  makeTest(gt(trueId), {});
  makeTest(gt(referenceDateEqual), {b24});
  makeTest(gt(referenceDate1), {b22, b23, bLastIncomplete}, true);
  makeTest(gt(referenceDate2), {bLastIncomplete}, true);
}

//______________________________________________________________________________
// Test GreaterEqualExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(PrefilterExpressionOnMetadataTest, testGreaterEqualExpression) {
  makeTest(ge(IntId(0)), {b5, b6, b7, b8, b11, b12, b13, b14});
  makeTest(ge(IntId(8)), {b8, b11, b14});
  makeTest(ge(DoubleId(9.98)), {b11, b14}, true);
  makeTest(ge(IntId(-3)), {b5, b6, b7, b8, b11, b12, b13, b14, b15}, true);
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
  makeTest(ge(hamburg), {b18, b19, b20, b21, b22}, true);
  makeTest(ge(düsseldorf), {b18, b19, b20, b21, b22});
  makeTest(ge(münchen), {b18, b21, b22});
  makeTest(ge(undef), {}, true);
  makeTest(ge(falseId), {b2, b3, b4}, true);
  makeTest(ge(trueId), {b4});
  makeTest(ge(referenceDateEqual), {b23, b24});
}

//______________________________________________________________________________
// Test EqualExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(PrefilterExpressionOnMetadataTest, testEqualExpression) {
  makeTest(eq(IntId(0)), {b4, b5, b6, b11});
  makeTest(eq(IntId(5)), {b6, b7, b11, b14}, true);
  makeTest(eq(IntId(22)), {});
  makeTest(eq(IntId(-10)), {b9, b11, b18});
  makeTest(eq(DoubleId(-6.25)), {b15, b16});
  makeTest(eq(IntId(-11)), {b17}, true);
  makeTest(eq(DoubleId(-14.02)), {b18});
  makeTest(eq(DoubleId(-0.001)), {b11});
  makeTest(eq(DoubleId(0)), {b4, b5, b6, b11}, true);
  makeTest(eq(IntId(2)), {b6, b11, b12}, true);
  makeTest(eq(DoubleId(5.5)), {b7, b11, b14});
  makeTest(eq(DoubleId(1.5)), {b6, b11});
  makeTest(eq(berlin), {b18});
  makeTest(eq(hamburg), {b18, b19, b20, b21}, true);
  makeTest(eq(frankfurt), {b18, b19});
  makeTest(eq(köln), {b18, b21});
  makeTest(eq(IntId(-4)), {b10, b11, b15}, true);
  makeTest(eq(trueId), {b4});
  makeTest(eq(referenceDate1), {b22});
  makeTest(eq(referenceDateEqual), {b23}, true);
  makeTest(eq(referenceDate2), {});
}

//______________________________________________________________________________
// Test NotEqualExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(PrefilterExpressionOnMetadataTest, testNotEqualExpression) {
  makeTest(neq(DoubleId(0.00)),
           {b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18}, true);
  makeTest(neq(IntId(-4)),
           {b5, b6, b7, b8, b9, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(neq(DoubleId(0.001)),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18},
           true);
  makeTest(neq(IntId(2)),
           {b5, b6, b7, b8, b9, b10, b11, b13, b14, b15, b16, b17, b18});
  makeTest(neq(DoubleId(-6.2500)),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b17, b18});
  makeTest(neq(IntId(5)),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18},
           true);
  makeTest(neq(DoubleId(-101.23)),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18},
           true);
  makeTest(neq(augsburg), {b19, b20, b21, b22});
  makeTest(neq(berlin), {b18, b19, b20, b21, b22}, true);
  makeTest(neq(hamburg), {b18, b19, b21, b22});
  makeTest(neq(münchen), {b18, b19, b20, b21, b22});
  makeTest(neq(undef), {});
  makeTest(neq(falseId), {b4}, true);
  makeTest(neq(referenceDateEqual), {b22, b24});
  makeTest(neq(referenceDate1), {b22, b23, b24});
}

// Test Logical Expressions
//______________________________________________________________________________
// Test AndExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(PrefilterExpressionOnMetadataTest, testAndExpression) {
  makeTest(andExpr(ge(düsseldorf), gt(düsseldorf)), {b19, b20, b21, b22});
  makeTest(andExpr(ge(düsseldorf), ge(düsseldorf)), {b19, b20, b21, b22}, true);
  makeTest(andExpr(ge(frankfurt), gt(münchen)), {b22});
  makeTest(andExpr(ge(frankfurt), gt(münchen)), {b22}, true);
  makeTest(andExpr(ge(düsseldorf), lt(hamburg)), {b19}, true);
  makeTest(andExpr(le(augsburg), lt(düsseldorf)), {b18});
  makeTest(andExpr(le(münchen), lt(münchen)), {b18, b19, b20, b21});
  makeTest(andExpr(ge(DoubleId(-6.25)), lt(IntId(-7))), {});
  makeTest(andExpr(gt(DoubleId(-6.25)), lt(DoubleId(-6.25))), {});
  makeTest(andExpr(gt(IntId(0)), lt(IntId(0))), {});
  makeTest(andExpr(gt(IntId(-10)), lt(DoubleId(0))), {b9, b10, b11, b15, b16},
           true);
  makeTest(andExpr(gt(IntId(0)), eq(DoubleId(0))), {b6, b11});
  makeTest(andExpr(ge(IntId(0)), eq(IntId(0))), {b5, b6, b11}, true);
  makeTest(andExpr(gt(DoubleId(-34.23)), ge(DoubleId(15.1))), {}, true);
  makeTest(andExpr(lt(IntId(0)), le(DoubleId(-4))),
           {b9, b10, b11, b15, b16, b17, b18});
  makeTest(andExpr(neq(IntId(0)), neq(IntId(-4))),
           {b6, b7, b8, b9, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(andExpr(neq(DoubleId(-3.141)), eq(DoubleId(4.5))),
           {b6, b11, b14, b18}, true);
  makeTest(andExpr(neq(DoubleId(-6.25)), lt(IntId(0))),
           {b9, b10, b11, b15, b17, b18});
  makeTest(andExpr(le(DoubleId(-4)), ge(DoubleId(1))), {});
  makeTest(andExpr(le(DoubleId(-2)), eq(IntId(-3))), {b11, b15});
  makeTest(andExpr(andExpr(le(IntId(10)), gt(DoubleId(0))), eq(undef)), {});
  makeTest(andExpr(gt(referenceDate1), le(IntId(10))), {});
  makeTest(andExpr(gt(IntId(4)), andExpr(gt(DoubleId(8)), lt(IntId(10)))),
           {b8, b14}, true);
  makeTest(andExpr(eq(IntId(0)), andExpr(lt(IntId(-20)), gt(IntId(30)))), {});
  makeTest(andExpr(eq(IntId(0)), andExpr(le(IntId(0)), ge(IntId(0)))),
           {b4, b5, b6, b11});
}

//______________________________________________________________________________
// Test OrExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(PrefilterExpressionOnMetadataTest, testOrExpression) {
  makeTest(orExpr(lt(stuttgart), le(augsburg)), {b18, b19, b20, b21});
  makeTest(orExpr(le(augsburg), ge(köln)), {b18, b21, b22});
  makeTest(orExpr(gt(münchen), ge(münchen)), {b21, b22});
  makeTest(orExpr(lt(DoubleId(-5.95)), eq(hamburg)),
           {b9, b15, b16, b17, b18, b19, b20, b21});
  makeTest(orExpr(eq(DoubleId(0)), neq(hamburg)), {b5, b6, b11, b18, b19, b21},
           true);
  makeTest(orExpr(eq(DoubleId(0)), eq(DoubleId(-6.25))),
           {b5, b6, b11, b15, b16, b18}, true);
  makeTest(orExpr(gt(undef), le(IntId(-6))), {b9, b15, b16, b17, b18});
  makeTest(orExpr(le(trueId), gt(referenceDate1)), {b2, b3, b4, b22, b23, b24});
  makeTest(orExpr(eq(IntId(0)), orExpr(lt(IntId(-10)), gt(IntId(8)))),
           {b5, b6, b8, b11, b14, b17, b18}, true);
  makeTest(orExpr(gt(referenceDate2), eq(trueId)), {b4});
  makeTest(orExpr(eq(münchen), orExpr(lt(augsburg), gt(stuttgart))), {b21, b22},
           true);
  makeTest(orExpr(eq(undef), gt(referenceDateEqual)), {b24});
  makeTest(orExpr(gt(IntId(8)), gt(DoubleId(22.1))), {b8, b14});
  makeTest(orExpr(lt(DoubleId(-8.25)), le(IntId(-10))), {b9, b17, b18}, true);
  makeTest(orExpr(eq(IntId(0)), neq(DoubleId(0.25))),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(orExpr(gt(referenceDate1), orExpr(gt(trueId), eq(IntId(0)))),
           {b4, b5, b6, b11, b22, b23, b24});
  makeTest(orExpr(gt(DoubleId(-6.25)), lt(DoubleId(-6.25))),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b17, b18});
  makeTest(orExpr(orExpr(eq(IntId(0)), eq(IntId(5))),
                  orExpr(eq(DoubleId(-6.25)), lt(DoubleId(-12)))),
           {b4, b5, b6, b7, b11, b14, b15, b16, b18});
  makeTest(orExpr(le(trueId), gt(falseId)), {b2, b3, b4}, true);
  makeTest(orExpr(eq(augsburg), eq(DoubleId(0.25))), {b6, b11, b18}, true);
}

//______________________________________________________________________________
// Test NotExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(PrefilterExpressionOnMetadataTest, testNotExpression) {
  makeTest(notExpr(eq(berlin)), {b18, b19, b20, b21, b22}, true);
  makeTest(notExpr(eq(hamburg)), {b18, b19, b21, b22});
  makeTest(notExpr(neq(hamburg)), {b19, b20, b21}, true);
  makeTest(notExpr(gt(berlin)), {b18});
  makeTest(notExpr(lt(DoubleId(-14.01))),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(notExpr(ge(DoubleId(-14.01))), {b18});
  makeTest(notExpr(gt(DoubleId(-4.00))), {b9, b10, b11, b15, b16, b17, b18},
           true);
  makeTest(notExpr(ge(DoubleId(-24.4))), {b18});
  makeTest(notExpr(gt(referenceDate2)), {b22, b23});
  makeTest(notExpr(le(trueId)), {});
  makeTest(notExpr(le(IntId(0))), {b6, b7, b8, b11, b12, b13, b14}, true);
  makeTest(notExpr(gt(undef)), {});
  makeTest(notExpr(eq(DoubleId(-6.25))),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b17, b18});
  makeTest(notExpr(neq(DoubleId(4))), {b6, b11, b13, b14, b18});
  makeTest(notExpr(gt(DoubleId(0))),
           {b4, b5, b6, b9, b10, b11, b15, b16, b17, b18}, true);
  makeTest(notExpr(notExpr(eq(IntId(0)))), {b4, b5, b6, b11}, true);
  makeTest(notExpr(notExpr(neq(DoubleId(-6.25)))),
           {b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b17, b18});
  makeTest(notExpr(notExpr(lt(düsseldorf))), {b18});
  makeTest(notExpr(notExpr(ge(DoubleId(3.99)))), {b6, b7, b8, b11, b13, b14},
           true);
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
           {b6, b7, b11, b12, b13, b14}, true);
  makeTest(notExpr(orExpr(ge(DoubleId(0)), gt(IntId(-10)))),
           {b9, b11, b17, b18}, true);
  makeTest(notExpr(orExpr(lt(düsseldorf), gt(düsseldorf))), {b19});
  makeTest(notExpr(orExpr(lt(DoubleId(-4)), gt(IntId(-4)))), {b10, b11, b15},
           true);
  makeTest(notExpr(orExpr(gt(IntId(-42)), ge(augsburg))), {b11}, true);
  makeTest(notExpr(orExpr(ge(hamburg), gt(köln))), {b18, b19});
}

//______________________________________________________________________________
// Test PrefilterExpressions mixed
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(PrefilterExpressionOnMetadataTest,
       testGeneralPrefilterExprCombinations) {
  makeTest(andExpr(notExpr(gt(DoubleId(-14.01))), lt(IntId(0))), {b18});
  makeTest(
      orExpr(andExpr(gt(DoubleId(8.25)), le(IntId(10))), eq(DoubleId(-6.25))),
      {b8, b14, b15, b16}, true);
  makeTest(
      orExpr(andExpr(gt(DoubleId(8.25)), le(IntId(10))), lt(DoubleId(-6.25))),
      {b8, b9, b14, b17, b18});
  makeTest(andExpr(orExpr(ge(trueId), le(falseId)), eq(referenceDate1)), {});
  makeTest(andExpr(eq(IntId(0)), orExpr(lt(IntId(-11)), le(IntId(-12)))), {},
           true);
  makeTest(
      andExpr(eq(DoubleId(-4)), orExpr(gt(IntId(-4)), lt(DoubleId(-1.25)))),
      {b10, b11, b15});
  makeTest(orExpr(notExpr(andExpr(lt(IntId(10)), gt(IntId(5)))), eq(IntId(0))),
           {b4, b5, b6, b7, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(andExpr(orExpr(gt(köln), le(berlin)), gt(DoubleId(7.25))), {}, true);
  makeTest(andExpr(lt(falseId), orExpr(lt(IntId(10)), gt(DoubleId(17.25)))),
           {});
  makeTest(orExpr(andExpr(gt(köln), ge(münchen)), gt(DoubleId(7.25))),
           {b8, b14, b18, b21, b22});
  makeTest(orExpr(eq(trueId), andExpr(gt(referenceDate1), lt(referenceDate2))),
           {b4, b22, b23}, true);
}

//______________________________________________________________________________
// Test PrefilterExpression explicitly on date related values (Date-ValueId)
TEST_F(PrefilterExpressionOnMetadataTest, testRelationalPrefilteringDates) {
  makeTestDate(
      gt(makeIdForDate(2000, 1, 2)),
      {b5Date, b6Date, b7Date, b8Date, b9Date, b10Date, b11Date, b12Date});
  makeTestDate(
      andExpr(gt(makeIdForDate(2000, 1, 2)), lt(makeIdForDate(2040, 0, 0))),
      {b5Date, b6Date, b7Date});
  makeTestDate(ge(makeIdForDate(2000, 1, 2)),
               {b4Date, b5Date, b6Date, b7Date, b8Date, b9Date, b10Date,
                b11Date, b12Date});
  makeTestDate(neq(makeIdForLYearDate(12000)),
               {b1Date, b2Date, b3Date, b4Date, b5Date, b6Date, b7Date, b8Date,
                b9Date, b10Date, b11Date, b12Date});
  makeTestDate(gt(makeIdForLYearDate(12000)), {b12Date});
  makeTestDate(lt(makeIdForLYearDate(12000)),
               {b1Date, b2Date, b3Date, b4Date, b5Date, b6Date, b7Date, b8Date,
                b9Date, b10Date, b11Date});
  makeTestDate(le(makeIdForDate(2027, 0, 1)),
               {b1Date, b2Date, b3Date, b4Date, b5Date, b6Date, b7Date});
  makeTestDate(orExpr(gt(makeIdForDate(2030, 6, 5, 14, 15, 30)),
                      le(makeIdForLYearDate(-16100))),
               {b1Date, b2Date, b8Date, b9Date, b10Date, b11Date, b12Date});
}

//______________________________________________________________________________
// Test that correct errors are thrown for invalid input (condition)
TEST_F(PrefilterExpressionOnMetadataTest, testInputConditionCheck) {
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
      gt(DoubleId(2)), blocksInconsistent1,
      "The values in the columns up to the evaluation column must be "
      "consistent.",
      1);
  makeTestErrorCheck(
      gt(DoubleId(2)), blocksInconsistent2,
      "The values in the columns up to the evaluation column must be "
      "consistent.",
      2);
}

//______________________________________________________________________________
// Check for correctness given only one BlockMetadata value is provided.
TEST_F(PrefilterExpressionOnMetadataTest, testWithFewBlockMetadataValues) {
  auto expr = orExpr(eq(DoubleId(-6.25)), eq(IntId(0)));
  std::vector<BlockMetadata> input = {b16};
  EXPECT_EQ(expr->evaluate(input, 0), input);
  EXPECT_EQ(expr->evaluate(input, 1), input);
  EXPECT_EQ(expr->evaluate(input, 2), input);
  expr = eq(DoubleId(-6.25));
  input = {b15, b16, b17};
  EXPECT_EQ(expr->evaluate(input, 2), (std::vector<BlockMetadata>{b15, b16}));
  EXPECT_EQ(expr->evaluate(input, 1), std::vector<BlockMetadata>{});
  EXPECT_EQ(expr->evaluate(input, 0), std::vector<BlockMetadata>{});
}

//______________________________________________________________________________
// Test method clone. clone() creates a copy of the complete PrefilterExpression
// tree.
TEST_F(PrefilterExpressionOnMetadataTest, testMethodClonePrefilterExpression) {
  makeTestClone(lt(VocabId(10)));
  makeTestClone(gt(referenceDate2));
  makeTestClone(andExpr(lt(VocabId(20)), gt(VocabId(10))));
  makeTestClone(neq(IntId(10)));
  makeTestClone(le(LVE("\"Hello World\"")));
  makeTestClone(orExpr(eq(IntId(10)), neq(DoubleId(10))));
  makeTestClone(notExpr(ge(referenceDate1)));
  makeTestClone(notExpr(notExpr(neq(VocabId(0)))));
  makeTestClone(notExpr(andExpr(eq(IntId(10)), neq(DoubleId(10)))));
  makeTestClone(orExpr(orExpr(eq(VocabId(101)), lt(IntId(100))),
                       andExpr(gt(referenceDate1), lt(referenceDate2))));
  makeTestClone(andExpr(andExpr(neq(IntId(10)), neq(DoubleId(100.23))),
                        orExpr(gt(DoubleId(0.001)), lt(IntId(250)))));
  makeTestClone(orExpr(orExpr(eq(VocabId(101)), lt(IntId(100))),
                       notExpr(andExpr(lt(VocabId(0)), neq(IntId(100))))));
  makeTestClone(orExpr(orExpr(le(LVE("<iri/id5>")), gt(LVE("<iri/id22>"))),
                       neq(LVE("<iri/id10>"))));
}

//______________________________________________________________________________
// Test PrefilterExpression equality operator.
TEST_F(PrefilterExpressionOnMetadataTest, testEqualityOperator) {
  // Relational PrefilterExpressions
  ASSERT_FALSE(*ge(referenceDate1) == *ge(referenceDate2));
  ASSERT_FALSE(*neq(BoolId(true)) == *eq(BoolId(true)));
  ASSERT_TRUE(*eq(IntId(1)) == *eq(IntId(1)));
  ASSERT_TRUE(*ge(referenceDate1) == *ge(referenceDate1));
  ASSERT_TRUE(*eq(LVE("<iri>")) == *eq(LVE("<iri>")));
  ASSERT_FALSE(*gt(LVE("<iri>")) == *gt(LVE("\"iri\"")));
  // NotExpression
  ASSERT_TRUE(*notExpr(eq(IntId(0))) == *notExpr(eq(IntId(0))));
  ASSERT_TRUE(*notExpr(notExpr(ge(VocabId(0)))) ==
              *notExpr(notExpr(ge(VocabId(0)))));
  ASSERT_TRUE(*notExpr(le(LVE("<iri>"))) == *notExpr(le(LVE("<iri>"))));
  ASSERT_FALSE(*notExpr(gt(IntId(0))) == *eq(IntId(0)));
  ASSERT_FALSE(*notExpr(andExpr(eq(IntId(1)), eq(IntId(0)))) ==
               *notExpr(ge(VocabId(0))));
  // Binary PrefilterExpressions (AND and OR)
  ASSERT_TRUE(*orExpr(eq(IntId(0)), le(IntId(0))) ==
              *orExpr(eq(IntId(0)), le(IntId(0))));
  ASSERT_TRUE(*orExpr(lt(LVE("\"L\"")), gt(LVE("\"O\""))) ==
              *orExpr(lt(LVE("\"L\"")), gt(LVE("\"O\""))));
  ASSERT_TRUE(*andExpr(le(VocabId(1)), le(IntId(0))) ==
              *andExpr(le(VocabId(1)), le(IntId(0))));
  ASSERT_FALSE(*orExpr(eq(IntId(0)), le(IntId(0))) ==
               *andExpr(le(VocabId(1)), le(IntId(0))));
  ASSERT_FALSE(*notExpr(orExpr(eq(IntId(0)), le(IntId(0)))) ==
               *orExpr(eq(IntId(0)), le(IntId(0))));
}

//______________________________________________________________________________
// Test PrefilterExpression content formatting for debugging.
TEST(PrefilterExpressionExpressionOnMetadataTest,
     checkPrintFormattedPrefilterExpression) {
  auto exprToString = [](const auto& expr) {
    return (std::stringstream{} << expr).str();
  };
  auto matcher = [&exprToString](const std::string& substring) {
    return ::testing::ResultOf(exprToString, ::testing::Eq(substring));
  };

  EXPECT_THAT(*lt(IntId(10)),
              matcher("Prefilter RelationalExpression<LT(<)>\nreferenceValue_ "
                      ": I:10 .\n.\n"));
  EXPECT_THAT(
      *orExpr(eq(VocabId(0)), eq(VocabId(10))),
      matcher("Prefilter LogicalExpression<OR(||)>\nchild1 {Prefilter "
              "RelationalExpression<EQ(=)>\nreferenceValue_ : V:0 .\n}child2 "
              "{Prefilter RelationalExpression<EQ(=)>\nreferenceValue_ : V:10 "
              ".\n}\n.\n"));
  EXPECT_THAT(
      *neq(DoubleId(8.21)),
      matcher("Prefilter RelationalExpression<NE(!=)>\nreferenceValue_ : "
              "D:8.210000 .\n.\n"));
  EXPECT_THAT(
      *notExpr(eq(VocabId(0))),
      matcher("Prefilter NotExpression:\nchild {Prefilter "
              "RelationalExpression<NE(!=)>\nreferenceValue_ : V:0 .\n}\n.\n"));
  EXPECT_THAT(
      *orExpr(le(IntId(0)), ge(IntId(5))),
      matcher("Prefilter LogicalExpression<OR(||)>\nchild1 {Prefilter "
              "RelationalExpression<LE(<=)>\nreferenceValue_ : I:0 .\n}child2 "
              "{Prefilter RelationalExpression<GE(>=)>\nreferenceValue_ : I:5 "
              ".\n}\n.\n"));
  EXPECT_THAT(
      *andExpr(lt(IntId(20)), gt(IntId(10))),
      matcher("Prefilter LogicalExpression<AND(&&)>\nchild1 {Prefilter "
              "RelationalExpression<LT(<)>\nreferenceValue_ : I:20 .\n}child2 "
              "{Prefilter RelationalExpression<GT(>)>\nreferenceValue_ : I:10 "
              ".\n}\n.\n"));
  EXPECT_THAT(*eq(LVE("\"Sophia\"")),
              matcher("Prefilter RelationalExpression<EQ(=)>\nreferenceValue_ "
                      ": \"Sophia\" .\n.\n"));
  EXPECT_THAT(*neq(LVE("<Iri/custom/value>")),
              matcher("Prefilter RelationalExpression<NE(!=)>\nreferenceValue_ "
                      ": <Iri/custom/value> .\n.\n"));
  EXPECT_THAT(
      *andExpr(orExpr(lt(LVE("\"Bob\"")), ge(LVE("\"Max\""))),
               neq(LVE("\"Lars\""))),
      matcher(
          "Prefilter LogicalExpression<AND(&&)>\nchild1 {Prefilter "
          "LogicalExpression<OR(||)>\nchild1 {Prefilter "
          "RelationalExpression<LT(<)>\nreferenceValue_ : \"Bob\" .\n}child2 "
          "{Prefilter RelationalExpression<GE(>=)>\nreferenceValue_ : \"Max\" "
          ".\n}\n}child2 {Prefilter "
          "RelationalExpression<NE(!=)>\nreferenceValue_ : \"Lars\" "
          ".\n}\n.\n"));
  EXPECT_THAT(
      *orExpr(neq(LVE("<iri/custom/v10>")), neq(LVE("<iri/custom/v66>"))),
      matcher(
          "Prefilter LogicalExpression<OR(||)>\nchild1 {Prefilter "
          "RelationalExpression<NE(!=)>\nreferenceValue_ : <iri/custom/v10> "
          ".\n}child2 {Prefilter RelationalExpression<NE(!=)>\nreferenceValue_ "
          ": <iri/custom/v66> .\n}\n.\n"));
}

//______________________________________________________________________________
// Test PrefilterExpression unknown `CompOp comparison` value detection.
TEST(PrefilterExpressionExpressionOnMetadataTest,
     checkMakePrefilterVecDetectsAndThrowsForInvalidComparisonOp) {
  using namespace prefilterExpressions::detail;
  AD_EXPECT_THROW_WITH_MESSAGE(
      makePrefilterExpressionYearImpl(static_cast<CompOp>(10), 0),
      ::testing::HasSubstr(
          "Set unknown (relational) comparison operator for the creation of "
          "PrefilterExpression on date-values: Undefined CompOp value: 10."));
}
