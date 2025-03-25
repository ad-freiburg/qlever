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
using namespace valueIdComparators;

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
using IdxPair = std::pair<size_t, size_t>;
using IdxPairRanges = std::vector<IdxPair>;
// Convert `IdxPairRanges` to `BlockSubranges` with respect to `BlockIt
// beginBlockSpan` (first possible `std::span<const BlockMetadata>::iterator`).
static BlockSubranges convertFromSpanIdxToSpanBlockItRanges(
    const BlockIt& beginBlockSpan, const IdxPairRanges idxRanges) {
  BlockSubranges blockItRanges;
  blockItRanges.reserve(idxRanges.size());
  ql::ranges::for_each(idxRanges, [&](const IdxPair& idxPair) {
    const auto& [beginIdx, endIdx] = idxPair;
    blockItRanges.emplace_back(BlockIt{beginBlockSpan + beginIdx},
                               BlockIt{beginBlockSpan + endIdx});
  });
  return blockItRanges;
}

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
  // Given that we depend on LocalVocab and Vocab values during evaluation an
  // active Index + global vocabulary is required.
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
  const LocalVocabEntry iri0 = LVE("<a>");
  const LocalVocabEntry iri1 = LVE("<iri>");
  const LocalVocabEntry iri2 = LVE("<iri>");
  const LocalVocabEntry iri3 = LVE("<randomiriref>");
  const LocalVocabEntry iri4 = LVE("<someiri>");
  const LocalVocabEntry iri5 = LVE("<www-iri.de>");
  const LocalVocabEntry iriBegin = LVE("<");
  const Id idAugsburg = getId(augsburg, vocab);
  const Id idBerlin = getId(berlin, vocab);
  const Id idDüsseldorf = getId(düsseldorf, vocab);
  const Id idFrankfurt = getId(frankfurt, vocab);
  const Id idHamburg = getId(hamburg, vocab);
  const Id idKöln = getId(köln, vocab);
  const Id idMünchen = getId(münchen, vocab);
  const Id idStuttgart = getId(stuttgart, vocab);
  const Id idIri0 = getId(iri0, vocab);
  const Id idIri1 = getId(iri1, vocab);
  const Id idIri2 = getId(iri2, vocab);
  const Id idIri3 = getId(iri3, vocab);
  const Id idIri4 = getId(iri4, vocab);
  const Id idIri5 = getId(iri5, vocab);
  const Id iriStart = getId(iriBegin, vocab);

  // Define BlockMetadata
  const BlockMetadata b1 = makeBlock(undef, undef);
  const BlockMetadata bFirstIncomplete =
      makeBlock(undef, undef, IntId(10), IntId(10), IntId(10), IntId(11));
  const BlockMetadata b2 = makeBlock(undef, falseId);
  const BlockMetadata b3 = makeBlock(falseId, falseId);
  const BlockMetadata b4 = makeBlock(trueId, IntId(0));
  const BlockMetadata b4GapNumeric = makeBlock(trueId, idBerlin);
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
  const BlockMetadata b18GapIriAndLiteral =
      makeBlock(DoubleId(-14.01), DateId(DateParser, "1999-01-01"));
  const BlockMetadata b19 = makeBlock(idDüsseldorf, idHamburg);
  const BlockMetadata b20 = makeBlock(idHamburg, idHamburg);
  const BlockMetadata b21 = makeBlock(idHamburg, idMünchen);
  const BlockMetadata b22 = makeBlock(idStuttgart, idIri0);
  const BlockMetadata b23 = makeBlock(idIri1, idIri2);
  const BlockMetadata b24 = makeBlock(idIri3, idIri4);
  const BlockMetadata b25 = makeBlock(idIri5, DateId(DateParser, "1999-01-01"));
  const BlockMetadata b26 =
      makeBlock(idStuttgart, DateId(DateParser, "1999-12-12"));
  const BlockMetadata b27 = makeBlock(DateId(DateParser, "2000-01-01"),
                                      DateId(DateParser, "2000-01-01"));
  const BlockMetadata b28 =
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
  const std::vector<BlockMetadata> mixedBlocks = {b2, b4, b11, b18, b26, b28};

  // All blocks that contain mixed types over column 2 + the first and last
  // incomplete block.
  const std::vector<BlockMetadata> mixedAndIncompleteBlocks = {
      bFirstIncomplete, b2, b4, b11, b18, b26, bLastIncomplete};

  // Vector containing unique and ordered BlockMetadata values.
  const std::vector<BlockMetadata> blocks = {
      b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,  b10, b11, b12,
      b13, b14, b15, b16, b17, b18, b19, b20, b21, b26, b27, b28};

  // Vector containing unique and ordered BlockMetadata values.
  const std::vector<BlockMetadata> allTestBlocksIsDatatype = {
      b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,  b10, b11, b12, b13, b14,
      b15, b16, b17, b18, b19, b20, b21, b22, b23, b24, b25, b27, b28};

  const std::vector<BlockMetadata> mixedBlocksTestIsDatatype = {b2,  b4,  b11,
                                                                b18, b25, b28};

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
                                                       b26,
                                                       b27,
                                                       bLastIncomplete};

  const std::vector<BlockMetadata> blocksInvalidOrder1 = {
      b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,  b10, b11, b12,
      b13, b14, b15, b16, b17, b18, b19, b20, b21, b26, b28, b27};

  const std::vector<BlockMetadata> blocksInvalidOrder2 = {
      b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,  b10, b11, b12,
      b14, b10, b15, b16, b17, b18, b19, b20, b21, b26, b27, b28};

  const std::vector<BlockMetadata> blocksWithDuplicate1 = {b1, b1, b2,
                                                           b3, b4, b5};

  const std::vector<BlockMetadata> blocksWithDuplicate2 = {b1,  b2,  b3,
                                                           b27, b28, b28};

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

  // Helper function for adding blocks containing mixed datatypes.
  auto addBlocksMixedDatatype(const std::vector<BlockMetadata> expected,
                              const std::vector<BlockMetadata> mixedBlocks) {
    std::vector<BlockMetadata> expectedAdjusted;
    ql::ranges::set_union(expected, mixedBlocks,
                          std::back_inserter(expectedAdjusted),
                          [](const BlockMetadata& b1, const BlockMetadata& b2) {
                            return b1.blockIndex_ < b2.blockIndex_;
                          });
    return expectedAdjusted;
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
                bool useBlocksIncomplete = false, bool addMixedBlocks = true) {
    std::vector<BlockMetadata> expectedAdjusted;
    // This is for convenience, we automatically insert all mixed and possibly
    // incomplete blocks which must be always returned.
    if (addMixedBlocks) {
      expectedAdjusted = addBlocksMixedDatatype(
          expected,
          useBlocksIncomplete ? mixedAndIncompleteBlocks : mixedBlocks);
    }
    std::vector<BlockMetadata> testBlocks =
        useBlocksIncomplete ? blocksIncomplete : blocks;
    ASSERT_EQ(expr->evaluate(testBlocks, 2),
              addMixedBlocks ? expectedAdjusted : expected);
  }

  // Helper for testing `isLiteral`, `isIri`, `isNum` and `isBlank`.
  // We define this additional test-helper-function for convenience given that
  // we had to extend `blocks` by `BlockMetadata` values containing `LITERAL`
  // and `IRI` datatypes. `allTestBlocksIsDatatype` contains exactly this
  // necessary wider range of `BlockMetadata` values (compared to `blocks`)
  // which are relevant for testing the expressions `isIri` and `isLiteral`.
  auto makeTestIsDatatype(std::unique_ptr<PrefilterExpression> expr,
                          std::vector<BlockMetadata>&& expected,
                          bool testIsIriOrIsLit = false,
                          std::vector<BlockMetadata>&& input = {}) {
    // The evaluation implementation of `isLiteral()` and `isIri()` uses two
    // conjuncted relational `PrefilterExpression`s. Thus we have to add all
    // BlockMetadata values containing mixed datatypes.
    std::vector<BlockMetadata> adjustedExpected =
        testIsIriOrIsLit
            ? addBlocksMixedDatatype(expected, mixedBlocksTestIsDatatype)
            : expected;
    ASSERT_EQ(
        expr->evaluate(input.empty() ? allTestBlocksIsDatatype : input, 2),
        adjustedExpected);
  }

  // Check if `BlockSubranges r1` and `BlockSubranges r2` contain
  // equivalent sub-ranges.
  bool assertEqRelevantBlockItRanges(const BlockSubranges& r1,
                                     const BlockSubranges& r2) {
    return ql::ranges::equal(r1, r2, ql::ranges::equal);
  }

  // Test the `mapValueIdItRangesToBlockItRangesComplemented` (if
  // `testComplement = true`) or `mapValueIdItRangesToBlockItRanges` helper
  // function of `PrefilterExpression`s. We assert that the retrieved
  // relevant iterators with respect to the containerized `std::span<const
  // BlockMetadata> evalBlocks` are correctly mapped back to
  // `RelevantBlockRange`s (index values regarding `evalBlocks`).
  auto makeTestDetailIndexMapping(CompOp compOp, ValueId referenceId,
                                  IdxPairRanges&& relevantIdxRanges,
                                  bool testComplement) {
    std::span<const BlockMetadata> evalBlocks(allTestBlocksIsDatatype);
    // Make `ValueId`s of `evalBlocks` accessible by iterators.
    // For the defined `BlockMetadata` values above, evaluation column at index
    // 2 is relevant.
    AccessValueIdFromBlockMetadata accessValueIdOp(2);
    ValueIdSubrange inputRange{
        ValueIdIt{&evalBlocks, 0, accessValueIdOp},
        ValueIdIt{&evalBlocks, evalBlocks.size() * 2, accessValueIdOp}};
    std::vector<ValueIdItPair> iteratorRanges = getRangesForId(
        inputRange.begin(), inputRange.end(), referenceId, compOp);
    using namespace prefilterExpressions::detail::mapping;
    ASSERT_TRUE(assertEqRelevantBlockItRanges(
        convertFromSpanIdxToSpanBlockItRanges(evalBlocks.begin(),
                                              relevantIdxRanges),
        testComplement ? mapValueIdItRangesToBlockItRangesComplemented(
                             iteratorRanges, inputRange, evalBlocks)
                       : mapValueIdItRangesToBlockItRanges(
                             iteratorRanges, inputRange, evalBlocks)));
  }

  // Simple `ASSERT_EQ` on date blocks
  auto makeTestDate(std::unique_ptr<PrefilterExpression> expr,
                    std::vector<BlockMetadata>&& expected) {
    ASSERT_EQ(expr->evaluate(dateBlocks, 2), expected);
  }

  // Test `PrefilterExpression` helper `mergeRelevantBlockItRanges<bool>`.
  // The `IdxPairRanges` will be mapped to their corresponding
  // `BlockSubranges`.
  // (1) Set `TestUnion = true`: test logical union (`OR(||)`) on
  // `BlockSubrange`s.
  // (2) Set `TestUnion = false`: test logical intersection (`AND(&&)`) on
  // `BlockItRanges`s.
  template <bool TestUnion>
  auto makeTestAndOrOrMergeBlocks(IdxPairRanges&& r1, IdxPairRanges&& r2,
                                  IdxPairRanges&& rExpected) {
    std::span<const BlockMetadata> blockSpan(allTestBlocksIsDatatype);
    auto spanBegin = blockSpan.begin();
    auto mergedBlockItRanges =
        prefilterExpressions::detail::logicalOps::mergeRelevantBlockItRanges<
            TestUnion>(convertFromSpanIdxToSpanBlockItRanges(spanBegin, r1),
                       convertFromSpanIdxToSpanBlockItRanges(spanBegin, r2));
    auto expectedBlockItRanges =
        convertFromSpanIdxToSpanBlockItRanges(spanBegin, rExpected);
    ASSERT_EQ(mergedBlockItRanges.size(), expectedBlockItRanges.size());
    ASSERT_TRUE(assertEqRelevantBlockItRanges(mergedBlockItRanges,
                                              expectedBlockItRanges));
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

//______________________________________________________________________________
// Test PrefilterExpression helper functions
// mapValueIdItRangesToBlockItRangesComplemented and
// mapValueIdItRangesToBlockItRanges.
TEST_F(PrefilterExpressionOnMetadataTest, testValueIdItToBlockItRangeMapping) {
  // Remark: If testComplement is set to true, the complement over all datatypes
  // is computed.
  makeTestDetailIndexMapping(CompOp::LT, IntId(10), {{3, 18}}, false);
  makeTestDetailIndexMapping(CompOp::LT, IntId(10),
                             {{0, 4}, {13, 14}, {17, 27}}, true);
  makeTestDetailIndexMapping(CompOp::LE, IntId(5), {{3, 7}, {8, 18}}, false);
  makeTestDetailIndexMapping(CompOp::LE, IntId(5),
                             {{0, 4}, {6, 8}, {13, 14}, {17, 27}}, true);
  // This will yield an empty range. However, in the actual evaluation those
  // empty ranges will be removed by valueIdComparators::detail::simplifyRanges
  makeTestDetailIndexMapping(CompOp::GT, DoubleId(10.00), {}, false);
  makeTestDetailIndexMapping(CompOp::GT, DoubleId(10.00), {{0, 27}}, true);
  // b11 at index 10 is also relevant. But given that this block contains mixed
  // datatypes, the possibly contained DoubleId(0.00) is hidden for
  // getRangesForId. This is solved in the overall computation by adding all
  // blocks holding mixed datatype values at the end.
  makeTestDetailIndexMapping(CompOp::EQ, DoubleId(0.00), {{3, 6}}, false);
  makeTestDetailIndexMapping(CompOp::EQ, DoubleId(0.00), {{0, 4}, {5, 27}},
                             true);
  makeTestDetailIndexMapping(CompOp::LE, DoubleId(-6.25), {{8, 9}, {14, 18}},
                             false);
  makeTestDetailIndexMapping(CompOp::GE, DoubleId(-8.00), {{3, 16}}, false);
  makeTestDetailIndexMapping(CompOp::GE, DoubleId(-8.00),
                             {{0, 4}, {8, 9}, {16, 27}}, true);
  makeTestDetailIndexMapping(CompOp::EQ, DoubleId(-9.42), {}, false);
  makeTestDetailIndexMapping(CompOp::NE, DoubleId(-9.42), {{3, 18}}, false);
  makeTestDetailIndexMapping(CompOp::GT, idAugsburg, {{18, 25}}, false);
  makeTestDetailIndexMapping(CompOp::LT, idHamburg, {{17, 19}}, false);
  makeTestDetailIndexMapping(CompOp::GT, idHamburg, {{0, 21}, {24, 27}}, true);
  makeTestDetailIndexMapping(CompOp::GT, idAugsburg, {{0, 18}, {24, 27}}, true);
  makeTestDetailIndexMapping(CompOp::GE, idAugsburg, {{17, 25}}, false);
  makeTestDetailIndexMapping(CompOp::LT, referenceDate1, {{24, 25}}, false);
  makeTestDetailIndexMapping(CompOp::LT, referenceDate1, {{0, 27}}, true);
  makeTestDetailIndexMapping(CompOp::LT, undef, {}, false);
  makeTestDetailIndexMapping(CompOp::LT, undef, {{0, 27}}, true);
  makeTestDetailIndexMapping(CompOp::EQ, falseId, {{1, 3}}, false);
  makeTestDetailIndexMapping(CompOp::NE, falseId, {{3, 4}}, false);
  makeTestDetailIndexMapping(CompOp::EQ, falseId, {{0, 2}, {3, 27}}, true);
  // Test corner case regarding last ValueId of last BlockMetadata value.
  makeTestDetailIndexMapping(CompOp::LT, BlankNodeId(10), {{0, 27}}, true);
  makeTestDetailIndexMapping(CompOp::NE, BlankNodeId(10), {{0, 27}}, true);
  makeTestDetailIndexMapping(CompOp::GT, BlankNodeId(0), {{26, 27}}, false);
  makeTestDetailIndexMapping(CompOp::GT, BlankNodeId(0), {{0, 27}}, true);
  makeTestDetailIndexMapping(CompOp::EQ, BlankNodeId(10), {{26, 27}}, false);
  makeTestDetailIndexMapping(CompOp::LT, BlankNodeId(11), {{26, 27}}, false);
  makeTestDetailIndexMapping(CompOp::GT, BlankNodeId(10), {}, false);
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
  makeTest(lt(referenceDateEqual), {b26}, true);
  makeTest(lt(referenceDate2), {b26, b27, b28});
  makeTest(lt(BlankNodeId(11)), {b28});
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
  makeTest(le(referenceDateEqual), {b26, b27});
  makeTest(le(BlankNodeId(11)), {b28});
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
  makeTest(gt(stuttgart), {b26});
  makeTest(gt(hamburg), {b21, b26}, true);
  makeTest(gt(berlin), {b19, b20, b21, b26});
  makeTest(gt(undef), {}, true);
  makeTest(gt(falseId), {b4}, true);
  makeTest(gt(trueId), {});
  makeTest(gt(referenceDateEqual), {b28});
  makeTest(gt(referenceDate1), {b26, b27, bLastIncomplete}, true);
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
  makeTest(ge(hamburg), {b18, b19, b20, b21, b26}, true);
  makeTest(ge(düsseldorf), {b18, b19, b20, b21, b26});
  makeTest(ge(münchen), {b18, b21, b26});
  makeTest(ge(undef), {}, true);
  makeTest(ge(falseId), {b2, b3, b4}, true);
  makeTest(ge(trueId), {b4});
  makeTest(ge(referenceDateEqual), {b27, b28});
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
  makeTest(eq(referenceDate1), {b26});
  makeTest(eq(referenceDateEqual), {b27}, true);
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
  makeTest(neq(augsburg), {b19, b20, b21, b26});
  makeTest(neq(berlin), {b18, b19, b20, b21, b26}, true);
  makeTest(neq(hamburg), {b18, b19, b21, b26});
  makeTest(neq(münchen), {b18, b19, b20, b21, b26});
  makeTest(neq(undef), {});
  makeTest(neq(falseId), {b4}, true);
  makeTest(neq(referenceDateEqual), {b26, b28});
  makeTest(neq(referenceDate1), {b26, b27, b28});
}

// Test IsDatatype Expressions
//______________________________________________________________________________
TEST_F(PrefilterExpressionOnMetadataTest, testIsDatatypeExpression) {
  // Test isLiteral
  // Blocks b18 - b22 contain LITERAL values.
  makeTestIsDatatype(isLit(), {b18, b19, b20, b21, b22}, true);
  // Block b18GapiriAndLiteral contains possibly hidden literal values.
  // Remark: b28 is a block holding mixed datatypes, this block should also be
  // returned with the current implementation of getSetDifference (see
  // PrefilterExpressionIndex.cpp).
  makeTestIsDatatype(isLit(), {b18GapIriAndLiteral, b28}, false,
                     {b16, b17, b18GapIriAndLiteral, b27, b28});
  makeTestIsDatatype(isLit(), {b18GapIriAndLiteral}, false,
                     {b18GapIriAndLiteral});
  makeTestIsDatatype(isLit(), {b18GapIriAndLiteral, b28}, false,
                     {b18GapIriAndLiteral, b28});
  makeTestIsDatatype(isLit(), {b18GapIriAndLiteral}, false,
                     {b14, b15, b16, b18GapIriAndLiteral});

  // Test isIri
  // Blocks b22 - b25 contain IRI values.
  makeTestIsDatatype(isIri(), {b22, b23, b24, b25}, true);
  makeTestIsDatatype(isIri(), {b18GapIriAndLiteral}, false,
                     {b18GapIriAndLiteral});
  makeTestIsDatatype(isIri(), {b18GapIriAndLiteral}, false,
                     {b17, b18GapIriAndLiteral, b27});

  // Test isNum
  // Blocks b4 - b18 contain numeric values.
  makeTestIsDatatype(
      isNum(),
      {b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18},
      true);
  // Test case with b4GapNumeric. b4GapNumeric contains potentially hidden
  // numeric values (Its bounding ValueIds are not of type INT or DOUBLE).
  makeTestIsDatatype(isNum(), {b4GapNumeric}, false, {b4GapNumeric});
  makeTestIsDatatype(isNum(), {b2, b4GapNumeric, b28}, false,
                     {b1, b2, b3, b4GapNumeric, b27, b28});
  makeTestIsDatatype(isNum(), {b4GapNumeric, b25, b28}, false,
                     {b4GapNumeric, b25, b27, b28});
  makeTestIsDatatype(isNum(), {b2, b4GapNumeric}, false,
                     {b2, b3, b4GapNumeric});
  makeTestIsDatatype(isNum(), {b2}, false,
                     {b1, b2, b3, b19, b21, b22, b23, b24});
  makeTestIsDatatype(isNum(), {b2, b18}, false,
                     {b1, b2, b3, b18, b19, b21, b22, b23, b24});

  // Test isBlank
  makeTestIsDatatype(isBlank(), {b28}, true);

  // Test implicitly the complementing procedure defined in
  // PrefilterExpressionIndex.cpp.

  // Test !isBlank.
  // All blocks are relevant given that even b28 is partially relevant here.
  makeTestIsDatatype(notExpr(isBlank()),
                     std::vector<BlockMetadata>(allTestBlocksIsDatatype),
                     false);

  // Test !isNum
  makeTestIsDatatype(notExpr(isNum()),
                     {b1, b2, b3, b4GapNumeric, b25, b27, b28}, false,
                     {b1, b2, b3, b4GapNumeric, b25, b27, b28});
  makeTestIsDatatype(notExpr(isNum()), {b4GapNumeric, b25, b27, b28}, false,
                     {b4GapNumeric, b25, b27, b28});
  makeTestIsDatatype(notExpr(isNum()), {b1, b2, b3, b4GapNumeric}, false,
                     {b1, b2, b3, b4GapNumeric});

  // Test !isLiteral
  // Blocks b19 - b21 contain only IRI related Ids (not contained in expected)
  makeTestIsDatatype(notExpr(isLit()), {b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,
                                        b9,  b10, b11, b12, b13, b14, b15, b16,
                                        b17, b18, b22, b23, b24, b25, b27, b28},
                     true);
  // b18GapIriAndLiteral should be considered relevant when evaluating
  // expression !isLit.
  makeTestIsDatatype(notExpr(isLit()), {b18GapIriAndLiteral}, false,
                     {b18GapIriAndLiteral});
  makeTestIsDatatype(notExpr(isLit()), {b1, b2, b3, b17, b18GapIriAndLiteral},
                     false, {b1, b2, b3, b17, b18GapIriAndLiteral});
  makeTestIsDatatype(notExpr(isLit()),
                     {b1, b2, b3, b17, b18GapIriAndLiteral, b27, b28}, false,
                     {b1, b2, b3, b17, b18GapIriAndLiteral, b27, b28});

  // Test !isIri
  // Blocks b23 - b24 contain only IRI related Ids (not contained in expected)
  makeTestIsDatatype(
      notExpr(isIri()),
      {b1,  b2,  b3,  b4,  b5,  b6,  b7,  b8,  b9,  b10, b11, b12, b13,
       b14, b15, b16, b17, b18, b19, b20, b21, b22, b25, b27, b28},
      true);
  makeTestIsDatatype(notExpr(isIri()), {b18GapIriAndLiteral}, false,
                     {b18GapIriAndLiteral});
  makeTestIsDatatype(notExpr(isIri()), {b1, b2, b3, b17, b18GapIriAndLiteral},
                     false, {b1, b2, b3, b17, b18GapIriAndLiteral});
  makeTestIsDatatype(notExpr(isIri()), {b18GapIriAndLiteral, b27, b28}, false,
                     {b18GapIriAndLiteral, b27, b28});
}

// Test Logical Expressions
//______________________________________________________________________________
// Test AndExpression
// Note: the `makeTest` function automatically adds the blocks with mixed
// datatypes to the expected result.
TEST_F(PrefilterExpressionOnMetadataTest, testAndExpression) {
  makeTest(andExpr(ge(düsseldorf), gt(düsseldorf)), {b19, b20, b21, b26});
  makeTest(andExpr(ge(düsseldorf), ge(düsseldorf)), {b19, b20, b21, b26}, true);
  makeTest(andExpr(ge(frankfurt), gt(münchen)), {b26});
  makeTest(andExpr(ge(frankfurt), gt(münchen)), {b26}, true);
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
  makeTest(orExpr(le(augsburg), ge(köln)), {b18, b21, b26});
  makeTest(orExpr(gt(münchen), ge(münchen)), {b21, b26});
  makeTest(orExpr(lt(DoubleId(-5.95)), eq(hamburg)),
           {b9, b15, b16, b17, b18, b19, b20, b21});
  makeTest(orExpr(eq(DoubleId(0)), neq(hamburg)), {b5, b6, b11, b18, b19, b21},
           true);
  makeTest(orExpr(eq(DoubleId(0)), eq(DoubleId(-6.25))),
           {b5, b6, b11, b15, b16, b18}, true);
  makeTest(orExpr(gt(undef), le(IntId(-6))), {b9, b15, b16, b17, b18});
  makeTest(orExpr(le(trueId), gt(referenceDate1)), {b2, b3, b4, b26, b27, b28});
  makeTest(orExpr(eq(IntId(0)), orExpr(lt(IntId(-10)), gt(IntId(8)))),
           {b5, b6, b8, b11, b14, b17, b18}, true);
  makeTest(orExpr(gt(referenceDate2), eq(trueId)), {b4});
  makeTest(orExpr(eq(münchen), orExpr(lt(augsburg), gt(stuttgart))), {b21, b26},
           true);
  makeTest(orExpr(eq(undef), gt(referenceDateEqual)), {b28});
  makeTest(orExpr(gt(IntId(8)), gt(DoubleId(22.1))), {b8, b14});
  makeTest(orExpr(lt(DoubleId(-8.25)), le(IntId(-10))), {b9, b17, b18}, true);
  makeTest(orExpr(eq(IntId(0)), neq(DoubleId(0.25))),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(orExpr(gt(referenceDate1), orExpr(gt(trueId), eq(IntId(0)))),
           {b4, b5, b6, b11, b26, b27, b28});
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
  makeTest(notExpr(eq(berlin)), {b18, b19, b20, b21, b26}, true);
  makeTest(notExpr(eq(hamburg)), {b18, b19, b21, b26});
  makeTest(notExpr(neq(hamburg)), {b19, b20, b21}, true);
  makeTest(notExpr(gt(berlin)), {b18});
  makeTest(notExpr(lt(DoubleId(-14.01))),
           {b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18});
  makeTest(notExpr(ge(DoubleId(-14.01))), {b18});
  makeTest(notExpr(gt(DoubleId(-4.00))), {b9, b10, b11, b15, b16, b17, b18},
           true);
  makeTest(notExpr(ge(DoubleId(-24.4))), {b18});
  makeTest(notExpr(gt(referenceDate2)), {b26, b27});
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
           {b8, b14, b18, b21, b26});
  makeTest(orExpr(eq(trueId), andExpr(gt(referenceDate1), lt(referenceDate2))),
           {b4, b26, b27}, true);
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
  makeTestClone(isLit(false));
  makeTestClone(isLit(true));
  makeTestClone(isIri(false));
  makeTestClone(isNum(false));
  makeTestClone(isBlank(true));
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
  // IsDatatypeExpression
  ASSERT_TRUE(*isBlank() == *isBlank());
  ASSERT_FALSE(*isLit() == *isNum());
  ASSERT_TRUE(*isIri(true) == *isIri(true));
  ASSERT_FALSE(*isNum(true) == *isNum(false));
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
  ASSERT_TRUE(*orExpr(isIri(), isLit()) == *orExpr(isIri(), isLit()));
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
// Test `mergeRelevantBlockItRanges<true>` over `BlockSubrange` values.
TEST_F(PrefilterExpressionOnMetadataTest, testOrMergeBlockItRanges) {
  // r1 UNION r2 should yield rExpected.
  makeTestAndOrOrMergeBlocks<true>({}, {}, {});
  makeTestAndOrOrMergeBlocks<true>({{0, 5}}, {}, {{0, 5}});
  makeTestAndOrOrMergeBlocks<true>({{0, 5}}, {{4, 7}}, {{0, 7}});
  makeTestAndOrOrMergeBlocks<true>({}, {{0, 1}, {3, 10}}, {{0, 1}, {3, 10}});
  makeTestAndOrOrMergeBlocks<true>({{0, 1}, {3, 10}}, {}, {{0, 1}, {3, 10}});
  makeTestAndOrOrMergeBlocks<true>({{0, 10}}, {{2, 3}, {4, 8}, {9, 12}},
                                   {{0, 12}});
  makeTestAndOrOrMergeBlocks<true>({{0, 1}, {1, 8}, {8, 9}}, {}, {{0, 9}});
  makeTestAndOrOrMergeBlocks<true>({{2, 10}, {15, 16}, {20, 23}},
                                   {{4, 6}, {8, 9}, {15, 22}},
                                   {{2, 10}, {15, 23}});
  makeTestAndOrOrMergeBlocks<true>({{0, 5}}, {{0, 5}}, {{0, 5}});
  makeTestAndOrOrMergeBlocks<true>({{1, 4}}, {{10, 25}, {25, 27}},
                                   {{1, 4}, {10, 27}});
}

//______________________________________________________________________________
// Test `mergeRelevantBlockItRanges<false>` over `BlockSubrange` values.
TEST_F(PrefilterExpressionOnMetadataTest, testAndMergeBlockItRanges) {
  // r1 INTERSECTION r2 should yield rExpected.
  makeTestAndOrOrMergeBlocks<false>({}, {}, {});
  makeTestAndOrOrMergeBlocks<false>({{0, 3}, {3, 5}}, {}, {});
  makeTestAndOrOrMergeBlocks<false>({{3, 9}}, {{3, 9}}, {{3, 9}});
  makeTestAndOrOrMergeBlocks<false>({{3, 9}, {9, 12}}, {{3, 9}, {9, 12}},
                                    {{3, 12}});
  makeTestAndOrOrMergeBlocks<false>({{0, 10}}, {{2, 4}}, {{2, 4}});
  makeTestAndOrOrMergeBlocks<false>({{3, 9}, {9, 12}}, {{0, 10}, {10, 14}},
                                    {{3, 12}});
  makeTestAndOrOrMergeBlocks<false>({{0, 26}}, {{0, 9}, {9, 11}, {20, 26}},
                                    {{0, 11}, {20, 26}});
  makeTestAndOrOrMergeBlocks<false>({{0, 9}, {9, 11}, {20, 26}}, {{0, 26}},
                                    {{0, 11}, {20, 26}});
  makeTestAndOrOrMergeBlocks<false>({{0, 8}, {10, 14}}, {{6, 12}},
                                    {{6, 8}, {10, 12}});
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
  EXPECT_THAT(*isIri(), matcher("Prefilter IsDatatypeExpression:\nPrefilter "
                                "for datatype: Iri\nis negated: false.\n.\n"));
  EXPECT_THAT(*isBlank(),
              matcher("Prefilter IsDatatypeExpression:\nPrefilter "
                      "for datatype: Blank\nis negated: false.\n.\n"));
  EXPECT_THAT(*isLit(),
              matcher("Prefilter IsDatatypeExpression:\nPrefilter "
                      "for datatype: Literal\nis negated: false.\n.\n"));
  EXPECT_THAT(*isNum(),
              matcher("Prefilter IsDatatypeExpression:\nPrefilter "
                      "for datatype: Numeric\nis negated: false.\n.\n"));
  EXPECT_THAT(*isBlank(true),
              matcher("Prefilter IsDatatypeExpression:\nPrefilter "
                      "for datatype: Blank\nis negated: true.\n.\n"));
  EXPECT_THAT(*notExpr(isNum()),
              matcher("Prefilter NotExpression:\nchild {Prefilter "
                      "IsDatatypeExpression:\nPrefilter for datatype: "
                      "Numeric\nis negated: true.\n}\n.\n"));
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
