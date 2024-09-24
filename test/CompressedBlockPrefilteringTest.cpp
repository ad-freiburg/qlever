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
using ad_utility::testing::UndefId;
using sparqlExpression::TestContext;
using namespace prefilterExpressions;

//______________________________________________________________________________
struct MetadataBlocks {
  // Define five blocks, for convenience all of their columns are sorted on
  // their data when appended in the given order to a vector.
  // Columns indexed:  COLUMN 0 | COLUMN 1 | COLUMN 2
  BlockMetadata nb1{{},
                    0,
                    {IntId(16), IntId(0), DoubleId(0.0)},    // firstTriple
                    {IntId(38), IntId(0), DoubleId(12.5)}};  // lastTriple
  BlockMetadata nb2{{},
                    0,
                    {IntId(42), IntId(0), DoubleId(12.5)},
                    {IntId(42), IntId(2), DoubleId(14.575)}};
  BlockMetadata nb3{{},
                    0,
                    {IntId(42), IntId(2), DoubleId(16.33)},
                    {IntId(45), IntId(2), DoubleId(18.32)}};
  BlockMetadata nb4{{},
                    0,
                    {IntId(46), IntId(2), DoubleId(22.29)},
                    {IntId(47), IntId(6), DoubleId(111.223)}};
  BlockMetadata nb5{{},
                    0,
                    {IntId(48), IntId(6), DoubleId(111.333)},
                    {IntId(51), IntId(6), DoubleId(112.00)}};
  std::vector<BlockMetadata> numericBlocks = {nb1, nb2, nb3, nb4, nb5};

  //____________________________________________________________________________
  // Define Blocks containing VocabIds
  // Use TestContext from SparqlExpressionTestHelpers.h
  sparqlExpression::TestContext tc{};
  Id Undef = UndefId();
  // Columns indexed:  COLUMN 0  |  COLUMN 1  |  COLUMN 2
  //                    undef.   | LocalVocab |   undef.
  BlockMetadata vb1{{}, 0, {Undef, tc.bonn, Undef}, {Undef, tc.cologne, Undef}};
  BlockMetadata vb2{
      {}, 0, {Undef, tc.dortmund, Undef}, {Undef, tc.essen, Undef}};
  BlockMetadata vb3{
      {}, 0, {Undef, tc.frankfurt, Undef}, {Undef, tc.frankfurt, Undef}};
  BlockMetadata vb4{
      {}, 0, {Undef, tc.hamburg, Undef}, {Undef, tc.karlsruhe, Undef}};
  BlockMetadata vb5{
      {}, 0, {Undef, tc.karlsruhe, Undef}, {Undef, tc.karlsruhe, Undef}};
  std::vector<BlockMetadata> vocabBlocks = {vb1, vb2, vb3, vb4, vb5};
};

// Static tests, they focus on corner case values for the given block triples.
//______________________________________________________________________________
//______________________________________________________________________________
// Test Relational Expressions

// Test LessThanExpression
TEST(RelationalExpression, testLessThanExpressions) {
  MetadataBlocks blocks{};
  std::vector<BlockMetadata> expectedResult = {};
  // NUMERIC
  EXPECT_EQ(expectedResult,
            LessThanExpression{IntId(10)}.evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(expectedResult,
            LessThanExpression{IntId(16)}.evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb1};
  EXPECT_EQ(expectedResult,
            LessThanExpression{IntId(40)}.evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(expectedResult,
            LessThanExpression{IntId(42)}.evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb1, blocks.nb2, blocks.nb3};
  EXPECT_EQ(expectedResult,
            LessThanExpression{IntId(46)}.evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(blocks.numericBlocks,
            LessThanExpression{IntId(100)}.evaluate(blocks.numericBlocks, 0));
  // VOCAB
  TestContext tc{};
  expectedResult = {};
  // tc.alpha is an Id of type Vocab, w.r.t. a lexicographical order, all the
  // citiy IDs (LocalVocab) should be greater than the ID (Vocab) of "alpha".
  // Thus we expect that none of the blocks are relevant.
  EXPECT_EQ(expectedResult,
            LessThanExpression{tc.alpha}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(expectedResult,
            LessThanExpression{tc.berlin}.evaluate(blocks.vocabBlocks, 1));
  // All cities used within vocabBlocks should be smaller than city "munich"
  // (given their respective LocalVocab IDs).
  EXPECT_EQ(blocks.vocabBlocks,
            LessThanExpression{tc.munich}.evaluate(blocks.vocabBlocks, 1));
  // All blocks from vocabBlocks should contain values less-than the (Vocab)
  // ID for "zz".
  EXPECT_EQ(blocks.vocabBlocks,
            LessThanExpression{tc.zz}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb1, blocks.vb2};
  EXPECT_EQ(expectedResult,
            LessThanExpression{tc.düsseldorf}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb1, blocks.vb2, blocks.vb3};
  EXPECT_EQ(expectedResult, LessThanExpression{tc.frankfurt_oder}.evaluate(
                                blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb1, blocks.vb2, blocks.vb3, blocks.vb4};
  EXPECT_EQ(expectedResult,
            LessThanExpression{tc.ingolstadt}.evaluate(blocks.vocabBlocks, 1));
}

//______________________________________________________________________________
// Test LessEqualExpression
TEST(RelationalExpression, testLessEqualExpressions) {
  MetadataBlocks blocks{};
  std::vector<BlockMetadata> expectedResult = {};
  // NUMERIC
  EXPECT_EQ(expectedResult,
            LessEqualExpression{IntId(10)}.evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb1};
  EXPECT_EQ(expectedResult,
            LessEqualExpression{IntId(16)}.evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(expectedResult,
            LessEqualExpression{IntId(40)}.evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb1, blocks.nb2, blocks.nb3};
  EXPECT_EQ(expectedResult,
            LessEqualExpression{IntId(42)}.evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb1, blocks.nb2, blocks.nb3, blocks.nb4};
  EXPECT_EQ(expectedResult,
            LessEqualExpression{IntId(46)}.evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(blocks.numericBlocks,
            LessEqualExpression{IntId(100)}.evaluate(blocks.numericBlocks, 0));
  // VOCAB
  TestContext tc{};
  expectedResult = {};
  EXPECT_EQ(expectedResult,
            LessEqualExpression{tc.alpha}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(expectedResult,
            LessEqualExpression{tc.berlin}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb1, blocks.vb2};
  EXPECT_EQ(expectedResult,
            LessEqualExpression{tc.dortmund}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb1, blocks.vb2, blocks.vb3, blocks.vb4};
  EXPECT_EQ(expectedResult,
            LessEqualExpression{tc.hannover}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(blocks.vocabBlocks,
            LessEqualExpression{tc.karlsruhe}.evaluate(blocks.vocabBlocks, 1));
}

//______________________________________________________________________________
// Test GreaterThanExpression
TEST(RelationalExpression, testGreaterThanExpression) {
  MetadataBlocks blocks{};
  EXPECT_EQ(blocks.numericBlocks,
            GreaterThanExpression{IntId(10)}.evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(blocks.numericBlocks,
            GreaterThanExpression{IntId(16)}.evaluate(blocks.numericBlocks, 0));
  std::vector<BlockMetadata> expectedResult = {blocks.nb2, blocks.nb3,
                                               blocks.nb4, blocks.nb5};
  EXPECT_EQ(expectedResult,
            GreaterThanExpression{IntId(38)}.evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb3, blocks.nb4, blocks.nb5};
  EXPECT_EQ(expectedResult,
            GreaterThanExpression{IntId(42)}.evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb4, blocks.nb5};
  EXPECT_EQ(expectedResult,
            GreaterThanExpression{IntId(46)}.evaluate(blocks.numericBlocks, 0));
  expectedResult = {};
  EXPECT_EQ(expectedResult,
            GreaterThanExpression{IntId(52)}.evaluate(blocks.numericBlocks, 0));
  // VOCAB
  TestContext tc{};
  expectedResult = {};
  EXPECT_EQ(expectedResult,
            GreaterThanExpression{tc.munich}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(expectedResult, GreaterThanExpression{tc.karlsruhe}.evaluate(
                                blocks.vocabBlocks, 1));
  EXPECT_EQ(blocks.vocabBlocks,
            GreaterThanExpression{tc.alpha}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb4, blocks.vb5};
  EXPECT_EQ(expectedResult,
            GreaterThanExpression{tc.hamburg}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb4, blocks.vb5};
  EXPECT_EQ(expectedResult,
            GreaterThanExpression{tc.hannover}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb2, blocks.vb3, blocks.vb4, blocks.vb5};
  EXPECT_EQ(expectedResult, GreaterThanExpression{tc.düsseldorf}.evaluate(
                                blocks.vocabBlocks, 1));
}

//______________________________________________________________________________
// Test GreaterEqualExpression
TEST(RelationalExpression, testGreaterEqualExpression) {
  MetadataBlocks blocks{};
  EXPECT_EQ(blocks.numericBlocks, GreaterEqualExpression{IntId(10)}.evaluate(
                                      blocks.numericBlocks, 0));
  EXPECT_EQ(blocks.numericBlocks, GreaterEqualExpression{IntId(38)}.evaluate(
                                      blocks.numericBlocks, 0));
  std::vector<BlockMetadata> expectedResult = {blocks.nb2, blocks.nb3,
                                               blocks.nb4, blocks.nb5};
  EXPECT_EQ(expectedResult, GreaterEqualExpression{IntId(40)}.evaluate(
                                blocks.numericBlocks, 0));
  EXPECT_EQ(expectedResult, GreaterEqualExpression{IntId(42)}.evaluate(
                                blocks.numericBlocks, 0));
  expectedResult = {blocks.nb3, blocks.nb4, blocks.nb5};
  EXPECT_EQ(expectedResult, GreaterEqualExpression{IntId(45)}.evaluate(
                                blocks.numericBlocks, 0));
  expectedResult = {blocks.nb4, blocks.nb5};
  EXPECT_EQ(expectedResult, GreaterEqualExpression{IntId(47)}.evaluate(
                                blocks.numericBlocks, 0));
  expectedResult = {};
  EXPECT_EQ(expectedResult, GreaterEqualExpression{IntId(100)}.evaluate(
                                blocks.numericBlocks, 0));
  // VOCAB
  TestContext tc{};
  EXPECT_EQ(blocks.vocabBlocks,
            GreaterEqualExpression{tc.alpha}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(blocks.vocabBlocks,
            GreaterEqualExpression{tc.bonn}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(blocks.vocabBlocks,
            GreaterEqualExpression{tc.cologne}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb2, blocks.vb3, blocks.vb4, blocks.vb5};
  EXPECT_EQ(expectedResult, GreaterEqualExpression{tc.düsseldorf}.evaluate(
                                blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb4, blocks.vb5};
  EXPECT_EQ(expectedResult, GreaterEqualExpression{tc.frankfurt_oder}.evaluate(
                                blocks.vocabBlocks, 1));
  EXPECT_EQ(expectedResult, GreaterEqualExpression{tc.karlsruhe}.evaluate(
                                blocks.vocabBlocks, 1));
  expectedResult = {};
  EXPECT_EQ(expectedResult,
            GreaterEqualExpression{tc.munich}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(expectedResult,
            GreaterEqualExpression{tc.zz}.evaluate(blocks.vocabBlocks, 1));
}

//______________________________________________________________________________
// Test EqualExpression
TEST(RelationalExpression, testEqualExpression) {
  MetadataBlocks blocks{};
  std::vector<BlockMetadata> expectedResult = {};
  EXPECT_EQ(expectedResult,
            EqualExpression{IntId(10)}.evaluate(blocks.numericBlocks, 1));
  EXPECT_EQ(expectedResult,
            EqualExpression{IntId(10)}.evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb4};
  EXPECT_EQ(expectedResult,
            EqualExpression{IntId(5)}.evaluate(blocks.numericBlocks, 1));
  expectedResult = {blocks.nb2, blocks.nb3, blocks.nb4};
  EXPECT_EQ(expectedResult,
            EqualExpression{IntId(2)}.evaluate(blocks.numericBlocks, 1));
  expectedResult = {blocks.nb4, blocks.nb5};
  EXPECT_EQ(expectedResult,
            EqualExpression{IntId(6)}.evaluate(blocks.numericBlocks, 1));
  expectedResult = {blocks.nb2, blocks.nb3};
  EXPECT_EQ(expectedResult,
            EqualExpression{IntId(42)}.evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb5};
  EXPECT_EQ(expectedResult,
            EqualExpression{IntId(112)}.evaluate(blocks.numericBlocks, 2));
  // VOCAB
  TestContext tc{};
  expectedResult = {};
  EXPECT_EQ(expectedResult,
            EqualExpression{tc.zz}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(expectedResult,
            EqualExpression{tc.alpha}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(expectedResult,
            EqualExpression{tc.munich}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(expectedResult,
            EqualExpression{tc.frankfurt_oder}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb4, blocks.vb5};
  EXPECT_EQ(expectedResult,
            EqualExpression{tc.karlsruhe}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb3};
  EXPECT_EQ(expectedResult,
            EqualExpression{tc.frankfurt}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb2};
  EXPECT_EQ(expectedResult,
            EqualExpression{tc.düsseldorf}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb1};
  EXPECT_EQ(expectedResult,
            EqualExpression{tc.bonn}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(expectedResult,
            EqualExpression{tc.cologne}.evaluate(blocks.vocabBlocks, 1));
}

//______________________________________________________________________________
// Test NotEqualExpression
TEST(RelationalExpression, testNotEqualExpression) {
  MetadataBlocks blocks{};
  std::vector<BlockMetadata> expectedResult{};
  EXPECT_EQ(blocks.numericBlocks,
            NotEqualExpression{IntId(8)}.evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(blocks.numericBlocks,
            NotEqualExpression{IntId(16)}.evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(blocks.numericBlocks,
            NotEqualExpression{IntId(45)}.evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(blocks.numericBlocks,
            NotEqualExpression{IntId(51)}.evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(blocks.numericBlocks,
            NotEqualExpression{IntId(48)}.evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(blocks.numericBlocks, NotEqualExpression{DoubleId(18.32)}.evaluate(
                                      blocks.numericBlocks, 2));
  EXPECT_EQ(blocks.numericBlocks, NotEqualExpression{DoubleId(22.33)}.evaluate(
                                      blocks.numericBlocks, 2));
  EXPECT_EQ(blocks.numericBlocks,
            NotEqualExpression{IntId(17)}.evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb1, blocks.nb3, blocks.nb4, blocks.nb5};
  EXPECT_EQ(expectedResult,
            NotEqualExpression{IntId(42)}.evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb1, blocks.nb2, blocks.nb4, blocks.nb5};
  EXPECT_EQ(expectedResult,
            NotEqualExpression{IntId(2)}.evaluate(blocks.numericBlocks, 1));
  expectedResult = {blocks.nb1, blocks.nb2, blocks.nb3, blocks.nb4};
  EXPECT_EQ(expectedResult, NotEqualExpression{DoubleId(6.00)}.evaluate(
                                blocks.numericBlocks, 1));
  expectedResult = {blocks.nb2, blocks.nb3, blocks.nb4, blocks.nb5};
  EXPECT_EQ(expectedResult,
            NotEqualExpression{IntId(0)}.evaluate(blocks.numericBlocks, 1));
  // VOCAB
  TestContext tc{};
  expectedResult = {};
  EXPECT_EQ(blocks.vocabBlocks,
            NotEqualExpression{tc.zz}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(blocks.vocabBlocks,
            NotEqualExpression{tc.alpha}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(blocks.vocabBlocks, NotEqualExpression{tc.frankfurt_oder}.evaluate(
                                    blocks.vocabBlocks, 1));
  EXPECT_EQ(blocks.vocabBlocks,
            NotEqualExpression{tc.munich}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(blocks.vocabBlocks,
            NotEqualExpression{tc.bonn}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(blocks.vocabBlocks,
            NotEqualExpression{tc.cologne}.evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(blocks.vocabBlocks,
            NotEqualExpression{tc.düsseldorf}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb1, blocks.vb2, blocks.vb4, blocks.vb5};
  EXPECT_EQ(expectedResult,
            NotEqualExpression{tc.frankfurt}.evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb1, blocks.vb2, blocks.vb3, blocks.vb4};
  EXPECT_EQ(expectedResult,
            NotEqualExpression{tc.karlsruhe}.evaluate(blocks.vocabBlocks, 1));
}

//______________________________________________________________________________
//______________________________________________________________________________
// Test Logical Expressions

// Test AndExpression
TEST(LogicalExpression, testAndExpression) {
  MetadataBlocks blocks{};
  std::vector<BlockMetadata> expectedResult{};
  EXPECT_EQ(expectedResult,
            AndExpression(std::make_unique<LessThanExpression>(IntId(42)),
                          std::make_unique<GreaterThanExpression>(IntId(45)))
                .evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(
      expectedResult,
      AndExpression(std::make_unique<EqualExpression>(IntId(42)),
                    std::make_unique<GreaterThanExpression>(DoubleId(52.33)))
          .evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(
      expectedResult,
      AndExpression(std::make_unique<AndExpression>(
                        std::make_unique<LessThanExpression>(IntId(42)),
                        std::make_unique<GreaterThanExpression>(IntId(45))),
                    std::make_unique<NotEqualExpression>(IntId(49)))
          .evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb1, blocks.nb2};
  EXPECT_EQ(expectedResult,
            AndExpression(std::make_unique<AndExpression>(
                              std::make_unique<EqualExpression>(IntId(0)),
                              std::make_unique<LessEqualExpression>(IntId(0))),
                          std::make_unique<NotEqualExpression>(IntId(6)))
                .evaluate(blocks.numericBlocks, 1));
  // !!! Reason that we don't expect an empty result here:
  // Block blocks.nb3 contains value within the range [42, 45] on column 0.
  // Thus, this block is a valid candidate for values unequal to 42 and values
  // that are equal to 45.
  expectedResult = {blocks.nb3};
  EXPECT_EQ(expectedResult,
            AndExpression(std::make_unique<EqualExpression>(IntId(42)),
                          std::make_unique<NotEqualExpression>(DoubleId(42.00)))
                .evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb1, blocks.nb2, blocks.nb3, blocks.nb4};
  EXPECT_EQ(expectedResult,
            AndExpression(std::make_unique<LessThanExpression>(DoubleId(6.00)),
                          std::make_unique<LessEqualExpression>(IntId(2.00)))
                .evaluate(blocks.numericBlocks, 1));
  expectedResult = {blocks.nb4, blocks.nb5};
  EXPECT_EQ(
      expectedResult,
      AndExpression(std::make_unique<AndExpression>(
                        std::make_unique<LessThanExpression>(IntId(7)),
                        std::make_unique<GreaterThanExpression>(IntId(5))),
                    std::make_unique<NotEqualExpression>(IntId(0)))
          .evaluate(blocks.numericBlocks, 1));
  expectedResult = {blocks.nb1, blocks.nb2};
  EXPECT_EQ(
      expectedResult,
      AndExpression(std::make_unique<LessThanExpression>(DoubleId(14.575)),
                    std::make_unique<NotEqualExpression>(IntId(12)))
          .evaluate(blocks.numericBlocks, 2));
  expectedResult = {blocks.nb2, blocks.nb3, blocks.nb4};
  EXPECT_EQ(expectedResult,
            AndExpression(std::make_unique<NotEqualExpression>(DoubleId(0.00)),
                          std::make_unique<NotEqualExpression>(DoubleId(6.00)))
                .evaluate(blocks.numericBlocks, 1));
  expectedResult = {blocks.nb1, blocks.nb2};
  EXPECT_EQ(expectedResult,
            AndExpression(std::make_unique<LessEqualExpression>(DoubleId(1.99)),
                          std::make_unique<LessThanExpression>(DoubleId(1.5)))
                .evaluate(blocks.numericBlocks, 1));
}

//______________________________________________________________________________
// Test OrExpression
TEST(LogicalExpression, testOrExpression) {
  MetadataBlocks blocks{};
  std::vector<BlockMetadata> expectedResult = {blocks.nb1, blocks.nb4,
                                               blocks.nb5};
  EXPECT_EQ(expectedResult,
            OrExpression(std::make_unique<LessThanExpression>(IntId(42)),
                         std::make_unique<GreaterThanExpression>(IntId(45)))
                .evaluate(blocks.numericBlocks, 0));
  expectedResult = {};
  EXPECT_EQ(expectedResult,
            OrExpression(std::make_unique<LessThanExpression>(DoubleId(-14.23)),
                         std::make_unique<GreaterThanExpression>(IntId(51)))
                .evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb2, blocks.nb3, blocks.nb4, blocks.nb5};
  EXPECT_EQ(
      expectedResult,
      OrExpression(std::make_unique<NotEqualExpression>(IntId(0)),
                   std::make_unique<AndExpression>(
                       std::make_unique<GreaterEqualExpression>(IntId(5)),
                       std::make_unique<LessThanExpression>(DoubleId(1.00))))
          .evaluate(blocks.numericBlocks, 1));
  expectedResult = {blocks.nb1, blocks.nb2, blocks.nb4, blocks.nb5};
  EXPECT_EQ(expectedResult,
            OrExpression(std::make_unique<EqualExpression>(IntId(0)),
                         std::make_unique<OrExpression>(
                             std::make_unique<EqualExpression>(IntId(3)),
                             std::make_unique<EqualExpression>(DoubleId(6))))
                .evaluate(blocks.numericBlocks, 1));
  expectedResult = {blocks.nb4, blocks.nb5};
  EXPECT_EQ(
      expectedResult,
      OrExpression(std::make_unique<GreaterThanExpression>(IntId(20)),
                   std::make_unique<GreaterEqualExpression>(DoubleId(113.3)))
          .evaluate(blocks.numericBlocks, 2));
  expectedResult = {blocks.nb1, blocks.nb5};
  EXPECT_EQ(
      expectedResult,
      OrExpression(std::make_unique<LessThanExpression>(IntId(42)),
                   std::make_unique<AndExpression>(
                       std::make_unique<EqualExpression>(IntId(49)),
                       std::make_unique<GreaterThanExpression>(DoubleId(2.00))))
          .evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb1, blocks.nb2, blocks.nb3, blocks.nb4};
  EXPECT_EQ(expectedResult,
            OrExpression(std::make_unique<OrExpression>(
                             std::make_unique<EqualExpression>(IntId(0)),
                             std::make_unique<EqualExpression>(DoubleId(2.00))),
                         std::make_unique<LessThanExpression>(IntId(6)))
                .evaluate(blocks.numericBlocks, 1));
  expectedResult = {blocks.nb1, blocks.nb2, blocks.nb3};
  EXPECT_EQ(expectedResult,
            OrExpression(std::make_unique<EqualExpression>(DoubleId(17.00)),
                         std::make_unique<LessThanExpression>(IntId(16.35)))
                .evaluate(blocks.numericBlocks, 2));
}

//______________________________________________________________________________
// Test NotExpression
TEST(LogicalExpression, testNotExpression) {
  MetadataBlocks blocks{};
  TestContext tc{};
  std::vector<BlockMetadata> expectedResult = {};
  EXPECT_EQ(expectedResult,
            NotExpression(std::make_unique<GreaterEqualExpression>(IntId(16)))
                .evaluate(blocks.numericBlocks, 0));
  EXPECT_EQ(
      expectedResult,
      NotExpression(std::make_unique<NotExpression>(
                        std::make_unique<GreaterEqualExpression>(tc.munich)))
          .evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(expectedResult,
            NotExpression(std::make_unique<NotExpression>(
                              std::make_unique<GreaterEqualExpression>(tc.zz)))
                .evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb4, blocks.vb5};
  EXPECT_EQ(
      expectedResult,
      NotExpression(std::make_unique<NotExpression>(
                        std::make_unique<GreaterEqualExpression>(tc.karlsruhe)))
          .evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(
      blocks.numericBlocks,
      NotExpression(std::make_unique<NotExpression>(
                        std::make_unique<GreaterEqualExpression>(IntId(16))))
          .evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.vb1, blocks.vb2, blocks.vb3};
  EXPECT_EQ(expectedResult,
            NotExpression(std::make_unique<GreaterThanExpression>(tc.frankfurt))
                .evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(blocks.vocabBlocks,
            NotExpression(std::make_unique<LessEqualExpression>(tc.berlin))
                .evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb1, blocks.vb2};
  EXPECT_EQ(
      expectedResult,
      NotExpression(std::make_unique<GreaterThanExpression>(tc.düsseldorf))
          .evaluate(blocks.vocabBlocks, 1));
  EXPECT_EQ(blocks.numericBlocks,
            NotExpression(
                std::make_unique<AndExpression>(
                    std::make_unique<LessThanExpression>(IntId(13)),
                    std::make_unique<GreaterEqualExpression>(DoubleId(111.01))))
                .evaluate(blocks.numericBlocks, 2));
  expectedResult = {};
  EXPECT_EQ(
      expectedResult,
      NotExpression(
          std::make_unique<NotExpression>(std::make_unique<AndExpression>(
              std::make_unique<LessThanExpression>(IntId(13)),
              std::make_unique<GreaterEqualExpression>(DoubleId(111.01)))))
          .evaluate(blocks.numericBlocks, 2));
  expectedResult = {blocks.vb4, blocks.vb5};
  EXPECT_EQ(
      expectedResult,
      NotExpression(std::make_unique<AndExpression>(
                        std::make_unique<LessThanExpression>(tc.munich),
                        std::make_unique<LessEqualExpression>(tc.ingolstadt)))
          .evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.vb2, blocks.vb3, blocks.vb4};
  EXPECT_EQ(
      expectedResult,
      NotExpression(std::make_unique<OrExpression>(
                        std::make_unique<GreaterThanExpression>(tc.hamburg),
                        std::make_unique<LessEqualExpression>(tc.düsseldorf)))
          .evaluate(blocks.vocabBlocks, 1));
  expectedResult = {blocks.nb2, blocks.nb3, blocks.nb4};
  EXPECT_EQ(expectedResult,
            NotExpression(std::make_unique<OrExpression>(
                              std::make_unique<EqualExpression>(DoubleId(0.0)),
                              std::make_unique<EqualExpression>(IntId(6))))
                .evaluate(blocks.numericBlocks, 1));
  expectedResult = {blocks.nb1, blocks.nb2, blocks.nb4, blocks.nb5};
  EXPECT_EQ(
      expectedResult,
      NotExpression(std::make_unique<AndExpression>(
                        std::make_unique<NotEqualExpression>(DoubleId(0.0)),
                        std::make_unique<NotEqualExpression>(IntId(6))))
          .evaluate(blocks.numericBlocks, 1));
  expectedResult = {blocks.nb3, blocks.nb4};
  EXPECT_EQ(
      expectedResult,
      NotExpression(std::make_unique<OrExpression>(
                        std::make_unique<LessEqualExpression>(DoubleId(42.0)),
                        std::make_unique<GreaterEqualExpression>(IntId(48))))
          .evaluate(blocks.numericBlocks, 0));
  expectedResult = {blocks.nb1, blocks.nb2, blocks.nb3, blocks.nb5};
  EXPECT_EQ(expectedResult,
            NotExpression(
                std::make_unique<NotExpression>(std::make_unique<OrExpression>(
                    std::make_unique<LessEqualExpression>(DoubleId(42.25)),
                    std::make_unique<GreaterEqualExpression>(IntId(51)))))
                .evaluate(blocks.numericBlocks, 0));
}
