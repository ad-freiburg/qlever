//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#pragma once

#include <memory>
#include <vector>

#include "global/Id.h"
#include "global/ValueIdComparators.h"
#include "index/CompressedRelation.h"

// The compressed block metadata (see `CompressedRelation.h`) that we use to
// filter out the non-relevant blocks by checking their content of
// `firstTriple_` and `lastTriple_` (`PermutedTriple`)
using BlockMetadata = CompressedBlockMetadata;

//______________________________________________________________________________
/*
`PrefilterExpression` represents a base class for the following sub-classes that
implement the block-filtering procedure for the specific relational + logical
operations/expressions.

Remark: We do not actually evaluate the respective SPARQL Expression. We only
pre-filter w.r.t. blocks that contain relevant data for the actual evaluation of
those expressions to make the evaluation procedure more efficient.

The block-filtering is applied with the following operations:
Relational Expressions - `<=`, `>=`, `<`, `>`, `==` and `!=`
Logical Operations - `and` and `or`
*/

class PrefilterExpression {
 public:
  // The respective metadata to the blocks is expected to be provided in
  // a sorted order (w.r.t. the relevant column).
  virtual std::vector<BlockMetadata> evaluate(std::vector<BlockMetadata>& input,
                                              size_t evaluationColumn) = 0;
  virtual ~PrefilterExpression() = default;
};

//______________________________________________________________________________
// For the actual comparison of the relevant ValueIds from the metadata triples,
// we use the implementations from ValueIdComparators.
//
// Supported comparisons are:
//  - LessThan, LessEqual, Equal, NotEqual, GreaterEqual, GreaterThan
using CompOp = valueIdComparators::Comparison;

//______________________________________________________________________________
// Given a PermutedTriple, retrieve the suitable Id w.r.t. a column (index).
constexpr auto getIdFromColumnIndex =
    [](const BlockMetadata::PermutedTriple& permutedTriple,
       const size_t columnIndex) {
      switch (columnIndex) {
        case 0:
          return permutedTriple.col0Id_;
        case 1:
          return permutedTriple.col1Id_;
        case 2:
          return permutedTriple.col2Id_;
        default:
          // Triple!
          AD_FAIL();
      }
    };

//______________________________________________________________________________
template <CompOp Comparison>
class RelationalExpressions : public PrefilterExpression {
 public:
  explicit RelationalExpressions(ValueId referenceId)
      : referenceId_(referenceId) {}

  std::vector<BlockMetadata> evaluate(std::vector<BlockMetadata>& input,
                                      size_t evaluationColumn) override;

 private:
  // The ValueId the comparison refers to.
  ValueId referenceId_;
  // Helper function that implements the relational comparison on the block
  // values.
  bool compare(const ValueId& tripleId, const ValueId& otherId);
  // Helper for providing the lower indices during the evaluation procedure.
  auto lowerIndex(std::vector<BlockMetadata>& input, size_t evaluationColumn);
  // Helper to get the upper indices, necessary for EQ and NE.
  auto upperIndex(std::vector<BlockMetadata>& input, size_t evaluationColumn)
      requires(Comparison == CompOp::EQ || Comparison == CompOp::NE);
};

//______________________________________________________________________________
// Helper struct for a compact class implementation regarding the logical
// operations `AND`, `OR` and `NOT`.
enum struct LogicalOperators { AND, OR, NOT };

//______________________________________________________________________________
template <LogicalOperators Operation>
class LogicalExpressions : public PrefilterExpression {
 public:
  explicit LogicalExpressions(std::unique_ptr<PrefilterExpression> child1,
                              std::unique_ptr<PrefilterExpression> child2)
      : child1_(std::move(child1)), child2_(std::move(child2)) {}

  std::vector<BlockMetadata> evaluate(std::vector<BlockMetadata>& input,
                                      size_t evaluationColumn) override;

 private:
  std::unique_ptr<PrefilterExpression> child1_;
  std::unique_ptr<PrefilterExpression> child2_;
  // Introduce a helper method for the specification LogicalOperators::OR
  std::vector<BlockMetadata> evaluateOr(std::vector<BlockMetadata>& input,
                                        size_t evaluationColumn)
      requires(Operation == LogicalOperators::OR);
};

//______________________________________________________________________________
// Definition of the RelationalExpressions for LT, LE, EQ, NE, GE and GT.
using LessThanExpression = RelationalExpressions<CompOp::LT>;
using LessEqualExpression = RelationalExpressions<CompOp::LE>;
using EqualExpression = RelationalExpressions<CompOp::EQ>;
using NotEqualExpression = RelationalExpressions<CompOp::NE>;
using GreaterEqualExpression = RelationalExpressions<CompOp::GE>;
using GreaterThanExpression = RelationalExpressions<CompOp::GT>;

//______________________________________________________________________________
// Defintion of the LogicalExpressions for AND, OR and NOT.
using AndExpression = LogicalExpressions<LogicalOperators::AND>;
using OrExpression = LogicalExpressions<LogicalOperators::OR>;
