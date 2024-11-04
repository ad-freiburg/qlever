//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#pragma once

#include <memory>
#include <vector>

#include "global/Id.h"
#include "global/ValueIdComparators.h"
#include "index/CompressedRelation.h"

namespace prefilterExpressions {

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
Relational Expressions - `<=`, `>=`, `<`, `>`, `==` and `!=`.
Logical Operations - `and`, `or` and `not`.
*/

class PrefilterExpression {
 public:
  virtual ~PrefilterExpression() = default;

  // Needed for implementing the `NotExpression`. This method is required,
  // because we logically operate on `BlockMetadata` values which define ranges
  // given the `ValueIds` from last and first triple.
  // E.g. the `BlockMetadata` that defines the range [IntId(0),... IntId(5)],
  // should be considered relevant for the expression `?x >= IntId(3)`, but also
  // for expression `!(?x >= IntId(3))`. Thus we can't retrieve the negation by
  // simply taking the complementing set of `BlockMetadata`, instead we
  // retrieve it by directly negating/complementing the child expression itself.
  // Every derived class can return it's respective logical complement
  // (negation) when being called on `logicalCoplement()`. E.g. for a call
  // w.r.t. `RelationalExpression<LT>(IntId(5))` (< 5), the returned logical
  // complement is `RelationalExpression<GE>(IntId(5))` (>= 5). On a
  // `LogicalExpression` (`AND` or `OR`), we respectively apply De-Morgan's law
  // and return the resulting `LogicalExpression`. In case of the
  // `NotExpression`, we just return its child expression given that two
  // negations (complementations) cancel out. For a more concise explanation
  // take a look at the actual implementation for derived classes.
  virtual std::unique_ptr<PrefilterExpression> logicalComplement() const = 0;

  // The respective metadata to the blocks is expected to be provided in
  // a sorted order (w.r.t. the relevant column).
  std::vector<BlockMetadata> evaluate(const std::vector<BlockMetadata>& input,
                                      size_t evaluationColumn) const;

 private:
  virtual std::vector<BlockMetadata> evaluateImpl(
      const std::vector<BlockMetadata>& input,
      size_t evaluationColumn) const = 0;
};

//______________________________________________________________________________
// For the actual comparison of the relevant ValueIds from the metadata triples,
// we use the implementations from ValueIdComparators.
//
// Supported comparisons are:
//  - LessThan, LessEqual, Equal, NotEqual, GreaterEqual, GreaterThan
using CompOp = valueIdComparators::Comparison;

//______________________________________________________________________________
template <CompOp Comparison>
class RelationalExpression : public PrefilterExpression {
 private:
  // The ValueId on which we perform the relational comparison on.
  ValueId referenceId_;

 public:
  explicit RelationalExpression(const ValueId referenceId)
      : referenceId_(referenceId) {}

  std::unique_ptr<PrefilterExpression> logicalComplement() const override;

 private:
  std::vector<BlockMetadata> evaluateImpl(
      const std::vector<BlockMetadata>& input,
      size_t evaluationColumn) const override;
};

//______________________________________________________________________________
// Helper struct for a compact class implementation regarding the logical
// operations `AND` and `OR`. `NOT` is implemented separately given that the
// expression is unary (single child expression).
enum struct LogicalOperators { AND, OR };

//______________________________________________________________________________
template <LogicalOperators Operation>
class LogicalExpression : public PrefilterExpression {
 private:
  std::unique_ptr<PrefilterExpression> child1_;
  std::unique_ptr<PrefilterExpression> child2_;

 public:
  // AND and OR
  explicit LogicalExpression(std::unique_ptr<PrefilterExpression> child1,
                             std::unique_ptr<PrefilterExpression> child2)
      : child1_(std::move(child1)), child2_(std::move(child2)) {}

  std::unique_ptr<PrefilterExpression> logicalComplement() const override;

 private:
  std::vector<BlockMetadata> evaluateImpl(
      const std::vector<BlockMetadata>& input,
      size_t evaluationColumn) const override;
};

//______________________________________________________________________________
class NotExpression : public PrefilterExpression {
 private:
  std::unique_ptr<PrefilterExpression> child_;

 public:
  explicit NotExpression(std::unique_ptr<PrefilterExpression> child)
      : child_(child->logicalComplement()) {}

  std::unique_ptr<PrefilterExpression> logicalComplement() const override;

 private:
  std::vector<BlockMetadata> evaluateImpl(
      const std::vector<BlockMetadata>& input,
      size_t evaluationColumn) const override;
};

//______________________________________________________________________________
// Definition of the RelationalExpression for LT, LE, EQ, NE, GE and GT.
using LessThanExpression = prefilterExpressions::RelationalExpression<
    prefilterExpressions::CompOp::LT>;
using LessEqualExpression = prefilterExpressions::RelationalExpression<
    prefilterExpressions::CompOp::LE>;
using EqualExpression = prefilterExpressions::RelationalExpression<
    prefilterExpressions::CompOp::EQ>;
using NotEqualExpression = prefilterExpressions::RelationalExpression<
    prefilterExpressions::CompOp::NE>;
using GreaterEqualExpression = prefilterExpressions::RelationalExpression<
    prefilterExpressions::CompOp::GE>;
using GreaterThanExpression = prefilterExpressions::RelationalExpression<
    prefilterExpressions::CompOp::GT>;

//______________________________________________________________________________
// Definition of the LogicalExpression for AND and OR.
using AndExpression = prefilterExpressions::LogicalExpression<
    prefilterExpressions::LogicalOperators::AND>;
using OrExpression = prefilterExpressions::LogicalExpression<
    prefilterExpressions::LogicalOperators::OR>;

}  // namespace prefilterExpressions
