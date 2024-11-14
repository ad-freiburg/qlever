//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#pragma once

#include <memory>
#include <vector>

#include "global/Id.h"
#include "global/ValueIdComparators.h"
#include "index/CompressedRelation.h"

// For certain SparqlExpressions it is possible to perform a prefiltering
// procedure w.r.t. relevant data blocks / ValueId values by making use of the
// available metadata (see CompressedBlockMetadata in CompressedRelation.h)
// while performing the index scan. As a result, the actual SparqlExpression
// evaluation is performed for a smaller IdTable if a PrefilterExpression
// (declared in this file) for the respective SparqlExpression is available and
// compatible with the IndexScan. The following SparqlExpressions construct a
// PrefilterExpression if possible: logical-or, logical-and, logical-negate
// (unary), relational-ops. and strstarts.

namespace prefilterExpressions {

//______________________________________________________________________________
// The maximum recursion depth for `info()` / `operator<<()`. A depth of `3`
// should be sufficient for most `PrefilterExpressions` in our use case.
constexpr size_t maxInfoRecursion = 3;

//______________________________________________________________________________
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

  virtual bool operator==(const PrefilterExpression& other) const = 0;

  // Create a copy of this `PrefilterExpression`.
  virtual std::unique_ptr<PrefilterExpression> clone() const = 0;

  // Format content for debugging.
  virtual std::string asString(size_t depth) const = 0;

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

  // Format for debugging
  friend std::ostream& operator<<(std::ostream& str,
                                  const PrefilterExpression& expression) {
    str << expression.asString(0) << "." << std::endl;
    return str;
  }

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
  bool operator==(const PrefilterExpression& other) const override;
  std::unique_ptr<PrefilterExpression> clone() const override;
  std::string asString(size_t depth) const override;

 private:
  std::vector<BlockMetadata> evaluateImpl(
      const std::vector<BlockMetadata>& input,
      size_t evaluationColumn) const override;
};

//______________________________________________________________________________
// Helper struct for a compact class implementation regarding the logical
// operations `AND` and `OR`. `NOT` is implemented separately given that the
// expression is unary (single child expression).
enum struct LogicalOperator { AND, OR };

//______________________________________________________________________________
template <LogicalOperator Operation>
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
  bool operator==(const PrefilterExpression& other) const override;
  std::unique_ptr<PrefilterExpression> clone() const override;
  std::string asString(size_t depth) const override;

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
  // `makeCopy` should always be set to `false`, except when it is called within
  // the implementation of the `clone()` method. For the copy construction,
  // the `logicalComplement` for the child is omitted because it has
  // already been complemented for the original expression.
  explicit NotExpression(std::unique_ptr<PrefilterExpression> child,
                         bool makeCopy = false)
      : child_(makeCopy ? std::move(child) : child->logicalComplement()) {}

  std::unique_ptr<PrefilterExpression> logicalComplement() const override;
  bool operator==(const PrefilterExpression& other) const override;
  std::unique_ptr<PrefilterExpression> clone() const override;
  std::string asString(size_t depth) const override;

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
    prefilterExpressions::LogicalOperator::AND>;
using OrExpression = prefilterExpressions::LogicalExpression<
    prefilterExpressions::LogicalOperator::OR>;

namespace detail {
//______________________________________________________________________________
// Pair containing a `PrefilterExpression` and its corresponding `Variable`.
using PrefilterExprVariablePair =
    std::pair<std::unique_ptr<PrefilterExpression>, Variable>;
//______________________________________________________________________________
// Helper function to perform a check regarding the following conditions.
// (1) For each relevant Variable, the vector contains exactly one pair.
// (2) The vector contains those pairs (`<PrefilterExpression, Variable>`) in
// sorted order w.r.t. the Variable value.
void checkPropertiesForPrefilterConstruction(
    const std::vector<PrefilterExprVariablePair>& vec);

}  // namespace detail
}  // namespace prefilterExpressions
