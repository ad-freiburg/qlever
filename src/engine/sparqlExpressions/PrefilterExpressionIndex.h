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

using IdOrLocalVocabEntry = std::variant<ValueId, LocalVocabEntry>;

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

  // This method is required for implementing the `NotExpression`. This method
  // is required, because we logically operate on `BlockMetadata` values which
  // define ranges given the `ValueIds` from last and first triple. E.g. the
  // `BlockMetadata` that defines the range [IntId(0),... IntId(5)], should be
  // considered relevant for the expression `?x >= IntId(3)`, but also for
  // expression `!(?x >= IntId(3))`. Thus we can't retrieve the negation by
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

  // It's expected that the provided `BlockMetadata` vector adheres to the
  // following conditions:
  // (1) unqiueness of blocks
  // (2) sorted (order)
  // (3) Constant values for all columns `< evaluationColumn`
  // To indicate that the possibly incomplete first and last block should be
  // handled appropriately, the `stripIncompleteBlocks` flag is set to `true`.
  // The flag value shouldn't be changed in general, because `evaluate()` only
  // removes the respective block if it is conditionally (inconsistent columns)
  // necessary.
  std::vector<BlockMetadata> evaluate(std::span<const BlockMetadata> input,
                                      size_t evaluationColumn,
                                      bool stripIncompleteBlocks = true) const;

  // Format for debugging
  friend std::ostream& operator<<(std::ostream& str,
                                  const PrefilterExpression& expression) {
    str << expression.asString(0) << "." << std::endl;
    return str;
  }

  // Static helper to retrieve the reference `ValueId` from the
  // `IdOrLocalVocabEntry` variant.
  static ValueId getValueIdFromIdOrLocalVocabEntry(
      const IdOrLocalVocabEntry& refernceValue, LocalVocab& vocab);

 private:
  // Note: Use `evaluate` for general evaluation of `PrefilterExpression`
  // instead of this method.
  // Performs the following conditional checks on
  // the provided `BlockMetadata` values: (1) unqiueness of blocks (2) sorted
  // (order) (3) Constant values for all columns `< evaluationColumn` This
  // function subsequently invokes the `evaluateImpl` method and checks the
  // corresponding result for those conditions again. If a respective condition
  // is violated, the function performing the checks will throw a
  // `std::runtime_error`.
  std::vector<BlockMetadata> evaluateAndCheckImpl(
      std::span<const BlockMetadata> input, size_t evaluationColumn) const;

  virtual std::vector<BlockMetadata> evaluateImpl(
      std::span<const BlockMetadata> input, size_t evaluationColumn) const = 0;
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
  // This is the right hand side value of the relational expression. The left
  // hand value is indirectly supplied during the evaluation process via the
  // `evaluationColumn` argument. `evaluationColumn` represents the column index
  // associated with the `Variable` column of the `IndexScan`.
  // E.g., a less-than expression with a value of 3 will represent the logical
  // relation ?var < 3. A equal-to expression with a value of "Freiburg" will
  // represent ?var = "Freiburg".
  IdOrLocalVocabEntry rightSideReferenceValue_;

 public:
  explicit RelationalExpression(const IdOrLocalVocabEntry& referenceValue)
      : rightSideReferenceValue_(referenceValue) {}

  std::unique_ptr<PrefilterExpression> logicalComplement() const override;
  bool operator==(const PrefilterExpression& other) const override;
  std::unique_ptr<PrefilterExpression> clone() const override;
  std::string asString(size_t depth) const override;

 private:
  std::vector<BlockMetadata> evaluateImpl(
      std::span<const BlockMetadata> input,
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
      std::span<const BlockMetadata> input,
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
      std::span<const BlockMetadata> input,
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

//______________________________________________________________________________
// Creates a `RelationalExpression<comparison>` prefilter expression based on
// the templated `CompOp` comparison operation and the reference
// `IdOrLocalVocabEntry` value. With the next step, the corresponding
// `<RelationalExpression<comparison>, Variable>` pair is created, and finally
// returned in a vector. The `mirrored` flag indicates if the given
// `RelationalExpression<comparison>` should be mirrored. The mirroring
// procedure changes the (asymmetrical) comparison operations:
// e.g. `5 < ?x` is changed to `?x > 5`.
template <CompOp comparison>
std::vector<PrefilterExprVariablePair> makePrefilterExpressionVec(
    const IdOrLocalVocabEntry& referenceValue, const Variable& variable,
    bool mirrored);

}  // namespace detail
}  // namespace prefilterExpressions
