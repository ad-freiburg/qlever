//  Copyright 2024 - 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#pragma once

#include <memory>
#include <vector>

#include "global/Id.h"
#include "global/ValueIdComparators.h"
#include "index/CompressedRelation.h"
#include "util/Iterators.h"

// For certain SparqlExpressions it is possible to perform a pre-filtering
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
// The alias `BlockRange` represents a range of relevant `BlockMetadata`
// values. The relevant range is defined by a `std::pair` of indices (as
// `size_t` values).
using BlockRange = std::pair<size_t, size_t>;

//______________________________________________________________________________
// A `std::vector` containing `BlockRange`s. `BlockRange` is an alias for
// `std::pair<size_t, size_t>` (defines a relevant `BlockMetadata` value
// section).
using RelevantBlockRanges = std::vector<BlockRange>;

//______________________________________________________________________________
// `AccessValueIdFromBlockMetadata` implements the `ValueId` access operator for
// containierized `std::span<cont BlockMetadata>` objects.
// Each `BlockMetadata` contains the descriptive triples `firstTriple_` and
// `lastTriple_`. Value `evaluationColumn` specifies for those triples the
// column/element index at which the relevant (bounding) `ValueId`s regarding
// our pre-filter procedure are positioned.
struct AccessValueIdFromBlockMetadata {
  size_t evaluationColumn_;
  explicit AccessValueIdFromBlockMetadata(size_t evaluationColumn)
      : evaluationColumn_{evaluationColumn} {}

  ValueId operator()(auto&& randomAccessContainer, uint64_t i) const;
};

// Specialized `Iterator` with `ValueId` access (retrieve `ValueId`s from
// corresponding `BlockMetadata`) on containerized `std::span<const
// BlockMetadata>` objects.
using ValueIdFromBlocksIt =
    ad_utility::IteratorForAccessOperator<std::span<const BlockMetadata>,
                                          AccessValueIdFromBlockMetadata,
                                          ad_utility::IsConst::True>;

//______________________________________________________________________________
/*
Remark: We don't evaluate the actual SPARQL Expression. We only pre-filter
w.r.t. blocks that contain potentially relevant data for the actual evaluation of
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

  // This method is required for implementing the `NotExpression`, because we
  // logically operate on `BlockMetadata` values which define ranges given the
  // `ValueId`s from last and first triple. E.g. the `BlockMetadata` that
  // defines the range [IntId(0),... IntId(5)] should be considered relevant
  // for the expression `?x >= IntId(3)`, but also for expression `!(?x >=
  // IntId(3))`. Thus we can't retrieve the negation by simply taking the
  // complementing set of `BlockMetadata`, instead we retrieve it by directly
  // negating/complementing the child expression itself. Every derived class can
  // return it's respective logical complement (negation) when being called on
  // `logicalCoplement()`. E.g. for a call w.r.t.
  // `RelationalExpression<LT>(IntId(5))` (< 5), the returned logical complement
  // is `RelationalExpression<GE>(IntId(5))` (>= 5). On a `LogicalExpression`
  // (`AND` or `OR`), we respectively apply De-Morgan's law and return the
  // resulting `LogicalExpression`. In case of the `NotExpression`, we just
  // return its child expression given that two negations (complementations)
  // cancel out. For a more concise explanation take a look at the actual
  // implementation for derived classes.
  virtual std::unique_ptr<PrefilterExpression> logicalComplement() const = 0;

  // It's expected that the provided `BlockMetadata` vector adheres to the
  // following conditions:
  // (1) unqiueness of blocks
  // (2) sorted (order)
  // (3) Constant values for all columns `< evaluationColumn`
  // Remark: The potentially incomplete first/last `BlockMetadata` values in
  // input are handled automatically. They are stripped at the beginning and
  // added again when the evaluation procedure was successfully performed.
  std::vector<BlockMetadata> evaluate(std::span<const BlockMetadata> input,
                                      size_t evaluationColumn) const;

  // `evaluateImpl` is internally used for the actual pre-filter procedure.
  // The `start/end` iterators `idsBegin/idsEnd` enable indirect access to the
  // `ValueId`s at column index `evaluationColumn` over the containerized
  // `std::span<const BlockMetadata> input`.
  virtual RelevantBlockRanges evaluateImpl(
      ValueIdFromBlocksIt idsBegin, ValueIdFromBlocksIt idsEnd) const = 0;

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
  RelevantBlockRanges evaluateImpl(ValueIdFromBlocksIt idsBegin,
                                   ValueIdFromBlocksIt idsEnd) const override;
};

//______________________________________________________________________________
// Values to differentiate `PrefilterExpression` for the respective `isDatatype`
// SPARQL expressions. Supported by the following prefilter
// `IsDatatypeExpression`: `isIri`, `isBlank`, `isLiteral` and `isNumeric`.
enum struct IsDatatype { IRI, BLANK, LITERAL, NUMERIC };

//______________________________________________________________________________
// The specialized `PrefilterExpression` class that actually applies the
// pre-filter procedure w.r.t. the datatypes defined with `IsDatatype`.
template <IsDatatype Datatype>
class IsDatatypeExpression : public PrefilterExpression {
 private:
  bool isNegated_;

 public:
  explicit IsDatatypeExpression(bool isNegated = false)
      : isNegated_(isNegated){};
  std::unique_ptr<PrefilterExpression> logicalComplement() const override;
  bool operator==(const PrefilterExpression& other) const override;
  std::unique_ptr<PrefilterExpression> clone() const override;
  std::string asString(size_t depth) const override;

 private:
  RelevantBlockRanges evaluateIsIriOrIsLiteralImpl(ValueIdFromBlocksIt idsBegin,
                                                   ValueIdFromBlocksIt idsEnd,
                                                   const bool isNegated) const;
  RelevantBlockRanges evaluateIsBlankOrIsNumericImpl(
      ValueIdFromBlocksIt idsBegin, ValueIdFromBlocksIt idsEnd,
      const bool isNegated) const;
  RelevantBlockRanges evaluateImpl(ValueIdFromBlocksIt idsBegin,
                                   ValueIdFromBlocksIt idsEnd) const override;
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
  // `IsDatatypeExpression` (for `IsDatatype::IRI` and `IsDatatype::LITERAL`)
  // requires access to `evaluateOptGetCompleteComplementImpl` for proper
  // complement computation while considering all datatype `ValueId`s.
  template <IsDatatype Datatype>
  friend class IsDatatypeExpression;

 public:
  explicit RelationalExpression(const IdOrLocalVocabEntry& referenceValue)
      : rightSideReferenceValue_(referenceValue) {}

  std::unique_ptr<PrefilterExpression> logicalComplement() const override;
  bool operator==(const PrefilterExpression& other) const override;
  std::unique_ptr<PrefilterExpression> clone() const override;
  std::string asString(size_t depth) const override;

 private:
  // If `getComplementOverAllDatatypes` is set to `true`, this method returns
  // the total complement over all datatype `ValueId`s from the
  // provided `BlockMetadata` values.
  RelevantBlockRanges evaluateOptGetCompleteComplementImpl(
      ValueIdFromBlocksIt idsBegin, ValueIdFromBlocksIt idsEnd,
      bool getComplementOverAllDatatypes = false) const;
  // Calls `evaluateOptGetCompleteComplementImpl` with
  // `getComplementOverAllDatatypes` set to `false` (standard evaluation
  // procedure).
  RelevantBlockRanges evaluateImpl(ValueIdFromBlocksIt idsBegin,
                                   ValueIdFromBlocksIt idsEnd) const override;
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
  RelevantBlockRanges evaluateImpl(ValueIdFromBlocksIt idsBegin,
                                   ValueIdFromBlocksIt idsEnd) const override;
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
// Define convenient names for the templated `IsDatatypeExpression`s.
using IsIriExpression = prefilterExpressions::IsDatatypeExpression<
    prefilterExpressions::IsDatatype::IRI>;
using IsBlankExpression = prefilterExpressions::IsDatatypeExpression<
    prefilterExpressions::IsDatatype::BLANK>;
using IsLiteralExpression = prefilterExpressions::IsDatatypeExpression<
    prefilterExpressions::IsDatatype::LITERAL>;
using IsNumericExpression = prefilterExpressions::IsDatatypeExpression<
    prefilterExpressions::IsDatatype::NUMERIC>;

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
