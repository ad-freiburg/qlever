//  Copyright 2024 - 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_PREFILTEREXPRESSIONINDEX_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_PREFILTEREXPRESSIONINDEX_H

#include <memory>
#include <vector>

#include "global/Id.h"
#include "global/ValueIdComparators.h"
#include "index/CompressedRelation.h"
#include "util/Iterators.h"

// For certain SparqlExpressions it is possible to perform a pre-filtering
// procedure w.r.t. relevant data blocks/ValueId values, by making use of the
// available metadata (see CompressedBlockMetadata in CompressedRelation.h)
// while performing the index scan. As a result, the actual SparqlExpression
// evaluation is performed for a smaller IdTable if a PrefilterExpression
// (declared in this file) for the respective SparqlExpression is available and
// compatible with the IndexScan. The following SparqlExpressions construct a
// PrefilterExpression if possible: logical-or, logical-and, logical-negate
// (unary), relational-ops and strstarts.

namespace prefilterExpressions {

using IdOrLocalVocabEntry = std::variant<ValueId, LocalVocabEntry>;

//______________________________________________________________________________
// The maximum recursion depth for `info()` / `operator<<()`. A depth of `3`
// should be sufficient for most `PrefilterExpressions` in our use case.
constexpr size_t maxInfoRecursion = 3;

//______________________________________________________________________________
// `std::span` containing `CompressedBlockMetadata` (defined in
// CompressedRelation.h) values.
using BlockMetadataSpan = std::span<const CompressedBlockMetadata>;
// Iterator with respect to a `CompressedBlockMetadata` value of
// `std::span<const CompressedBlockMetadata>` (`BlockMetadataSpan`).
using BlockMetadataIt = BlockMetadataSpan::iterator;
// Section of relevant blocks as a subrange defined by `BlockMetadataIt`s.
using BlockMetadataRange = ql::ranges::subrange<BlockMetadataIt>;
// Vector containing `BlockMetadataRange`s.
using BlockMetadataRanges = std::vector<BlockMetadataRange>;

//______________________________________________________________________________
// `AccessValueIdFromBlockMetadata` implements the `ValueId` access operator on
// containerized `std::span<cont CompressedBlockMetadata>` objects. This
// (indexable) containerization procedure allows us to efficiently define
// relevant ranges by indices/iterators, instead of returning the relevant
// `CompressedBlockMetadata` values itself. `operator()(std::span<const
// CompressedBlockMetadata> randomAccessContainer,uint64_t i)` implements access
// to the i-th `ValueId` regarding our containerized `std::span<const
// CompressedBlockMetadata> inputSpan`. Each `CompressedBlockMetadata` value
// holds exactly two bound `ValueId`s (one in `firstTriple_` and `lastTriple_`
// respectively) over the specified column `evaluationColumn_`. This leads to an
// valid index range `[0, 2 * inputSpan.size())` for `i`. Under those conditions
// and an given `ValueId` index `i`, we can simply determine that the
// corresponding `ValueId` must be contained in `CompressedBlockMetadata` value
// at position `i/2`. `i % 2` specifies in the following if we have to access
// the ValueId from `fristTriple_` or `lastTriple_` of previously determined
// `CompressedBlockMetadata` value. (1) `i % 2 == 0`: retrieve `ValueId` from
// `firstTriple_`. (2) `i % 2 != 0`: retrieve `ValueId` from `lastTriple_`.
struct AccessValueIdFromBlockMetadata {
  size_t evaluationColumn_ = 0;
  // `ql::ranges::subrange` requires default constructor
  AccessValueIdFromBlockMetadata() = default;
  explicit AccessValueIdFromBlockMetadata(size_t evaluationColumn)
      : evaluationColumn_{evaluationColumn} {}

  ValueId operator()(BlockMetadataSpan randomAccessContainer, uint64_t i) const;
};

// Specialized `Iterator` with `ValueId` access (retrieve `ValueId`s from
// corresponding `CompressedBlockMetadata`) on containerized `std::span<const
// CompressedBlockMetadata>` objects.
using ValueIdIt = ad_utility::IteratorForAccessOperator<
    std::span<const CompressedBlockMetadata>, AccessValueIdFromBlockMetadata,
    ad_utility::IsConst::True>;

//______________________________________________________________________________
// `ValueIdSubrange` represents a (sub) range of relevant `ValueId`s over
// the containerized `std::span<const CompressedBlockMetadata> input`:
using ValueIdSubrange = ql::ranges::subrange<ValueIdIt>;

//______________________________________________________________________________
// Required because `valueIdComparators::getRangesForId` directly returns pairs
// of `ValueIdIt`s, and not sub ranges (`ValueIdSubrange`).
// Remark: The pair defines a relevant range of `ValueId`s over containerized
// `std::span<const CompressedBlockMetadata> input` by iterators.
using ValueIdItPair = std::pair<ValueIdIt, ValueIdIt>;

//______________________________________________________________________________
/*
Remark: We don't evaluate the actual SPARQL Expression. We only pre-filter
w.r.t. blocks that contain potentially relevant data for the actual evaluation
of those expressions to make the evaluation procedure more efficient.

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
  // logically operate on `CompressedBlockMetadata` values which define ranges
  // given the `ValueId`s from last and first triple. E.g. the
  // `CompressedBlockMetadata` that defines the range [IntId(0),... IntId(5)]
  // should be considered relevant for the expression `?x >= IntId(3)`, but also
  // for expression `!(?x >= IntId(3))`. Thus we can't retrieve the negation by
  // simply taking the complementing set of `CompressedBlockMetadata`, instead
  // we retrieve it by directly negating/complementing the child expression
  // itself. Every derived class can return it's respective logical complement
  // (negation) when being called on `logicalCoplement()`. E.g. for a call
  // w.r.t. `RelationalExpression<LT>(IntId(5))` (< 5), the returned logical
  // complement is `RelationalExpression<GE>(IntId(5))` (>= 5). On a
  // `LogicalExpression`
  // (`AND` or `OR`), we respectively apply De-Morgan's law and return the
  // resulting `LogicalExpression`. In case of the `NotExpression`, we just
  // return its child expression given that two negations (complementations)
  // cancel out. For a more concise explanation take a look at the actual
  // implementation for derived classes.
  virtual std::unique_ptr<PrefilterExpression> logicalComplement() const = 0;

  // It's expected that the provided `CompressedBlockMetadata` span adheres to
  // the following conditions: (1) unqiueness of blocks (2) sorted (order) (3)
  // Constant values for all columns `< evaluationColumn` Remark: The
  // potentially incomplete first/last `CompressedBlockMetadata` values in input
  // are handled automatically. They are stripped at the beginning and added
  // again when the evaluation procedure was successfully performed.
  //
  // TODO: `evaluate` should also return `BlockMetadataRanges` to avoid deep
  // copies. This requires additional changes in `IndexScan` and
  // `CompressedRelation`.
  std::vector<CompressedBlockMetadata> evaluate(BlockMetadataSpan blockRange,
                                                size_t evaluationColumn) const;

  // `evaluateImpl` is internally used for the actual pre-filter procedure.
  // `ValueIdSubrange idRange` enables indirect access to all `ValueId`s at
  // column index `evaluationColumn` over the containerized `std::span<const
  // CompressedBlockMetadata> input` (`BlockMetadataSpan`).
  virtual BlockMetadataRanges evaluateImpl(
      const ValueIdSubrange& idRange, BlockMetadataSpan blockRange) const = 0;

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
  BlockMetadataRanges evaluateImpl(const ValueIdSubrange& idRange,
                                   BlockMetadataSpan blockRange) const override;
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
  BlockMetadataRanges evaluateImpl(const ValueIdSubrange& idRange,
                                   BlockMetadataSpan blockRange) const override;
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
  // `evaluateIsIriOrisLiteralImpl` (for `IsDatatype::IRI` and
  // `IsDatatype::LITERAL`) requires access to
  // `evaluateOptGetCompleteComplementImpl`.
  template <IsDatatype Datatype>
  friend BlockMetadataRanges evaluateIsIriOrIsLiteralImpl(
      const ValueIdSubrange& idRange, BlockMetadataSpan blockRange,
      const bool isNegated);

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
  // provided `CompressedBlockMetadata` values.
  BlockMetadataRanges evaluateOptGetCompleteComplementImpl(
      const ValueIdSubrange& idRange, BlockMetadataSpan blockRange,
      bool getComplementOverAllDatatypes = false) const;
  // Calls `evaluateOptGetCompleteComplementImpl` with
  // `getComplementOverAllDatatypes` set to `false` (standard evaluation
  // procedure).
  BlockMetadataRanges evaluateImpl(const ValueIdSubrange& idRange,
                                   BlockMetadataSpan blockRange) const override;
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
  BlockMetadataRanges evaluateImpl(const ValueIdSubrange& idRange,
                                   BlockMetadataSpan blockRange) const override;
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
namespace logicalOps {
// `This internal helper function is only exposed for unit tests!`
// (1) `mergeRelevantBlockItRanges<true>` returns the `union` (`logical-or
// (||)`) of `BlockMetadataRanges r1` and `BlockMetadataRanges r2`.
// (2) `mergeRelevantBlockItRanges<false>` returns the `intersection`
// (`logical-and &&)`) of `BlockMetadataRanges r1` and `BlockMetadataRanges
// r2`.
template <bool GetUnion>
BlockMetadataRanges mergeRelevantBlockItRanges(const BlockMetadataRanges& r1,
                                               const BlockMetadataRanges& r2);
}  // namespace logicalOps

//______________________________________________________________________________
namespace mapping {
// `This internal helper function is only exposed for unit tests!`
// Map the complement of the given `ValueIdItPair`s, which directly refer to the
// `ValueId`s held by the containerized `std::span<const
// CompressedBlockMetadata>`, to their corresponding `BlockMetadataIt`s. The
// ranges defined by those `BlockMetadataIt`s directly refer to the (relevant)
// `CompressedBlockMetadata` values of `std::span<const
// CompressedBlockMetadata>` (`BlockMetadataSpan`).
BlockMetadataRanges mapValueIdItRangesToBlockItRangesComplemented(
    const std::vector<ValueIdItPair>& relevantIdRanges,
    const ValueIdSubrange& idRange, BlockMetadataSpan blockRange);
// `This internal helper function is only exposed for unit tests!`
// Map the given `ValueIdItPair`s, which directly refer to the
// `ValueId`s held by the containerized `std::span<const
// CompressedBlockMetadata>`, to their corresponding `BlockMetadataIt`s. The
// ranges defined by those `BlockMetadataIt`s directly refer to the (relevant)
// `CompressedBlockMetadata` values of `std::span<const
// CompressedBlockMetadata>` (`BlockMetadataSpan`).
BlockMetadataRanges mapValueIdItRangesToBlockItRanges(
    const std::vector<ValueIdItPair>& relevantIdRanges,
    const ValueIdSubrange& idRange, BlockMetadataSpan blockRange);
}  // namespace mapping

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
// This function is public s.t. we can test the corner case by providing an
// invalid/unknown `CompOp comparison` value.
// Use `makePrefilterExpressionVec` to retrieve the actual `PrefilterExpression`
// (with corresponding `Variable`).
std::unique_ptr<PrefilterExpression> makePrefilterExpressionYearImpl(
    CompOp comparison, const int year);

//______________________________________________________________________________
// Creates a `RelationalExpression<comparison>` prefilter expression based on
// the specified `CompOp` comparison operation and the reference
// `IdOrLocalVocabEntry` value. With the next step, the corresponding
// `<RelationalExpression<comparison>, Variable>` pair is created, and finally
// returned in a vector.
// The `mirrored` flag indicates if the given `RelationalExpression<comparison>`
// should be mirrored. The mirroring procedure changes the (asymmetrical)
// comparison operations: e.g. `5 < ?x` is changed to `?x > 5`.
// The `prefilterDateByYear` flag indicates that the constructed
// `PrefilterExpression<comp>` needs to consider `Date`/`DateYearOrDuration`
// value ranges for the `CompressedBlockMetadata` pre-selection (w.r.t. the year
// provided as an Int-`ValueId` over `referenceValue`). This requires a slightly
// different logic when constructing the expression, hence set
// `prefilterDateByYear` as an indicator.
template <CompOp comparison>
std::vector<PrefilterExprVariablePair> makePrefilterExpressionVec(
    const IdOrLocalVocabEntry& referenceValue, const Variable& variable,
    bool mirrored, bool prefilterDateByYear = false);

}  // namespace detail
}  // namespace prefilterExpressions

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_PREFILTEREXPRESSIONINDEX_H
