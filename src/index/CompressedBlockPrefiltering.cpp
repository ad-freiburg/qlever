//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "index/CompressedBlockPrefiltering.h"

#include "global/ValueIdComparators.h"

namespace prefilterExpressions {

// HELPER FUNCTION
//______________________________________________________________________________
// Given a PermutedTriple retrieve the suitable Id w.r.t. a column (index).
static constexpr auto getIdFromColumnIndex(
    const BlockMetadata::PermutedTriple& permutedTriple, size_t columnIndex) {
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

// SECTION RELATIONAL OPERATIONS
//______________________________________________________________________________
template <CompOp Comparison>
std::unique_ptr<PrefilterExpression>
RelationalExpression<Comparison>::logicalComplement() const {
  using enum CompOp;
  switch (Comparison) {
    case LT:
      // Complement X < Y: X >= Y
      return std::make_unique<GreaterEqualExpression>(referenceId_);
    case LE:
      // Complement X <= Y: X > Y
      return std::make_unique<GreaterThanExpression>(referenceId_);
    case EQ:
      // Complement X == Y: X != Y
      return std::make_unique<NotEqualExpression>(referenceId_);
    case NE:
      // Complement X != Y: X == Y
      return std::make_unique<EqualExpression>(referenceId_);
    case GE:
      // Complement X >= Y: X < Y
      return std::make_unique<LessThanExpression>(referenceId_);
    case GT:
      // Complement X > Y: X <= Y
      return std::make_unique<LessEqualExpression>(referenceId_);
    default:
      AD_FAIL();
  }
};

//______________________________________________________________________________
template <CompOp Comparison>
std::vector<BlockMetadata> RelationalExpression<Comparison>::evaluate(
    const std::vector<BlockMetadata>& input, size_t evaluationColumn) const {
  using namespace valueIdComparators;
  std::vector<ValueId> valueIdsInput;
  // For each BlockMetadata value in vector input, we have a respective Id for
  // firstTriple and lastTriple
  valueIdsInput.reserve(2 * input.size());

  for (const auto& block : input) {
    valueIdsInput.push_back(
        getIdFromColumnIndex(block.firstTriple_, evaluationColumn));
    valueIdsInput.push_back(
        getIdFromColumnIndex(block.lastTriple_, evaluationColumn));
  }
  // Use getRangesForId (from valueIdComparators) to extract the ranges
  // containing the relevant ValueIds.
  // For pre-filtering with CompOp::EQ, we have to consider empty ranges.
  // Reason: The referenceId_ could be contained within the bounds formed by
  // the IDs of firstTriple_ and lastTriple_ (set false flag to keep
  // empty ranges).
  auto relevantIdRanges =
      Comparison != CompOp::EQ
          ? getRangesForId(valueIdsInput.begin(), valueIdsInput.end(),
                           referenceId_, Comparison)
          : getRangesForId(valueIdsInput.begin(), valueIdsInput.end(),
                           referenceId_, Comparison, false);
  // The vector for relevant BlockMetadata values that contain ValueIds defined
  // as relevant relevantIdRanges.
  std::vector<BlockMetadata> relevantBlocks;
  // Reserve memory, input.size() is upper bound.
  relevantBlocks.reserve(input.size());
  // Given the relevant Id ranges, retrieve the corresponding relevant
  // BlockMetadata values from vector input and add them to the relevantBlocks
  // vector.
  for (const auto& range : relevantIdRanges) {
    relevantBlocks.insert(
        relevantBlocks.end(),
        input.begin() + std::distance(valueIdsInput.begin(), range.first) / 2,
        // Round up, for Ids contained within the bounding Ids of firstTriple
        // and lastTriple we have to include the respective metadata block
        // (that block is partially relevant).
        input.begin() +
            std::distance(valueIdsInput.begin(), range.second + 1) / 2);
  }
  relevantBlocks.shrink_to_fit();
  return relevantBlocks;
};

// SECTION LOGICAL OPERATIONS
//______________________________________________________________________________
template <LogicalOperators Operation>
std::unique_ptr<PrefilterExpression>
LogicalExpression<Operation>::logicalComplement() const {
  using enum LogicalOperators;
  switch (Operation) {
    case NOT:
      // Logically we complement (negate) a NOT here => NOT cancels out.
      // Therefore, we can simply return the child of the respective NOT
      // expression after undoing its previous complementation.
      return std::move(child1_->logicalComplement());
    // Source De-Morgan's laws: De Morgan's laws, Wikipedia.
    // Reference: https://en.wikipedia.org/wiki/De_Morgan%27s_laws
    case OR:
      AD_CONTRACT_CHECK(child2_.has_value());
      // De Morgan's law: not (A or B) = (not A) and (not B)
      return std::make_unique<AndExpression>(
          child1_->logicalComplement(), child2_.value()->logicalComplement());
    case AND:
      AD_CONTRACT_CHECK(child2_.has_value());
      // De Morgan's law: not (A and B) = (not A) or (not B)
      return std::make_unique<OrExpression>(
          child1_->logicalComplement(), child2_.value()->logicalComplement());
    default:
      AD_FAIL();
  }
};

//______________________________________________________________________________
template <LogicalOperators Operation>
std::vector<BlockMetadata> LogicalExpression<Operation>::evaluateOr(
    const std::vector<BlockMetadata>& input, size_t evaluationColumn) const
    requires(Operation == LogicalOperators::OR) {
  auto relevantBlocksChild1 = child1_->evaluate(input, evaluationColumn);
  auto relevantBlocksChild2 =
      child2_.value()->evaluate(input, evaluationColumn);
  // The result vector, reserve space for the logical upper bound (num blocks)
  // |relevantBlocksChild1| + |relevantBlocksChild2| regarding the OR
  // operation (union of blocks)
  std::vector<BlockMetadata> unionedRelevantBlocks;
  unionedRelevantBlocks.reserve(relevantBlocksChild1.size() +
                                relevantBlocksChild2.size());
  // Lambda function that implements the projection to retrieve ValueIds from
  // BlockMetadata values.
  auto retrieveValueId = [evaluationColumn](const BlockMetadata& block) {
    return getIdFromColumnIndex(block.lastTriple_, evaluationColumn);
  };
  // Given that we have vectors with sorted (BlockMedata) values, we can
  // use std::ranges::set_union, thus the complexity is O(n + m).
  std::ranges::set_union(relevantBlocksChild1, relevantBlocksChild2,
                         std::back_inserter(unionedRelevantBlocks),
                         std::ranges::less{}, retrieveValueId, retrieveValueId);
  return unionedRelevantBlocks;
};

//______________________________________________________________________________
template <LogicalOperators Operation>
std::vector<BlockMetadata> LogicalExpression<Operation>::evaluate(
    const std::vector<BlockMetadata>& input, size_t evaluationColumn) const {
  using enum LogicalOperators;
  if constexpr (Operation == AND) {
    auto resultChild1 = child1_->evaluate(input, evaluationColumn);
    AD_CONTRACT_CHECK(child2_.has_value());
    return child2_.value()->evaluate(resultChild1, evaluationColumn);
  } else if constexpr (Operation == OR) {
    AD_CONTRACT_CHECK(child2_.has_value());
    return evaluateOr(input, evaluationColumn);
  } else {
    static_assert(Operation == NOT);
    // Given that the child expression has already been negated during the (NOT)
    // expression construction, we just have to evaluate it.
    return child1_->evaluate(input, evaluationColumn);
  }
};

//______________________________________________________________________________
// Necessary instantiation of template specializations
template class RelationalExpression<CompOp::LT>;
template class RelationalExpression<CompOp::LE>;
template class RelationalExpression<CompOp::GE>;
template class RelationalExpression<CompOp::GT>;
template class RelationalExpression<CompOp::EQ>;
template class RelationalExpression<CompOp::NE>;

template class LogicalExpression<LogicalOperators::AND>;
template class LogicalExpression<LogicalOperators::OR>;
template class LogicalExpression<LogicalOperators::NOT>;

}  //  namespace prefilterExpressions
