//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "index/CompressedBlockPrefiltering.h"

#include "global/ValueIdComparators.h"

namespace prefilterExpressions {

// HELPER FUNCTION
//______________________________________________________________________________
// Given a PermutedTriple retrieve the suitable Id w.r.t. a column (index).
static auto getIdFromColumnIndex(const BlockMetadata::PermutedTriple& triple,
                                 size_t columnIndex) {
  switch (columnIndex) {
    case 0:
      return triple.col0Id_;
    case 1:
      return triple.col1Id_;
    case 2:
      return triple.col2Id_;
    default:
      // columnIndex out of bounds
      AD_FAIL();
  }
};

//______________________________________________________________________________
// Extract the Ids from the given `PermutedTriple` in a tuple up to the
// position (column index) defined by `ignoreIndex`. The ignored positions are
// filled with Ids `Id::makeUndefined()`. `Id::makeUndefined()` is guaranteed
// to be smaller than Ids of all other types.
static auto getTiedMaskedPermutedTriple(
    const BlockMetadata::PermutedTriple& triple, size_t ignoreIndex = 3) {
  const auto undefined = Id::makeUndefined();
  switch (ignoreIndex) {
    case 3:
      return std::tie(triple.col0Id_, triple.col1Id_, triple.col2Id_);
    case 2:
      return std::tie(triple.col0Id_, triple.col1Id_, undefined);
    case 1:
      return std::tie(triple.col0Id_, undefined, undefined);
    case 0:
      return std::tie(undefined, undefined, undefined);
    default:
      // ignoreIndex out of bounds
      AD_FAIL();
  }
};

//______________________________________________________________________________
// Check required conditions.
static auto checkEvalRequirements(const std::vector<BlockMetadata>& input,
                                  size_t evaluationColumn) {
  auto throwRuntimeError = [](const std::string& errorMessage) {
    throw std::runtime_error(errorMessage);
  };

  for (size_t i = 1; i < input.size(); i++) {
    const auto& block1 = input[i - 1];
    const auto& block2 = input[i];
    if (block1 == block2) {
      throwRuntimeError("The provided data blocks must be unique.");
    }
    const auto& triple1 = block1.lastTriple_;
    const auto& triple2 = block2.firstTriple_;
    if (getIdFromColumnIndex(triple1, evaluationColumn) >
        getIdFromColumnIndex(triple2, evaluationColumn)) {
      throwRuntimeError("The data blocks must be provided in sorted order.");
    }
    if (getTiedMaskedPermutedTriple(triple1, evaluationColumn) !=
        getTiedMaskedPermutedTriple(triple2, evaluationColumn)) {
      throwRuntimeError(
          "The columns up to the evaluation column must contain the same "
          "values.");
    }
  }
};

// SECTION PREFILTER EXPRESSION (BASE CLASS)
//______________________________________________________________________________
std::vector<BlockMetadata> PrefilterExpression::evaluate(
    const std::vector<BlockMetadata>& input, size_t evaluationColumn) const {
  checkEvalRequirements(input, evaluationColumn);
  const auto& relevantBlocks = evaluateImpl(input, evaluationColumn);
  checkEvalRequirements(relevantBlocks, evaluationColumn);
  return relevantBlocks;
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
std::vector<BlockMetadata> RelationalExpression<Comparison>::evaluateImpl(
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
  // as relevant by relevantIdRanges.
  std::vector<BlockMetadata> relevantBlocks;
  // Reserve memory, input.size() is upper bound.
  relevantBlocks.reserve(input.size());
  // Given the relevant Id ranges, retrieve the corresponding relevant
  // BlockMetadata values from vector input and add them to the relevantBlocks
  // vector.
  auto endValueIdsInput = valueIdsInput.end();
  for (const auto& [firstId, secondId] : relevantIdRanges) {
    // Ensures that index is within bounds of index vector.
    auto secondIdAdjusted =
        secondId < endValueIdsInput ? secondId + 1 : secondId;
    relevantBlocks.insert(
        relevantBlocks.end(),
        input.begin() + std::distance(valueIdsInput.begin(), firstId) / 2,
        // Round up, for Ids contained within the bounding Ids of firstTriple
        // and lastTriple we have to include the respective metadata block
        // (that block is partially relevant).
        input.begin() +
            std::distance(valueIdsInput.begin(), secondIdAdjusted) / 2);
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
  // Source De-Morgan's laws: De Morgan's laws, Wikipedia.
  // Reference: https://en.wikipedia.org/wiki/De_Morgan%27s_laws
  if constexpr (Operation == OR) {
    // De Morgan's law: not (A or B) = (not A) and (not B)
    return std::make_unique<AndExpression>(child1_->logicalComplement(),
                                           child2_->logicalComplement());
  } else {
    static_assert(Operation == AND);
    // De Morgan's law: not (A and B) = (not A) or (not B)
    return std::make_unique<OrExpression>(child1_->logicalComplement(),
                                          child2_->logicalComplement());
  }
};

//______________________________________________________________________________
std::unique_ptr<PrefilterExpression> NotExpression::logicalComplement() const {
  // Logically we complement (negate) a NOT here => NOT cancels out.
  // Therefore, we can simply return the child of the respective NOT
  // expression after undoing its previous complementation.
  return child_->logicalComplement();
};

//______________________________________________________________________________
template <LogicalOperators Operation>
std::vector<BlockMetadata> LogicalExpression<Operation>::evaluateOrImpl(
    const std::vector<BlockMetadata>& input, size_t evaluationColumn) const
    requires(Operation == LogicalOperators::OR) {
  auto relevantBlocksChild1 = child1_->evaluate(input, evaluationColumn);
  auto relevantBlocksChild2 = child2_->evaluate(input, evaluationColumn);

  std::vector<BlockMetadata> unionedRelevantBlocks;
  unionedRelevantBlocks.reserve(relevantBlocksChild1.size() +
                                relevantBlocksChild2.size());

  auto blockLessThanBlock = [](const BlockMetadata& block1,
                               const BlockMetadata& block2) {
    return getTiedMaskedPermutedTriple(block1.lastTriple_) <
           getTiedMaskedPermutedTriple(block2.firstTriple_);
  };
  // Given that we have vectors with sorted (BlockMedata) values, we can
  // use std::ranges::set_union, thus the complexity is O(n + m).
  std::ranges::set_union(relevantBlocksChild1, relevantBlocksChild2,
                         std::back_inserter(unionedRelevantBlocks),
                         blockLessThanBlock);
  return unionedRelevantBlocks;
};

//______________________________________________________________________________
template <LogicalOperators Operation>
std::vector<BlockMetadata> LogicalExpression<Operation>::evaluateImpl(
    const std::vector<BlockMetadata>& input, size_t evaluationColumn) const {
  using enum LogicalOperators;
  if constexpr (Operation == AND) {
    auto resultChild1 = child1_->evaluate(input, evaluationColumn);
    return child2_->evaluate(resultChild1, evaluationColumn);
  } else {
    static_assert(Operation == OR);
    return evaluateOrImpl(input, evaluationColumn);
  }
};

//______________________________________________________________________________
std::vector<BlockMetadata> NotExpression::evaluateImpl(
    const std::vector<BlockMetadata>& input, size_t evaluationColumn) const {
  return child_->evaluate(input, evaluationColumn);
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

}  //  namespace prefilterExpressions
