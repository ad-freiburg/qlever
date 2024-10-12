//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "index/CompressedBlockPrefiltering.h"

#include "global/ValueIdComparators.h"

namespace prefilterExpressions {

// HELPER FUNCTIONS
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
static auto getTiedMaskedTriple(const BlockMetadata::PermutedTriple& triple,
                                size_t ignoreIndex = 3) {
  static const auto undefined = Id::makeUndefined();
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
    const auto& triple1First = block1.firstTriple_;
    const auto& triple1Last = block1.lastTriple_;
    const auto& triple2First = block2.firstTriple_;
    const auto& triple2Last = block2.lastTriple_;

    const auto checkInvalid = [triple1First, triple1Last, triple2First,
                               triple2Last](auto comp, size_t evalCol) {
      const auto& maskedT1Last = getTiedMaskedTriple(triple1Last, evalCol);
      const auto& maskedT2First = getTiedMaskedTriple(triple2First, evalCol);
      return comp(getTiedMaskedTriple(triple1First, evalCol), maskedT1Last) ||
             comp(maskedT1Last, maskedT2First) ||
             comp(maskedT2First, getTiedMaskedTriple(triple2Last, evalCol));
    };
    // Check for equality of IDs up to the evaluation column.
    if (checkInvalid(std::not_equal_to<>{}, evaluationColumn)) {
      throwRuntimeError(
          "The columns up to the evaluation column must contain the same "
          "values.");
    }
    // Check for order of IDs on evaluation Column.
    if (checkInvalid(std::greater<>{}, evaluationColumn + 1)) {
      throwRuntimeError(
          "The data blocks must be provided in sorted order regarding the "
          "evaluation column.");
    }
  }
};

//______________________________________________________________________________
// Given two sorted `vector`s containing `BlockMetadata`, this function
// returns their merged `BlockMetadata` content in a `vector` which is free of
// duplicates and ordered.
static auto getSetUnion(const std::vector<BlockMetadata>& blocks1,
                        const std::vector<BlockMetadata>& blocks2,
                        size_t evalCol) {
  evalCol += 1;
  std::vector<BlockMetadata> mergedVectors;
  mergedVectors.reserve(blocks1.size() + blocks2.size());
  auto blockLessThanBlock = [evalCol](const BlockMetadata& block1,
                                      const BlockMetadata& block2) {
    const auto& lastTripleBlock1 =
        getTiedMaskedTriple(block1.lastTriple_, evalCol);
    const auto& firstTripleBlock2 =
        getTiedMaskedTriple(block2.firstTriple_, evalCol);
    // Given two blocks with the respective ranges [-4, -4] (block1)
    // and [-4, 0.0] (block2) on the evaluation column, we see that [-4, -4] <
    // [-4, 0.0] should hold true. To assess if block1 is less than block2, it
    // is not enough to just compare -4 (ID from last triple of block1) to -4
    // (ID from first triple of block2). With this simple comparison [-4, -4] <
    // [-4, 0.0] wouldn't hold, because the comparison -4 < -4 will yield false.
    // Thus for proper implementation of the < operator, we have to check here
    // on the IDs of the last triple. -4 < 0.0 will yield true => [-4, -4] <
    // [-4, 0.0] holds true.
    if (lastTripleBlock1 == firstTripleBlock2) {
      return lastTripleBlock1 <
             getTiedMaskedTriple(block2.lastTriple_, evalCol);
    } else {
      return lastTripleBlock1 < firstTripleBlock2;
    }
  };
  // Given that we have vectors with sorted (BlockMedata) values, we can
  // use std::ranges::set_union. Thus the complexity is O(n + m).
  std::ranges::set_union(blocks1, blocks2, std::back_inserter(mergedVectors),
                         blockLessThanBlock);
  mergedVectors.shrink_to_fit();
  return mergedVectors;
}

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
  std::vector<BlockMetadata> mixedDatatypeBlocks;

  for (const auto& block : input) {
    const auto firstId =
        getIdFromColumnIndex(block.firstTriple_, evaluationColumn);
    const auto secondId =
        getIdFromColumnIndex(block.lastTriple_, evaluationColumn);
    valueIdsInput.push_back(firstId);
    valueIdsInput.push_back(secondId);

    if (firstId.getDatatype() != secondId.getDatatype()) {
      mixedDatatypeBlocks.push_back(block);
    }
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

  // The vector for relevant BlockMetadata values which contain ValueIds
  // defined as relevant by relevantIdRanges.
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
  // Merge mixedDatatypeBlocks into relevantBlocks while maintaining order and
  // avoiding duplicates.
  return getSetUnion(relevantBlocks, mixedDatatypeBlocks, evaluationColumn);
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
std::vector<BlockMetadata> LogicalExpression<Operation>::evaluateImpl(
    const std::vector<BlockMetadata>& input, size_t evaluationColumn) const {
  using enum LogicalOperators;
  if constexpr (Operation == AND) {
    auto resultChild1 = child1_->evaluate(input, evaluationColumn);
    return child2_->evaluate(resultChild1, evaluationColumn);
  } else {
    static_assert(Operation == OR);
    return getSetUnion(child1_->evaluate(input, evaluationColumn),
                       child2_->evaluate(input, evaluationColumn),
                       evaluationColumn);
  }
};

//______________________________________________________________________________
std::vector<BlockMetadata> NotExpression::evaluateImpl(
    const std::vector<BlockMetadata>& input, size_t evaluationColumn) const {
  return child_->evaluate(input, evaluationColumn);
};

}  //  namespace prefilterExpressions
