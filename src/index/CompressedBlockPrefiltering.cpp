//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "index/CompressedBlockPrefiltering.h"

using prefilterExpressions::CompOp;
using prefilterExpressions::LogicalExpressions;
using prefilterExpressions::LogicalOperators;
using prefilterExpressions::RelationalExpressions;

// SECTION RELATIONAL OPERATIONS
//______________________________________________________________________________
template <CompOp Comparison>
bool RelationalExpressions<Comparison>::compare(const ValueId& firstId,
                                                const ValueId& secondId) {
  // For compactness we combine LE and GT here. In evaluate (see below), for
  // LE we select all blocks at a smaller (lower_bound) index position, and
  // for GT all the respective blocks at larger or equal indices.
  if constexpr (Comparison == CompOp::LE || Comparison == CompOp::GT) {
    return compareIds(firstId, secondId, CompOp::LE) ==
           valueIdComparators::ComparisonResult::True;
  } else {
    // Follows the same logic for evaluation as explained above.
    // Used for CompOp::LT, CompOp::GE, CompOp::EQ and CompOp::NE.
    // For CompOp::EQ we also use the LT operation: This is possible because in
    // evaluate we calculate a lower_bound and upper_bound over this LT
    // operation, and those bounds act as the plausible range for equality
    // (evaluation of CompOp::NE follows a similar approach).
    return compareIds(firstId, secondId, CompOp::LT) ==
           valueIdComparators::ComparisonResult::True;
  }
}

//______________________________________________________________________________
template <CompOp Comparison>
auto RelationalExpressions<Comparison>::lowerIndex(
    std::vector<BlockMetadata>& input, size_t evaluationColumn) {
  // Custom compare function for binary search (lower bound).
  auto makeCompare = [this, evaluationColumn](
                         const BlockMetadata& blockMetadata,
                         const ValueId& referenceId) -> bool {
    ValueId blockId =
        Comparison == CompOp::LT || Comparison == CompOp::LE ||
                Comparison == CompOp::NE
            ? getIdFromColumnIndex(blockMetadata.firstTriple_, evaluationColumn)
            : getIdFromColumnIndex(blockMetadata.lastTriple_, evaluationColumn);
    return compare(blockId, referenceId);
  };
  return std::lower_bound(input.begin(), input.end(), referenceId_,
                          makeCompare);
};

//______________________________________________________________________________
template <CompOp Comparison>
auto RelationalExpressions<Comparison>::upperIndex(
    std::vector<BlockMetadata>& input, size_t evaluationColumn)
    requires(Comparison == CompOp::EQ || Comparison == CompOp::NE) {
  // Custom compare function for binary search (upper bound).
  auto makeCompare = [this, evaluationColumn](const ValueId& referenceId,
                                              BlockMetadata& blockMetadata) {
    return compare(
        referenceId,
        Comparison == CompOp::EQ
            ? getIdFromColumnIndex(blockMetadata.firstTriple_, evaluationColumn)
            : getIdFromColumnIndex(blockMetadata.lastTriple_,
                                   evaluationColumn));
  };
  // Use the same comparison operations as for the method lowerIndex
  // (std::lower_bound). However, given that std::upper_bound should return an
  // index to the first block that is larger than the provided reference value,
  // we have to swap the input values when calling compare() in makeCompare.
  // compare(blockId, referenceId) -> compare(referenceId, blockId)
  return std::upper_bound(input.begin(), input.end(), referenceId_,
                          makeCompare);
};

//______________________________________________________________________________
template <CompOp Comparison>
std::vector<BlockMetadata> RelationalExpressions<Comparison>::evaluate(
    std::vector<BlockMetadata>& input, size_t evaluationColumn) {
  if constexpr (Comparison == CompOp::LT || Comparison == CompOp::LE) {
    return std::vector<BlockMetadata>(input.begin(),
                                      lowerIndex(input, evaluationColumn));
  } else if constexpr (Comparison == CompOp::GE || Comparison == CompOp::GT) {
    // Complementary block selection to LT and LE
    return std::vector<BlockMetadata>(lowerIndex(input, evaluationColumn),
                                      input.end());
  } else if constexpr (Comparison == CompOp::EQ) {
    return std::vector<BlockMetadata>(lowerIndex(input, evaluationColumn),
                                      upperIndex(input, evaluationColumn));
  } else {
    // CompOP::NE
    // Get the indices that define the boundaries w.r.t. blocks potentially
    // containing non-equal values.
    auto firstBound = lowerIndex(input, evaluationColumn);
    auto secondBound = upperIndex(input, evaluationColumn);
    if (firstBound > secondBound) {
      return input;  // all blocks are relevant
    }
    // Extract the values from the respective ranges defined by the bounding
    // indices from vector input.
    // [input.begin(), firstBound) and [secondBound, input.end()]
    std::vector<BlockMetadata> vecNe;
    vecNe.reserve(std::distance(input.begin(), firstBound) +
                  std::distance(secondBound, input.end()));
    vecNe.insert(vecNe.end(), input.begin(), firstBound);
    vecNe.insert(vecNe.end(), secondBound, input.end());
    return vecNe;
  }
};

//______________________________________________________________________________
// Necessary instantiation of template specializations
template class RelationalExpressions<CompOp::LT>;
template class RelationalExpressions<CompOp::LE>;
template class RelationalExpressions<CompOp::GE>;
template class RelationalExpressions<CompOp::GT>;
template class RelationalExpressions<CompOp::EQ>;
template class RelationalExpressions<CompOp::NE>;

// SECTION LOGICAL OPERATIONS
//______________________________________________________________________________
template <LogicalOperators Operation>
std::vector<BlockMetadata> LogicalExpressions<Operation>::evaluateOr(
    std::vector<BlockMetadata>& input, size_t evaluationColumn)
    requires(Operation == LogicalOperators::OR) {
  auto relevantBlocksChild1 = child1_->evaluate(input, evaluationColumn);
  auto relevantBlocksChild2 = child2_->evaluate(input, evaluationColumn);
  // The result vector, reserve space for the logical upper bound (num blocks)
  // |relevantBlocksChild1| + |relevantBlocksChild2| regarding the OR
  // operation (union of blocks)
  std::vector<BlockMetadata> unionedRelevantBlocks;
  unionedRelevantBlocks.reserve(relevantBlocksChild1.size() +
                                relevantBlocksChild2.size());
  // lambda function that implements the less-than comparison required for
  // std::set_union
  auto lessThan = [evaluationColumn](const BlockMetadata& block1,
                                     const BlockMetadata& block2) {
    return getIdFromColumnIndex(block1.lastTriple_, evaluationColumn) <
           getIdFromColumnIndex(block2.lastTriple_, evaluationColumn);
  };
  // Given that we have vectors with sorted (BlockMedata) values, we can
  // use std::set_union, thus the complexity is O(n + m).
  std::set_union(relevantBlocksChild1.begin(), relevantBlocksChild1.end(),
                 relevantBlocksChild2.begin(), relevantBlocksChild2.end(),
                 std::back_inserter(unionedRelevantBlocks), lessThan);
  return unionedRelevantBlocks;
};

//______________________________________________________________________________
template <LogicalOperators Operation>
std::vector<BlockMetadata> LogicalExpressions<Operation>::evaluate(
    std::vector<BlockMetadata>& input, size_t evaluationColumn) {
  if constexpr (Operation == LogicalOperators::AND) {
    auto resultChild1 = child1_->evaluate(input, evaluationColumn);
    return child2_->evaluate(resultChild1, evaluationColumn);
  } else {
    return evaluateOr(input, evaluationColumn);
  }
};

//______________________________________________________________________________
// Necessary instantiation of template specializations
template class LogicalExpressions<LogicalOperators::AND>;
template class LogicalExpressions<LogicalOperators::OR>;
