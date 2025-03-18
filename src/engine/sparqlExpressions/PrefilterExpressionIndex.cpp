//  Copyright 2024 - 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/PrefilterExpressionIndex.h"

#include <ranges>

#include "global/ValueIdComparators.h"
#include "util/ConstexprMap.h"
#include "util/OverloadCallOperator.h"

namespace prefilterExpressions {

// HELPER FUNCTIONS
//______________________________________________________________________________
// Create and return `std::unique_ptr<PrefilterExpression>(args...)`.
template <typename PrefilterT, typename... Args>
std::unique_ptr<PrefilterExpression> make(Args&&... args) {
  return std::make_unique<PrefilterT>(std::forward<Args>(args)...);
}

//______________________________________________________________________________
// Given a PermutedTriple retrieve the suitable Id w.r.t. a column (index).
static Id getIdFromColumnIndex(const BlockMetadata::PermutedTriple& triple,
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
// Extract the Ids from the given `PermutedTriple` in a tuple w.r.t. the
// position (column index) defined by `ignoreIndex`. The ignored positions are
// filled with Ids `Id::min()`. `Id::min()` is guaranteed
// to be smaller than Ids of all other types.
static auto getMaskedTriple(const BlockMetadata::PermutedTriple& triple,
                            size_t ignoreIndex = 3) {
  const Id& undefined = Id::min();
  switch (ignoreIndex) {
    case 3:
      return std::make_tuple(triple.col0Id_, triple.col1Id_, triple.col2Id_);
    case 2:
      return std::make_tuple(triple.col0Id_, triple.col1Id_, undefined);
    case 1:
      return std::make_tuple(triple.col0Id_, undefined, undefined);
    case 0:
      return std::make_tuple(undefined, undefined, undefined);
    default:
      // ignoreIndex out of bounds
      AD_FAIL();
  }
};

//______________________________________________________________________________
// Check for constant values in all columns `< evaluationColumn`
static bool checkBlockIsInconsistent(const BlockMetadata& block,
                                     size_t evaluationColumn) {
  return getMaskedTriple(block.firstTriple_, evaluationColumn) !=
         getMaskedTriple(block.lastTriple_, evaluationColumn);
}

//______________________________________________________________________________
// Check for the following conditions.
// (1) All `BlockMetadata` values in `input` must be unique.
// (2) `input` must contain those `BlockMetadata` values in sorted order.
// (3) Columns with `column index < evaluationColumn` must contain consistent
// values (`ValueId`s).
static void checkRequirementsBlockMetadata(std::span<const BlockMetadata> input,
                                           size_t evaluationColumn) {
  const auto throwRuntimeError = [](const std::string& errorMessage) {
    throw std::runtime_error(errorMessage);
  };
  // Check for duplicates.
  if (auto it = ql::ranges::adjacent_find(input); it != input.end()) {
    throwRuntimeError("The provided data blocks must be unique.");
  }
  // Helper to check for fully sorted blocks. Return `true` if `b1 < b2` is
  // satisfied.
  const auto checkOrder = [](const BlockMetadata& b1, const BlockMetadata& b2) {
    if (b1.blockIndex_ < b2.blockIndex_) {
      AD_CORRECTNESS_CHECK(getMaskedTriple(b1.lastTriple_) <=
                           getMaskedTriple(b2.lastTriple_));
      return true;
    }
    if (b1.blockIndex_ == b2.blockIndex_) {
      // Given the previous check detects duplicates in the input, the
      // correctness check here will never evaluate to true.
      // => blockIndex_ assignment issue.
      AD_CORRECTNESS_CHECK(b1 == b2);
    } else {
      AD_CORRECTNESS_CHECK(getMaskedTriple(b1.lastTriple_) >
                           getMaskedTriple(b2.firstTriple_));
    }
    return false;
  };
  if (!ql::ranges::is_sorted(input, checkOrder)) {
    throwRuntimeError("The blocks must be provided in sorted order.");
  }
  // Helper to check for column consistency. Returns `true` if the columns for
  // `b1` and `b2` up to the evaluation are inconsistent.
  const auto checkColumnConsistency =
      [evaluationColumn](const BlockMetadata& b1, const BlockMetadata& b2) {
        return checkBlockIsInconsistent(b1, evaluationColumn) ||
               getMaskedTriple(b1.lastTriple_, evaluationColumn) !=
                   getMaskedTriple(b2.firstTriple_, evaluationColumn) ||
               checkBlockIsInconsistent(b2, evaluationColumn);
      };
  if (auto it = ql::ranges::adjacent_find(input, checkColumnConsistency);
      it != input.end()) {
    throwRuntimeError(
        "The values in the columns up to the evaluation column must be "
        "consistent.");
  }
};

//______________________________________________________________________________
static std::vector<BlockMetadata> getRelevantBlocks(
    const RelevantBlockRanges& relevantBlockRanges,
    std::span<const BlockMetadata> blocks) {
  [[maybe_unused]] auto blocksEnd = blocks.size();
  auto blocksBegin = blocks.begin();

  std::vector<BlockMetadata> relevantBlocks;
  for (const auto& [rangeBegin, rangeEnd] : relevantBlockRanges) {
    assert(rangeBegin >= 0 && rangeBegin <= rangeEnd && rangeEnd <= blocksEnd);
    relevantBlocks.insert(relevantBlocks.end(), blocksBegin + rangeBegin,
                          blocksBegin + rangeEnd);
  }
  return relevantBlocks;
}

namespace detail::mapping {
//______________________________________________________________________________
// Helper for the following index-map functions that adds the relevant
// ranges to `blockRangeIndices`. `addRangeAndSimplify` ensures that the ranges
// added are non-overlapping and merged together if adjacent.
auto addRangeAndSimplify = [](RelevantBlockRanges& blockRangeIndices,
                              const size_t rangeIdxFirst,
                              const size_t rangeIdxSecond) {
  // Empty ranges aren't relevant.
  if (rangeIdxFirst == rangeIdxSecond) return;
  // Simplify the mapped ranges if adjacent or overlapping.
  // This simplification procedure also ensures that no `BlockMetadata`
  // value is retrieved twice later on (no overlapping indices for adjacient
  // `BlockRange` values).
  if (!blockRangeIndices.empty() &&
      blockRangeIndices.back().second >= rangeIdxFirst) {
    // They are adjacent or even overlap: merge/simplify!
    blockRangeIndices.back().second = rangeIdxSecond;
  } else {
    // The new range (specified by `BlockRange`) is not adjacent and
    // doesn't overlap.
    blockRangeIndices.emplace_back(rangeIdxFirst, rangeIdxSecond);
  }
};

//______________________________________________________________________________
// For general mapping description see comment to
// `mapRandomItRangesToBlockRanges`. The only difference here is that we map
// over the complementing `RandomValueIdItPair`s with respect to the
// `RandomValueIdItPair`s contained in `relevantIdRanges`.
RelevantBlockRanges mapRandomItRangesToBlockRangesComplemented(
    const std::vector<RandomValueIdItPair>& relevantIdRanges,
    ValueIdFromBlocksIt idsBegin, ValueIdFromBlocksIt idsEnd) {
  // The MAX index (allowed difference).
  const auto maxIndex = std::distance(idsBegin, idsEnd) / 2;
  if (relevantIdRanges.empty()) return {std::make_pair(0, maxIndex)};

  // Vector containing the `BlockRange` mapped indices.
  RelevantBlockRanges blockRanges;
  blockRanges.reserve(relevantIdRanges.size());

  ValueIdFromBlocksIt lastIdRangeIt = idsBegin;
  for (const auto& [firstIdRangeIt, secondIdRangeIt] : relevantIdRanges) {
    addRangeAndSimplify(
        blockRanges,
        static_cast<size_t>(std::distance(idsBegin, lastIdRangeIt) / 2),
        // std::min ensures that we respect the bounds of the
        // `BlockMetadata` value span later on.
        static_cast<size_t>(std::min(
            (std::distance(idsBegin, firstIdRangeIt) + 1) / 2, maxIndex)));
    // Set start index for the next complement section.
    lastIdRangeIt = secondIdRangeIt;
  }
  // Handle the last complementing section.
  const auto& mappedBlockRangeFirst =
      std::distance(idsBegin, lastIdRangeIt) / 2;
  if (mappedBlockRangeFirst < maxIndex) {
    addRangeAndSimplify(blockRanges, static_cast<size_t>(mappedBlockRangeFirst),
                        static_cast<size_t>(maxIndex));
  }
  return blockRanges;
}

//______________________________________________________________________________
// EXAMPLE MAPPING PROCEDURE AND WIDER CONTEXT
//
// (1)
// Assume for simplicity that the following construct (only one column)
// represents the `BlockMetadata` span (`input`) provided to
// `PrefilterExpression::evaluate` earlier on. Each bracket representing the
// relevant first and last `ValueId` for a single `BlockMetadata` value. Lets
// assume `std::span<const BlockMetadata> input` is:
// `{[1021, 1082], [1083, 1115], [1121, 1140], [1140, 1148], [1150, 1158]}`
//
// (2)
// The iterators `ValueIdFromBlocksIt idsBegin` and `ValueIdFromBlocksIt idsEnd`
// provide access to the `ValueId`s over the containerized `BlockMetadata`
// values.
// The overall accessible `ValueId` range is:
// `{1021, 1082, 1083, 1115, 1121, 1140, 1140, 1148, 1150, 1158}`
//
// (3)
// Now let's assume that the reference `ValueId` for which we want to
// pre-filter the relation `less-than` (`<`) has the value `1123`.
//
// (4)
// When we analyze the total `ValueId` range `{1021, 1082, 1083, 1115, 1121,
// 1140, 1140, 1148, 1150, 1158}`; it becomes clear that the `ValueId`s `1021 -
// 1140` are relevant (pre-filtered values). As a consequence the `ValueId`s
// `1140 - 1158` should be dropped (filtered out).
//
// (5)
// When `valueIdComparators::getRangesForId` was applied with respect to the
// overall `ValueId` range (in `RelationalExpression::evaluateImpl`), the
// `iterator`s representing the range `1021 - 1140` were retrieved. Remark:
// the second index should point to the first `ValueId` that does not satisfy
// `< 1123` (our pre-filter reference `ValueId`).
// `valueIdComparators::getRangesForId` returned `{std::pair<iterator(1021),
// iterator(1140)>}`
//
// (6)
// With step (5) gollows:
// `relevantIdRanges = {std::pair<iterator(1021), iterator(1140)>}`
//
// (7)
// `mapRandomItRanges` will now map the range `std::pair<iterator(1021),
// iterator(1140)>` contained in `relevantIdRanges` to and index range
// `std::pair<size_t, size_t>` suitable for retrieving relevant `BlockMetadata`
// values from `std::span<const BlockMetadata> input`.
//
// (8)
// Remark: For every `BlockMetadata` value we retrieved two bounding
// `ValueId`s. This is the reason why we have to divide by two. We also round up
// with `(std::distance(idsBegin, secondIdRangeIt) + 1) / 2` because the
// `iterator`s specify ranges up-to-but-not-including.
//
// The added range is
// `size_t begin = std::distance(iterator(1021), iterator(1021)) / 2
//               = 0`
// `size_t end = (std::distance(iterator(1021), iterator(1140)) + 1) / 2
//             = (5 + 1) / 2
//             = 3`
//
// (9)
// Recap:
// `std::pair<iterator(1021), iterator(1140)>` is mapped to
// `std::pair<0, 3>` (`BlockRange`).
//
// (10)
// With `std::pair<0, 3>` the `BlockMetadata` values `input[0, 3]` will be
// retrieved later on.
// `input = {[1021, 1082], [1083, 1115], [1121, 1140], [1140, 1148], [1150,
// 1158]}`
//
// => return blocks `{[1021, 1082], [1083, 1115], [1121, 1140]}`
//
// And indeed, those blocks are relevant when pre-filtering for `< 1123`
// (specified for (3))
RelevantBlockRanges mapRandomItRangesToBlockRanges(
    const std::vector<RandomValueIdItPair>& relevantIdRanges,
    ValueIdFromBlocksIt idsBegin, ValueIdFromBlocksIt idsEnd) {
  if (relevantIdRanges.empty()) return {};
  // Vector containing the `BlockRange` mapped indices.
  RelevantBlockRanges blockRanges;
  blockRanges.reserve(relevantIdRanges.size());
  // The MAX index (allowed difference).
  const auto maxIndex = std::distance(idsBegin, idsEnd) / 2;

  for (const auto& [firstIdRangeIt, secondIdRangeIt] : relevantIdRanges) {
    assert(firstIdRangeIt <= secondIdRangeIt);
    addRangeAndSimplify(
        blockRanges,
        static_cast<size_t>(std::distance(idsBegin, firstIdRangeIt) / 2),
        // std::min ensures that we respect the bounds of the `BlockMetadata`
        // value span later on.
        static_cast<size_t>(std::min(
            (std::distance(idsBegin, secondIdRangeIt) + 1) / 2, maxIndex)));
  }
  return blockRanges;
}
}  // namespace detail::mapping

// SECTION HELPER LOGICAL OPERATORS
namespace detail::logicalOps {

//______________________________________________________________________________
// Helper for `getUnion` and `getIntersection`.
// Add the range defined by `startIndex`/`endIndex` to `RelevantBlockRanges
// ranges` and simplify/merge with respect to the last added range if possible.
auto addAndSimplifyRange = [](RelevantBlockRanges& ranges, size_t startIndex,
                              size_t endIndex) {
  if (ranges.empty() || startIndex > ranges.back().second) {
    ranges.emplace_back(startIndex, endIndex);
  } else {
    ranges.back().second = std::max(ranges.back().second, endIndex);
  }
};

//______________________________________________________________________________
// `getUnion` implements `logical-or (||)` on `BlockRange` values. The function
// unifies index (`size_t`) ranges.
// `BlockRange` is simply an alias for `std::pair<size_t, size_t>`.
//
// EXAMPLE
// Given input `r1` and `r2`
// `RelevantBlockRanges r1`: `[<2, 10>, <15, 16>, <20, 23>]`
// `RelevantBlockRanges r2`: `[<4, 6>, <8, 9>, <15, 22>]`
// The result is
// `RelevantBlocksRanges result`: `[<2, 10>, <15, 23>]`
RelevantBlockRanges getUnion(const RelevantBlockRanges& r1,
                             const RelevantBlockRanges& r2) {
  // (1) Given the  ranges r1 and r2 are empty, return the empty range.
  if (r1.empty() && r2.empty()) return {};
  // (2) Merge, unite and simplify the ranges of r1 and r2.
  RelevantBlockRanges unionedRanges;
  unionedRanges.reserve(r1.size() + r2.size());
  auto idx1 = r1.begin();
  auto idx2 = r2.begin();

  while (idx1 != r1.end() && idx2 != r2.end()) {
    const auto& [idx1First, idx1Second] = *idx1;
    const auto& [idx2First, idx2Second] = *idx2;
    if (idx2Second < idx1First) {
      // The current ranges of r1 and r2 don't intersect.
      // BlockRange of r2 < BlockRange of r1 overall.
      addAndSimplifyRange(unionedRanges, idx2First, idx2Second);
      idx2++;
    } else if (idx1Second < idx2First) {
      // The current ranges of r1 and r2 don't intersect.
      // BlockRange of r1 < BlockRange of r2 overall.
      addAndSimplifyRange(unionedRanges, idx1First, idx1Second);
      idx1++;
    } else {
      // Overlapping ranges r1 and r2.
      addAndSimplifyRange(unionedRanges, std::min(idx1First, idx2First),
                          std::max(idx1Second, idx2Second));
      idx1++;
      idx2++;
    }
  }
  const auto optAddRestRanges = [&unionedRanges](const BlockRange& range) {
    addAndSimplifyRange(unionedRanges, range.first, range.second);
  };
  // Add the additional `BlockRange`s of r1 or r2 to unionedRanges.
  ql::ranges::for_each(idx1, r1.end(), optAddRestRanges);
  ql::ranges::for_each(idx2, r2.end(), optAddRestRanges);
  return unionedRanges;
}

//______________________________________________________________________________
// `getIntersection` implements `logical-and (&&)` on `BlockRange` values. The
// function intersects index (`size_t`) ranges. `BlockRange` is simply an alias
// for `std::pair<size_t, size_t>`.
//
// EXAMPLE
// Given input `r1` and `r2`
// `RelevantBlockRanges r1`: `[<2, 10>, <15, 16>, <20, 23>]`
// `RelevantBlockRanges r2`: `[<4, 6>, <8, 9>, <15, 22>]`
// The result is
// `RelevantBlocksRanges result`: `[<4, 6>, <8, 9>, <15, 16>, <20, 22>]`
RelevantBlockRanges getIntersection(const RelevantBlockRanges& r1,
                                    const RelevantBlockRanges& r2) {
  // If one of the ranges is empty, the intersection by definition is empty.
  if (r1.empty() || r2.empty()) return {};

  RelevantBlockRanges intersectedRanges;
  intersectedRanges.reserve(r1.size() + r2.size());
  auto idx1 = r1.begin();
  auto idx2 = r2.begin();

  while (idx1 != r1.end() && idx2 != r2.end()) {
    const auto& [idx1First, idx1Second] = *idx1;
    const auto& [idx2First, idx2Second] = *idx2;
    // Handle no overlap w.r.t. current ranges, the intersection is empty.
    if (idx1Second < idx2First) {
      idx1++;
      continue;
    }
    if (idx2Second < idx1First) {
      idx2++;
      continue;
    }
    // The ranges r1 and r2 overlap, add the intersction to intersectedRanges.
    addAndSimplifyRange(intersectedRanges, std::max(idx1First, idx2First),
                        std::min(idx1Second, idx2Second));
    if (idx1Second < idx2Second) {
      idx1++;
    } else if (idx2Second < idx1Second) {
      idx2++;
    } else {
      idx1++;
      idx2++;
    }
  }
  // The remaining BlockRange values from r1 or r2 are simply discarded given
  // we completely iterated over r1 and/or r2. No additional intersections are
  // possible.
  return intersectedRanges;
}
}  // namespace detail::logicalOps

//______________________________________________________________________________
// This function retrieves the `BlockRange`s for `BlockMetadata` values that
// contain bounding `ValueId`s with different underlying datatypes.
static std::vector<BlockRange> getRangesMixedDatatypeBlocks(
    ValueIdFromBlocksIt idsBegin, ValueIdFromBlocksIt idsEnd) {
  if (idsBegin == idsEnd) return {};
  ValueIdFromBlocksIt beginCopy = idsBegin;
  std::vector<RandomValueIdItPair> mixedDatatypeRanges;
  while (idsBegin != idsEnd) {
    ValueId firstId = *idsBegin++;
    // We shouldn't have access to an uneven number of `ValueId`s over
    // `BlockMetadata` values. By contract the next iterator value should be
    // valid.
    AD_CORRECTNESS_CHECK(idsBegin != idsEnd);
    ValueId secondId = *idsBegin;
    if (firstId.getDatatype() != secondId.getDatatype()) {
      mixedDatatypeRanges.emplace_back(idsBegin - 1, idsBegin);
    }
    idsBegin++;
  }
  return detail::mapping::mapRandomItRangesToBlockRanges(mixedDatatypeRanges,
                                                         beginCopy, idsEnd);
}

//______________________________________________________________________________
// Return `CompOp`s as string.
static std::string getRelationalOpStr(const CompOp relOp) {
  using enum CompOp;
  switch (relOp) {
    case LT:
      return "LT(<)";
    case LE:
      return "LE(<=)";
    case EQ:
      return "EQ(=)";
    case NE:
      return "NE(!=)";
    case GE:
      return "GE(>=)";
    case GT:
      return "GT(>)";
    default:
      AD_FAIL();
  }
}

//______________________________________________________________________________
// Return `Datatype`s (for `isDatatype` pre-filter) as string.
static std::string getDatatypeIsTypeStr(const IsDatatype isDtype) {
  using enum IsDatatype;
  switch (isDtype) {
    case IRI:
      return "Iri";
    case BLANK:
      return "Blank";
    case LITERAL:
      return "Literal";
    case NUMERIC:
      return "Numeric";
    default:
      AD_FAIL();
  }
}

//______________________________________________________________________________
// Return `LogicalOperator`s as string.
static std::string getLogicalOpStr(const LogicalOperator logOp) {
  using enum LogicalOperator;
  switch (logOp) {
    case AND:
      return "AND(&&)";
    case OR:
      return "OR(||)";
    default:
      AD_FAIL();
  }
}

// CUSTOM VALUE-ID ACCESS (STRUCT) OPERATOR
//______________________________________________________________________________
// Enables access to the i-th `ValueId` regarding our containerized
// `std::span<const BlockMetadata> inputSpan`.
// Each `BlockMetadata` value holds exactly two bound `ValueId`s (one in
// `firstTriple_` and `lastTriple_` respectively) over the specified column
// `evaluationColumn_`.
// Thus, the valid index range over `i` is `[0, 2 * inputSpan.size())`.
ValueId AccessValueIdFromBlockMetadata::operator()(
    std::span<const BlockMetadata> randomAccessContainer, uint64_t i) const {
  const BlockMetadata& block = randomAccessContainer[i / 2];
  return i % 2 == 0
             ? getIdFromColumnIndex(block.firstTriple_, evaluationColumn_)
             : getIdFromColumnIndex(block.lastTriple_, evaluationColumn_);
};

// SECTION PREFILTER EXPRESSION (BASE CLASS)
//______________________________________________________________________________
std::vector<BlockMetadata> PrefilterExpression::evaluate(
    std::span<const BlockMetadata> input, size_t evaluationColumn) const {
  if (input.size() < 3) {
    return std::vector<BlockMetadata>(input.begin(), input.end());
  }

  std::optional<BlockMetadata> firstBlock = std::nullopt;
  std::optional<BlockMetadata> lastBlock = std::nullopt;
  if (checkBlockIsInconsistent(input.front(), evaluationColumn)) {
    firstBlock = input.front();
    input = input.subspan(1);
  }
  if (checkBlockIsInconsistent(input.back(), evaluationColumn)) {
    lastBlock = input.back();
    input = input.subspan(0, input.size() - 1);
  }

  std::vector<BlockMetadata> result;
  if (!input.empty()) {
    checkRequirementsBlockMetadata(input, evaluationColumn);
    AccessValueIdFromBlockMetadata accessValueIdOp(evaluationColumn);
    ValueIdFromBlocksIt idsBegin{&input, 0, accessValueIdOp};
    ValueIdFromBlocksIt idsEnd{&input, input.size() * 2, accessValueIdOp};
    result =
        getRelevantBlocks(detail::logicalOps::getUnion(
                              evaluateImpl(idsBegin, idsEnd),
                              // always add mixed datatype blocks
                              getRangesMixedDatatypeBlocks(idsBegin, idsEnd)),
                          input);
    checkRequirementsBlockMetadata(result, evaluationColumn);
  }

  if (firstBlock.has_value()) {
    result.insert(result.begin(), firstBlock.value());
  }
  if (lastBlock.has_value()) {
    result.push_back(lastBlock.value());
  }
  return result;
};

//______________________________________________________________________________
ValueId PrefilterExpression::getValueIdFromIdOrLocalVocabEntry(
    const IdOrLocalVocabEntry& referenceValue, LocalVocab& vocab) {
  return std::visit(ad_utility::OverloadCallOperator{
                        [](const ValueId& referenceId) { return referenceId; },
                        [&vocab](const LocalVocabEntry& referenceLVE) {
                          return Id::makeFromLocalVocabIndex(
                              vocab.getIndexAndAddIfNotContained(referenceLVE));
                        }},
                    referenceValue);
}

// SECTION RELATIONAL OPERATIONS
//______________________________________________________________________________
template <CompOp Comparison>
std::unique_ptr<PrefilterExpression>
RelationalExpression<Comparison>::logicalComplement() const {
  using enum CompOp;
  using namespace ad_utility;
  using P = std::pair<CompOp, CompOp>;
  // The complementation logic implemented with the following mapping
  // procedure:
  // (1) ?var < referenceValue -> ?var >= referenceValue
  // (2) ?var <= referenceValue -> ?var > referenceValue
  // (3) ?var >= referenceValue -> ?var < referenceValue
  // (4) ?var > referenceValue -> ?var <= referenceValue
  // (5) ?var = referenceValue -> ?var != referenceValue
  // (6) ?var != referenceValue -> ?var = referenceValue
  constexpr ConstexprMap<CompOp, CompOp, 6> complementMap(
      {P{LT, GE}, P{LE, GT}, P{GE, LT}, P{GT, LE}, P{EQ, NE}, P{NE, EQ}});
  return make<RelationalExpression<complementMap.at(Comparison)>>(
      rightSideReferenceValue_);
};

//______________________________________________________________________________
template <CompOp Comparison>
RelevantBlockRanges
RelationalExpression<Comparison>::evaluateOptGetCompleteComplementImpl(
    ValueIdFromBlocksIt idsBegin, ValueIdFromBlocksIt idsEnd,
    bool getComplementOverAllDatatypes) const {
  using namespace valueIdComparators;
  // If `rightSideReferenceValue_` contains a `LocalVocabEntry` value, we use
  // the here created `LocalVocab` to retrieve a corresponding `ValueId`.
  LocalVocab vocab{};
  auto referenceId =
      getValueIdFromIdOrLocalVocabEntry(rightSideReferenceValue_, vocab);
  // Use getRangesForId (from valueIdComparators) to extract the ranges
  // containing the relevant ValueIds.
  // For pre-filtering with CompOp::EQ, we have to consider empty ranges.
  // Reason: The referenceId could be contained within the bounds formed by
  // the IDs of firstTriple_ and lastTriple_ (set false flag to keep
  // empty ranges).
  auto relevantIdRanges =
      Comparison != CompOp::EQ
          ? getRangesForId(idsBegin, idsEnd, referenceId, Comparison)
          : getRangesForId(idsBegin, idsEnd, referenceId, Comparison, false);
  valueIdComparators::detail::simplifyRanges(relevantIdRanges);
  return getComplementOverAllDatatypes
             ? detail::mapping::mapRandomItRangesToBlockRangesComplemented(
                   relevantIdRanges, idsBegin, idsEnd)
             : detail::mapping::mapRandomItRangesToBlockRanges(
                   relevantIdRanges, idsBegin, idsEnd);
};

//______________________________________________________________________________
template <CompOp Comparison>
RelevantBlockRanges RelationalExpression<Comparison>::evaluateImpl(
    ValueIdFromBlocksIt idsBegin, ValueIdFromBlocksIt idsEnd) const {
  return evaluateOptGetCompleteComplementImpl(idsBegin, idsEnd, false);
};

//______________________________________________________________________________
template <CompOp Comparison>
bool RelationalExpression<Comparison>::operator==(
    const PrefilterExpression& other) const {
  const auto* otherRelational =
      dynamic_cast<const RelationalExpression<Comparison>*>(&other);
  if (!otherRelational) {
    return false;
  }
  return rightSideReferenceValue_ == otherRelational->rightSideReferenceValue_;
};

//______________________________________________________________________________
template <CompOp Comparison>
std::unique_ptr<PrefilterExpression> RelationalExpression<Comparison>::clone()
    const {
  return make<RelationalExpression<Comparison>>(rightSideReferenceValue_);
};

//______________________________________________________________________________
template <CompOp Comparison>
std::string RelationalExpression<Comparison>::asString(
    [[maybe_unused]] size_t depth) const {
  auto referenceValueToString = [](std::stringstream& stream,
                                   const IdOrLocalVocabEntry& referenceValue) {
    std::visit(
        ad_utility::OverloadCallOperator{
            [&stream](const ValueId& referenceId) { stream << referenceId; },
            [&stream](const LocalVocabEntry& referenceValue) {
              stream << referenceValue.toStringRepresentation();
            }},
        referenceValue);
  };

  std::stringstream stream;
  stream << "Prefilter RelationalExpression<" << getRelationalOpStr(Comparison)
         << ">\nreferenceValue_ : ";
  referenceValueToString(stream, rightSideReferenceValue_);
  stream << " ." << std::endl;
  return stream.str();
};

// SECTION ISDATATYPE
//______________________________________________________________________________
// Remark: The current `logicalComplement` implementation retrieves the full
// complement w.r.t. the datatypes defined and represented by the `ValueId`
// space.
template <IsDatatype Datatype>
std::unique_ptr<PrefilterExpression>
IsDatatypeExpression<Datatype>::logicalComplement() const {
  return make<IsDatatypeExpression<Datatype>>(!isNegated_);
};

//______________________________________________________________________________
template <IsDatatype Datatype>
std::unique_ptr<PrefilterExpression> IsDatatypeExpression<Datatype>::clone()
    const {
  return make<IsDatatypeExpression<Datatype>>(*this);
};

//______________________________________________________________________________
template <IsDatatype Datatype>
bool IsDatatypeExpression<Datatype>::operator==(
    const PrefilterExpression& other) const {
  const auto* otherIsDatatype =
      dynamic_cast<const IsDatatypeExpression<Datatype>*>(&other);
  if (!otherIsDatatype) {
    return false;
  }
  return isNegated_ == otherIsDatatype->isNegated_;
};

//______________________________________________________________________________
template <IsDatatype Datatype>
std::string IsDatatypeExpression<Datatype>::asString(
    [[maybe_unused]] size_t depth) const {
  return absl::StrCat(
      "Prefilter IsDatatypeExpression:", "\nPrefilter for datatype: ",
      getDatatypeIsTypeStr(Datatype),
      "\nis negated: ", isNegated_ ? "true" : "false", ".\n");
};

//______________________________________________________________________________
template <IsDatatype Datatype>
RelevantBlockRanges
IsDatatypeExpression<Datatype>::evaluateIsIriOrIsLiteralImpl(
    ValueIdFromBlocksIt idsBegin, ValueIdFromBlocksIt idsEnd,
    const bool isNegated) const {
  using enum IsDatatype;
  using LVE = LocalVocabEntry;

  return Datatype == IRI
             // Remark: Ids containing LITERAL values precede IRI related Ids
             // in order. The smallest possible IRI is represented by "<>", we
             // use its corresponding ValueId later on as a lower bound.
             ? GreaterThanExpression(LVE::fromStringRepresentation("<>"))
                   .evaluateOptGetCompleteComplementImpl(idsBegin, idsEnd,
                                                         isNegated)
             // For pre-filtering LITERAL related ValueIds we use the ValueId
             // representing the beginning of IRI values as an upper bound.
             : LessThanExpression(LVE::fromStringRepresentation("<"))
                   .evaluateOptGetCompleteComplementImpl(idsBegin, idsEnd,
                                                         isNegated);
}

//______________________________________________________________________________
template <IsDatatype Datatype>
RelevantBlockRanges
IsDatatypeExpression<Datatype>::evaluateIsBlankOrIsNumericImpl(
    ValueIdFromBlocksIt idsBegin, ValueIdFromBlocksIt idsEnd,
    const bool isNegated) const {
  using enum IsDatatype;

  std::vector<RandomValueIdItPair> relevantRanges;
  if constexpr (Datatype == BLANK) {
    relevantRanges = {valueIdComparators::getRangeForDatatype(
        idsBegin, idsEnd, Datatype::BlankNodeIndex)};
  } else {
    // Remark: Swapping the defined relational order for Datatype::Int
    // to Datatype::Double ValueIds (in ValueId.h) might affect the
    // correctness of the following lines!
    static_assert(Datatype::Int < Datatype::Double);

    const auto& rangeInt = valueIdComparators::getRangeForDatatype(
        idsBegin, idsEnd, Datatype::Int);
    const auto& rangeDouble = valueIdComparators::getRangeForDatatype(
        idsBegin, idsEnd, Datatype::Double);
    // Differentiate here regarding the adjacency of Int and Double
    // datatype values. This is relevant w.r.t.
    // getRelevantBlocksFromIdRanges(), because we technically add to
    // .second + 1 for the first datatype range. If we simply add
    // rangeInt and rangeDouble given that datatype Int and Double are
    // stored directly next to each other, we potentially add a block
    // twice (forbidden). Remark: This is necessary even if
    // simplifyRanges(relevantRanges) is performed later on.
    if (rangeInt.second == rangeDouble.first ||
        rangeDouble.second == rangeInt.first) {
      relevantRanges.emplace_back(
          std::min(rangeInt.first, rangeDouble.first),
          std::max(rangeInt.second, rangeDouble.second));
    } else {
      relevantRanges.emplace_back(rangeInt);
      relevantRanges.emplace_back(rangeDouble);
    }
  }
  // Sort and remove overlapping ranges.
  valueIdComparators::detail::simplifyRanges(relevantRanges);
  return isNegated
             ? detail::mapping::mapRandomItRangesToBlockRangesComplemented(
                   relevantRanges, idsBegin, idsEnd)
             : detail::mapping::mapRandomItRangesToBlockRanges(
                   relevantRanges, idsBegin, idsEnd);
}

//______________________________________________________________________________
template <IsDatatype Datatype>
RelevantBlockRanges IsDatatypeExpression<Datatype>::evaluateImpl(
    ValueIdFromBlocksIt idsBegin, ValueIdFromBlocksIt idsEnd) const {
  using enum IsDatatype;
  if constexpr (Datatype == BLANK || Datatype == NUMERIC) {
    return evaluateIsBlankOrIsNumericImpl(idsBegin, idsEnd, isNegated_);
  } else {
    static_assert(Datatype == IRI || Datatype == LITERAL);
    return evaluateIsIriOrIsLiteralImpl(idsBegin, idsEnd, isNegated_);
  }
};

// SECTION LOGICAL OPERATIONS
//______________________________________________________________________________
template <LogicalOperator Operation>
std::unique_ptr<PrefilterExpression>
LogicalExpression<Operation>::logicalComplement() const {
  using enum LogicalOperator;
  // Source De-Morgan's laws: De Morgan's laws, Wikipedia.
  // Reference: https://en.wikipedia.org/wiki/De_Morgan%27s_laws
  if constexpr (Operation == OR) {
    // De Morgan's law: not (A or B) = (not A) and (not B)
    return make<AndExpression>(child1_->logicalComplement(),
                               child2_->logicalComplement());
  } else {
    static_assert(Operation == AND);
    // De Morgan's law: not (A and B) = (not A) or (not B)
    return make<OrExpression>(child1_->logicalComplement(),
                              child2_->logicalComplement());
  }
};

//______________________________________________________________________________
template <LogicalOperator Operation>
RelevantBlockRanges LogicalExpression<Operation>::evaluateImpl(
    ValueIdFromBlocksIt idsBegin, ValueIdFromBlocksIt idsEnd) const {
  using enum LogicalOperator;
  if constexpr (Operation == AND) {
    return detail::logicalOps::getIntersection(
        child1_->evaluateImpl(idsBegin, idsEnd),
        child2_->evaluateImpl(idsBegin, idsEnd));
  } else {
    static_assert(Operation == OR);
    return detail::logicalOps::getUnion(
        child1_->evaluateImpl(idsBegin, idsEnd),
        child2_->evaluateImpl(idsBegin, idsEnd));
  }
};

//______________________________________________________________________________
template <LogicalOperator Operation>
bool LogicalExpression<Operation>::operator==(
    const PrefilterExpression& other) const {
  const auto* otherlogical =
      dynamic_cast<const LogicalExpression<Operation>*>(&other);
  if (!otherlogical) {
    return false;
  }
  return *child1_ == *otherlogical->child1_ &&
         *child2_ == *otherlogical->child2_;
};

//______________________________________________________________________________
template <LogicalOperator Operation>
std::unique_ptr<PrefilterExpression> LogicalExpression<Operation>::clone()
    const {
  return make<LogicalExpression<Operation>>(child1_->clone(), child2_->clone());
};

//______________________________________________________________________________
template <LogicalOperator Operation>
std::string LogicalExpression<Operation>::asString(size_t depth) const {
  std::string child1Info =
      depth < maxInfoRecursion ? child1_->asString(depth + 1) : "MAX_DEPTH";
  std::string child2Info =
      depth < maxInfoRecursion ? child2_->asString(depth + 1) : "MAX_DEPTH";
  std::stringstream stream;
  stream << "Prefilter LogicalExpression<" << getLogicalOpStr(Operation)
         << ">\n"
         << "child1 {" << child1Info << "}"
         << "child2 {" << child2Info << "}" << std::endl;
  return stream.str();
};

// SECTION NOT-EXPRESSION
//______________________________________________________________________________
std::unique_ptr<PrefilterExpression> NotExpression::logicalComplement() const {
  // Logically we complement (negate) a NOT here => NOT cancels out.
  // Therefore, we can simply return the child of the respective NOT
  // expression after undoing its previous complementation.
  return child_->logicalComplement();
};

//______________________________________________________________________________
RelevantBlockRanges NotExpression::evaluateImpl(
    ValueIdFromBlocksIt idsBegin, ValueIdFromBlocksIt idsEnd) const {
  return child_->evaluateImpl(idsBegin, idsEnd);
};

//______________________________________________________________________________
bool NotExpression::operator==(const PrefilterExpression& other) const {
  const auto* otherNotExpression = dynamic_cast<const NotExpression*>(&other);
  if (!otherNotExpression) {
    return false;
  }
  return *child_ == *otherNotExpression->child_;
}

//______________________________________________________________________________
std::unique_ptr<PrefilterExpression> NotExpression::clone() const {
  return make<NotExpression>((child_->clone()), true);
};

//______________________________________________________________________________
std::string NotExpression::asString(size_t depth) const {
  std::string childInfo =
      depth < maxInfoRecursion ? child_->asString(depth + 1) : "MAX_DEPTH";
  std::stringstream stream;
  stream << "Prefilter NotExpression:\n"
         << "child {" << childInfo << "}" << std::endl;
  return stream.str();
}

//______________________________________________________________________________
// Instanitate templates with specializations (for linking accessibility)
template class RelationalExpression<CompOp::LT>;
template class RelationalExpression<CompOp::LE>;
template class RelationalExpression<CompOp::GE>;
template class RelationalExpression<CompOp::GT>;
template class RelationalExpression<CompOp::EQ>;
template class RelationalExpression<CompOp::NE>;

template class IsDatatypeExpression<IsDatatype::IRI>;
template class IsDatatypeExpression<IsDatatype::BLANK>;
template class IsDatatypeExpression<IsDatatype::LITERAL>;
template class IsDatatypeExpression<IsDatatype::NUMERIC>;

template class LogicalExpression<LogicalOperator::AND>;
template class LogicalExpression<LogicalOperator::OR>;

namespace detail {
//______________________________________________________________________________
// Returns the corresponding mirrored `RelationalExpression<mirrored
// comparison>` for the given `CompOp comparison` template argument. For
// example, the mirroring procedure will transform the relational expression
// `referenceValue > ?var` into `?var < referenceValue`.
template <CompOp comparison>
static std::unique_ptr<PrefilterExpression> makeMirroredExpression(
    const IdOrLocalVocabEntry& referenceValue) {
  using enum CompOp;
  using namespace ad_utility;
  using P = std::pair<CompOp, CompOp>;
  constexpr ConstexprMap<CompOp, CompOp, 6> mirrorMap(
      {P{LT, GT}, P{LE, GE}, P{GE, LE}, P{GT, LT}, P{EQ, EQ}, P{NE, NE}});
  return make<RelationalExpression<mirrorMap.at(comparison)>>(referenceValue);
}

//______________________________________________________________________________
void checkPropertiesForPrefilterConstruction(
    const std::vector<PrefilterExprVariablePair>& vec) {
  auto viewVariable = vec | ql::views::values;
  if (!ql::ranges::is_sorted(viewVariable, std::less<>{})) {
    throw std::runtime_error(
        "The vector must contain the <PrefilterExpression, "
        "Variable> pairs in "
        "sorted order w.r.t. Variable value.");
  }
  if (auto it = ql::ranges::adjacent_find(viewVariable);
      it != ql::ranges::end(viewVariable)) {
    throw std::runtime_error(
        "For each relevant Variable must exist exactly one "
        "<PrefilterExpression, Variable> pair.");
  }
}

//______________________________________________________________________________
template <CompOp comparison>
std::vector<PrefilterExprVariablePair> makePrefilterExpressionVec(
    const IdOrLocalVocabEntry& referenceValue, const Variable& variable,
    const bool mirrored) {
  std::vector<PrefilterExprVariablePair> resVec{};
  resVec.emplace_back(
      mirrored ? makeMirroredExpression<comparison>(referenceValue)
               : make<RelationalExpression<comparison>>(referenceValue),
      variable);
  return resVec;
}

//______________________________________________________________________________
#define INSTANTIATE_MAKE_PREFILTER(Comparison)                       \
  template std::vector<PrefilterExprVariablePair>                    \
  makePrefilterExpressionVec<Comparison>(const IdOrLocalVocabEntry&, \
                                         const Variable&, const bool);
INSTANTIATE_MAKE_PREFILTER(CompOp::LT);
INSTANTIATE_MAKE_PREFILTER(CompOp::LE);
INSTANTIATE_MAKE_PREFILTER(CompOp::GE);
INSTANTIATE_MAKE_PREFILTER(CompOp::GT);
INSTANTIATE_MAKE_PREFILTER(CompOp::EQ);
INSTANTIATE_MAKE_PREFILTER(CompOp::NE);

}  //  namespace detail
}  //  namespace prefilterExpressions
