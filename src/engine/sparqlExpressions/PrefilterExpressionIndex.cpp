//  Copyright 2024 - 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/sparqlExpressions/PrefilterExpressionIndex.h"

#include <absl/functional/bind_front.h>

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
// (3) Columns with `column index < evaluationColumn` must contain equal
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
    const BlockSubranges& relevantBlockRanges) {
  std::vector<BlockMetadata> relevantBlocks;
  relevantBlocks.reserve(std::transform_reduce(relevantBlockRanges.begin(),
                                               relevantBlockRanges.end(), 0ULL,
                                               std::plus{}, ql::ranges::size));
  ql::ranges::copy(relevantBlockRanges | ql::views::join,
                   std::back_inserter(relevantBlocks));
  return relevantBlocks;
}

namespace detail {

//______________________________________________________________________________
// Merge `BlockSubrange blockRange` with the previous (relevant)
// `BlockSubrange`s.
static void mergeBlockRangeWithRanges(BlockSubranges& blockRanges,
                                      const BlockSubrange& blockRange) {
  if (blockRange.empty()) {
    return;
  }
  if (blockRanges.empty()) {
    blockRanges.push_back(blockRange);
    return;
  }
  auto blockRangeBegin = blockRange.begin();
  BlockSubrange& lastBlockRange = blockRanges.back();
  AD_CORRECTNESS_CHECK(blockRangeBegin >= lastBlockRange.begin());
  if (lastBlockRange.end() >= blockRangeBegin) {
    // The blockRange intersects with previous blockRange(s).
    lastBlockRange =
        BlockSubrange{lastBlockRange.begin(),
                      std::max(lastBlockRange.end(), blockRange.end())};
  } else {
    // The new blockRange doesn't intersect with previous blockRange(s).
    blockRanges.push_back(blockRange);
  }
};

namespace mapping {
//______________________________________________________________________________
// Map the given `ValueIdIt beginIdIt` and `ValueIdIt endIdIt` to their
// corresponding `BlockIdIt` values and return them as `BlockSubrange`.
static BlockSubrange mapValueIdItPairToBlockRange(const ValueIdIt& idRangeBegin,
                                                  const ValueIdIt& beginIdIt,
                                                  const ValueIdIt& endIdIt,
                                                  BlockSpan blockRange) {
  AD_CORRECTNESS_CHECK(beginIdIt <= endIdIt);
  auto blockRangeBegin = blockRange.begin();
  // Each `BlockMetadata` value contains two bounding `ValueId`s, one for
  // `firstTriple_` and `lastTriple_` respectively.
  // `ValueIdIt idRangeBegin` is the first valid iterator on our flattened
  // `std::span<const BlockMetadata> blockRange` with respect to the contained
  // `ValueId`s.
  // `blockRange.begin()` represents the first valid iterator on our original
  // `std::span<const BlockMetadata> blockRange`.
  //
  //  EXAMPLE
  // `BlockMetadata` view on `std::span<const BlockMetadata> blockRange`:
  // `{[1021, 1082], [1083, 1115], [1121, 1140], [1140, 1148], [1150,
  // 1158]}`. Range for `BlockIt` values.
  //
  // `ValueId` view on (flat) `std::span<const BlockMetadata> blockRange`:
  //`{1021, 1082, 1083, 1115, 1121, 1140, 1140, 1148, 1150, 1158}`.
  // Range for `ValueIdIt` values.
  //
  // Thus, we have twice as many valid `ValueIdIt`s (and indices) than
  // `BlockIdIt`s. This is why we have to divide by two in the following after
  // we calculated the respective `ValueId` index with
  // std::distance(idRangeBegin, idIt).
  auto blockOffsetBegin = std::distance(idRangeBegin, beginIdIt) / 2;
  auto blockOffsetEnd = (std::distance(idRangeBegin, endIdIt) + 1) / 2;
  AD_CORRECTNESS_CHECK(static_cast<size_t>(blockOffsetEnd) <=
                       blockRange.size());
  return {blockRangeBegin + blockOffsetBegin, blockRangeBegin + blockOffsetEnd};
}

//______________________________________________________________________________
// Map the complement on the given `ValueIdItPairs`s to their corresponding
// `BlockSubrange`s.
// The actual mapping is implemented by `mapValueIdItPairToBlockRange`.
BlockSubranges mapValueIdItRangesToBlockItRangesComplemented(
    const std::vector<ValueIdItPair>& relevantIdRanges,
    const ValueIdSubrange& idRange, BlockSpan blockRange) {
  auto idRangeBegin = idRange.begin();
  // Vector containing the `BlockRange` mapped indices.
  BlockSubranges blockRanges;
  blockRanges.reserve(relevantIdRanges.size());
  auto addRange =
      absl::bind_front(mergeBlockRangeWithRanges, std::ref(blockRanges));
  auto previousEndIt = idRange.begin();
  ql::ranges::for_each(
      relevantIdRanges, [&addRange, &idRangeBegin, &blockRange,
                         &previousEndIt](const ValueIdItPair& idItPair) {
        const auto& [beginIdIt, endIdIt] = idItPair;
        addRange(mapValueIdItPairToBlockRange(idRangeBegin, previousEndIt,
                                              beginIdIt, blockRange));
        previousEndIt = endIdIt;
      });
  addRange(mapValueIdItPairToBlockRange(idRangeBegin, previousEndIt,
                                        idRange.end(), blockRange));
  return blockRanges;
}

//______________________________________________________________________________
// Map the given `ValueIdItPair`s to their corresponding `BlockSubrange`s.
// The actual mapping is implemented by `mapValueIdItPairToBlockRange`.
BlockSubranges mapValueIdItRangesToBlockItRanges(
    const std::vector<ValueIdItPair>& relevantIdRanges,
    const ValueIdSubrange& idRange, BlockSpan blockRange) {
  if (relevantIdRanges.empty()) {
    return {};
  }
  auto idRangeBegin = idRange.begin();
  // Vector containing the `BlockRange` mapped iterators.
  BlockSubranges blockRanges;
  blockRanges.reserve(relevantIdRanges.size());
  auto addRange =
      absl::bind_front(mergeBlockRangeWithRanges, std::ref(blockRanges));
  ql::ranges::for_each(
      relevantIdRanges,
      [&addRange, &idRangeBegin, &blockRange](const ValueIdItPair& idItPair) {
        const auto& [beginIdIt, endIdIt] = idItPair;
        addRange(mapValueIdItPairToBlockRange(idRangeBegin, beginIdIt, endIdIt,
                                              blockRange));
      });
  return blockRanges;
}

}  // namespace mapping

// SECTION HELPER LOGICAL OPERATORS
namespace logicalOps {

//______________________________________________________________________________
// (1) `mergeRelevantBlockItRanges<true>` returns the `union` (`logical-or
// (||)`) of `BlockSubranges r1` and `BlockSubranges r2`.
// (2) `mergeRelevantBlockItRanges<false>` returns the `intersection`
// (`logical-and
// (&&)`) of `BlockSubranges r1` and `BlockSubranges r2`.
//
// EXAMPLE UNION
// Given input `r1` and `r2`
// `BlockSubranges r1`: `[<2, 10>, <15, 16>, <20, 23>]`
// `BlockSubranges r2`: `[<4, 6>, <8, 9>, <15, 22>]`
// The result is `RelevantBlocksRanges result`: `[<2, 10>, <15, 23>]`
//
// EXAMPLE INTERSECTION
// Given input `r1` and `r2`
// `BlockSubranges r1`: `[<2, 10>, <15, 16>, <20, 23>]`
// `BlockSubranges r2`: `[<4, 6>, <8, 9>, <15, 22>]`
// The result is
// `RelevantBlocksRanges result`: `[<4, 6>, <8, 9>, <15, 16>, <20, 22>]`
template <bool GetUnion>
BlockSubranges mergeRelevantBlockItRanges(const BlockSubranges& r1,
                                          const BlockSubranges& r2) {
  if constexpr (GetUnion) {
    if (r1.empty() && r2.empty()) return {};
  } else {
    if (r1.empty() || r2.empty()) return {};
  }
  BlockSubranges mergedRanges;
  mergedRanges.reserve(r1.size() + r2.size());
  auto addRange =
      absl::bind_front(mergeBlockRangeWithRanges, std::ref(mergedRanges));
  auto idx1 = r1.begin();
  auto idx2 = r2.begin();
  while (idx1 != r1.end() && idx2 != r2.end()) {
    auto& [idx1Begin, idx1End] = *idx1;
    auto& [idx2Begin, idx2End] = *idx2;
    if (idx1End < idx2Begin) {
      if constexpr (GetUnion) {
        addRange(*idx1);
      }
      idx1++;
    } else if (idx2End < idx1Begin) {
      if constexpr (GetUnion) {
        addRange(*idx2);
      }
      idx2++;
    } else {
      // Handle overlapping ranges.
      if constexpr (GetUnion) {
        // Add the union of r1 and r2.
        addRange(BlockSubrange{std::min(idx1Begin, idx2Begin),
                               std::max(idx1End, idx2End)});
        idx1++;
        idx2++;
      } else {
        // Add the intersection of r1 and r2.
        addRange(BlockSubrange{std::max(idx1Begin, idx2Begin),
                               std::min(idx1End, idx2End)});
        idx1End < idx2End ? idx1++ : idx2++;
      }
    }
  }
  if constexpr (!GetUnion) {
    return mergedRanges;
  }
  ql::ranges::for_each(idx1, r1.end(), addRange);
  ql::ranges::for_each(idx2, r2.end(), addRange);
  return mergedRanges;
}

}  // namespace logicalOps
}  // namespace detail

//______________________________________________________________________________
// This function retrieves the `BlockRange`s for `BlockMetadata` values that
// contain bounding `ValueId`s with different underlying datatypes.
static BlockSubranges getRangesMixedDatatypeBlocks(
    const ValueIdSubrange& idRange, BlockSpan blockRange) {
  if (idRange.empty()) {
    return {};
  }
  std::vector<ValueIdItPair> mixedDatatypeRanges;
  // Ensure that `idRange` holds access to even number of ValueIds.
  AD_CORRECTNESS_CHECK(idRange.size() % 2 == 0);
  const auto checkDifferentDtype = [](const auto& idPair) {
    auto firstId = idPair.begin();
    return (*firstId).getDatatype() != (*std::next(firstId)).getDatatype();
  };
  ql::ranges::for_each(idRange | ranges::views::chunk(2) |
                           ranges::views::filter(checkDifferentDtype),
                       [&mixedDatatypeRanges](const auto& idPair) {
                         auto firstId = idPair.begin();
                         mixedDatatypeRanges.emplace_back(firstId,
                                                          std::next(firstId));
                       });
  return detail::mapping::mapValueIdItRangesToBlockItRanges(
      mixedDatatypeRanges, idRange, blockRange);
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
      return absl::StrCat("Undefined CompOp value: ", static_cast<int>(relOp),
                          ".");
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
    BlockSpan randomAccessContainer, uint64_t i) const {
  const BlockMetadata& block = randomAccessContainer[i / 2];
  return i % 2 == 0
             ? getIdFromColumnIndex(block.firstTriple_, evaluationColumn_)
             : getIdFromColumnIndex(block.lastTriple_, evaluationColumn_);
};

// SECTION PREFILTER EXPRESSION (BASE CLASS)
//______________________________________________________________________________
std::vector<BlockMetadata> PrefilterExpression::evaluate(
    BlockSpan blockRange, size_t evaluationColumn) const {
  if (blockRange.size() < 3) {
    return std::vector<BlockMetadata>(blockRange.begin(), blockRange.end());
  }

  std::optional<BlockMetadata> firstBlock = std::nullopt;
  std::optional<BlockMetadata> lastBlock = std::nullopt;
  if (checkBlockIsInconsistent(blockRange.front(), evaluationColumn)) {
    firstBlock = blockRange.front();
    blockRange = blockRange.subspan(1);
  }
  if (checkBlockIsInconsistent(blockRange.back(), evaluationColumn)) {
    lastBlock = blockRange.back();
    blockRange = blockRange.subspan(0, blockRange.size() - 1);
  }

  std::vector<BlockMetadata> result;
  if (!blockRange.empty()) {
    checkRequirementsBlockMetadata(blockRange, evaluationColumn);
    AccessValueIdFromBlockMetadata accessValueIdOp(evaluationColumn);
    ValueIdSubrange idRange{
        ValueIdIt{&blockRange, 0, accessValueIdOp},
        ValueIdIt{&blockRange, blockRange.size() * 2, accessValueIdOp}};
    result =
        getRelevantBlocks(detail::logicalOps::mergeRelevantBlockItRanges<true>(
            evaluateImpl(idRange, blockRange),
            // always add mixed datatype blocks
            getRangesMixedDatatypeBlocks(idRange, blockRange)));
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
BlockSubranges
RelationalExpression<Comparison>::evaluateOptGetCompleteComplementImpl(
    const ValueIdSubrange& idRange, BlockSpan blockRange,
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
  auto relevantIdRanges = Comparison != CompOp::EQ
                              ? getRangesForId(idRange.begin(), idRange.end(),
                                               referenceId, Comparison)
                              : getRangesForId(idRange.begin(), idRange.end(),
                                               referenceId, Comparison, false);
  valueIdComparators::detail::simplifyRanges(relevantIdRanges);
  return getComplementOverAllDatatypes
             ? detail::mapping::mapValueIdItRangesToBlockItRangesComplemented(
                   relevantIdRanges, idRange, blockRange)
             : detail::mapping::mapValueIdItRangesToBlockItRanges(
                   relevantIdRanges, idRange, blockRange);
};

//______________________________________________________________________________
template <CompOp Comparison>
BlockSubranges RelationalExpression<Comparison>::evaluateImpl(
    const ValueIdSubrange& idRange, BlockSpan blockRange) const {
  return evaluateOptGetCompleteComplementImpl(idRange, blockRange, false);
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
static BlockSubranges evaluateIsIriOrIsLiteralImpl(
    const ValueIdSubrange& idRange, BlockSpan blockRange,
    const bool isNegated) {
  using enum IsDatatype;
  static_assert(Datatype == IRI || Datatype == LITERAL);
  using LVE = LocalVocabEntry;

  return Datatype == IRI
             // Remark: Ids containing LITERAL values precede IRI related Ids
             // in order. The smallest possible IRI is represented by "<>", we
             // use its corresponding ValueId later on as a lower bound.
             ? GreaterThanExpression(LVE::fromStringRepresentation("<>"))
                   .evaluateOptGetCompleteComplementImpl(idRange, blockRange,
                                                         isNegated)
             // For pre-filtering LITERAL related ValueIds we use the ValueId
             // representing the beginning of IRI values as an upper bound.
             : LessThanExpression(LVE::fromStringRepresentation("<"))
                   .evaluateOptGetCompleteComplementImpl(idRange, blockRange,
                                                         isNegated);
}

//______________________________________________________________________________
template <IsDatatype Datatype>
static BlockSubranges evaluateIsBlankOrIsNumericImpl(
    const ValueIdSubrange& idRange, BlockSpan blockRange,
    const bool isNegated) {
  using enum IsDatatype;
  static_assert(Datatype == BLANK || Datatype == NUMERIC);

  std::vector<ValueIdItPair> relevantRanges;
  if constexpr (Datatype == BLANK) {
    relevantRanges = {valueIdComparators::getRangeForDatatype(
        idRange.begin(), idRange.end(), Datatype::BlankNodeIndex)};
  } else {
    // Remark: Swapping the defined relational order for Datatype::Int
    // to Datatype::Double ValueIds (in ValueId.h) might affect the
    // correctness of the following lines!
    static_assert(Datatype::Int < Datatype::Double);

    const auto& rangeInt = valueIdComparators::getRangeForDatatype(
        idRange.begin(), idRange.end(), Datatype::Int);
    const auto& rangeDouble = valueIdComparators::getRangeForDatatype(
        idRange.begin(), idRange.end(), Datatype::Double);
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
             ? detail::mapping::mapValueIdItRangesToBlockItRangesComplemented(
                   relevantRanges, idRange, blockRange)
             : detail::mapping::mapValueIdItRangesToBlockItRanges(
                   relevantRanges, idRange, blockRange);
}

//______________________________________________________________________________
template <IsDatatype Datatype>
BlockSubranges IsDatatypeExpression<Datatype>::evaluateImpl(
    const ValueIdSubrange& idRange, BlockSpan blockRange) const {
  using enum IsDatatype;
  if constexpr (Datatype == BLANK || Datatype == NUMERIC) {
    return evaluateIsBlankOrIsNumericImpl<Datatype>(idRange, blockRange,
                                                    isNegated_);
  } else {
    static_assert(Datatype == IRI || Datatype == LITERAL);
    return evaluateIsIriOrIsLiteralImpl<Datatype>(idRange, blockRange,
                                                  isNegated_);
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
BlockSubranges LogicalExpression<Operation>::evaluateImpl(
    const ValueIdSubrange& idRange, BlockSpan blockRange) const {
  using enum LogicalOperator;
  if constexpr (Operation == AND) {
    return detail::logicalOps::mergeRelevantBlockItRanges<false>(
        child1_->evaluateImpl(idRange, blockRange),
        child2_->evaluateImpl(idRange, blockRange));
  } else {
    static_assert(Operation == OR);
    return detail::logicalOps::mergeRelevantBlockItRanges<true>(
        child1_->evaluateImpl(idRange, blockRange),
        child2_->evaluateImpl(idRange, blockRange));
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
BlockSubranges NotExpression::evaluateImpl(const ValueIdSubrange& idRange,
                                           BlockSpan blockRange) const {
  return child_->evaluateImpl(idRange, blockRange);
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
std::unique_ptr<PrefilterExpression> makePrefilterExpressionYearImpl(
    CompOp comparison, const int year) {
  using GeExpr = GreaterEqualExpression;
  using LtExpr = LessThanExpression;

  // `getDateId` returns an `Id` containing the smallest possible
  // `Date`
  // (`xsd::date`) for which `YEAR(Id) == adjustedYear` is valid.
  // This `Id` acts as a reference bound for the actual
  // `DateYearOrDuration` prefiltering procedure.
  const auto getDateId = [](const int adjustedYear) {
    return Id::makeFromDate(DateYearOrDuration(Date(adjustedYear, 0, 0)));
  };
  using enum CompOp;
  switch (comparison) {
    case EQ:
      return make<AndExpression>(make<LtExpr>(getDateId(year + 1)),
                                 make<GeExpr>(getDateId(year)));
    case LT:
      return make<LtExpr>(getDateId(year));
    case LE:
      return make<LtExpr>(getDateId(year + 1));
    case GE:
      return make<GeExpr>(getDateId(year));
    case GT:
      return make<GeExpr>(getDateId(year + 1));
    case NE:
      return make<OrExpression>(make<LtExpr>(getDateId(year)),
                                make<GeExpr>(getDateId(year + 1)));
    default:
      throw std::runtime_error(
          absl::StrCat("Set unknown (relational) comparison operator for "
                       "the creation of PrefilterExpression on date-values: ",
                       getRelationalOpStr(comparison)));
  }
};

//______________________________________________________________________________
template <CompOp comparison>
static std::unique_ptr<PrefilterExpression> makePrefilterExpressionVecImpl(
    const IdOrLocalVocabEntry& referenceValue, bool prefilterDateByYear) {
  using enum Datatype;
  // Standard pre-filtering procedure.
  if (!prefilterDateByYear) {
    return make<RelationalExpression<comparison>>(referenceValue);
  }
  // Helper to safely retrieve `ValueId/Id` values from the provided
  // `IdOrLocalVocabEntry referenceValue` if contained. Given no
  // `ValueId` is contained, a explanatory message per
  // `std::runtime_error` is thrown.
  const auto retrieveValueIdOrThrowErr =
      [](const IdOrLocalVocabEntry& referenceValue) {
        return std::visit(
            [](const auto& value) -> ValueId {
              using T = std::decay_t<decltype(value)>;
              if constexpr (ad_utility::isSimilar<T, ValueId>) {
                return value;
              } else {
                static_assert(ad_utility::isSimilar<T, LocalVocabEntry>);
                throw std::runtime_error(absl::StrCat(
                    "Provided Literal or Iri with value: ",
                    value.asLiteralOrIri().toStringRepresentation(),
                    ". This is an invalid reference value for filtering date "
                    "values over expression YEAR. Please provide an integer "
                    "value as reference year."));
              }
            },
            referenceValue);
      };
  // Handle year extraction and return a date-value adjusted
  // `PrefilterExpression` if possible. Given an unsuitable reference
  // value was provided, throw a std::runtime_error with an
  // explanatory message.
  const auto retrieveYearIntOrThrowErr =
      [&retrieveValueIdOrThrowErr](const IdOrLocalVocabEntry& referenceValue) {
        const ValueId& valueId = retrieveValueIdOrThrowErr(referenceValue);
        if (valueId.getDatatype() == Int) {
          return valueId.getInt();
        }
        throw std::runtime_error(absl::StrCat(
            "Reference value for filtering date values over "
            "expression YEAR is of invalid datatype: ",
            toString(valueId.getDatatype()),
            ".\nPlease provide an integer value as reference year."));
      };
  return makePrefilterExpressionYearImpl(
      comparison, retrieveYearIntOrThrowErr(referenceValue));
};

//______________________________________________________________________________
template <CompOp comparison>
std::vector<PrefilterExprVariablePair> makePrefilterExpressionVec(
    const IdOrLocalVocabEntry& referenceValue, const Variable& variable,
    bool mirrored, bool prefilterDateByYear) {
  using enum CompOp;
  std::vector<PrefilterExprVariablePair> resVec{};
  if (mirrored) {
    using P = std::pair<CompOp, CompOp>;
    // Retrieve by map the corresponding mirrored `CompOp` value for
    // the given `CompOp comparison` template argument. E.g., this
    // procedure will transform the relational expression
    // `referenceValue > ?var` into `?var < referenceValue`.
    constexpr ad_utility::ConstexprMap<CompOp, CompOp, 6> mirrorMap(
        {P{LT, GT}, P{LE, GE}, P{GE, LE}, P{GT, LT}, P{EQ, EQ}, P{NE, NE}});
    resVec.emplace_back(
        makePrefilterExpressionVecImpl<mirrorMap.at(comparison)>(
            referenceValue, prefilterDateByYear),
        variable);
  } else {
    resVec.emplace_back(makePrefilterExpressionVecImpl<comparison>(
                            referenceValue, prefilterDateByYear),
                        variable);
  }
  return resVec;
}

//______________________________________________________________________________
#define INSTANTIATE_MAKE_PREFILTER(Comparison)                       \
  template std::vector<PrefilterExprVariablePair>                    \
  makePrefilterExpressionVec<Comparison>(const IdOrLocalVocabEntry&, \
                                         const Variable&, bool, bool);
INSTANTIATE_MAKE_PREFILTER(CompOp::LT);
INSTANTIATE_MAKE_PREFILTER(CompOp::LE);
INSTANTIATE_MAKE_PREFILTER(CompOp::GE);
INSTANTIATE_MAKE_PREFILTER(CompOp::GT);
INSTANTIATE_MAKE_PREFILTER(CompOp::EQ);
INSTANTIATE_MAKE_PREFILTER(CompOp::NE);

}  //  namespace detail
}  //  namespace prefilterExpressions
