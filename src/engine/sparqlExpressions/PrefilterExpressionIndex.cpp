//  Copyright 2024, University of Freiburg,
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
// Helper for `getSetDifference` and `getSetUnion`.
// Returns the evaluation result of `b1.blockIndex_ < b2.blockIndex_`.
static bool blockLessThanBlock(const BlockMetadata& b1,
                               const BlockMetadata& b2) {
  return b1.blockIndex_ < b2.blockIndex_;
};

//______________________________________________________________________________
// Check for constant values in all columns `< evaluationColumn`
static bool checkBlockIsInconsistent(const BlockMetadata& block,
                                     size_t evaluationColumn) {
  return getMaskedTriple(block.firstTriple_, evaluationColumn) !=
         getMaskedTriple(block.lastTriple_, evaluationColumn);
}

//______________________________________________________________________________
// Check required conditions.
static void checkEvalRequirements(std::span<const BlockMetadata> input,
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
// Given two sorted `vector`s containing `BlockMetadata`, this function
// returns their merged `BlockMetadata` content in a `vector` which is free of
// duplicates and ordered.
static auto getSetUnion(const std::vector<BlockMetadata>& blocks1,
                        const std::vector<BlockMetadata>& blocks2) {
  std::vector<BlockMetadata> mergedVectors;
  mergedVectors.reserve(blocks1.size() + blocks2.size());
  // Given that we have vectors with sorted (BlockMedata) values, we can
  // use ql::ranges::set_union. Thus the complexity is O(n + m).
  ql::ranges::set_union(blocks1, blocks2, std::back_inserter(mergedVectors),
                        blockLessThanBlock);
  mergedVectors.shrink_to_fit();
  return mergedVectors;
}

//______________________________________________________________________________
// Refers to an `ValueId` vector `::iterator` (index).
using RandomIt = std::vector<ValueId>::iterator;
// A pair containing the `start` and `end` (iterator) indices for a specified
// single datatype range over a given `ValueId` vector.
// For the given cases below, the `ValueId` vector is retrieved from the
// `BlockMetadata` values. And those `ValueId` values define the bounds of the
// related `BlockMetadata` values.
using RandomItPair = std::pair<RandomIt, RandomIt>;
// Refers to a `BlockMetadata` span `::iterator` (index).
using BlockSpanIt =
    std::span<const prefilterExpressions::BlockMetadata>::iterator;
// A pair containing the `start` and `end` iterator (index) with respect to our
// `std::span<const BlockMetadata> input` value. This pair of iterators helps us
// to specify (relevant) `BlockMetadata` value-ranges.
using BlockSpanItPair = std::pair<BlockSpanIt, BlockSpanIt>;

//______________________________________________________________________________
// Helper for adding the relevant ranges to `mappedBlockRangeIts`.
// `addRangeAndSimplify` ensures that the ranges added are non-overlapping and
// merged together if adjacent.
const auto addRangeAndSimplify =
    [](std::vector<BlockSpanItPair>& mappedBlockRangeIts,
       const BlockSpanIt& relevantRangeBegin,
       const BlockSpanIt& relevantRangeEnd) {
      // Simplify the mapped ranges if adjacent or overlapping.
      // This simplification procedure also ensures that no `BlockMetadata`
      // value is retrieved twice later on (no overlapping ::iterator values).
      if (!mappedBlockRangeIts.empty() &&
          mappedBlockRangeIts.back().second >= relevantRangeBegin) {
        // They are adjacent or even overlap: merge/simplify!
        mappedBlockRangeIts.back().second = relevantRangeEnd;
      } else {
        // The new range (specified by `BlockSpanItPair`) is not adjacent and
        // doesn't overlap.
        mappedBlockRangeIts.emplace_back(relevantRangeBegin, relevantRangeEnd);
      }
    };

//______________________________________________________________________________
// `WIDER CONTEXT AND EXPLANATION`
// Relevant for the functions `mapRandomItBlockSpanIt`,
// `mapRandomItBlockSpanItComplemented` and `getRelevantBlocksFromIdRanges`
// (defined below).
// (1) `input` (type `std::span<const BlockMetadata>`) for
// `getRelevantBlocksFromIdRanges` contains the original `BlockMetadata` values
// which we want to pre-filter. We are especially interested in the bounding
// `ValueId`s of `firstTriple_` and `lastTriple_` given a `BlockMetadata` value.
//
// (2)
// Assume for simplicity that the following construct represents the
// `BlockMetadata` vector (`input`), with each bracket representing
// the relevant first and last `ValueId` for a single
// `BlockMetadata` value. `input`:
// `{[1021, 1082], [1083, 1115], [1121, 1140], [1140, 1148], [1150, 1158]}`
//
// (3)
// To enable the usage of `valueIdComparators::getRangesForId`, it's necessary
// to extract those bounding `ValueId`s from `input`. This results in `idVector`
// (`beginIdVec` refers to its first value):
// `{1021, 1082, 1083, 1115, 1121, 1140, 1140, 1148, 1150, 1158}`.
// Remark: This procedure is performed for
// `IsDatatypeExpression<Datatype>::evaluateImpl` and
// `RelationalExpression<Comparison>::evaluateImpl`.
//
// (4)
// Now let's assume that the reference `Id`, for which we want to pre-filter
// the relation `less-than` (`<`), has the value `1123`.
//
// (5)
// When we analyze the retrieved vector `idVector` `{1021, 1082, 1083, 1115,
// 1121, 1140, 1140, 1148, 1150, 1158}`; it becomes clear that the `Id`s
// `1021 - 1140` are relevant (pre-filtered values). As a consequence the
// `Id`s with value `1140 - 1158` should be dropped (filtered out).
//
// (6)
// When `valueIdComparators::getRangesForId` is applied for `idVector`, the
// retrieved `::iterator`s (as a `std::pair`) represent the range `1021 - 1140`.
// `valueIdComparators::getRangesForId` is applied for
// `PrefilterExpression<Comparison>::evaluateImpl`.
// Remark: the second `::iterator` should point to the first `Id` that does not
// satisfy `< 1123` (our reference `Id` for filtering).
// `valueIdComparators::getRangesForId` will return in case of our example
// values `{std::pair<::iterator(1021), ::iterator(1140)>}`.
//
// (7)
// `mapRandomItToBlockSpanIt` now maps the given `idVector` iterators which
// define the relevant `Id` sections
// (`{std::pair<::iterator(1021), ::iterator(1140)>}`), back to `input`
// (`std::span<const BlockMetadata>`) iterators.
// `input.begin()` is equivalent to `::iterator([1021, 1082])` (see (2)).
// A specific transformation example for the values used here is given with the
// commentary for `mapRandomItToBlockSpanIt`. The mapping procedure
// transforms `std::pair<::iterator(1021), ::iterator(1140)>` into
// `std::pair<::iterator([1021, 1082]), ::iterator([1140, 1148])>`.
//
// (8)
// Given those to `BlockSpanItPair`s mapped `RandomItPair`s, the function
// `getRelevantBlocksFromIdRanges` retrieves
// `{[1021, 1082], [1083, 1115], [1121, 1140]}` (relevant `BlockMetadata`
// values) from `input`.
//______________________________________________________________________________
static std::vector<BlockSpanItPair> mapRandomItToBlockSpanIt(
    const std::vector<RandomItPair>& relevantIdRangeIndices,
    const RandomIt& beginIdVec, std::span<const BlockMetadata> inputBlocks) {
  const auto beginBlocks = inputBlocks.begin();
  std::vector<BlockSpanItPair> mappedBlockRangeIts;
  // Helper for adding the relevant ranges to `mappedBlockRangeIts`.
  const auto addRange = [&mappedBlockRangeIts](const BlockSpanIt& begin,
                                               const BlockSpanIt& end) {
    addRangeAndSimplify(mappedBlockRangeIts, begin, end);
  };
  // Iterate over the ranges defined by (first) `RandomIt` and (second)
  // `RandomIt` (`RandomItPair`) and map those values to their corresponding
  // (first) `BlockSpanIt` and (second) `BlocksSpanIt` (`BlockSpanItPair`).
  // In addition it is ensured that those ranges defined by `BlockSpanItPair`
  // are simplified (and don't overlap).
  for (const auto& [firstIdRangeIt, secondIdRangeIt] : relevantIdRangeIndices) {
    // Regarding our example with values as defined above follows:
    // beginBlocks (input above) + dist(beginIdVec, ::iterator(1021))
    // ::iterator([1021, 1082]) + dist(::iterator(1021), ::iterator(1021)) / 2
    // Thus, the iterator value for our range is ::iterator([1021, 1082]).
    addRange(
        beginBlocks + std::distance(beginIdVec, firstIdRangeIt) / 2,
        // Round up, for Ids contained within the bounding Ids of
        // firstTriple_ and lastTriple_ (from `BlockMetadata`) we have to
        // include the respective metadata block (that block is partially
        // relevant).
        // Remark: Use `std::min` to ensure that the bounds of input are
        // respected (with round-up procedure).

        // The values for our example:
        // ::iterator([1021, 1082]) + dist(::iterator(1021), ::iterator(1140) +
        // 1) / 2
        //
        // Remark: We round up (+ 1) to ensure that also partially relevant
        // BlockMetadata values are contained.
        //
        // dist(::iterator(1021), ::iterator(1140) + 1) / 2 is equivalent to
        // (5 + 1) / 2, hence 3.
        // As a result, the second iterator value is ::iterator[1140, 1148].
        std::min(
            beginBlocks + (std::distance(beginIdVec, secondIdRangeIt) + 1) / 2,
            inputBlocks.end()));
  }
  // `std::pair<::iterator([1021, 1082]), ::iterator([1140, 1148])>` is the
  // return value regarding our example..
  return mappedBlockRangeIts;
}

//______________________________________________________________________________
static std::vector<BlockSpanItPair> mapRandomItToBlockSpanItComplemented(
    const std::vector<RandomItPair>& relevantIdRangeIndices,
    const RandomIt& beginIdVec, std::span<const BlockMetadata> inputBlocks) {
  const auto beginBlocks = inputBlocks.begin();
  std::vector<BlockSpanItPair> mappedBlockRangeIts;
  // Helper for adding the relevant ranges to `mappedBlockRangeIts`.
  const auto addRange = [&mappedBlockRangeIts](const BlockSpanIt& begin,
                                               const BlockSpanIt& end) {
    addRangeAndSimplify(mappedBlockRangeIts, begin, end);
  };
  // The procedure is similar to the one performed in
  // `mapRandomItToBlockSpanIt`. The difference is that the objective here
  // is to retrieve the complementing (mapped) `BlockSpanItPairs`s with respect
  // to relevantIdRangeIndices.
  // Implicitly handle the first complementing section:
  // inputBlocks[0, firstRelevantSectionBegin]
  RandomIt lastIdRangeIt = beginIdVec;
  for (const auto& [firstIdRangeIt, secondIdRangeIt] : relevantIdRangeIndices) {
    addRange(beginBlocks + std::distance(beginIdVec, lastIdRangeIt) / 2,
             std::min(beginBlocks +
                          (std::distance(beginIdVec, firstIdRangeIt) + 1) / 2,
                      inputBlocks.end()));
    // Set the start of the next complementing section.
    lastIdRangeIt = secondIdRangeIt;
  }
  // Handle the last complementing section:
  // inputBlocks[lastRelevantSectionEnd, inputBlocks.size()]
  const BlockSpanIt& mappedFirstBlockRangeIt =
      beginBlocks + std::distance(beginIdVec, lastIdRangeIt) / 2;
  if (mappedFirstBlockRangeIt < inputBlocks.end()) {
    addRange(mappedFirstBlockRangeIt, inputBlocks.end());
  }
  return mappedBlockRangeIts;
}

//______________________________________________________________________________
static std::vector<BlockMetadata> getRelevantBlocksFromIdRanges(
    const std::vector<RandomItPair>& relevantIdRanges,
    std::span<const BlockMetadata> input, const RandomIt& beginIdVec,
    const bool getComplementRange) {
  std::vector<BlockMetadata> relevantBlocks;
  // Reserve memory, input.size() is upper bound.
  relevantBlocks.reserve(input.size());
  // Get the mapped `BlockSpanItPair`s for the given `RandomItPair`s.
  const std::vector<BlockSpanItPair>& relevantRanges =
      getComplementRange
          ? mapRandomItToBlockSpanItComplemented(relevantIdRanges, beginIdVec,
                                                 input)
          : mapRandomItToBlockSpanIt(relevantIdRanges, beginIdVec, input);

  for (const auto& [firstIt, secondIt] : relevantRanges) {
    relevantBlocks.insert(relevantBlocks.end(), firstIt, secondIt);
  }
  relevantBlocks.shrink_to_fit();
  return relevantBlocks;
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

//______________________________________________________________________________
// Given the arguments `std::span<const BlockMetadata> input` and
// `evaluationColumn`, this function retrieves the `Id`s of column index
// `evaluationColumn` from BlockMetadata and optionally the `BlockMetadata`
// values containing mixed datatypes (if `getMixedDatatypeBlocks` is `true`).
template <bool getMixedDatatypeBlocks>
static std::pair<std::vector<ValueId>, std::vector<BlockMetadata>>
getValueIdsAndMixedDatatypeBlocks(std::span<const BlockMetadata> input,
                                  size_t evaluationColumn) {
  std::vector<ValueId> idVector;
  // For each BlockMetadata value in vector input, we have a respective Id for
  // firstTriple and lastTriple
  idVector.reserve(2 * input.size());
  std::vector<BlockMetadata> mixedDatatypeBlocks;

  ql::ranges::for_each(input, [&](const BlockMetadata& block) {
    const auto firstId =
        getIdFromColumnIndex(block.firstTriple_, evaluationColumn);
    const auto secondId =
        getIdFromColumnIndex(block.lastTriple_, evaluationColumn);

    idVector.push_back(firstId);
    idVector.push_back(secondId);

    if (getMixedDatatypeBlocks) {
      if (firstId.getDatatype() != secondId.getDatatype()) {
        mixedDatatypeBlocks.push_back(block);
      }
    }
  });
  return {idVector, mixedDatatypeBlocks};
}

// SECTION PREFILTER EXPRESSION (BASE CLASS)
//______________________________________________________________________________
std::vector<BlockMetadata> PrefilterExpression::evaluate(
    std::span<const BlockMetadata> input, size_t evaluationColumn,
    const bool stripIncompleteBlocks) const {
  if (!stripIncompleteBlocks) {
    return evaluateAndCheckImpl(input, evaluationColumn);
  }
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

  auto result = evaluateAndCheckImpl(input, evaluationColumn);
  if (firstBlock.has_value()) {
    result.insert(result.begin(), firstBlock.value());
  }
  if (lastBlock.has_value()) {
    result.push_back(lastBlock.value());
  }
  return result;
};

// _____________________________________________________________________________
std::vector<BlockMetadata> PrefilterExpression::evaluateAndCheckImpl(
    std::span<const BlockMetadata> input, size_t evaluationColumn) const {
  checkEvalRequirements(input, evaluationColumn);
  const auto& relevantBlocks = evaluateImpl(input, evaluationColumn);
  checkEvalRequirements(relevantBlocks, evaluationColumn);
  return relevantBlocks;
}

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
  // The complementation logic implemented with the following mapping procedure:
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
std::vector<BlockMetadata>
RelationalExpression<Comparison>::evaluateOptGetCompleteComplementImpl(
    std::span<const BlockMetadata> input, size_t evaluationColumn,
    bool getComplementOverAllDatatypes) const {
  using namespace valueIdComparators;
  auto [idVector, mixedDatatypeBlocks] =
      getValueIdsAndMixedDatatypeBlocks<true>(input, evaluationColumn);

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
                              ? getRangesForId(idVector.begin(), idVector.end(),
                                               referenceId, Comparison)
                              : getRangesForId(idVector.begin(), idVector.end(),
                                               referenceId, Comparison, false);
  return getSetUnion(
      getRelevantBlocksFromIdRanges(relevantIdRanges, input, idVector.begin(),
                                    getComplementOverAllDatatypes),
      mixedDatatypeBlocks);
};

//______________________________________________________________________________
template <CompOp Comparison>
std::vector<BlockMetadata> RelationalExpression<Comparison>::evaluateImpl(
    std::span<const BlockMetadata> input, size_t evaluationColumn) const {
  return evaluateOptGetCompleteComplementImpl(input, evaluationColumn, false);
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
std::vector<BlockMetadata>
IsDatatypeExpression<Datatype>::evaluateIsIriOrIsLiteralImpl(
    std::span<const BlockMetadata> input, const bool isNegated,
    size_t evaluationColumn) const {
  using enum IsDatatype;
  using LVE = LocalVocabEntry;

  return Datatype == IRI
             // Remark: Ids containing LITERAL values precede IRI related Ids in
             // order.
             // The smallest possible IRI is represented by "<>", we use its
             // corresponding ValueId later on as a lower bound.
             ? GreaterThanExpression(LVE::fromStringRepresentation("<>"))
                   .evaluateOptGetCompleteComplementImpl(
                       input, evaluationColumn, isNegated)
             // For pre-filtering LITERAL related ValueIds we use the ValueId
             // representing the beginning of IRI values as an upper bound.
             : LessThanExpression(LVE::fromStringRepresentation("<"))
                   .evaluateOptGetCompleteComplementImpl(
                       input, evaluationColumn, isNegated);
}

//______________________________________________________________________________
template <IsDatatype Datatype>
std::vector<BlockMetadata>
IsDatatypeExpression<Datatype>::evaluateIsBlankOrIsNumericImpl(
    std::span<const BlockMetadata> input, const bool isNegated,
    size_t evaluationColumn) const {
  using enum IsDatatype;

  auto [idVector, _] =
      getValueIdsAndMixedDatatypeBlocks<false>(input, evaluationColumn);
  if (idVector.empty()) return {};

  auto idsBegin = idVector.begin();
  auto idsEnd = idVector.end();
  std::vector<RandomItPair> relevantRanges;

  if constexpr (Datatype == BLANK) {
    relevantRanges.push_back(valueIdComparators::getRangeForDatatype(
        idsBegin, idsEnd, Datatype::BlankNodeIndex));
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
      relevantRanges.push_back({std::min(rangeInt.first, rangeDouble.first),
                                std::max(rangeInt.second, rangeDouble.second)});
    } else {
      relevantRanges.push_back(rangeInt);
      relevantRanges.push_back(rangeDouble);
    }
  }
  // Sort and remove overlapping ranges.
  valueIdComparators::detail::simplifyRanges(relevantRanges);
  if (relevantRanges.empty()) {
    return {};
  }
  return getRelevantBlocksFromIdRanges(relevantRanges, input, idVector.begin(),
                                       isNegated);
}

//______________________________________________________________________________
template <IsDatatype Datatype>
std::vector<BlockMetadata> IsDatatypeExpression<Datatype>::evaluateImpl(
    std::span<const BlockMetadata> input, size_t evaluationColumn) const {
  using enum IsDatatype;
  if constexpr (Datatype == BLANK || Datatype == NUMERIC) {
    return evaluateIsBlankOrIsNumericImpl(input, isNegated_, evaluationColumn);
  } else {
    static_assert(Datatype == IRI || Datatype == LITERAL);
    return evaluateIsIriOrIsLiteralImpl(input, isNegated_, evaluationColumn);
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
std::vector<BlockMetadata> LogicalExpression<Operation>::evaluateImpl(
    std::span<const BlockMetadata> input, size_t evaluationColumn) const {
  using enum LogicalOperator;
  if constexpr (Operation == AND) {
    auto resultChild1 = child1_->evaluate(input, evaluationColumn, false);
    return child2_->evaluate(resultChild1, evaluationColumn, false);
  } else {
    static_assert(Operation == OR);
    return getSetUnion(child1_->evaluate(input, evaluationColumn, false),
                       child2_->evaluate(input, evaluationColumn, false));
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
std::vector<BlockMetadata> NotExpression::evaluateImpl(
    std::span<const BlockMetadata> input, size_t evaluationColumn) const {
  return child_->evaluate(input, evaluationColumn, false);
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

  // `getDateId` returns an `Id` containing the smallest possible `Date`
  // (`xsd::date`) for which `YEAR(Id) == adjustedYear` is valid. This `Id` acts
  // as a reference bound for the actual `DateYearOrDuration` prefiltering
  // procedure.
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
  // `IdOrLocalVocabEntry referenceValue` if contained. Given no `ValueId` is
  // contained, a explanatory message per `std::runtime_error` is thrown.
  const auto retrieveValueIdOrThrowErr =
      [](const IdOrLocalVocabEntry& referenceValue) {
        return std::visit(
            []<typename T>(const T& value) -> ValueId {
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
              throw std::runtime_error(
                  "Reference value IdOrLocalVocabEntry contains unknown type.");
            },
            referenceValue);
      };
  // Handle year extraction and return a date-value adjusted
  // `PrefilterExpression` if possible. Given an unsuitable reference value was
  // provided, throw a std::runtime_error with an explanatory message.
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
    // Retrieve by map the corresponding mirrored `CompOp` value for the given
    // `CompOp comparison` template argument. E.g., this procedure will
    // transform the relational expression `referenceValue > ?var` into `?var
    // < referenceValue`.
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
