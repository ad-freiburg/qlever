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
  const auto blockLessThanBlock = [](const BlockMetadata& b1,
                                     const BlockMetadata& b2) {
    return b1.blockIndex_ < b2.blockIndex_;
  };
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

// Refers to a `BlockMetadata` span `::interator` (index).
using BlockSpanIt =
    std::span<const prefilterExpressions::BlockMetadata>::iterator;

//______________________________________________________________________________
static std::pair<BlockSpanIt, BlockSpanIt> getInputAdjustedRangeIndices(
    const BlockSpanIt& beginInput, const BlockSpanIt& endInput,
    const RandomIt& valueIdsInputBegin, const RandomIt& firstIt,
    const RandomIt& lastIt) {
  return {
      beginInput + std::distance(valueIdsInputBegin, firstIt) / 2,
      // Round up, for Ids contained within the bounding Ids of firstTriple
      // and lastTriple we have to include the respective metadata block
      // (that block is partially relevant).
      // Remark: Given lastIt refers to the highest possible input index
      // (endInput), select endInput to avoid out-of-bound errors later on.
      std::min(beginInput + (std::distance(valueIdsInputBegin, lastIt) + 1) / 2,
               endInput)};
}

//______________________________________________________________________________
// Similar to `getRelevantBlocksFromIdRanges`, but this function retrieves the
// relevant (and save) complementing `BlockMetadata` values w.r.t. `input` and
// `relevantIdRanges`.
static std::vector<BlockMetadata> getComplementBlocksForRelevantIdRanges(
    const std::vector<RandomItPair>& relevantIdRanges,
    std::span<const BlockMetadata> input, const RandomIt& valueIdsInputBegin,
    const RandomIt& valueIdsInputEnd) {
  // If relevantIdRanges is empty, the complement are simply all `BlockMetadata`
  // values (input).
  if (relevantIdRanges.empty()) {
    return std::vector<BlockMetadata>{input.begin(), input.end()};
  }
  // Use helper getInputAdjustedRangesIndices (w.r.t. std::span<const
  // BlockMetadata> input) and bind relevant constant arguments.
  const auto getInputAdjustedRange = std::bind(
      getInputAdjustedRangeIndices, input.begin(), input.end(),
      valueIdsInputBegin, std::placeholders::_1, std::placeholders::_2);
  // Section retrieve complementing BlockMetadata values.
  std::vector<BlockMetadata> complementBlocks;
  complementBlocks.reserve(input.size());
  // We implicitly handle the first complement range:
  // input[input.begin(), (first) inputAdjustedBegin]
  RandomIt currentBegin = valueIdsInputBegin;
  for (const auto& [firstRelevantIt, lastRelevantIt] : relevantIdRanges) {
    const auto& [inputAdjustedBegin, inputAdjustedEnd] =
        getInputAdjustedRange(currentBegin, firstRelevantIt);
    complementBlocks.insert(complementBlocks.end(), inputAdjustedBegin,
                            inputAdjustedEnd);
    currentBegin = lastRelevantIt;
  }
  // Handle the complementing range:
  // input[(last) inputAdjustedBegin, input.end()]
  if (currentBegin < valueIdsInputEnd) {
    const auto& [inputAdjustedBegin, inputAdjustedEnd] =
        getInputAdjustedRange(currentBegin, valueIdsInputEnd);
    complementBlocks.insert(complementBlocks.end(), inputAdjustedBegin,
                            inputAdjustedEnd);
  }
  complementBlocks.shrink_to_fit();
  return complementBlocks;
}

//______________________________________________________________________________
static std::vector<BlockMetadata> getRelevantBlocksFromIdRanges(
    const std::vector<RandomItPair>& relevantIdRanges,
    std::span<const BlockMetadata> input, const RandomIt& valueIdsInputBegin) {
  // The vector for relevant BlockMetadata values which contain ValueIds
  // defined as relevant by relevantIdRanges.
  std::vector<BlockMetadata> relevantBlocks;
  // Reserve memory, input.size() is upper bound.
  relevantBlocks.reserve(input.size());

  const auto getInputAdjustedRange = std::bind(
      getInputAdjustedRangeIndices, input.begin(), input.end(),
      valueIdsInputBegin, std::placeholders::_1, std::placeholders::_2);
  // Given the relevant Id ranges, retrieve the corresponding relevant
  // BlockMetadata values from vector input and add them to the relevantBlocks
  // vector. The direct Id ranges must be adjusted w.r.t. input by making use
  // of getInputAdjustedRange.
  for (const auto& [firstId, secondId] : relevantIdRanges) {
    const auto& [inputAdjustedBegin, inputAdjustedEnd] =
        getInputAdjustedRange(firstId, secondId);
    assert(inputAdjustedEnd >= inputAdjustedBegin);
    relevantBlocks.insert(relevantBlocks.end(), inputAdjustedBegin,
                          inputAdjustedEnd);
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

// SECTION PREFILTER EXPRESSION (BASE CLASS)
//______________________________________________________________________________
std::vector<BlockMetadata> PrefilterExpression::evaluate(
    std::span<const BlockMetadata> input, size_t evaluationColumn,
    bool stripIncompleteBlocks) const {
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
  return std::make_unique<RelationalExpression<complementMap.at(Comparison)>>(
      rightSideReferenceValue_);
};

//______________________________________________________________________________
template <CompOp Comparison>
std::vector<BlockMetadata> RelationalExpression<Comparison>::evaluateImpl(
    std::span<const BlockMetadata> input, size_t evaluationColumn) const {
  using namespace valueIdComparators;
  std::vector<ValueId> valueIdsInput;
  // For each BlockMetadata value in vector input, we have a respective Id for
  // firstTriple and lastTriple
  valueIdsInput.reserve(2 * input.size());
  std::vector<BlockMetadata> mixedDatatypeBlocks;

  ql::ranges::for_each(
      input, [&evaluationColumn, &valueIdsInput,
              &mixedDatatypeBlocks](const BlockMetadata& block) {
        const auto firstId =
            getIdFromColumnIndex(block.firstTriple_, evaluationColumn);
        const auto secondId =
            getIdFromColumnIndex(block.lastTriple_, evaluationColumn);

        valueIdsInput.push_back(firstId);
        valueIdsInput.push_back(secondId);

        if (firstId.getDatatype() != secondId.getDatatype()) {
          mixedDatatypeBlocks.push_back(block);
        }
      });

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
          ? getRangesForId(valueIdsInput.begin(), valueIdsInput.end(),
                           referenceId, Comparison)
          : getRangesForId(valueIdsInput.begin(), valueIdsInput.end(),
                           referenceId, Comparison, false);

  return getSetUnion(getRelevantBlocksFromIdRanges(relevantIdRanges, input,
                                                   valueIdsInput.begin()),
                     mixedDatatypeBlocks);
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
  return std::make_unique<RelationalExpression<Comparison>>(
      rightSideReferenceValue_);
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
  return std::make_unique<IsDatatypeExpression<Datatype>>(!isNegated_);
};

//______________________________________________________________________________
template <IsDatatype Datatype>
std::unique_ptr<PrefilterExpression> IsDatatypeExpression<Datatype>::clone()
    const {
  return std::make_unique<IsDatatypeExpression<Datatype>>(*this);
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
std::vector<BlockMetadata> IsDatatypeExpression<Datatype>::evaluateImpl(
    std::span<const BlockMetadata> input, size_t evaluationColumn) const {
  using enum IsDatatype;

  // Vector for storing bounding ValueIds to the BlockMetadata values.
  std::vector<ValueId> valueIdsInput;
  valueIdsInput.reserve(2 * input.size());

  ql::ranges::for_each(
      input, [&valueIdsInput, evaluationColumn](const BlockMetadata& block) {
        valueIdsInput.push_back(
            getIdFromColumnIndex(block.firstTriple_, evaluationColumn));
        valueIdsInput.push_back(
            getIdFromColumnIndex(block.lastTriple_, evaluationColumn));
      });

  auto idsBegin = valueIdsInput.begin();
  auto idsEnd = valueIdsInput.end();
  std::vector<RandomItPair> relevantRanges;
  if constexpr (Datatype == BLANK) {
    relevantRanges.push_back(valueIdComparators::getRangeForDatatype(
        idsBegin, idsEnd, Datatype::BlankNodeIndex));
  } else if constexpr (Datatype == NUMERIC) {
    // Remark: Swapping the defined relational order for Datatype::Int to
    // Datatype::Double ValueIds (in ValueId.h) might affect the correctness of
    // the following lines!
    static_assert(Datatype::Int < Datatype::Double);

    const auto& rangeInt = valueIdComparators::getRangeForDatatype(
        idsBegin, idsEnd, Datatype::Int);
    const auto& rangeDouble = valueIdComparators::getRangeForDatatype(
        idsBegin, idsEnd, Datatype::Double);
    // Differention here regarding the adjacency of Int and Double datatype
    // values.
    // This is relevant w.r.t. getRelevantBlocksFromIdRanges(), because we
    // technically add to .second + 1 for the first datatype range. If we simply
    // add rangeInt and rangeDouble given that datatype Int and Double are
    // stored directly next to each other, we potentially add a block twice
    // (forbidden).
    // Remark: This is necessary even if simplifyRanges(relevantRanges) is
    // performed later on.
    if (rangeInt.second == rangeDouble.first ||
        rangeDouble.second == rangeInt.first) {
      relevantRanges.push_back({std::min(rangeInt.first, rangeDouble.first),
                                std::max(rangeInt.second, rangeDouble.second)});
    } else {
      relevantRanges.push_back(rangeInt);
      relevantRanges.push_back(rangeDouble);
    }
  } else {
    assert(Datatype == IRI || Datatype == LITERAL);
    // Not implemented yet, i assume the prefiltering can be even more precise
    // by making use of `transformToFirstPossibleBiggerValue`.
    return std::vector<BlockMetadata>{input.begin(), input.end()};
  }

  // Sort and remove overlaps w.r.t. relevant ranges.
  valueIdComparators::detail::simplifyRanges(relevantRanges);
  // If this IsDatatypeExpression<Datatype> expression is negated, retrieve the
  // corresponding complementing ranges.
  if (isNegated_) {
    return getComplementBlocksForRelevantIdRanges(
        relevantRanges, input, valueIdsInput.begin(), valueIdsInput.end());
  }
  return getRelevantBlocksFromIdRanges(relevantRanges, input,
                                       valueIdsInput.begin());
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
  return std::make_unique<LogicalExpression<Operation>>(child1_->clone(),
                                                        child2_->clone());
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
  return std::make_unique<NotExpression>((child_->clone()), true);
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
  return std::make_unique<RelationalExpression<mirrorMap.at(comparison)>>(
      referenceValue);
}

//______________________________________________________________________________
void checkPropertiesForPrefilterConstruction(
    const std::vector<PrefilterExprVariablePair>& vec) {
  auto viewVariable = vec | ql::views::values;
  if (!ql::ranges::is_sorted(viewVariable, std::less<>{})) {
    throw std::runtime_error(
        "The vector must contain the <PrefilterExpression, Variable> pairs in "
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
      mirrored
          ? makeMirroredExpression<comparison>(referenceValue)
          : std::make_unique<RelationalExpression<comparison>>(referenceValue),
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
