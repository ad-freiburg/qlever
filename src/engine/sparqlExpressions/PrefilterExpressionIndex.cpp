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
static Id getIdFromColumnIndex(
    const CompressedBlockMetadata::PermutedTriple& triple, size_t columnIndex) {
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
}

//______________________________________________________________________________
// Check for the following conditions.
// (1) All `CompressedBlockMetadata` values in `input` must be unique.
// (2) `input` must contain those `CompressedBlockMetadata` values in sorted
// order.
// (3) Columns with `column index < evaluationColumn` must contain equal
// values (`ValueId`s).
static void checkRequirementsBlockMetadata(
    ql::span<const CompressedBlockMetadata> input, size_t evaluationColumn) {
  CompressedRelationReader::ScanSpecAndBlocks::checkBlockMetadataInvariant(
      input, evaluationColumn);
}

namespace detail {
//______________________________________________________________________________
// Merge `BlockMetadataRange blockRange` with the previous (relevant)
// `BlockMetadataRange`s.
static void mergeBlockRangeWithRanges(BlockMetadataRanges& blockRanges,
                                      const BlockMetadataRange& blockRange) {
  if (blockRange.empty()) {
    return;
  }
  if (blockRanges.empty()) {
    blockRanges.push_back(blockRange);
    return;
  }
  auto blockRangeBegin = blockRange.begin();
  BlockMetadataRange& lastBlockRange = blockRanges.back();
  AD_CORRECTNESS_CHECK(blockRangeBegin >= lastBlockRange.begin());
  if (lastBlockRange.end() >= blockRangeBegin) {
    // The blockRange intersects with previous blockRange(s).
    lastBlockRange =
        BlockMetadataRange{lastBlockRange.begin(),
                           std::max(lastBlockRange.end(), blockRange.end())};
  } else {
    // The new blockRange doesn't intersect with previous blockRange(s).
    blockRanges.push_back(blockRange);
  }
}

namespace mapping {
//______________________________________________________________________________
// Map the given `ValueIdIt beginIdIt` and `ValueIdIt endIdIt` to their
// corresponding `BlockIdIt` values and return them as `BlockMetadataRange`.
static BlockMetadataRange mapValueIdItPairToBlockRange(
    const ValueIdIt& idRangeBegin, const ValueIdIt& beginIdIt,
    const ValueIdIt& endIdIt, BlockMetadataSpan blockRange) {
  AD_CORRECTNESS_CHECK(beginIdIt <= endIdIt);
  auto blockRangeBegin = blockRange.begin();
  // Each `CompressedBlockMetadata` value contains two bounding `ValueId`s, one
  // for `firstTriple_` and `lastTriple_` respectively. `ValueIdIt idRangeBegin`
  // is the first valid iterator on our flattened `ql::span<const
  // CompressedBlockMetadata> blockRange` with respect to the contained
  // `ValueId`s.
  // `blockRange.begin()` represents the first valid iterator on our original
  // `ql::span<const CompressedBlockMetadata> blockRange`.
  //
  //  EXAMPLE
  // `CompressedBlockMetadata` view on `ql::span<const CompressedBlockMetadata>
  // blockRange`:
  // `{[1021, 1082], [1083, 1115], [1121, 1140], [1140, 1148], [1150,
  // 1158]}`. Range for `BlockMetadataIt` values.
  //
  // `ValueId` view on (flat) `ql::span<const CompressedBlockMetadata>
  // blockRange`:
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
// `BlockMetadataRange`s.
// The actual mapping is implemented by `mapValueIdItPairToBlockRange`.
BlockMetadataRanges mapValueIdItRangesToBlockItRangesComplemented(
    const std::vector<ValueIdItPair>& relevantIdRanges,
    const ValueIdSubrange& idRange, BlockMetadataSpan blockRange) {
  auto idRangeBegin = idRange.begin();
  // Vector containing the `BlockRange` mapped indices.
  BlockMetadataRanges blockRanges;
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
// Map the given `ValueIdItPair`s to their corresponding `BlockMetadataRange`s.
// The actual mapping is implemented by `mapValueIdItPairToBlockRange`.
BlockMetadataRanges mapValueIdItRangesToBlockItRanges(
    const std::vector<ValueIdItPair>& relevantIdRanges,
    const ValueIdSubrange& idRange, BlockMetadataSpan blockRange) {
  if (relevantIdRanges.empty()) {
    return {};
  }
  auto idRangeBegin = idRange.begin();
  // Vector containing the `BlockRange` mapped iterators.
  BlockMetadataRanges blockRanges;
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

namespace logicalOps {
// SECTION HELPER LOGICAL OPERATORS
//______________________________________________________________________________
// (1) `mergeRelevantBlockItRanges<true>` returns the `union` (`logical-or
// (||)`) of `BlockMetadataRanges r1` and `BlockMetadataRanges r2`.
// (2) `mergeRelevantBlockItRanges<false>` returns the `intersection`
// (`logical-and
// (&&)`) of `BlockMetadataRanges r1` and `BlockMetadataRanges r2`.
//
// EXAMPLE UNION
// Given input `r1` and `r2`
// `BlockMetadataRanges r1`: `[<2, 10>, <15, 16>, <20, 23>]`
// `BlockMetadataRanges r2`: `[<4, 6>, <8, 9>, <15, 22>]`
// The result is `RelevantBlocksRanges result`: `[<2, 10>, <15, 23>]`
//
// EXAMPLE INTERSECTION
// Given input `r1` and `r2`
// `BlockMetadataRanges r1`: `[<2, 10>, <15, 16>, <20, 23>]`
// `BlockMetadataRanges r2`: `[<4, 6>, <8, 9>, <15, 22>]`
// The result is
// `RelevantBlocksRanges result`: `[<4, 6>, <8, 9>, <15, 16>, <20, 22>]`
template <bool GetUnion>
BlockMetadataRanges mergeRelevantBlockItRanges(const BlockMetadataRanges& r1,
                                               const BlockMetadataRanges& r2) {
  if constexpr (GetUnion) {
    if (r1.empty() && r2.empty()) return {};
  } else {
    if (r1.empty() || r2.empty()) return {};
  }
  BlockMetadataRanges mergedRanges;
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
        addRange(BlockMetadataRange{std::min(idx1Begin, idx2Begin),
                                    std::max(idx1End, idx2End)});
        idx1++;
        idx2++;
      } else {
        // Add the intersection of r1 and r2.
        addRange(BlockMetadataRange{std::max(idx1Begin, idx2Begin),
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

BlockMetadataRanges getIntersectionOfBlockRanges(
    const BlockMetadataRanges& r1, const BlockMetadataRanges& r2) {
  return mergeRelevantBlockItRanges<false>(r1, r2);
}

BlockMetadataRanges getUnionOfBlockRanges(const BlockMetadataRanges& r1,
                                          const BlockMetadataRanges& r2) {
  return mergeRelevantBlockItRanges<true>(r1, r2);
}
}  // namespace logicalOps
}  // namespace detail

//______________________________________________________________________________
// This function retrieves the `BlockRange`s for `CompressedBlockMetadata`
// values that contain bounding `ValueId`s with different underlying datatypes.
static BlockMetadataRanges getRangesMixedDatatypeBlocks(
    const ValueIdSubrange& idRange, BlockMetadataSpan blockRange) {
  using enum Datatype;
  if (idRange.empty()) {
    return {};
  }
  std::vector<ValueIdItPair> mixedDatatypeRanges;
  // Ensure that `idRange` holds access to even number of ValueIds.
  AD_CORRECTNESS_CHECK(idRange.size() % 2 == 0);
  const auto checkDifferentDtype = [](const auto& idPair) {
    auto firstId = idPair.begin();
    auto dType1 = (*firstId).getDatatype();
    auto dType2 = (*std::next(firstId)).getDatatype();
    // ValueIds representing LocalVocab and Vocab entries are contained in mixed
    // and sorted order over the CompressedBlockMetadata values. Thus, we don't
    // discard them if they contain a mix of LocalVocab and Vocab ValueIds.
    if ((dType1 == VocabIndex && dType2 == LocalVocabIndex) ||
        (dType1 == LocalVocabIndex && dType2 == VocabIndex)) {
      return false;
    }
    return dType1 != dType2;
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
// `ql::span<const CompressedBlockMetadata> inputSpan`.
// Each `CompressedBlockMetadata` value holds exactly two bound `ValueId`s (one
// in `firstTriple_` and `lastTriple_` respectively) over the specified column
// `evaluationColumn_`.
// Thus, the valid index range over `i` is `[0, 2 * inputSpan.size())`.
ValueId AccessValueIdFromBlockMetadata::operator()(
    BlockMetadataSpan randomAccessContainer, uint64_t i) const {
  const CompressedBlockMetadata& block = randomAccessContainer[i / 2];
  return i % 2 == 0
             ? getIdFromColumnIndex(block.firstTriple_, evaluationColumn_)
             : getIdFromColumnIndex(block.lastTriple_, evaluationColumn_);
}

// SECTION PREFILTER EXPRESSION (BASE CLASS)
//______________________________________________________________________________
BlockMetadataRanges PrefilterExpression::evaluate(
    const Vocab& vocab, BlockMetadataSpan blockRange,
    size_t evaluationColumn) const {
  if (blockRange.size() < 3) {
    return {{blockRange.begin(), blockRange.end()}};
  }

  std::optional<BlockMetadataRange> firstBlockRange = std::nullopt;
  std::optional<BlockMetadataRange> lastBlockRange = std::nullopt;
  if (blockRange.front().containsInconsistentTriples(evaluationColumn)) {
    firstBlockRange = {blockRange.begin(), std::next(blockRange.begin())};
    blockRange = blockRange.subspan(1);
  }
  if (blockRange.back().containsInconsistentTriples(evaluationColumn)) {
    lastBlockRange = {std::prev(blockRange.end()), blockRange.end()};
    blockRange = blockRange.subspan(0, blockRange.size() - 1);
  }

  BlockMetadataRanges result;
  if (!blockRange.empty()) {
    checkRequirementsBlockMetadata(blockRange, evaluationColumn);
    AccessValueIdFromBlockMetadata accessValueIdOp(evaluationColumn);
    ValueIdSubrange idRange{
        ValueIdIt{&blockRange, 0, accessValueIdOp},
        ValueIdIt{&blockRange, blockRange.size() * 2, accessValueIdOp}};
    result = detail::logicalOps::mergeRelevantBlockItRanges<true>(
        evaluateImpl(vocab, idRange, blockRange, false),
        // always add mixed datatype blocks
        getRangesMixedDatatypeBlocks(idRange, blockRange));
  }

  if (firstBlockRange.has_value()) {
    result.insert(result.begin(), firstBlockRange.value());
  }
  if (lastBlockRange.has_value()) {
    result.push_back(lastBlockRange.value());
  }
  return result;
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

// SECTION PREFIX-REGEX
//______________________________________________________________________________
std::unique_ptr<PrefilterExpression> PrefixRegexExpression::logicalComplement()
    const {
  return make<PrefixRegexExpression>(prefixLiteral_, !isNegated_);
}

//______________________________________________________________________________
bool PrefixRegexExpression::operator==(const PrefilterExpression& other) const {
  const auto* otherPrefixRegex =
      dynamic_cast<const PrefixRegexExpression*>(&other);
  if (!otherPrefixRegex) {
    return false;
  }
  return isNegated_ == otherPrefixRegex->isNegated_ &&
         prefixLiteral_ == otherPrefixRegex->prefixLiteral_;
}

//______________________________________________________________________________
std::unique_ptr<PrefilterExpression> PrefixRegexExpression::clone() const {
  return make<PrefixRegexExpression>(*this);
}

//______________________________________________________________________________
std::string PrefixRegexExpression::asString(
    [[maybe_unused]] size_t depth) const {
  return absl::StrCat(
      "Prefilter PrefixRegexExpression with prefix ",
      prefixLiteral_.toStringRepresentation(),
      ".\nExpression is negated: ", isNegated_ ? "true.\n" : "false.\n");
}

//______________________________________________________________________________
BlockMetadataRanges PrefixRegexExpression::evaluateImpl(
    const Vocab& vocab, const ValueIdSubrange& idRange,
    BlockMetadataSpan blockRange, bool getTotalComplement) const {
  static_assert(Datatype::LocalVocabIndex > Datatype::VocabIndex);
  static_assert(Vocab::PrefixRanges::Ranges{}.size() == 1);
  using LVE = LocalVocabEntry;
  LocalVocab localVocab{};
  auto prefixQuoted =
      absl::StrCat("\"", asStringViewUnsafe(prefixLiteral_.getContent()));
  auto [lowerVocabIndex, upperVocabIndex] =
      vocab.prefixRanges(prefixQuoted).ranges().front();

  // Set lower reference.
  const auto& lowerIdVocab = Id::makeFromVocabIndex(lowerVocabIndex);
  const auto& beginIdIri = getValueIdFromIdOrLocalVocabEntry(
      LVE::fromStringRepresentation("<>"), localVocab);

  // The `vocab.prefixRanges` returns the correct bounds only for preindexed
  // vocab entries, there might be local vocab entries in `(lowerVocabIndex-1,
  // lowerVocabIndex]` which still match the prefix.
  if (isNegated_) {
    const auto& upperIdAdjusted =
        upperVocabIndex.get() == 0
            ? Id::makeFromVocabIndex(upperVocabIndex)
            : Id::makeFromVocabIndex(upperVocabIndex.decremented());
    // Case `!STRSTARTS(?var, "prefix")` or `!REGEX(?var, "^prefix")`.
    // Prefilter ?var >= Id(prev("prefix)) || ?var < Id("prefix).
    return OrExpression(
               make<LessThanExpression>(lowerIdVocab),
               make<AndExpression>(make<GreaterThanExpression>(upperIdAdjusted),
                                   make<LessThanExpression>(beginIdIri)))
        .evaluateImpl(vocab, idRange, blockRange, getTotalComplement);
  }

  // Set expression associated with the lower reference.
  auto lowerRefExpr = lowerVocabIndex.get() == 0
                          ? make<GreaterEqualExpression>(lowerIdVocab)
                          : make<GreaterThanExpression>(Id::makeFromVocabIndex(
                                lowerVocabIndex.decremented()));
  // Set expression associated with the upper reference.
  auto upperRefExpr =
      upperVocabIndex.get() == vocab.size()
          ? make<LessThanExpression>(beginIdIri)
          : make<LessThanExpression>(Id::makeFromVocabIndex(upperVocabIndex));
  // Case `STRSTARTS(?var, "prefix")` or `REGEX(?var, "^prefix")`.
  // Prefilter ?var > Id(prev("prefix)) && ?var < Id(next("prefix)).
  return AndExpression(std::move(lowerRefExpr), std::move(upperRefExpr))
      .evaluateImpl(vocab, idRange, blockRange, getTotalComplement);
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
}

//______________________________________________________________________________
template <CompOp Comparison>
BlockMetadataRanges RelationalExpression<Comparison>::evaluateImpl(
    [[maybe_unused]] const Vocab& vocab, const ValueIdSubrange& idRange,
    BlockMetadataSpan blockRange, bool getTotalComplement) const {
  using namespace valueIdComparators;
  // If `rightSideReferenceValue_` contains a `LocalVocabEntry` value, we use
  // the here created `LocalVocab` to retrieve a corresponding `ValueId`.
  LocalVocab localVocab{};
  auto referenceId =
      getValueIdFromIdOrLocalVocabEntry(rightSideReferenceValue_, localVocab);
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
  return getTotalComplement
             ? detail::mapping::mapValueIdItRangesToBlockItRangesComplemented(
                   relevantIdRanges, idRange, blockRange)
             : detail::mapping::mapValueIdItRangesToBlockItRanges(
                   relevantIdRanges, idRange, blockRange);
}

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
}

//______________________________________________________________________________
template <CompOp Comparison>
std::unique_ptr<PrefilterExpression> RelationalExpression<Comparison>::clone()
    const {
  return make<RelationalExpression<Comparison>>(rightSideReferenceValue_);
}

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
}

// SECTION ISDATATYPE
//______________________________________________________________________________
// Remark: The current `logicalComplement` implementation retrieves the full
// complement w.r.t. the datatypes defined and represented by the `ValueId`
// space.
template <IsDatatype Datatype>
std::unique_ptr<PrefilterExpression>
IsDatatypeExpression<Datatype>::logicalComplement() const {
  return make<IsDatatypeExpression<Datatype>>(!isNegated_);
}

//______________________________________________________________________________
template <IsDatatype Datatype>
std::unique_ptr<PrefilterExpression> IsDatatypeExpression<Datatype>::clone()
    const {
  return make<IsDatatypeExpression<Datatype>>(*this);
}

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
}

//______________________________________________________________________________
template <IsDatatype Datatype>
std::string IsDatatypeExpression<Datatype>::asString(
    [[maybe_unused]] size_t depth) const {
  return absl::StrCat(
      "Prefilter IsDatatypeExpression:", "\nPrefilter for datatype: ",
      getDatatypeIsTypeStr(Datatype),
      "\nis negated: ", isNegated_ ? "true" : "false", ".\n");
}

//______________________________________________________________________________
static BlockMetadataRanges getRangesForDatatypes(const ValueIdSubrange& idRange,
                                                 BlockMetadataSpan blockRange,
                                                 const bool isNegated,
                                                 ql::span<Datatype> datatypes) {
  std::vector<ValueIdItPair> relevantRanges;
  for (Datatype datatype : datatypes) {
    relevantRanges.emplace_back(valueIdComparators::getRangeForDatatype(
        idRange.begin(), idRange.end(), datatype));
  }
  // Sort and remove overlapping ranges.
  relevantRanges =
      valueIdComparators::detail::simplifyRanges(std::move(relevantRanges));
  return isNegated
             ? detail::mapping::mapValueIdItRangesToBlockItRangesComplemented(
                   relevantRanges, idRange, blockRange)
             : detail::mapping::mapValueIdItRangesToBlockItRanges(
                   relevantRanges, idRange, blockRange);
}

//______________________________________________________________________________
template <>
BlockMetadataRanges IsDatatypeExpression<IsDatatype::BLANK>::evaluateImpl(
    [[maybe_unused]] const Vocab& vocab, const ValueIdSubrange& idRange,
    BlockMetadataSpan blockRange,
    [[maybe_unused]] bool getTotalComplement) const {
  std::array datatypes{Datatype::BlankNodeIndex};
  return getRangesForDatatypes(idRange, blockRange, isNegated_, datatypes);
}

//______________________________________________________________________________
template <>
BlockMetadataRanges IsDatatypeExpression<IsDatatype::NUMERIC>::evaluateImpl(
    [[maybe_unused]] const Vocab& vocab, const ValueIdSubrange& idRange,
    BlockMetadataSpan blockRange,
    [[maybe_unused]] bool getTotalComplement) const {
  std::array datatypes{Datatype::Int, Datatype::Double};
  return getRangesForDatatypes(idRange, blockRange, isNegated_, datatypes);
}

//______________________________________________________________________________
template <>
BlockMetadataRanges IsDatatypeExpression<IsDatatype::IRI>::evaluateImpl(
    const Vocab& vocab, const ValueIdSubrange& idRange,
    BlockMetadataSpan blockRange,
    [[maybe_unused]] bool getTotalComplement) const {
  using LVE = LocalVocabEntry;
  // Remark: Ids containing LITERAL values precede IRI related Ids
  // in order. The smallest possible IRI is represented by "<>", we
  // use its corresponding ValueId later on as a lower bound.
  return make<GreaterThanExpression>(LVE::fromStringRepresentation("<>"))
      ->evaluateImpl(vocab, idRange, blockRange, isNegated_);
}

//______________________________________________________________________________
template <>
BlockMetadataRanges IsDatatypeExpression<IsDatatype::LITERAL>::evaluateImpl(
    const Vocab& vocab, const ValueIdSubrange& idRange,
    BlockMetadataSpan blockRange,
    [[maybe_unused]] bool getTotalComplement) const {
  using LVE = LocalVocabEntry;

  // For pre-filtering LITERAL related ValueIds we use the ValueId representing
  // the beginning of IRI values as an upper bound and add all the value types
  // that are literals inlined into a compact representation.
  std::array datatypes{Datatype::Int, Datatype::Double, Datatype::Date,
                       Datatype::Bool, Datatype::GeoPoint};
  auto inlinedRanges =
      getRangesForDatatypes(idRange, blockRange, isNegated_, datatypes);
  auto nonInlinedRanges =
      make<LessThanExpression>(LVE::fromStringRepresentation("<>"))
          ->evaluateImpl(vocab, idRange, blockRange, isNegated_);

  if (isNegated_) {
    return detail::logicalOps::mergeRelevantBlockItRanges<false>(
        inlinedRanges, nonInlinedRanges);
  } else {
    return detail::logicalOps::mergeRelevantBlockItRanges<true>(
        inlinedRanges, nonInlinedRanges);
  }
}

// SECTION IS-IN-EXPRESSION (and NOT-IS-IN-EXPRESSION)
//______________________________________________________________________________
std::unique_ptr<PrefilterExpression> IsInExpression::logicalComplement() const {
  return make<IsInExpression>(referenceValues_, true);
}

//______________________________________________________________________________
bool IsInExpression::operator==(const PrefilterExpression& other) const {
  const auto* otherIsIn = dynamic_cast<const IsInExpression*>(&other);
  if (!otherIsIn) {
    return false;
  }

  return isNegated_ == otherIsIn->isNegated_ &&
         ql::ranges::equal(referenceValues_, otherIsIn->referenceValues_);
}

//______________________________________________________________________________
std::unique_ptr<PrefilterExpression> IsInExpression::clone() const {
  return make<IsInExpression>(*this);
}

//______________________________________________________________________________
std::string IsInExpression::asString([[maybe_unused]] size_t depth) const {
  return absl::StrCat(
      "Prefilter IsInExpression\nisNegated: ", isNegated_ ? "true" : "false",
      "\nWith the following number of reference values: ",
      referenceValues_.size());
}

//______________________________________________________________________________
BlockMetadataRanges IsInExpression::evaluateImpl(
    const Vocab& vocab, const ValueIdSubrange& idRange,
    BlockMetadataSpan blockRange,
    [[maybe_unused]] bool getTotalComplement) const {
  if (referenceValues_.empty()) {
    if (!isNegated_) {
      return {};
    }
    return {{blockRange.begin(), blockRange.end()}};
  }

  // Construct PrefilterExpression: refVal1 || refVal2 || ... || refValN.
  auto prefilterExpr = ::ranges::fold_left_first(
      referenceValues_ | ::ranges::views::transform([](auto&& refVal) {
        return make<EqualExpression>(AD_FWD(refVal));
      }),
      [this](auto&& c1, auto&& c2) {
        return isNegated_ ? make<AndExpression>(AD_FWD(c1), AD_FWD(c2))
                          : make<OrExpression>(AD_FWD(c1), AD_FWD(c2));
      });

  return prefilterExpr.value()->evaluateImpl(vocab, idRange, blockRange,
                                             isNegated_);
}

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
}

//______________________________________________________________________________
template <LogicalOperator Operation>
BlockMetadataRanges LogicalExpression<Operation>::evaluateImpl(
    const Vocab& vocab, const ValueIdSubrange& idRange,
    BlockMetadataSpan blockRange, bool getTotalComplement) const {
  using enum LogicalOperator;
  if constexpr (Operation == AND) {
    return detail::logicalOps::mergeRelevantBlockItRanges<false>(
        child1_->evaluateImpl(vocab, idRange, blockRange, getTotalComplement),
        child2_->evaluateImpl(vocab, idRange, blockRange, getTotalComplement));
  } else {
    static_assert(Operation == OR);
    return detail::logicalOps::mergeRelevantBlockItRanges<true>(
        child1_->evaluateImpl(vocab, idRange, blockRange, getTotalComplement),
        child2_->evaluateImpl(vocab, idRange, blockRange, getTotalComplement));
  }
}

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
}

//______________________________________________________________________________
template <LogicalOperator Operation>
std::unique_ptr<PrefilterExpression> LogicalExpression<Operation>::clone()
    const {
  return make<LogicalExpression<Operation>>(child1_->clone(), child2_->clone());
}

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
}

// SECTION NOT-EXPRESSION
//______________________________________________________________________________
std::unique_ptr<PrefilterExpression> NotExpression::logicalComplement() const {
  // Logically we complement (negate) a NOT here => NOT cancels out.
  // Therefore, we can simply return the child of the respective NOT
  // expression after undoing its previous complementation.
  return child_->logicalComplement();
}

//______________________________________________________________________________
BlockMetadataRanges NotExpression::evaluateImpl(const Vocab& vocab,
                                                const ValueIdSubrange& idRange,
                                                BlockMetadataSpan blockRange,
                                                bool getTotalComplement) const {
  return child_->evaluateImpl(vocab, idRange, blockRange, getTotalComplement);
}

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
}

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
}

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
}

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
