// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_JOINWITHINDEXSCANHELPERS_H
#define QLEVER_SRC_ENGINE_JOINWITHINDEXSCANHELPERS_H

#include "engine/AddCombinedRowToTable.h"
#include "engine/IndexScan.h"
#include "engine/Result.h"
#include "index/CompressedRelation.h"
#include "util/Iterators.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"
#include "util/JoinAlgorithms/JoinColumnMapping.h"

namespace qlever::joinWithIndexScanHelpers {

// Tag types to indicate the join semantics
struct InnerJoinTag {};
struct OptionalJoinTag {};
struct MinusTag {};

// Helper to convert generators to the format expected by join algorithms
using IteratorWithSingleCol =
    ad_utility::InputRangeTypeErased<ad_utility::IdTableAndFirstCol<IdTable>>;

inline IteratorWithSingleCol convertGenerator(
    CompressedRelationReader::IdTableGeneratorInputRange&& gen,
    IndexScan& scan) {
  // Store the generator in a wrapper so we can access its details after moving
  auto generatorStorage =
      std::make_shared<CompressedRelationReader::IdTableGeneratorInputRange>(
          std::move(gen));

  using SendPriority = RuntimeInformation::SendPriority;

  auto range = ad_utility::CachingTransformInputRange(
      *generatorStorage,
      [generatorStorage, &scan,
       sendPriority = SendPriority::Always](auto& table) mutable {
        scan.updateRuntimeInfoForLazyScan(generatorStorage->details(),
                                          sendPriority);
        sendPriority = SendPriority::IfDue;
        // IndexScans don't have a local vocabulary, so we can just use an empty
        // one.
        return ad_utility::IdTableAndFirstCol{std::move(table), LocalVocab{}};
      });

  return IteratorWithSingleCol{std::move(range)};
}

// Helper to get blocks for join based on number of join columns
inline std::array<CompressedRelationReader::IdTableGeneratorInputRange, 2>
getBlocksForJoinOfTwoScans(const IndexScan& s1, const IndexScan& s2,
                           size_t numJoinColumns) {
  AD_CONTRACT_CHECK(s1.numVariables() <= 3 && s2.numVariables() <= 3);
  AD_CONTRACT_CHECK(s1.numVariables() >= 1 && s2.numVariables() >= 1);

  auto metaBlocks1 = s1.getMetadataForScan();
  auto metaBlocks2 = s2.getMetadataForScan();

  if (!metaBlocks1.has_value() || !metaBlocks2.has_value()) {
    return {{}};
  }

  std::array<std::vector<CompressedBlockMetadata>, 2> blocks;
  if (numJoinColumns == 1) {
    blocks = CompressedRelationReader::getBlocksForJoin(metaBlocks1.value(),
                                                        metaBlocks2.value());
  } else {
    blocks = CompressedRelationReader::getBlocksForJoinMultiColumn(
        metaBlocks1.value(), metaBlocks2.value(), numJoinColumns);
  }

  std::array<CompressedRelationReader::IdTableGeneratorInputRange, 2> result{
      s1.getLazyScan(blocks[0]), s2.getLazyScan(blocks[1])};
  result[0].details().numBlocksAll_ = metaBlocks1.value().sizeBlockMetadata_;
  result[1].details().numBlocksAll_ = metaBlocks2.value().sizeBlockMetadata_;
  return result;
}

// Helper to check if the first row of any of the specified columns contains
// UNDEF values. Returns true if any join column in the first row is undefined,
// false otherwise. Returns false if the table is empty.
inline bool firstRowHasUndef(
    const IdTable& table,
    const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
    size_t sideIndex) {
  if (table.empty()) {
    return false;
  }
  for (const auto& jc : joinColumns) {
    if (table.at(0, jc[sideIndex]).isUndefined()) {
      return true;
    }
  }
  return false;
}

// Helper to get blocks for join of a column with a scan (multi-column version)
inline CompressedRelationReader::IdTableGeneratorInputRange
getBlocksForJoinOfColumnsWithScan(
    const IdTable& idTable,
    const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
    const IndexScan& scan, ColumnIndex scanJoinColIndex) {
  AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(
      idTable.getColumn(joinColumns[scanJoinColIndex][0])));
  AD_CORRECTNESS_CHECK(scan.numVariables() <= 3 && scan.numVariables() > 0);

  auto metaBlocks = scan.getMetadataForScan();
  if (!metaBlocks.has_value()) {
    return {};
  }

  // Cannot prefilter if first row has UNDEF in any join column
  if (firstRowHasUndef(idTable, joinColumns, 0)) {
    return {};
  }

  CompressedRelationReader::GetBlocksForJoinResult blocksResult;

  if (joinColumns.size() == 1) {
    auto joinColumn = idTable.getColumn(joinColumns[0][0]);
    blocksResult = CompressedRelationReader::getBlocksForJoin(
        joinColumn, metaBlocks.value());
  } else if (joinColumns.size() == 2) {
    auto col1 = idTable.getColumn(joinColumns[0][0]);
    auto col2 = idTable.getColumn(joinColumns[1][0]);
    blocksResult = CompressedRelationReader::getBlocksForJoinMultiColumn(
        col1, col2, metaBlocks.value());
  } else if (joinColumns.size() == 3) {
    auto col1 = idTable.getColumn(joinColumns[0][0]);
    auto col2 = idTable.getColumn(joinColumns[1][0]);
    auto col3 = idTable.getColumn(joinColumns[2][0]);
    blocksResult = CompressedRelationReader::getBlocksForJoinMultiColumn(
        col1, col2, col3, metaBlocks.value());
  } else {
    AD_FAIL();
  }

  auto result = scan.getLazyScan(std::move(blocksResult.matchingBlocks_));
  result.details().numBlocksAll_ = metaBlocks.value().sizeBlockMetadata_;
  return result;
}

// Helper to convert prefiltered lazy generators to the format expected by
// zipperJoinForBlocksWithPotentialUndef. Takes the left and right generators
// from prefilterTablesForOptional and converts them to ranges of
// IdTableAndFirstCol with appropriate column permutations applied.
inline auto convertPrefilteredGenerators(
    std::shared_ptr<Result::LazyResult> leftGenerator,
    std::shared_ptr<Result::LazyResult> rightGenerator, size_t leftWidth,
    ColumnIndex rightJoinColumn) {
  // Create identity permutation for left (all columns in order)
  std::vector<ColumnIndex> identityPerm(leftWidth);
  std::iota(identityPerm.begin(), identityPerm.end(), 0);

  auto leftRange = ad_utility::CachingTransformInputRange(
      std::move(*leftGenerator), [identityPerm](auto& pair) {
        return ad_utility::IdTableAndFirstCol{
            pair.idTable_.asColumnSubsetView(identityPerm),
            std::move(pair.localVocab_)};
      });

  // Right permutation puts the join column first
  std::vector<ColumnIndex> rightPerm = {rightJoinColumn};
  auto rightRange = ad_utility::CachingTransformInputRange(
      std::move(*rightGenerator), [rightPerm](auto& pair) {
        return ad_utility::IdTableAndFirstCol{
            pair.idTable_.asColumnSubsetView(rightPerm),
            std::move(pair.localVocab_)};
      });

  return std::pair{std::move(leftRange), std::move(rightRange)};
}

// Helper to set scan status to lazily completed (variadic, accepts 1+ scans)
template <typename... Scans>
inline void setScanStatusToLazilyCompleted(Scans&... scans) {
  (void(scans.runtimeInfo().status_ =
            RuntimeInformation::Status::lazilyMaterializedCompleted),
   ...);
}

// Helper to get unfiltered blocks for the left scan and filtered blocks for
// the right scan. Used by OptionalJoin and Minus where the left side must be
// complete and only the right side can be prefiltered.
inline auto getUnfilteredLeftAndFilteredRightSideFromIndexScans(
    IndexScan& leftScan, IndexScan& rightScan, size_t numJoinColumns) {
  auto leftMetaBlocks = leftScan.getMetadataForScan();

  auto leftBlocks = leftScan.getLazyScan(std::nullopt);
  leftBlocks.details().numBlocksAll_ =
      leftMetaBlocks.value().sizeBlockMetadata_;

  auto rightBlocks =
      getBlocksForJoinOfTwoScans(leftScan, rightScan, numJoinColumns);

  return std::pair{std::move(leftBlocks), std::move(rightBlocks[1])};
}

}  // namespace qlever::joinWithIndexScanHelpers

#endif  // QLEVER_SRC_ENGINE_JOINWITHINDEXSCANHELPERS_H
