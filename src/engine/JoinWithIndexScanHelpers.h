// Copyright 2026, University of Freiburg
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
#include "util/MemoryHelpers.h"

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

}  // namespace qlever::joinWithIndexScanHelpers

#endif  // QLEVER_SRC_ENGINE_JOINWITHINDEXSCANHELPERS_H
