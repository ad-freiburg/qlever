// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#ifndef QLEVER_SRC_ENGINE_PARTITIONEDJOINHELPERS_H
#define QLEVER_SRC_ENGINE_PARTITIONEDJOINHELPERS_H

#include <vector>

#include "global/Id.h"
#include "index/CompressedRelation.h"
#include "util/MemorySize/MemorySize.h"

namespace qlever::partitionedJoin {

// Describes one partition of block ranges from an IndexScan. Values are
// owned canonically: a value V belongs to the smallest partition whose range
// includes V.
struct PartitionBoundary {
  // First join-column value owned by this partition.
  Id firstJoinValue;
  // Last join-column value owned by this partition.
  Id lastJoinValue;
  // First block index (inclusive) in the metadata view.
  size_t beginBlockIdx;
  // Last block index (exclusive), core blocks only.
  size_t endBlockIdx;
  // Extended to cover trailing blocks that share boundary values with the
  // next partition (for overlap reads).
  size_t endBlockIdxWithOverlap;
};

// Compute partition boundaries from block metadata. Each partition's
// decompressed data should fit within `ramBudget`. The `metadataAndBlocks`
// provides block metadata and the scan specification needed to extract the
// join column value from each block's first/last triple.
inline std::vector<PartitionBoundary> computePartitionBoundaries(
    const CompressedRelationReader::ScanSpecAndBlocksAndBounds&
        metadataAndBlocks,
    ad_utility::MemorySize ramBudget, size_t numColumnsInScan) {
  std::vector<CompressedBlockMetadata> blockVec(
      metadataAndBlocks.getBlockMetadataView().begin(),
      metadataAndBlocks.getBlockMetadataView().end());

  if (blockVec.empty()) {
    return {};
  }

  size_t budgetBytes = ramBudget.getBytes();
  std::vector<PartitionBoundary> partitions;

  size_t currentBegin = 0;
  size_t accumulatedSize = 0;

  auto getJoinValue =
      [&metadataAndBlocks](
          const CompressedBlockMetadata::PermutedTriple& triple) -> Id {
    return CompressedRelationReader::getRelevantIdFromTriple(triple,
                                                             metadataAndBlocks);
  };

  auto finalizePartition = [&](size_t endIdx) {
    PartitionBoundary pb;
    pb.firstJoinValue = getJoinValue(blockVec[currentBegin].firstTriple_);
    pb.lastJoinValue = getJoinValue(blockVec[endIdx - 1].lastTriple_);
    pb.beginBlockIdx = currentBegin;
    pb.endBlockIdx = endIdx;
    pb.endBlockIdxWithOverlap = endIdx;
    partitions.push_back(pb);
    currentBegin = endIdx;
    accumulatedSize = 0;
  };

  for (size_t i = 0; i < blockVec.size(); ++i) {
    size_t blockSize = blockVec[i].numRows_ * numColumnsInScan * sizeof(Id);
    accumulatedSize += blockSize;

    // If this single block exceeds the budget, it forms its own partition.
    if (accumulatedSize > budgetBytes && i == currentBegin) {
      finalizePartition(i + 1);
      continue;
    }

    // If accumulated size exceeds budget, finalize the partition up to (but
    // not including) this block.
    if (accumulatedSize > budgetBytes) {
      finalizePartition(i);
      // Re-account the current block in the new partition.
      accumulatedSize = blockSize;
    }
  }

  // Finalize the last partition.
  if (currentBegin < blockVec.size()) {
    finalizePartition(blockVec.size());
  }

  // Compute overlap: if P_i.lastJoinValue equals the first join value
  // of blocks at the start of P_{i+1}, extend P_i's overlap range to
  // include those blocks.
  for (size_t i = 0; i + 1 < partitions.size(); ++i) {
    Id boundaryValue = partitions[i].lastJoinValue;
    size_t overlapEnd = partitions[i].endBlockIdx;
    // Extend into subsequent partition's blocks that start with the same
    // value.
    while (overlapEnd < partitions[i + 1].endBlockIdx) {
      Id nextBlockFirst = getJoinValue(blockVec[overlapEnd].firstTriple_);
      if (nextBlockFirst <= boundaryValue) {
        ++overlapEnd;
      } else {
        break;
      }
    }
    partitions[i].endBlockIdxWithOverlap = overlapEnd;
  }

  return partitions;
}

// Binary search for the smallest partition whose range includes `joinVal`.
// Returns the partition index, or `std::nullopt` if no partition contains
// the value.
inline std::optional<size_t> findPartition(
    Id joinVal, const std::vector<PartitionBoundary>& partitions) {
  if (partitions.empty()) {
    return std::nullopt;
  }

  // Binary search: find the first partition where lastJoinValue >= joinVal.
  size_t lo = 0;
  size_t hi = partitions.size();
  while (lo < hi) {
    size_t mid = lo + (hi - lo) / 2;
    if (partitions[mid].lastJoinValue < joinVal) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }

  if (lo >= partitions.size()) {
    return std::nullopt;
  }

  // Verify that joinVal >= firstJoinValue of this partition.
  if (joinVal < partitions[lo].firstJoinValue) {
    return std::nullopt;
  }

  return lo;
}

}  // namespace qlever::partitionedJoin

#endif  // QLEVER_SRC_ENGINE_PARTITIONEDJOINHELPERS_H
