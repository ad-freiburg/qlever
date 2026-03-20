// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#ifndef QLEVER_SRC_ENGINE_PARTITIONBUCKET_H
#define QLEVER_SRC_ENGINE_PARTITIONBUCKET_H

#include <cstring>
#include <vector>

#include "engine/Engine.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "storage/buffer/block_handle.h"
#include "storage/buffer/buffer_handle.h"
#include "storage/memory_tag.h"
#include "storage/standard_buffer_manager.h"

namespace qlever::partitionedJoin {

// A bucket that accumulates rows for a single partition during the
// partitioning pass of a partitioned join. Rows are stored as flat row-major
// bytes in buffer-managed blocks. Completed blocks are unpinned and eligible
// for eviction to disk under memory pressure.
class PartitionBucket {
 public:
  // `numColumns` is the width of each row.
  explicit PartitionBucket(size_t numColumns,
                           ad_utility::AllocatorWithLimit<Id> allocator,
                           duckdb::StandardBufferManager& bufferManager)
      : numColumns_(numColumns),
        allocator_(std::move(allocator)),
        bufferManager_(bufferManager),
        rowBytes_(numColumns * sizeof(Id)),
        currentHandle_(
            bufferManager_.Allocate(duckdb::MemoryTag::ORDER_BY,
                                    bufferManager_.GetBlockAllocSize(), false)),
        pageCapacity_(currentHandle_.GetFileBuffer().size / rowBytes_) {}

  // Append a single row to this bucket.
  template <typename Row>
  void append(const Row& row) {
    if (currentPageRows_ >= pageCapacity_) {
      flushCurrentPage();
    }
    duckdb::data_ptr_t dst =
        currentHandle_.Ptr() + currentPageRows_ * rowBytes_;
    for (size_t c = 0; c < numColumns_; ++c) {
      std::memcpy(dst + c * sizeof(Id), &row[c], sizeof(Id));
    }
    ++currentPageRows_;
    ++numRows_;
  }

  // Materialize all pages into a single IdTable sorted by `joinCol`.
  IdTable materializeAndSort(ColumnIndex joinCol) {
    IdTable result(numColumns_, allocator_);
    result.resize(numRows_);
    size_t resultRow = 0;

    // Copy from completed (possibly evicted) blocks.
    for (size_t b = 0; b < completedBlocks_.size(); ++b) {
      auto handle = bufferManager_.Pin(completedBlocks_[b]);
      const Id* src = reinterpret_cast<const Id*>(handle.Ptr());
      size_t rows = completedBlockRowCounts_[b];
      for (size_t r = 0; r < rows; ++r) {
        for (size_t c = 0; c < numColumns_; ++c) {
          result(resultRow, c) = src[r * numColumns_ + c];
        }
        ++resultRow;
      }
    }

    // Copy from current (still pinned) page.
    const Id* src = reinterpret_cast<const Id*>(currentHandle_.Ptr());
    for (size_t r = 0; r < currentPageRows_; ++r) {
      for (size_t c = 0; c < numColumns_; ++c) {
        result(resultRow, c) = src[r * numColumns_ + c];
      }
      ++resultRow;
    }

    // Sort by join column using the engine's parallel sort.
    Engine::sort(result, std::vector<ColumnIndex>{joinCol});

    // Release all blocks.
    completedBlocks_.clear();
    completedBlockRowCounts_.clear();
    currentHandle_ = duckdb::BufferHandle{};

    return result;
  }

  // Call `callback(IdTable&)` for each block's worth of rows, one block at a
  // time. Only one block is pinned at a time, keeping RAM usage constant. After
  // iteration, all blocks are released.
  template <typename Callback>
  void forEachBlock(Callback callback) {
    for (size_t b = 0; b < completedBlocks_.size(); ++b) {
      auto handle = bufferManager_.Pin(completedBlocks_[b]);
      const Id* src = reinterpret_cast<const Id*>(handle.Ptr());
      IdTable table(numColumns_, allocator_);
      table.resize(completedBlockRowCounts_[b]);
      for (size_t r = 0; r < completedBlockRowCounts_[b]; ++r) {
        for (size_t c = 0; c < numColumns_; ++c) {
          table(r, c) = src[r * numColumns_ + c];
        }
      }
      callback(table);
    }
    // Current (still pinned) page.
    if (currentPageRows_ > 0) {
      const Id* src = reinterpret_cast<const Id*>(currentHandle_.Ptr());
      IdTable table(numColumns_, allocator_);
      table.resize(currentPageRows_);
      for (size_t r = 0; r < currentPageRows_; ++r) {
        for (size_t c = 0; c < numColumns_; ++c) {
          table(r, c) = src[r * numColumns_ + c];
        }
      }
      callback(table);
    }
    // Release all blocks.
    completedBlocks_.clear();
    completedBlockRowCounts_.clear();
    currentHandle_ = duckdb::BufferHandle{};
  }

  bool empty() const { return numRows_ == 0; }
  size_t numRows() const { return numRows_; }

 private:
  void flushCurrentPage() {
    completedBlocks_.push_back(currentHandle_.GetBlockHandle());
    completedBlockRowCounts_.push_back(currentPageRows_);
    // Unpin the current block by replacing the handle.
    currentHandle_ = duckdb::BufferHandle{};
    currentHandle_ = bufferManager_.Allocate(
        duckdb::MemoryTag::ORDER_BY, bufferManager_.GetBlockAllocSize(), false);
    currentPageRows_ = 0;
  }

  size_t numColumns_;
  ad_utility::AllocatorWithLimit<Id> allocator_;
  duckdb::StandardBufferManager& bufferManager_;
  size_t rowBytes_;
  // Current page: pinned for active writes.
  duckdb::BufferHandle currentHandle_;
  size_t pageCapacity_;
  // Completed pages: unpinned block handles eligible for eviction.
  std::vector<duckdb::shared_ptr<duckdb::BlockHandle>> completedBlocks_;
  std::vector<size_t> completedBlockRowCounts_;
  size_t currentPageRows_ = 0;
  size_t numRows_ = 0;
};

}  // namespace qlever::partitionedJoin

#endif  // QLEVER_SRC_ENGINE_PARTITIONBUCKET_H
