// Copyright 2024, DuckDB contributors. Adapted for QLever.
// BufferPool manages memory and eviction across buffer managers.

#pragma once

#include <array>
#include <chrono>

#include "storage/buffer/block_handle.h"
#include "storage/compat/duckdb_mutex.h"
#include "storage/compat/duckdb_types.h"
#include "storage/file_buffer.h"
#include "storage/memory_tag.h"

namespace duckdb {

struct EvictionQueue;

struct BufferEvictionNode {
  BufferEvictionNode() {}
  BufferEvictionNode(weak_ptr<BlockHandle> handle_p, idx_t eviction_seq_num);

  weak_ptr<BlockHandle> handle;
  idx_t handle_sequence_number;

  bool CanUnload(BlockHandle& handle_p);
  shared_ptr<BlockHandle> TryGetBlockHandle();
};

// The BufferPool is in charge of handling memory management for one or more
// databases.
class BufferPool {
  friend class BlockHandle;
  friend class BlockManager;
  friend class BufferManager;
  friend class StandardBufferManager;

 public:
  BufferPool(idx_t maximum_memory, bool track_eviction_timestamps);
  virtual ~BufferPool();

  // Set a new memory limit to the buffer pool.
  void SetLimit(idx_t limit, const char* exception_postscript);

  void UpdateUsedMemory(MemoryTag tag, int64_t size);

  idx_t GetUsedMemory() const;

  idx_t GetMaxMemory() const;

  virtual idx_t GetQueryMaxMemory() const;

 protected:
  // Eviction result — indicates success and holds the reservation.
  struct EvictionResult {
    bool success;
    TempBufferPoolReservation reservation;
  };
  virtual EvictionResult EvictBlocks(MemoryTag tag, idx_t extra_memory,
                                     idx_t memory_limit,
                                     unique_ptr<FileBuffer>* buffer = nullptr);
  virtual EvictionResult EvictBlocksInternal(
      EvictionQueue& queue, MemoryTag tag, idx_t extra_memory,
      idx_t memory_limit, unique_ptr<FileBuffer>* buffer = nullptr);

  // Purge all blocks that haven't been pinned within the last N seconds.
  idx_t PurgeAgedBlocks(uint32_t max_age_sec);
  idx_t PurgeAgedBlocksInternal(EvictionQueue& queue, uint32_t max_age_sec,
                                int64_t now, int64_t limit);
  // Garbage collect dead nodes in the eviction queue.
  void PurgeQueue(const BlockHandle& handle);
  // Add a buffer handle to the eviction queue.
  bool AddToEvictionQueue(shared_ptr<BlockHandle>& handle);
  // Gets the eviction queue for the specified type.
  EvictionQueue& GetEvictionQueueForBlockHandle(const BlockHandle& handle);
  // Increments the dead nodes for the queue with specified type.
  void IncrementDeadNodes(const BlockHandle& handle);

  // How many eviction queues for the different FileBufferTypes.
  static constexpr idx_t BLOCK_QUEUE_SIZE = 1;
  static constexpr idx_t MANAGED_BUFFER_QUEUE_SIZE = 6;
  static constexpr idx_t TINY_BUFFER_QUEUE_SIZE = 1;
  // Mapping and priority order for the eviction queues.
  const std::array<idx_t, FILE_BUFFER_TYPE_COUNT> eviction_queue_sizes;

 protected:
  enum class MemoryUsageCaches {
    FLUSH,
    NO_FLUSH,
  };

  struct MemoryUsage {
    static constexpr idx_t MEMORY_USAGE_CACHE_COUNT = 64;
    static constexpr idx_t MEMORY_USAGE_CACHE_THRESHOLD = 32 << 10;
    static constexpr idx_t TOTAL_MEMORY_USAGE_INDEX = MEMORY_TAG_COUNT;
    using MemoryUsageCounters =
        std::array<atomic<int64_t>, MEMORY_TAG_COUNT + 1>;

    // Global memory usage counters.
    MemoryUsageCounters memory_usage;
    // Cache memory usage to improve performance.
    std::array<MemoryUsageCounters, MEMORY_USAGE_CACHE_COUNT>
        memory_usage_caches;

    MemoryUsage();

    idx_t GetUsedMemory(MemoryUsageCaches cache) {
      return GetUsedMemory(TOTAL_MEMORY_USAGE_INDEX, cache);
    }

    idx_t GetUsedMemory(MemoryTag tag, MemoryUsageCaches cache) {
      return GetUsedMemory((idx_t)tag, cache);
    }

    idx_t GetUsedMemory(idx_t index, MemoryUsageCaches cache) {
      if (cache == MemoryUsageCaches::NO_FLUSH) {
        auto used_memory = memory_usage[index].load(std::memory_order_relaxed);
        return used_memory > 0 ? static_cast<idx_t>(used_memory) : 0;
      }
      int64_t cached = 0;
      for (auto& c : memory_usage_caches) {
        cached += c[index].exchange(0, std::memory_order_relaxed);
      }
      auto used_memory =
          memory_usage[index].fetch_add(cached, std::memory_order_relaxed) +
          cached;
      return used_memory > 0 ? static_cast<idx_t>(used_memory) : 0;
    }

    void UpdateUsedMemory(MemoryTag tag, int64_t size);
  };

  // The lock for changing the memory limit.
  mutex limit_lock;
  // The maximum amount of memory that the buffer manager can keep (in bytes).
  atomic<idx_t> maximum_memory;
  // Record timestamps of buffer manager unpin() events.
  bool track_eviction_timestamps;
  // Eviction queues.
  vector<unique_ptr<EvictionQueue>> queues;
  // Memory usage tracking.
  mutable MemoryUsage memory_usage;
};

}  // namespace duckdb
