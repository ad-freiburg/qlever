// Copyright 2024, DuckDB contributors. Adapted for QLever.
// StandardBufferManager is the concrete implementation of BufferManager.
// Unlike the DuckDB original, it does not depend on DatabaseInstance.

#pragma once

#include "storage/block_manager.h"
#include "storage/buffer/block_handle.h"
#include "storage/buffer/buffer_pool.h"
#include "storage/buffer_manager.h"
#include "storage/compat/duckdb_allocator.h"
#include "storage/compat/duckdb_atomic.h"
#include "storage/compat/duckdb_config.h"
#include "storage/compat/duckdb_mutex.h"

namespace duckdb {

class BlockManager;
struct EvictionQueue;

// The StandardBufferManager manages memory buffers. It cooperatively shares a
// BufferPool with other buffer managers.
class StandardBufferManager : public BufferManager {
  friend class BufferHandle;
  friend class BlockHandle;
  friend class BlockManager;

 public:
  // Construct with a StorageConfig (replaces DatabaseInstance dependency).
  StandardBufferManager(BufferPool& pool, StorageConfig config);
  ~StandardBufferManager() override;

 public:
  // Registers an in-memory buffer that cannot be unloaded until destroyed.
  shared_ptr<BlockHandle> RegisterSmallMemory(MemoryTag tag,
                                              const idx_t size) final;

  idx_t GetUsedMemory() const final;
  idx_t GetMaxMemory() const final;
  idx_t GetUsedSwap() final;
  optional_idx GetMaxSwap() const final;
  idx_t GetBlockAllocSize() const final;
  idx_t GetBlockSize() const final;

  // Allocate an in-memory buffer with a single pin.
  BufferHandle Allocate(MemoryTag tag, idx_t block_size,
                        bool can_destroy = true) final;

  void ReAllocate(shared_ptr<BlockHandle>& handle, idx_t block_size) final;

  BufferHandle Pin(shared_ptr<BlockHandle>& handle) final;
  void Prefetch(vector<shared_ptr<BlockHandle>>& handles) final;
  void Unpin(shared_ptr<BlockHandle>& handle) final;

  void SetMemoryLimit(idx_t limit = (idx_t)-1) final;
  void SetSwapLimit(optional_idx limit = optional_idx()) final;

  vector<MemoryInformation> GetMemoryUsageInfo() const override;

  vector<TemporaryFileInformation> GetTemporaryFiles() final;

  const string& GetTemporaryDirectory() const final {
    return temporary_directory_;
  }

  void SetTemporaryDirectory(const string& new_dir) final;

  Allocator& GetBufferAllocator() final;

  unique_ptr<FileBuffer> ConstructManagedBuffer(
      idx_t size, unique_ptr<FileBuffer>&& source,
      FileBufferType type = FileBufferType::MANAGED_BUFFER) override;

  void ReserveMemory(idx_t size) final;
  void FreeReservedMemory(idx_t size) final;
  bool HasTemporaryDirectory() const final;

 protected:
  template <typename... ARGS>
  TempBufferPoolReservation EvictBlocksOrThrow(MemoryTag tag,
                                               idx_t memory_delta,
                                               unique_ptr<FileBuffer>* buffer,
                                               ARGS...);

  shared_ptr<BlockHandle> RegisterMemory(MemoryTag tag, idx_t block_size,
                                         bool can_destroy);

  void PurgeQueue(const BlockHandle& handle) final;

  BufferPool& GetBufferPool() const final;

  void WriteTemporaryBuffer(MemoryTag tag, block_id_t block_id,
                            FileBuffer& buffer) final;
  unique_ptr<FileBuffer> ReadTemporaryBuffer(
      MemoryTag tag, BlockHandle& block,
      unique_ptr<FileBuffer> buffer = nullptr) final;
  void DeleteTemporaryFile(BlockHandle& block) final;

  void AddToEvictionQueue(shared_ptr<BlockHandle>& handle) final;

  const char* InMemoryWarning();

 protected:
  // The buffer pool.
  BufferPool& buffer_pool;
  // The storage configuration.
  StorageConfig config_;
  // Temporary directory path.
  string temporary_directory_;
  // The temporary id used for managed buffers.
  atomic<block_id_t> temporary_id;
  // Allocator associated with the buffer manager.
  Allocator buffer_allocator;
  // Block manager for temp data.
  unique_ptr<BlockManager> temp_block_manager;
  // Temporary evicted memory data per tag.
  atomic<idx_t> evicted_data_per_tag[MEMORY_TAG_COUNT];
};

}  // namespace duckdb
