// Copyright 2024, DuckDB contributors. Adapted for QLever.
// BufferManager is the abstract interface for managing memory buffers.

#pragma once

#include "storage/buffer/buffer_handle.h"
#include "storage/compat/duckdb_api.h"
#include "storage/compat/duckdb_optional_idx.h"
#include "storage/compat/duckdb_types.h"
#include "storage/file_buffer.h"
#include "storage/memory_tag.h"
#include "storage/storage_info.h"

namespace duckdb {

class Allocator;
class BlockHandle;
class BufferPool;

// Memory information per tag.
struct MemoryInformation {
  MemoryTag tag;
  idx_t size;
  idx_t evicted_data;
};

// Temporary file information.
struct TemporaryFileInformation {
  string path;
  idx_t size;
};

class BufferManager {
  friend class BufferHandle;
  friend class BlockHandle;
  friend class BlockManager;

 public:
  BufferManager() {}
  virtual ~BufferManager() {}

 public:
  virtual BufferHandle Allocate(MemoryTag tag, idx_t block_size,
                                bool can_destroy = true) = 0;
  virtual void ReAllocate(shared_ptr<BlockHandle>& handle,
                          idx_t block_size) = 0;
  virtual BufferHandle Pin(shared_ptr<BlockHandle>& handle) = 0;
  virtual void Prefetch(vector<shared_ptr<BlockHandle>>& handles) = 0;
  virtual void Unpin(shared_ptr<BlockHandle>& handle) = 0;

  virtual idx_t GetUsedMemory() const = 0;
  virtual idx_t GetMaxMemory() const = 0;
  virtual idx_t GetUsedSwap() = 0;
  virtual optional_idx GetMaxSwap() const = 0;
  virtual idx_t GetBlockAllocSize() const = 0;
  virtual idx_t GetBlockSize() const = 0;

  virtual shared_ptr<BlockHandle> RegisterSmallMemory(MemoryTag tag,
                                                      const idx_t size);

  virtual Allocator& GetBufferAllocator();
  virtual void ReserveMemory(idx_t size);
  virtual void FreeReservedMemory(idx_t size);
  virtual vector<MemoryInformation> GetMemoryUsageInfo() const = 0;

  virtual void SetMemoryLimit(idx_t limit = (idx_t)-1);
  virtual void SetSwapLimit(optional_idx limit = optional_idx());

  virtual vector<TemporaryFileInformation> GetTemporaryFiles();
  virtual const string& GetTemporaryDirectory() const;
  virtual void SetTemporaryDirectory(const string& new_dir);
  virtual bool HasTemporaryDirectory() const;

  virtual unique_ptr<FileBuffer> ConstructManagedBuffer(
      idx_t size, unique_ptr<FileBuffer>&& source,
      FileBufferType type = FileBufferType::MANAGED_BUFFER);

  virtual BufferPool& GetBufferPool() const;

  static idx_t GetAllocSize(const idx_t block_size) {
    return AlignValue<idx_t, Storage::SECTOR_SIZE>(
        block_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);
  }

  idx_t GetQueryMaxMemory() const;

 protected:
  virtual void PurgeQueue(const BlockHandle& handle) = 0;
  virtual void AddToEvictionQueue(shared_ptr<BlockHandle>& handle);
  virtual void WriteTemporaryBuffer(MemoryTag tag, block_id_t block_id,
                                    FileBuffer& buffer);
  virtual unique_ptr<FileBuffer> ReadTemporaryBuffer(
      MemoryTag tag, BlockHandle& block, unique_ptr<FileBuffer> buffer);
  virtual void DeleteTemporaryFile(BlockHandle& block);
};

}  // namespace duckdb
