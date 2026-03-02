// Copyright 2024, DuckDB contributors. Adapted for QLever.
// BlockManager is the abstract interface for managing blocks on disk.

#pragma once

#include <unordered_map>

#include "storage/block.h"
#include "storage/compat/duckdb_exceptions.h"
#include "storage/compat/duckdb_mutex.h"
#include "storage/compat/duckdb_optional_idx.h"
#include "storage/compat/duckdb_types.h"
#include "storage/storage_info.h"

namespace duckdb {

class BlockHandle;
class BufferHandle;
class BufferManager;

// BlockManager is an abstract representation to manage blocks.
class BlockManager {
 public:
  BlockManager() = delete;
  BlockManager(BufferManager& buffer_manager,
               const optional_idx block_alloc_size_p);
  virtual ~BlockManager() = default;

  // The buffer manager.
  BufferManager& buffer_manager;

 public:
  // Creates a new block inside the block manager.
  virtual unique_ptr<Block> ConvertBlock(block_id_t block_id,
                                         FileBuffer& source_buffer) = 0;
  virtual unique_ptr<Block> CreateBlock(block_id_t block_id,
                                        FileBuffer* source_buffer) = 0;
  // Return the next free block id.
  virtual block_id_t GetFreeBlockId() = 0;
  virtual block_id_t PeekFreeBlockId() = 0;
  // Returns whether or not a specified block is the root block.
  virtual bool IsRootBlock(MetaBlockPointer root) = 0;
  // Mark a block as "free".
  virtual void MarkBlockAsFree(block_id_t block_id) = 0;
  // Mark a block as "used".
  virtual void MarkBlockAsUsed(block_id_t block_id) = 0;
  // Mark a block as "modified".
  virtual void MarkBlockAsModified(block_id_t block_id) = 0;
  // Increase the reference count of a block.
  virtual void IncreaseBlockReferenceCount(block_id_t block_id) = 0;
  // Get the first meta block id.
  virtual idx_t GetMetaBlock() = 0;
  // Read the content of the block from disk.
  virtual void Read(Block& block) = 0;
  // Read a range of blocks.
  virtual void ReadBlocks(FileBuffer& buffer, block_id_t start_block,
                          idx_t block_count) = 0;
  // Write the block to disk.
  virtual void Write(FileBuffer& block, block_id_t block_id) = 0;
  void Write(Block& block) { Write(block, block.id); }
  // Write the header; should be the final step of a checkpoint.
  virtual void WriteHeader(DatabaseHeader header) = 0;

  virtual idx_t TotalBlocks() = 0;
  virtual idx_t FreeBlocks() = 0;
  virtual bool IsRemote() { return false; }
  virtual bool InMemory() = 0;

  virtual void FileSync() = 0;
  virtual void Truncate();

  // Register a block with the given block id.
  shared_ptr<BlockHandle> RegisterBlock(block_id_t block_id);
  // Convert an existing in-memory buffer into a persistent disk-backed block.
  shared_ptr<BlockHandle> ConvertToPersistent(block_id_t block_id,
                                              shared_ptr<BlockHandle> old_block,
                                              BufferHandle old_handle);
  shared_ptr<BlockHandle> ConvertToPersistent(
      block_id_t block_id, shared_ptr<BlockHandle> old_block);

  void UnregisterBlock(BlockHandle& block);
  void UnregisterBlock(block_id_t id);

  // Returns the block allocation size of this block manager.
  inline idx_t GetBlockAllocSize() const { return block_alloc_size.GetIndex(); }
  inline optional_idx GetOptionalBlockAllocSize() const {
    return block_alloc_size;
  }
  // Returns the block size of this block manager.
  inline idx_t GetBlockSize() const {
    return block_alloc_size.GetIndex() - Storage::DEFAULT_BLOCK_HEADER_SIZE;
  }
  // Sets the block allocation size.
  void SetBlockAllocSize(const optional_idx block_alloc_size_p) {
    if (block_alloc_size.IsValid()) {
      if (block_alloc_size.GetIndex() == block_alloc_size_p.GetIndex()) {
        return;  // Already set to the same value, no-op.
      }
      throw InternalException("the block allocation size must be set once");
    }
    block_alloc_size = block_alloc_size_p.GetIndex();
  }

 private:
  // The lock for the set of blocks.
  mutex blocks_lock;
  // A mapping of block id -> BlockHandle.
  std::unordered_map<block_id_t, weak_ptr<BlockHandle>> blocks;
  // The allocation size of blocks managed by this block manager.
  optional_idx block_alloc_size;
};

}  // namespace duckdb
