// Copyright 2024, DuckDB contributors. Adapted for QLever.

#include "storage/block_manager.h"

#include "storage/buffer/block_handle.h"
#include "storage/buffer/buffer_pool.h"
#include "storage/buffer_manager.h"

namespace duckdb {

BlockManager::BlockManager(BufferManager& buffer_manager,
                           const optional_idx block_alloc_size_p)
    : buffer_manager(buffer_manager), block_alloc_size(block_alloc_size_p) {}

shared_ptr<BlockHandle> BlockManager::RegisterBlock(block_id_t block_id) {
  lock_guard<mutex> lock(blocks_lock);
  // Check if the block already exists.
  auto entry = blocks.find(block_id);
  if (entry != blocks.end()) {
    auto existing_ptr = entry->second.lock();
    if (existing_ptr) {
      return existing_ptr;
    }
  }
  // Create a new block pointer for this block.
  auto result =
      make_shared_ptr<BlockHandle>(*this, block_id, MemoryTag::BASE_TABLE);
  // Register the block pointer as a weak pointer.
  blocks[block_id] = weak_ptr<BlockHandle>(result);
  return result;
}

shared_ptr<BlockHandle> BlockManager::ConvertToPersistent(
    block_id_t block_id, shared_ptr<BlockHandle> old_block,
    BufferHandle old_handle) {
  // Register a block with the new block id.
  auto new_block = RegisterBlock(block_id);
  D_ASSERT(new_block->GetState() == BlockState::BLOCK_UNLOADED);
  D_ASSERT(new_block->Readers() == 0);

  auto lock = old_block->GetLock();
  D_ASSERT(old_block->GetState() == BlockState::BLOCK_LOADED);
  D_ASSERT(old_block->GetBuffer(lock));
  if (old_block->Readers() > 1) {
    throw InternalException(
        "BlockManager::ConvertToPersistent - cannot be called for block %d as "
        "old_block has multiple readers active",
        block_id);
  }

  D_ASSERT(old_block->GetBuffer(lock)->AllocSize() <= GetBlockAllocSize());

  // Convert the buffer to a block.
  auto converted_buffer = ConvertBlock(block_id, *old_block->GetBuffer(lock));

  // Persist the new block to disk.
  Write(*converted_buffer, block_id);

  // Now convert the actual block.
  old_block->ConvertToPersistent(lock, *new_block, std::move(converted_buffer));

  // Destroy the old buffer.
  lock.unlock();
  old_handle.Destroy();
  old_block.reset();

  // Potentially purge the queue.
  auto purge_queue =
      buffer_manager.GetBufferPool().AddToEvictionQueue(new_block);
  if (purge_queue) {
    buffer_manager.GetBufferPool().PurgeQueue(*new_block);
  }
  return new_block;
}

shared_ptr<BlockHandle> BlockManager::ConvertToPersistent(
    block_id_t block_id, shared_ptr<BlockHandle> old_block) {
  auto handle = buffer_manager.Pin(old_block);
  return ConvertToPersistent(block_id, std::move(old_block), std::move(handle));
}

void BlockManager::UnregisterBlock(block_id_t id) {
  D_ASSERT(id < MAXIMUM_BLOCK);
  lock_guard<mutex> lock(blocks_lock);
  blocks.erase(id);
}

void BlockManager::UnregisterBlock(BlockHandle& block) {
  auto id = block.BlockId();
  if (id >= MAXIMUM_BLOCK) {
    buffer_manager.DeleteTemporaryFile(block);
  } else {
    lock_guard<mutex> lock(blocks_lock);
    blocks.erase(id);
  }
}

void BlockManager::Truncate() {}

}  // namespace duckdb
