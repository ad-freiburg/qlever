// Copyright 2024, DuckDB contributors. Adapted for QLever.
// BlockHandle manages a single block of memory in the buffer pool.

#pragma once

#include "storage/compat/duckdb_assert.h"
#include "storage/compat/duckdb_atomic.h"
#include "storage/compat/duckdb_mutex.h"
#include "storage/compat/duckdb_optional_idx.h"
#include "storage/compat/duckdb_types.h"
#include "storage/file_buffer.h"
#include "storage/memory_tag.h"
#include "storage/storage_info.h"

namespace duckdb {

class BlockManager;
class BufferHandle;
class BufferPool;

enum class BlockState : uint8_t { BLOCK_UNLOADED = 0, BLOCK_LOADED = 1 };

// Controls when the data buffer is destroyed.
enum class DestroyBufferUpon : uint8_t {
  // Destroy on unpin (buffer is not written to temp file).
  UNPIN = 0,
  // Destroy on eviction (buffer may be written to temp file).
  EVICTION = 1,
  // Never destroy implicitly (block must persist).
  BLOCK = 2
};

struct BufferPoolReservation {
  MemoryTag tag;
  idx_t size{0};
  BufferPool& pool;

  BufferPoolReservation(MemoryTag tag, BufferPool& pool);
  BufferPoolReservation(const BufferPoolReservation&) = delete;
  BufferPoolReservation& operator=(const BufferPoolReservation&) = delete;

  BufferPoolReservation(BufferPoolReservation&&) noexcept;
  BufferPoolReservation& operator=(BufferPoolReservation&&) noexcept;

  ~BufferPoolReservation();

  void Resize(idx_t new_size);
  void Merge(BufferPoolReservation src);
};

struct TempBufferPoolReservation : BufferPoolReservation {
  TempBufferPoolReservation(MemoryTag tag, BufferPool& pool, idx_t size)
      : BufferPoolReservation(tag, pool) {
    Resize(size);
  }
  TempBufferPoolReservation(TempBufferPoolReservation&&) = default;
  ~TempBufferPoolReservation() { Resize(0); }
};

using BlockLock = unique_lock<mutex>;

class BlockHandle : public enable_shared_from_this<BlockHandle> {
 public:
  BlockHandle(BlockManager& block_manager, block_id_t block_id, MemoryTag tag);
  BlockHandle(BlockManager& block_manager, block_id_t block_id, MemoryTag tag,
              unique_ptr<FileBuffer> buffer,
              DestroyBufferUpon destroy_buffer_upon, idx_t block_size,
              BufferPoolReservation&& reservation);
  ~BlockHandle();

  BlockManager& block_manager;

 public:
  block_id_t BlockId() const { return block_id; }

  idx_t EvictionSequenceNumber() const { return eviction_seq_num; }

  idx_t NextEvictionSequenceNumber() { return ++eviction_seq_num; }

  int32_t Readers() const { return readers; }
  int32_t DecrementReaders() { return --readers; }

  inline bool IsSwizzled() const { return !unswizzled; }

  inline void SetSwizzling(const char* unswizzler) { unswizzled = unswizzler; }

  MemoryTag GetMemoryTag() const { return tag; }

  inline void SetDestroyBufferUpon(DestroyBufferUpon destroy_buffer_upon_p) {
    destroy_buffer_upon = destroy_buffer_upon_p;
  }

  inline bool MustAddToEvictionQueue() const {
    return destroy_buffer_upon != DestroyBufferUpon::UNPIN;
  }

  inline bool MustWriteToTemporaryFile() const {
    return destroy_buffer_upon == DestroyBufferUpon::BLOCK;
  }

  inline idx_t GetMemoryUsage() const { return memory_usage; }

  bool IsUnloaded() const { return state == BlockState::BLOCK_UNLOADED; }

  void SetEvictionQueueIndex(const idx_t index) {
    D_ASSERT(eviction_queue_idx == DConstants::INVALID_INDEX);
    D_ASSERT(GetBufferType() == FileBufferType::MANAGED_BUFFER);
    eviction_queue_idx = index;
  }

  idx_t GetEvictionQueueIndex() const { return eviction_queue_idx; }

  FileBufferType GetBufferType() const { return buffer_type; }

  BlockState GetState() const { return state; }

  int64_t GetLRUTimestamp() const { return lru_timestamp_msec; }

  void SetLRUTimestamp(int64_t timestamp_msec) {
    lru_timestamp_msec = timestamp_msec;
  }

  BlockLock GetLock() { return BlockLock(lock); }

  // Gets a reference to the buffer — the lock must be held.
  unique_ptr<FileBuffer>& GetBuffer(BlockLock& l);

  void ChangeMemoryUsage(BlockLock& l, int64_t delta);
  BufferPoolReservation& GetMemoryCharge(BlockLock& l);
  // Merge a new memory reservation.
  void MergeMemoryReservation(BlockLock&, BufferPoolReservation reservation);
  // Resize the memory allocation.
  void ResizeMemory(BlockLock&, idx_t alloc_size);

  // Resize the actual buffer.
  void ResizeBuffer(BlockLock&, idx_t block_size, int64_t memory_delta);
  BufferHandle Load(unique_ptr<FileBuffer> buffer = nullptr);
  BufferHandle LoadFromBuffer(BlockLock& l, data_ptr_t data,
                              unique_ptr<FileBuffer> reusable_buffer,
                              BufferPoolReservation reservation);
  unique_ptr<FileBuffer> UnloadAndTakeBlock(BlockLock&);
  void Unload(BlockLock&);

  // Returns whether or not the block can be unloaded.
  bool CanUnload() const;

  void ConvertToPersistent(BlockLock&, BlockHandle& new_block,
                           unique_ptr<FileBuffer> new_buffer);

 private:
  void VerifyMutex(unique_lock<mutex>& l) const;

 private:
  // The block-level lock.
  mutex lock;
  // Whether or not the block is loaded/unloaded.
  atomic<BlockState> state;
  // Amount of concurrent readers.
  atomic<int32_t> readers;
  // The block id of the block.
  const block_id_t block_id;
  // Memory tag.
  const MemoryTag tag;
  // File buffer type.
  const FileBufferType buffer_type;
  // Pointer to loaded data (if any).
  unique_ptr<FileBuffer> buffer;
  // Internal eviction sequence number.
  atomic<idx_t> eviction_seq_num;
  // LRU timestamp (for age-based eviction).
  atomic<int64_t> lru_timestamp_msec;
  // When to destroy the data buffer.
  atomic<DestroyBufferUpon> destroy_buffer_upon;
  // The memory usage of the block (when loaded).
  atomic<idx_t> memory_usage;
  // Current memory reservation / usage.
  BufferPoolReservation memory_charge;
  // Does the block contain any memory pointers?
  const char* unswizzled;
  // Index for eviction queue (FileBufferType::MANAGED_BUFFER only).
  atomic<idx_t> eviction_queue_idx;
};

}  // namespace duckdb
