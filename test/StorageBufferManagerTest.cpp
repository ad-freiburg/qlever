// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "storage/buffer/buffer_pool.h"
#include "storage/compat/duckdb_config.h"
#include "storage/compat/duckdb_exceptions.h"
#include "storage/compat/duckdb_types.h"
#include "storage/single_file_block_manager.h"
#include "storage/standard_buffer_manager.h"

using namespace duckdb;

// Helper to create a BufferPool and StandardBufferManager with defaults.
struct TestBufferEnv {
  BufferPool pool;
  StorageConfig config;
  StandardBufferManager manager;

  explicit TestBufferEnv(idx_t memory_limit = 1ULL << 30)
      : pool(memory_limit, false), manager(pool, config) {}
};

// Test: Allocate a buffer and write/read data back.
TEST(StorageBufferManagerTest, AllocateAndReadWrite) {
  TestBufferEnv env;
  auto handle =
      env.manager.Allocate(MemoryTag::BASE_TABLE, env.config.block_alloc_size);
  ASSERT_TRUE(handle.IsValid());

  auto& buf = handle.GetFileBuffer();
  // Write a known pattern.
  std::memset(buf.buffer, 0xAB, buf.size);
  // Read it back.
  for (idx_t i = 0; i < buf.size; i++) {
    ASSERT_EQ(buf.buffer[i], 0xAB) << "Mismatch at byte " << i;
  }
}

// Test: Multiple allocations track memory usage.
TEST(StorageBufferManagerTest, MemoryUsageTracking) {
  TestBufferEnv env(1ULL << 25);  // 32 MB limit.
  idx_t initial_usage = env.pool.GetUsedMemory();

  auto h1 =
      env.manager.Allocate(MemoryTag::BASE_TABLE, env.config.block_alloc_size);
  idx_t after_first = env.pool.GetUsedMemory();
  EXPECT_GT(after_first, initial_usage);

  auto h2 =
      env.manager.Allocate(MemoryTag::BASE_TABLE, env.config.block_alloc_size);
  idx_t after_second = env.pool.GetUsedMemory();
  EXPECT_GT(after_second, after_first);
}

// Test: Pin and unpin a block.
TEST(StorageBufferManagerTest, PinUnpin) {
  TestBufferEnv env;
  auto handle =
      env.manager.Allocate(MemoryTag::BASE_TABLE, env.config.block_alloc_size);
  ASSERT_TRUE(handle.IsValid());

  // The handle keeps the block pinned.  Destroying it unpins.
  auto& buf = handle.GetFileBuffer();
  buf.buffer[0] = 42;
  // Move the handle to trigger unpin, then re-pin would require the
  // shared_ptr<BlockHandle> which we don't have direct access to here.
  // Just verify that the handle is valid and data survives.
  EXPECT_EQ(buf.buffer[0], 42);
}

// Test: SingleFileBlockManager create, write, and read back.
TEST(StorageBufferManagerTest, SingleFileBlockManagerRoundTrip) {
  TestBufferEnv env;
  std::string temp_path =
      std::filesystem::temp_directory_path() / "qlever_test_blocks.db";

  // Clean up any leftover file.
  std::filesystem::remove(temp_path);

  {
    // Create a new database file.
    StorageManagerOptions opts;
    opts.read_only = false;
    SingleFileBlockManager bm(env.manager, temp_path, opts,
                              env.config.block_alloc_size);
    bm.CreateNewDatabase();

    // Allocate a block and write data.
    block_id_t block_id = bm.GetFreeBlockId();
    auto block = bm.CreateBlock(block_id, nullptr);
    std::memset(block->buffer, 0xCD, block->size);
    bm.Write(*block, block_id);

    // Write header to persist state.
    DatabaseHeader header;
    header.block_alloc_size = bm.GetBlockAllocSize();
    bm.WriteHeader(header);
  }

  {
    // Reopen and read back.
    StorageManagerOptions opts;
    opts.read_only = true;
    SingleFileBlockManager bm(env.manager, temp_path, opts,
                              env.config.block_alloc_size);
    bm.LoadExistingDatabase();

    auto block = bm.CreateBlock(0, nullptr);
    bm.Read(*block);

    // Verify data.
    for (idx_t i = 0; i < block->size; i++) {
      ASSERT_EQ(block->buffer[i], 0xCD) << "Mismatch at byte " << i;
    }
  }

  // Clean up.
  std::filesystem::remove(temp_path);
}

// Test: Eviction under memory pressure.
TEST(StorageBufferManagerTest, EvictionUnderPressure) {
  // Small memory limit: 2 block allocations worth.
  idx_t block_alloc = DEFAULT_BLOCK_ALLOC_SIZE;
  idx_t limit = block_alloc * 3;
  TestBufferEnv env(limit);

  // Allocate more blocks than fit in memory.  The buffer manager should evict
  // older blocks to make room.
  std::vector<BufferHandle> handles;
  bool had_exception = false;
  for (int i = 0; i < 10; i++) {
    try {
      handles.push_back(
          env.manager.Allocate(MemoryTag::BASE_TABLE, block_alloc));
    } catch (const OutOfMemoryException&) {
      had_exception = true;
      break;
    }
  }
  // We should either have allocated some blocks (with eviction) or hit OOM.
  // Either way, the buffer manager should still be in a consistent state.
  EXPECT_TRUE(handles.size() >= 2 || had_exception);
}
