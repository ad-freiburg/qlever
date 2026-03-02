// Copyright 2024, DuckDB contributors. Adapted for QLever.
// SingleFileBlockManager manages blocks in a single file on disk.
// Simplified: no MetadataManager, no checksum, no WriteStream/ReadStream.

#pragma once

#include <set>
#include <unordered_map>
#include <unordered_set>

#include "storage/block.h"
#include "storage/block_manager.h"
#include "storage/compat/duckdb_allocator.h"
#include "storage/compat/duckdb_config.h"
#include "storage/compat/duckdb_file_system.h"
#include "storage/compat/duckdb_mutex.h"

namespace duckdb {

// Options for the block manager.
struct StorageManagerOptions {
  bool read_only = false;
};

// SingleFileBlockManager manages blocks in a single file.
class SingleFileBlockManager : public BlockManager {
  // The location in the file where block writing starts.
  static constexpr uint64_t BLOCK_START = Storage::FILE_HEADER_SIZE * 3;

 public:
  SingleFileBlockManager(BufferManager& buffer_manager, const string& path,
                         const StorageManagerOptions& options,
                         optional_idx block_alloc_size);

  // Creates a new database file.
  void CreateNewDatabase();
  // Loads an existing database file.
  void LoadExistingDatabase();

  unique_ptr<Block> ConvertBlock(block_id_t block_id,
                                 FileBuffer& source_buffer) override;
  unique_ptr<Block> CreateBlock(block_id_t block_id,
                                FileBuffer* source_buffer) override;
  block_id_t GetFreeBlockId() override;
  block_id_t PeekFreeBlockId() override;
  bool IsRootBlock(MetaBlockPointer root) override;
  void MarkBlockAsFree(block_id_t block_id) override;
  void MarkBlockAsUsed(block_id_t block_id) override;
  void MarkBlockAsModified(block_id_t block_id) override;
  void IncreaseBlockReferenceCount(block_id_t block_id) override;
  idx_t GetMetaBlock() override;
  void Read(Block& block) override;
  void ReadBlocks(FileBuffer& buffer, block_id_t start_block,
                  idx_t block_count) override;
  void Write(FileBuffer& block, block_id_t block_id) override;
  void WriteHeader(DatabaseHeader header) override;
  void FileSync() override;
  void Truncate() override;

  bool InMemory() override { return false; }
  idx_t TotalBlocks() override;
  idx_t FreeBlocks() override;
  bool IsRemote() override { return false; }

 private:
  void Initialize(const DatabaseHeader& header,
                  const optional_idx block_alloc_size);
  idx_t GetBlockLocation(block_id_t block_id);
  void IncreaseBlockReferenceCountInternal(block_id_t block_id);

 private:
  // Allocator for block memory.
  Allocator allocator_;
  // Whether the database is read-only.
  bool read_only_;
  // The active DatabaseHeader, either 0 (h1) or 1 (h2).
  uint8_t active_header;
  // The path where the file is stored.
  string path;
  // The file handle.
  unique_ptr<FileHandle> handle;
  // The buffer used to read/write to the headers.
  FileBuffer header_buffer;
  // The list of free blocks.
  std::set<block_id_t> free_list;
  // The list of blocks freed since last checkpoint.
  std::set<block_id_t> newly_freed_list;
  // Multi-use block reference counts.
  std::unordered_map<block_id_t, uint32_t> multi_use_blocks;
  // Blocks that will be added to the free list.
  std::unordered_set<block_id_t> modified_blocks;
  // The current meta block id.
  idx_t meta_block;
  // The current maximum block id.
  block_id_t max_block;
  // The block id where the free list can be found.
  idx_t free_list_id;
  // The current header iteration count.
  uint64_t iteration_count;
  // Lock for operations.
  mutex block_lock;
};

}  // namespace duckdb
