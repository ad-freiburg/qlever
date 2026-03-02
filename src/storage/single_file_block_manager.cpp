// Copyright 2024, DuckDB contributors. Adapted for QLever.
// Simplified SingleFileBlockManager without checksums, MetadataManager,
// or serialization streams.

#include "storage/single_file_block_manager.h"

#include <algorithm>
#include <cstring>

#include "storage/buffer_manager.h"
#include "storage/compat/duckdb_allocator.h"
#include "storage/compat/duckdb_exceptions.h"

namespace duckdb {

SingleFileBlockManager::SingleFileBlockManager(
    BufferManager& buffer_manager, const string& path_p,
    const StorageManagerOptions& options, optional_idx block_alloc_size_p)
    : BlockManager(buffer_manager, block_alloc_size_p),
      read_only_(options.read_only),
      path(path_p),
      header_buffer(
          allocator_, FileBufferType::MANAGED_BUFFER,
          Storage::FILE_HEADER_SIZE - Storage::DEFAULT_BLOCK_HEADER_SIZE),
      iteration_count(0) {}

void SingleFileBlockManager::CreateNewDatabase() {
  // Open the file for writing.
  handle = OpenFile(path, "c");

  // Write empty headers.
  header_buffer.Clear();

  // Write the database header.
  DatabaseHeader h1;
  h1.iteration = 0;
  h1.meta_block = idx_t(INVALID_BLOCK);
  h1.free_list = idx_t(INVALID_BLOCK);
  h1.block_count = 0;
  h1.block_alloc_size = GetBlockAllocSize();

  // Write h1 at FILE_HEADER_SIZE offset.
  memcpy(header_buffer.buffer, &h1, sizeof(DatabaseHeader));
  header_buffer.Write(*handle, Storage::FILE_HEADER_SIZE);

  // Write h2.
  header_buffer.Clear();
  DatabaseHeader h2 = h1;
  memcpy(header_buffer.buffer, &h2, sizeof(DatabaseHeader));
  header_buffer.Write(*handle, Storage::FILE_HEADER_SIZE * 2ULL);

  handle->Sync();
  iteration_count = 0;
  active_header = 1;
  max_block = 0;
  meta_block = idx_t(INVALID_BLOCK);
  free_list_id = idx_t(INVALID_BLOCK);
}

void SingleFileBlockManager::LoadExistingDatabase() {
  const char* mode = read_only_ ? "r" : "w";
  handle = OpenFile(path, mode);

  // Read h1.
  header_buffer.Read(*handle, Storage::FILE_HEADER_SIZE);
  DatabaseHeader h1;
  memcpy(&h1, header_buffer.buffer, sizeof(DatabaseHeader));

  // Read h2.
  header_buffer.Read(*handle, Storage::FILE_HEADER_SIZE * 2ULL);
  DatabaseHeader h2;
  memcpy(&h2, header_buffer.buffer, sizeof(DatabaseHeader));

  // Choose the header with the highest iteration count.
  if (h1.iteration > h2.iteration) {
    active_header = 0;
    Initialize(h1, GetOptionalBlockAllocSize());
  } else {
    active_header = 1;
    Initialize(h2, GetOptionalBlockAllocSize());
  }
}

void SingleFileBlockManager::Initialize(
    const DatabaseHeader& header,
    const optional_idx provided_block_alloc_size) {
  free_list_id = header.free_list;
  meta_block = header.meta_block;
  iteration_count = header.iteration;
  max_block = NumericCast<block_id_t>(header.block_count);
  if (provided_block_alloc_size.IsValid() &&
      provided_block_alloc_size.GetIndex() != header.block_alloc_size) {
    throw InvalidInputException(
        "Error opening \"%s\": cannot initialize with a different block size: "
        "provided %llu, file has %llu",
        path.c_str(), provided_block_alloc_size.GetIndex(),
        header.block_alloc_size);
  }
  SetBlockAllocSize(header.block_alloc_size);
}

block_id_t SingleFileBlockManager::GetFreeBlockId() {
  lock_guard<mutex> lock(block_lock);
  block_id_t block;
  if (!free_list.empty()) {
    block = *free_list.begin();
    free_list.erase(free_list.begin());
    newly_freed_list.erase(block);
  } else {
    block = max_block++;
  }
  return block;
}

block_id_t SingleFileBlockManager::PeekFreeBlockId() {
  lock_guard<mutex> lock(block_lock);
  if (!free_list.empty()) {
    return *free_list.begin();
  }
  return max_block;
}

bool SingleFileBlockManager::IsRootBlock(MetaBlockPointer root) {
  return root.block_pointer == meta_block;
}

void SingleFileBlockManager::MarkBlockAsFree(block_id_t block_id) {
  lock_guard<mutex> lock(block_lock);
  D_ASSERT(block_id >= 0);
  D_ASSERT(block_id < max_block);
  if (free_list.find(block_id) != free_list.end()) {
    throw InternalException(
        "MarkBlockAsFree called but block %lld was already freed!", block_id);
  }
  multi_use_blocks.erase(block_id);
  free_list.insert(block_id);
  newly_freed_list.insert(block_id);
}

void SingleFileBlockManager::MarkBlockAsUsed(block_id_t block_id) {
  lock_guard<mutex> lock(block_lock);
  D_ASSERT(block_id >= 0);
  if (max_block <= block_id) {
    while (max_block < block_id) {
      free_list.insert(max_block);
      max_block++;
    }
    max_block++;
  } else if (free_list.find(block_id) != free_list.end()) {
    free_list.erase(block_id);
    newly_freed_list.erase(block_id);
  } else {
    IncreaseBlockReferenceCountInternal(block_id);
  }
}

void SingleFileBlockManager::MarkBlockAsModified(block_id_t block_id) {
  lock_guard<mutex> lock(block_lock);
  D_ASSERT(block_id >= 0);
  D_ASSERT(block_id < max_block);
  auto entry = multi_use_blocks.find(block_id);
  if (entry != multi_use_blocks.end()) {
    entry->second--;
    if (entry->second <= 1) {
      multi_use_blocks.erase(entry);
    }
    return;
  }
  D_ASSERT(free_list.find(block_id) == free_list.end());
  modified_blocks.insert(block_id);
}

void SingleFileBlockManager::IncreaseBlockReferenceCountInternal(
    block_id_t block_id) {
  D_ASSERT(block_id >= 0);
  D_ASSERT(block_id < max_block);
  D_ASSERT(free_list.find(block_id) == free_list.end());
  auto entry = multi_use_blocks.find(block_id);
  if (entry != multi_use_blocks.end()) {
    entry->second++;
  } else {
    multi_use_blocks[block_id] = 2;
  }
}

void SingleFileBlockManager::IncreaseBlockReferenceCount(block_id_t block_id) {
  lock_guard<mutex> lock(block_lock);
  IncreaseBlockReferenceCountInternal(block_id);
}

idx_t SingleFileBlockManager::GetMetaBlock() { return meta_block; }

idx_t SingleFileBlockManager::TotalBlocks() {
  lock_guard<mutex> lock(block_lock);
  return NumericCast<idx_t>(max_block);
}

idx_t SingleFileBlockManager::FreeBlocks() {
  lock_guard<mutex> lock(block_lock);
  return free_list.size();
}

unique_ptr<Block> SingleFileBlockManager::ConvertBlock(
    block_id_t block_id, FileBuffer& source_buffer) {
  D_ASSERT(source_buffer.AllocSize() == GetBlockAllocSize());
  return make_uniq<Block>(source_buffer, block_id);
}

unique_ptr<Block> SingleFileBlockManager::CreateBlock(
    block_id_t block_id, FileBuffer* source_buffer) {
  unique_ptr<Block> result;
  if (source_buffer) {
    result = ConvertBlock(block_id, *source_buffer);
  } else {
    result = make_uniq<Block>(allocator_, block_id, GetBlockSize());
  }
  return result;
}

idx_t SingleFileBlockManager::GetBlockLocation(block_id_t block_id) {
  return BLOCK_START + NumericCast<idx_t>(block_id) * GetBlockAllocSize();
}

void SingleFileBlockManager::Read(Block& block) {
  D_ASSERT(block.id >= 0);
  block.Read(*handle, GetBlockLocation(block.id));
}

void SingleFileBlockManager::ReadBlocks(FileBuffer& buffer,
                                        block_id_t start_block,
                                        idx_t block_count) {
  D_ASSERT(start_block >= 0);
  D_ASSERT(block_count >= 1);
  auto location = GetBlockLocation(start_block);
  buffer.Read(*handle, location);
}

void SingleFileBlockManager::Write(FileBuffer& buffer, block_id_t block_id) {
  D_ASSERT(block_id >= 0);
  buffer.Write(*handle, BLOCK_START +
                            NumericCast<idx_t>(block_id) * GetBlockAllocSize());
}

void SingleFileBlockManager::Truncate() {
  BlockManager::Truncate();
  idx_t blocks_to_truncate = 0;
  for (auto entry = free_list.rbegin(); entry != free_list.rend(); entry++) {
    auto block_id = *entry;
    if (block_id + 1 != max_block) {
      break;
    }
    blocks_to_truncate++;
    max_block--;
  }
  if (blocks_to_truncate == 0) {
    return;
  }
  free_list.erase(free_list.lower_bound(max_block), free_list.end());
  newly_freed_list.erase(newly_freed_list.lower_bound(max_block),
                         newly_freed_list.end());
  handle->Truncate(NumericCast<int64_t>(
      BLOCK_START + NumericCast<idx_t>(max_block) * GetBlockAllocSize()));
}

void SingleFileBlockManager::WriteHeader(DatabaseHeader header) {
  lock_guard<mutex> lock(block_lock);
  header.iteration = ++iteration_count;

  for (auto& block : modified_blocks) {
    free_list.insert(block);
    newly_freed_list.insert(block);
  }
  modified_blocks.clear();

  header.block_count = NumericCast<idx_t>(max_block);
  header.free_list = free_list_id;
  header.meta_block = meta_block;

  // Write the header.
  handle->Sync();

  header_buffer.Clear();
  memcpy(header_buffer.buffer, &header, sizeof(DatabaseHeader));
  auto write_offset = active_header == 1 ? Storage::FILE_HEADER_SIZE
                                         : Storage::FILE_HEADER_SIZE * 2;
  header_buffer.Write(*handle, write_offset);
  active_header = 1 - active_header;
  handle->Sync();
  newly_freed_list.clear();
}

void SingleFileBlockManager::FileSync() { handle->Sync(); }

}  // namespace duckdb
