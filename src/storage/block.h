// Copyright 2024, DuckDB contributors. Adapted for QLever.
// Block class — a FileBuffer with an associated block_id.

#pragma once

#include "storage/compat/duckdb_types.h"
#include "storage/file_buffer.h"
#include "storage/storage_info.h"

namespace duckdb {

class Block : public FileBuffer {
 public:
  Block(Allocator& allocator, const block_id_t id, const idx_t block_size)
      : FileBuffer(allocator, FileBufferType::BLOCK, block_size), id(id) {}

  Block(Allocator& allocator, block_id_t id, uint32_t internal_size)
      : FileBuffer(allocator, FileBufferType::BLOCK, internal_size), id(id) {}

  Block(FileBuffer& source, block_id_t id)
      : FileBuffer(source, FileBufferType::BLOCK), id(id) {}

  block_id_t id;
};

struct BlockPointer {
  BlockPointer(block_id_t block_id_p, uint32_t offset_p)
      : block_id(block_id_p), offset(offset_p) {}
  BlockPointer() : block_id(INVALID_BLOCK), offset(0) {}

  block_id_t block_id;
  uint32_t offset;

  bool IsValid() const { return block_id != INVALID_BLOCK; }
};

struct MetaBlockPointer {
  MetaBlockPointer(idx_t block_pointer, uint32_t offset_p)
      : block_pointer(block_pointer), offset(offset_p) {}
  MetaBlockPointer() : block_pointer(DConstants::INVALID_INDEX), offset(0) {}

  idx_t block_pointer;
  uint32_t offset;

  bool IsValid() const { return block_pointer != DConstants::INVALID_INDEX; }
  block_id_t GetBlockId() const {
    return static_cast<block_id_t>(block_pointer);
  }
};

}  // namespace duckdb
