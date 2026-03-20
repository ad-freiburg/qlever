// Copyright 2024, DuckDB contributors. Adapted for QLever.
// BufferHandle is a RAII handle for a pinned buffer block.

#pragma once

#include "storage/compat/duckdb_api.h"
#include "storage/compat/duckdb_assert.h"
#include "storage/compat/duckdb_optional_ptr.h"
#include "storage/compat/duckdb_types.h"
#include "storage/file_buffer.h"
#include "storage/storage_info.h"

namespace duckdb {
class BlockHandle;

class BufferHandle {
 public:
  BufferHandle();
  explicit BufferHandle(shared_ptr<BlockHandle> handle,
                        optional_ptr<FileBuffer> node);
  ~BufferHandle();
  // Disable copy constructors.
  BufferHandle(const BufferHandle& other) = delete;
  BufferHandle& operator=(const BufferHandle&) = delete;
  // Enable move constructors.
  BufferHandle(BufferHandle&& other) noexcept;
  BufferHandle& operator=(BufferHandle&&) noexcept;

 public:
  // Returns whether or not the BufferHandle is valid.
  bool IsValid() const;
  // Returns a pointer to the buffer data. Handle must be valid.
  inline data_ptr_t Ptr() const {
    D_ASSERT(IsValid());
    return node->buffer;
  }
  // Returns a pointer to the buffer data. Handle must be valid.
  inline data_ptr_t Ptr() {
    D_ASSERT(IsValid());
    return node->buffer;
  }
  // Gets the underlying file buffer. Handle must be valid.
  FileBuffer& GetFileBuffer();
  // Destroys the buffer handle.
  void Destroy();

  const shared_ptr<BlockHandle>& GetBlockHandle() const { return handle; }

 private:
  // The block handle.
  shared_ptr<BlockHandle> handle;
  // The managed buffer node.
  optional_ptr<FileBuffer> node;
};

}  // namespace duckdb
