// Copyright 2024, DuckDB contributors. Adapted for QLever.
// FileBuffer represents a buffer that can be read/written to a Direct IO
// FileHandle.

#pragma once

#include "storage/compat/duckdb_allocator.h"
#include "storage/compat/duckdb_assert.h"
#include "storage/compat/duckdb_config.h"
#include "storage/compat/duckdb_file_system.h"
#include "storage/compat/duckdb_types.h"
#include "storage/storage_info.h"

namespace duckdb {

enum class FileBufferType : uint8_t {
  BLOCK = 1,
  MANAGED_BUFFER = 2,
  TINY_BUFFER = 3
};

static constexpr idx_t FILE_BUFFER_TYPE_COUNT = 3;

// The FileBuffer represents a buffer that can be read or written to a Direct IO
// FileHandle.
class FileBuffer {
 public:
  // Allocates a buffer of the specified size, with room for additional header
  // bytes.
  FileBuffer(Allocator& allocator, FileBufferType type, uint64_t user_size);
  FileBuffer(FileBuffer& source, FileBufferType type);

  virtual ~FileBuffer();

  Allocator& allocator;
  // The buffer that users can write to.
  data_ptr_t buffer;
  // The user-facing size of the buffer.
  uint64_t size;

 public:
  // Read into the FileBuffer from the specified location.
  void Read(FileHandle& handle, uint64_t location);
  // Write the contents of the FileBuffer to the specified location.
  void Write(FileHandle& handle, uint64_t location);

  void Clear();

  FileBufferType GetBufferType() const { return type; }

  // Resize the buffer.
  void Resize(uint64_t user_size);

  uint64_t AllocSize() const { return internal_size; }
  uint64_t Size() const { return size; }
  data_ptr_t InternalBuffer() { return internal_buffer; }

  struct MemoryRequirement {
    idx_t alloc_size;
    idx_t header_size;
  };

  MemoryRequirement CalculateMemory(uint64_t user_size);
  void Initialize(DebugInitialize info);

 protected:
  // The type of the buffer.
  FileBufferType type;
  // The pointer to the internal buffer (including header).
  data_ptr_t internal_buffer;
  // The aligned size as passed to the constructor.
  uint64_t internal_size;

  void ReallocBuffer(idx_t new_size);
  void Init();
};

}  // namespace duckdb
