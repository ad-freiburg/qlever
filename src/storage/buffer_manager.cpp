// Copyright 2024, DuckDB contributors. Adapted for QLever.
// Default implementations for BufferManager virtual methods.

#include "storage/buffer_manager.h"

#include "storage/buffer/buffer_pool.h"
#include "storage/compat/duckdb_exceptions.h"

namespace duckdb {

// Default: not supported.
shared_ptr<BlockHandle> BufferManager::RegisterSmallMemory(
    MemoryTag /*tag*/, const idx_t /*size*/) {
  throw InternalException("RegisterSmallMemory not implemented");
}

Allocator& BufferManager::GetBufferAllocator() {
  throw InternalException("GetBufferAllocator not implemented");
}

void BufferManager::ReserveMemory(idx_t /*size*/) {
  throw InternalException("ReserveMemory not implemented");
}

void BufferManager::FreeReservedMemory(idx_t /*size*/) {
  throw InternalException("FreeReservedMemory not implemented");
}

void BufferManager::SetMemoryLimit(idx_t /*limit*/) {
  throw InternalException("SetMemoryLimit not implemented");
}

void BufferManager::SetSwapLimit(optional_idx /*limit*/) {
  throw InternalException("SetSwapLimit not implemented");
}

vector<TemporaryFileInformation> BufferManager::GetTemporaryFiles() {
  return {};
}

static string empty_string;

const string& BufferManager::GetTemporaryDirectory() const {
  return empty_string;
}

void BufferManager::SetTemporaryDirectory(const string& /*new_dir*/) {
  throw InternalException("SetTemporaryDirectory not implemented");
}

bool BufferManager::HasTemporaryDirectory() const { return false; }

unique_ptr<FileBuffer> BufferManager::ConstructManagedBuffer(
    idx_t size, unique_ptr<FileBuffer>&& source, FileBufferType type) {
  static Allocator default_allocator;
  if (source) {
    return make_uniq<FileBuffer>(*source, type);
  }
  return make_uniq<FileBuffer>(default_allocator, type, size);
}

BufferPool& BufferManager::GetBufferPool() const {
  throw InternalException("GetBufferPool not implemented");
}

idx_t BufferManager::GetQueryMaxMemory() const {
  return GetBufferPool().GetQueryMaxMemory();
}

void BufferManager::AddToEvictionQueue(shared_ptr<BlockHandle>& handle) {
  GetBufferPool().AddToEvictionQueue(handle);
}

void BufferManager::WriteTemporaryBuffer(MemoryTag /*tag*/,
                                         block_id_t /*block_id*/,
                                         FileBuffer& /*buffer*/) {
  throw InternalException("WriteTemporaryBuffer not implemented");
}

unique_ptr<FileBuffer> BufferManager::ReadTemporaryBuffer(
    MemoryTag /*tag*/, BlockHandle& /*block*/,
    unique_ptr<FileBuffer> /*buffer*/) {
  throw InternalException("ReadTemporaryBuffer not implemented");
}

void BufferManager::DeleteTemporaryFile(BlockHandle& /*block*/) {
  // Default: no-op.
}

}  // namespace duckdb
