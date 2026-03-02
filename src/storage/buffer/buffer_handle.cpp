// Copyright 2024, DuckDB contributors. Adapted for QLever.

#include "storage/buffer/buffer_handle.h"

#include "storage/block_manager.h"
#include "storage/buffer/block_handle.h"
#include "storage/buffer_manager.h"

namespace duckdb {

BufferHandle::BufferHandle() : handle(nullptr), node(nullptr) {}

BufferHandle::BufferHandle(shared_ptr<BlockHandle> handle_p,
                           optional_ptr<FileBuffer> node_p)
    : handle(std::move(handle_p)), node(node_p) {}

BufferHandle::BufferHandle(BufferHandle&& other) noexcept : node(nullptr) {
  std::swap(node, other.node);
  std::swap(handle, other.handle);
}

BufferHandle& BufferHandle::operator=(BufferHandle&& other) noexcept {
  std::swap(node, other.node);
  std::swap(handle, other.handle);
  return *this;
}

BufferHandle::~BufferHandle() { Destroy(); }

bool BufferHandle::IsValid() const { return node != nullptr; }

void BufferHandle::Destroy() {
  if (!handle || !IsValid()) {
    return;
  }
  handle->block_manager.buffer_manager.Unpin(handle);
  handle.reset();
  node = nullptr;
}

FileBuffer& BufferHandle::GetFileBuffer() {
  D_ASSERT(node);
  return *node;
}

}  // namespace duckdb
