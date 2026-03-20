// Copyright 2024, DuckDB contributors. Adapted for QLever.
// This shim replaces DuckDB's Allocator class with a minimal implementation
// that uses aligned allocation (required for Direct I/O compatibility).

#pragma once

#include <cstdlib>
#include <cstring>
#include <new>

#include "storage/compat/duckdb_types.h"

namespace duckdb {

// Minimal allocator that provides aligned memory allocation.
class Allocator {
 public:
  Allocator() = default;

  data_ptr_t AllocateData(idx_t size) {
    // Use aligned_alloc for Direct I/O compatibility (4096-byte alignment).
    // The size must be a multiple of the alignment.
    idx_t aligned_size = AlignValue<idx_t, 4096>(size);
    void* ptr = std::aligned_alloc(4096, aligned_size);
    if (!ptr) {
      throw std::bad_alloc();
    }
    return static_cast<data_ptr_t>(ptr);
  }

  void FreeData(data_ptr_t ptr, idx_t /*size*/) { std::free(ptr); }

  data_ptr_t ReallocateData(data_ptr_t old_ptr, idx_t old_size,
                            idx_t new_size) {
    idx_t aligned_new_size = AlignValue<idx_t, 4096>(new_size);
    void* new_ptr = std::aligned_alloc(4096, aligned_new_size);
    if (!new_ptr) {
      throw std::bad_alloc();
    }
    if (old_ptr) {
      idx_t copy_size =
          old_size < aligned_new_size ? old_size : aligned_new_size;
      std::memcpy(new_ptr, old_ptr, copy_size);
      std::free(old_ptr);
    }
    return static_cast<data_ptr_t>(new_ptr);
  }

  // DuckDB's Allocator has static FlushAll / SupportsFlush methods.
  // We provide no-op stubs.
  static bool SupportsFlush() { return false; }
  static void FlushAll() {}
};

}  // namespace duckdb
