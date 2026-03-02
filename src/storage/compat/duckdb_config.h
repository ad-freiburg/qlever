// Copyright 2024, DuckDB contributors. Adapted for QLever.
// This shim provides a minimal StorageConfig struct replacing DuckDB's
// DatabaseInstance / DBConfig.

#pragma once

#include <string>

#include "storage/compat/duckdb_types.h"

namespace duckdb {

// Simplified debug-initialize enum.
enum class DebugInitialize : uint8_t {
  NO_INITIALIZE = 0,
  DEBUG_ZERO_INITIALIZE = 1,
  DEBUG_ONE_INITIALIZE = 2
};

// Configuration for the storage/buffer layer.
struct StorageConfig {
  // Block allocation size (default 256 KB).
  idx_t block_alloc_size = 262144ULL;
  // Maximum memory for the buffer pool (default 1 GB).
  idx_t maximum_memory = 1ULL << 30;
  // Maximum swap space (0 = unlimited).
  idx_t maximum_swap = 0;
  // Temporary directory for spilling blocks to disk.
  std::string temporary_directory;
  // Debug initialization mode.
  DebugInitialize debug_initialize = DebugInitialize::NO_INITIALIZE;
};

}  // namespace duckdb
