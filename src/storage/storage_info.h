// Copyright 2024, DuckDB contributors. Adapted for QLever.
// Storage constants and header structures.

#pragma once

#include "storage/compat/duckdb_types.h"

// The definition of an invalid block.
#define INVALID_BLOCK (-1)
// The maximum block id is 2^62.
#define MAXIMUM_BLOCK 4611686018427388000LL

// The default block allocation size.
#define DEFAULT_BLOCK_ALLOC_SIZE 262144ULL

namespace duckdb {

struct Storage {
  // The size of a hard disk sector, only really needed for Direct IO.
  constexpr static idx_t SECTOR_SIZE = 4096U;
  // The size of the headers.
  constexpr static idx_t FILE_HEADER_SIZE = 4096U;
  // The minimum block allocation size.
  constexpr static idx_t MIN_BLOCK_ALLOC_SIZE = 16384ULL;
  // The maximum block allocation size.
  constexpr static idx_t MAX_BLOCK_ALLOC_SIZE = 262144ULL;
  // The default block header size for blocks written to storage.
  constexpr static idx_t DEFAULT_BLOCK_HEADER_SIZE = sizeof(idx_t);
  // The default block size.
  constexpr static idx_t DEFAULT_BLOCK_SIZE =
      DEFAULT_BLOCK_ALLOC_SIZE - DEFAULT_BLOCK_HEADER_SIZE;
};

// The DatabaseHeader contains information about the current state of the
// database. Every storage file has two DatabaseHeaders.
struct DatabaseHeader {
  // The iteration count, increases by 1 every time the storage is
  // checkpointed.
  uint64_t iteration;
  // A pointer to the initial meta block.
  idx_t meta_block;
  // A pointer to the block containing the free list.
  idx_t free_list;
  // The number of blocks that is in the file as of this database header.
  uint64_t block_count;
  // The allocation size of blocks in this database file.
  idx_t block_alloc_size;
};

}  // namespace duckdb
