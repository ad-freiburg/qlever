// Copyright 2024, DuckDB contributors. Adapted for QLever.
// Memory tag enum used to categorize buffer memory usage.

#pragma once

#include <cstdint>

#include "storage/compat/duckdb_types.h"

namespace duckdb {

enum class MemoryTag : uint8_t {
  BASE_TABLE = 0,
  HASH_TABLE = 1,
  PARQUET_READER = 2,
  CSV_READER = 3,
  ORDER_BY = 4,
  ART_INDEX = 5,
  COLUMN_DATA = 6,
  METADATA = 7,
  OVERFLOW_STRINGS = 8,
  IN_MEMORY_TABLE = 9,
  ALLOCATOR = 10,
  EXTENSION = 11,
  TRANSACTION = 12
};

static constexpr const idx_t MEMORY_TAG_COUNT = 13;

}  // namespace duckdb
