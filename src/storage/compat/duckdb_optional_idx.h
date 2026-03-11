// Copyright 2024, DuckDB contributors. Adapted for QLever.
// This shim provides DuckDB's optional_idx — an index value that may be
// invalid.

#pragma once

#include "storage/compat/duckdb_types.h"

namespace duckdb {

class optional_idx {
 public:
  optional_idx() : index_(DConstants::INVALID_INDEX) {}
  optional_idx(idx_t index) : index_(index) {}  // NOLINT

  bool IsValid() const { return index_ != DConstants::INVALID_INDEX; }
  idx_t GetIndex() const { return index_; }

 private:
  idx_t index_;
};

}  // namespace duckdb
