// Copyright 2024, DuckDB contributors. Adapted for QLever.
// This shim re-exports std::atomic under the duckdb namespace.

#pragma once

#include <atomic>

namespace duckdb {

using std::atomic;

}  // namespace duckdb
