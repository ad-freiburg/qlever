// Copyright 2024, DuckDB contributors. Adapted for QLever.
// This shim re-exports std::mutex and std::lock_guard under the duckdb
// namespace (DuckDB uses duckdb/common/mutex.hpp for this purpose).

#pragma once

#include <mutex>

namespace duckdb {

using std::lock_guard;
using std::mutex;
using std::unique_lock;

}  // namespace duckdb
