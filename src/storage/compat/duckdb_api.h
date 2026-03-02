// Copyright 2024, DuckDB contributors. Adapted for QLever.
// This shim replaces duckdb/common/api.hpp — we link statically, so
// DUCKDB_API is an empty macro.

#pragma once

#define DUCKDB_API
