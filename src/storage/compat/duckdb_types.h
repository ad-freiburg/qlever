// Copyright 2024, DuckDB contributors. Adapted for QLever.
// This shim provides the fundamental type aliases used throughout DuckDB's
// storage layer: idx_t, data_ptr_t, block_id_t, smart-pointer aliases, etc.

#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace duckdb {

// Fundamental numeric types.
using idx_t = uint64_t;
using row_t = int64_t;
using hash_t = uint64_t;

// Data pointer types.
using data_t = uint8_t;
using data_ptr_t = data_t*;
using const_data_ptr_t = const data_t*;

// Block identifiers.
using block_id_t = int64_t;

// Smart pointer aliases (DuckDB uses its own wrappers; we forward to std).
using std::enable_shared_from_this;
using std::make_shared;
using std::make_unique;
using std::shared_ptr;
using std::unique_ptr;
using std::weak_ptr;

// String and vector aliases.
using std::set;
using std::string;
using std::to_string;
using std::vector;

// Helper to create shared_ptr (DuckDB calls this make_shared_ptr).
template <class T, class... Args>
shared_ptr<T> make_shared_ptr(Args&&... args) {
  return std::make_shared<T>(std::forward<Args>(args)...);
}

// Helper to create unique_ptr (DuckDB calls this make_uniq).
template <class T, class... Args>
unique_ptr<T> make_uniq(Args&&... args) {
  return std::make_unique<T>(std::forward<Args>(args)...);
}

// Cast helpers.
template <class SRC>
data_ptr_t data_ptr_cast(SRC* src) {
  return reinterpret_cast<data_ptr_t>(src);
}

template <class SRC>
const_data_ptr_t const_data_ptr_cast(const SRC* src) {
  return reinterpret_cast<const_data_ptr_t>(src);
}

template <class SRC>
char* char_ptr_cast(SRC* src) {
  return reinterpret_cast<char*>(src);
}

template <class SRC>
const char* const_char_ptr_cast(const SRC* src) {
  return reinterpret_cast<const char*>(src);
}

// Numeric cast (simplified — just static_cast for our purposes).
template <class DST, class SRC>
DST NumericCast(SRC src) {
  return static_cast<DST>(src);
}

template <class DST, class SRC>
DST UnsafeNumericCast(SRC src) {
  return static_cast<DST>(src);
}

// AlignValue: round up `n` to the next multiple of `ALIGNMENT`.
template <class T, T ALIGNMENT>
T AlignValue(T n) {
  return ((n + (ALIGNMENT - 1)) / ALIGNMENT) * ALIGNMENT;
}

// AbsValue helper.
template <class T>
T AbsValue(T val) {
  return val < 0 ? -val : val;
}

// DConstants replacement.
struct DConstants {
  static constexpr idx_t INVALID_INDEX = idx_t(-1);
};

// Load/Store helpers for reading/writing values from raw byte pointers.
template <class T>
T Load(const_data_ptr_t ptr) {
  T val;
  memcpy(&val, ptr, sizeof(T));
  return val;
}

template <class T>
void Store(T val, data_ptr_t ptr) {
  memcpy(ptr, &val, sizeof(T));
}

// unique_ptr_cast.
template <class SRC, class DST>
unique_ptr<DST> unique_ptr_cast(unique_ptr<SRC> src) {
  return unique_ptr<DST>(static_cast<DST*>(src.release()));
}

// RefersToSameObject helper.
template <class T>
bool RefersToSameObject(const T& a, const T& b) {
  return &a == &b;
}

}  // namespace duckdb
