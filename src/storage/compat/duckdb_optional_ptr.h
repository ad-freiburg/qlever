// Copyright 2024, DuckDB contributors. Adapted for QLever.
// This shim provides DuckDB's optional_ptr<T> — a non-owning pointer that can
// be null.

#pragma once

#include <cstddef>

namespace duckdb {

// A simple non-owning nullable pointer wrapper.
template <class T>
class optional_ptr {
 public:
  optional_ptr() : ptr_(nullptr) {}
  optional_ptr(T* ptr) : ptr_(ptr) {}              // NOLINT
  optional_ptr(std::nullptr_t) : ptr_(nullptr) {}  // NOLINT

  T& operator*() const { return *ptr_; }
  T* operator->() const { return ptr_; }

  explicit operator bool() const { return ptr_ != nullptr; }
  bool operator!() const { return ptr_ == nullptr; }
  bool operator!=(std::nullptr_t) const { return ptr_ != nullptr; }
  bool operator==(std::nullptr_t) const { return ptr_ == nullptr; }

  T* get() const { return ptr_; }

 private:
  T* ptr_;
};

}  // namespace duckdb
