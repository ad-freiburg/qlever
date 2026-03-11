// Copyright 2024, DuckDB contributors. Adapted for QLever.
// This shim provides exception types used in DuckDB's storage layer.

#pragma once

#include <cstdio>
#include <stdexcept>
#include <string>

namespace duckdb {

class InternalException : public std::runtime_error {
 public:
  template <typename... Args>
  explicit InternalException(const char* fmt, Args... args)
      : std::runtime_error(FormatMessage(fmt, args...)) {}

  explicit InternalException(const std::string& msg)
      : std::runtime_error(msg) {}

 private:
  static std::string FormatMessage(const char* fmt) { return std::string(fmt); }

  template <typename... Args>
  static std::string FormatMessage(const char* fmt, Args... args) {
    // Simple printf-style formatting.
    char buf[2048];
    snprintf(buf, sizeof(buf), fmt, args...);
    return std::string(buf);
  }
};

class OutOfMemoryException : public std::runtime_error {
 public:
  template <typename... Args>
  explicit OutOfMemoryException(const char* fmt, Args... args)
      : std::runtime_error(FormatMessage(fmt, args...)) {}

  explicit OutOfMemoryException(const std::string& msg)
      : std::runtime_error(msg) {}

 private:
  static std::string FormatMessage(const char* fmt) { return std::string(fmt); }

  template <typename... Args>
  static std::string FormatMessage(const char* fmt, Args... args) {
    char buf[2048];
    snprintf(buf, sizeof(buf), fmt, args...);
    return std::string(buf);
  }
};

class IOException : public std::runtime_error {
 public:
  template <typename... Args>
  explicit IOException(const char* fmt, Args... args)
      : std::runtime_error(FormatMessage(fmt, args...)) {}

  explicit IOException(const std::string& msg) : std::runtime_error(msg) {}

 private:
  static std::string FormatMessage(const char* fmt) { return std::string(fmt); }

  template <typename... Args>
  static std::string FormatMessage(const char* fmt, Args... args) {
    char buf[2048];
    snprintf(buf, sizeof(buf), fmt, args...);
    return std::string(buf);
  }
};

class NotImplementedException : public std::runtime_error {
 public:
  explicit NotImplementedException(const std::string& msg)
      : std::runtime_error(msg) {}
};

class InvalidInputException : public std::runtime_error {
 public:
  template <typename... Args>
  explicit InvalidInputException(const char* fmt, Args... args)
      : std::runtime_error(FormatMessage(fmt, args...)) {}

  explicit InvalidInputException(const std::string& msg)
      : std::runtime_error(msg) {}

 private:
  static std::string FormatMessage(const char* fmt) { return std::string(fmt); }

  template <typename... Args>
  static std::string FormatMessage(const char* fmt, Args... args) {
    char buf[2048];
    snprintf(buf, sizeof(buf), fmt, args...);
    return std::string(buf);
  }
};

class FatalException : public std::runtime_error {
 public:
  explicit FatalException(const std::string& msg) : std::runtime_error(msg) {}
};

}  // namespace duckdb
