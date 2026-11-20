// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDEXPECTED_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDEXPECTED_H

#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "util/Exception.h"

// The error handling of the ZSTD frame helpers (`ZstdFrameHeader.h`,
// `ZstdFrameBlocks.h`, `ZstdSkippableFrame.h`, and the modules that build on
// them). These helpers parse data that may be corrupted, so they do not throw,
// but return an `Expected<T>`, which holds either a `T` or a `ZstdError` with a
// message that describes what is wrong. Exceptions are only thrown by
// `AD_CORRECTNESS_CHECK`s, which detect bugs in the helpers themselves (and if
// memory cannot be allocated).
namespace ad_utility::zstd {

// An error of the ZSTD frame helpers.
struct ZstdError {
  // A human-readable description of the error.
  std::string message_;
};

// Either a value of type `T` or a `ZstdError` (similar to `std::expected` from
// C++23).
template <typename T>
class [[nodiscard]] Expected {
 private:
  std::variant<T, ZstdError> valueOrError_;

 public:
  // Implicitly construct from a value or from an error, such that functions
  // can simply `return value;` or `return ZstdError{...};`.
  Expected(T value) : valueOrError_{std::move(value)} {}
  Expected(ZstdError error) : valueOrError_{std::move(error)} {}

  // Return true iff this holds a value (and not an error).
  bool hasValue() const { return valueOrError_.index() == 0; }

  // Return the value. Calling this on an error is a bug.
  T& value() & {
    AD_CORRECTNESS_CHECK(hasValue(), [this] { return error().message_; });
    return std::get<0>(valueOrError_);
  }
  const T& value() const& {
    AD_CORRECTNESS_CHECK(hasValue(), [this] { return error().message_; });
    return std::get<0>(valueOrError_);
  }
  T&& value() && { return std::move(value()); }
  T* operator->() { return &value(); }
  const T* operator->() const { return &value(); }

  // Return the error. Calling this on a value is a bug.
  const ZstdError& error() const& {
    AD_CORRECTNESS_CHECK(!hasValue());
    return std::get<1>(valueOrError_);
  }
  ZstdError&& error() && {
    AD_CORRECTNESS_CHECK(!hasValue());
    return std::get<1>(std::move(valueOrError_));
  }
};

// The result of a function that has no value to return, but can fail.
template <>
class [[nodiscard]] Expected<void> {
 private:
  std::optional<ZstdError> error_;

 public:
  // Construct a success.
  Expected() = default;
  // Implicitly construct from an error.
  Expected(ZstdError error) : error_{std::move(error)} {}

  // Return true iff this is a success (and not an error).
  bool hasValue() const { return !error_.has_value(); }

  // Return the error. Calling this on a success is a bug.
  const ZstdError& error() const& {
    AD_CORRECTNESS_CHECK(!hasValue());
    return error_.value();
  }
  ZstdError&& error() && {
    AD_CORRECTNESS_CHECK(!hasValue());
    return std::move(error_).value();
  }
};

}  // namespace ad_utility::zstd

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDEXPECTED_H
