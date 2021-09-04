// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#pragma once

#include <zstd.h>

#include <vector>

#include "../Exception.h"

class ZstdWrapper {
 public:
  static constexpr auto noop = []([[maybe_unused]] auto&&... args) {};

  template <typename Callback>
  static void compress(void* src, size_t numBytes, Callback whatToDoWithResult,
                       int compressionLevel = 3) {
    std::vector<char> result(ZSTD_compressBound(numBytes));
    auto compressedSize = ZSTD_compress(result.data(), result.size(), src,
                                        numBytes, compressionLevel);
    result.resize(compressedSize);
    whatToDoWithResult(std::move(result));
  }

  static std::vector<char> compressAndReturn(void* src, size_t numBytes,
                                             int compressionLevel = 3) {
    std::vector<char> result;
    auto toResult = [&](std::vector<char> passedResult) {
      result = std::move(passedResult);
    };
    compress(src, numBytes, toResult, compressionLevel);
    return result;
  }

  template <typename T>
  requires(std::is_trivially_copyable_v<T>) static std::vector<T> decompress(
      void* src, size_t numBytes, size_t knownOriginalSize) {
    knownOriginalSize *= sizeof(T);
    std::vector<T> result(knownOriginalSize / sizeof(T));
    auto compressedSize =
        ZSTD_decompress(result.data(), knownOriginalSize, src, numBytes);
    AD_CHECK(compressedSize == knownOriginalSize);
    return result;
  }

  template <typename T>
  requires(std::is_trivially_copyable_v<T>) static size_t
      decompressToBuffer(const char* src, size_t numBytes, T* buffer,
                         size_t bufferCapacity) {
    auto decompressedSize = ZSTD_decompress(buffer, bufferCapacity * sizeof(T),
                                            const_cast<char*>(src), numBytes);
    if (ZSTD_isError(decompressedSize)) {
      throw std::runtime_error(std::string("error during decompression : ") +
                               ZSTD_getErrorName(decompressedSize));
    }
    return decompressedSize / sizeof(T);
  }
};
