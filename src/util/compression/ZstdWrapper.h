// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSION_ZSTDWRAPPER_H
#define QLEVER_SRC_UTIL_COMPRESSION_ZSTDWRAPPER_H

#include <zstd.h>

#include <vector>

#include "util/Exception.h"

class ZstdWrapper {
 public:
  // Compress the given byte array and return the result;
  static std::vector<char> compress(const void* src, size_t numBytes,
                                    int compressionLevel = 3) {
    std::vector<char> result(ZSTD_compressBound(numBytes));
    auto compressedSize = ZSTD_compress(result.data(), result.size(), src,
                                        numBytes, compressionLevel);
    result.resize(compressedSize);
    return result;
  }

  // Decompress the given byte array, assuming that the size of the decompressed
  // data is known.
  CPP_template(typename T)(
      requires(std::is_trivially_copyable_v<
               T>)) static std::vector<T> decompress(void* src, size_t numBytes,
                                                     size_t knownOriginalSize) {
    knownOriginalSize *= sizeof(T);
    std::vector<T> result(knownOriginalSize / sizeof(T));
    auto compressedSize =
        ZSTD_decompress(result.data(), knownOriginalSize, src, numBytes);
    AD_CONTRACT_CHECK(compressedSize == knownOriginalSize);
    return result;
  }

  // Decompress the given byte array to the given buffer of the given size,
  // returning the number of bytes of the decompressed data.
  CPP_template(typename T)(
      requires(std::is_trivially_copyable_v<T>)) static size_t
      decompressToBuffer(const char* src, size_t numBytes, T* buffer,
                         size_t bufferCapacity) {
    auto decompressedSize =
        ZSTD_decompress(buffer, bufferCapacity, src, numBytes);
    if (ZSTD_isError(decompressedSize)) {
      throw std::runtime_error(std::string("error during decompression : ") +
                               ZSTD_getErrorName(decompressedSize));
    }
    return decompressedSize;
  }
};

#endif  // QLEVER_SRC_UTIL_COMPRESSION_ZSTDWRAPPER_H
