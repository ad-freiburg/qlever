// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSION_LZ4WRAPPER_H
#define QLEVER_SRC_UTIL_COMPRESSION_LZ4WRAPPER_H

#ifdef QLEVER_HAS_LZ4

#include <lz4.h>
#include <lz4hc.h>

#include <vector>

#include "util/Exception.h"

// Implement compression using `Lz4`. Currently always uses the
// `high-compression` mode, which is slow on compression, but has a good
// compression ratio (decompression is always fast for Lz4).
class Lz4Wrapper {
 public:
  // Compress the given byte array using LZ4_HC and return the result.
  static std::vector<char> compress(const void* src, size_t numBytes) {
    int maxSize = LZ4_compressBound(static_cast<int>(numBytes));
    std::vector<char> result(maxSize);
    int compressedSize =
        LZ4_compress_HC(static_cast<const char*>(src), result.data(),
                        static_cast<int>(numBytes), maxSize, LZ4HC_CLEVEL_MAX);
    AD_CONTRACT_CHECK(compressedSize > 0);
    result.resize(static_cast<size_t>(compressedSize));
    return result;
  }

  // Decompress the given byte array, assuming that the size of the
  // decompressed data is known.
  CPP_template(typename T)(
      requires(std::is_trivially_copyable_v<
               T>)) static std::vector<T> decompress(void* src, size_t numBytes,
                                                     size_t knownOriginalSize) {
    size_t originalBytes = knownOriginalSize * sizeof(T);
    std::vector<T> result(knownOriginalSize);
    int decompressedSize = LZ4_decompress_safe(
        static_cast<const char*>(src), reinterpret_cast<char*>(result.data()),
        static_cast<int>(numBytes), static_cast<int>(originalBytes));
    AD_CONTRACT_CHECK(static_cast<size_t>(decompressedSize) == originalBytes);
    return result;
  }

  // Decompress the given byte array to the given buffer of the given size,
  // returning the number of bytes of the decompressed data.
  CPP_template(typename T)(
      requires(std::is_trivially_copyable_v<T>)) static size_t
      decompressToBuffer(const char* src, size_t numBytes, T* buffer,
                         size_t bufferCapacity) {
    // Handle the edge case of empty compressed data (e.g. virtual blocks from
    // located triples). LZ4_decompress_safe returns -1 for 0 input bytes,
    // while ZSTD returns 0 successfully.
    if (numBytes == 0) {
      return 0;
    }
    int decompressedSize = LZ4_decompress_safe(
        src, reinterpret_cast<char*>(buffer), static_cast<int>(numBytes),
        static_cast<int>(bufferCapacity));
    if (decompressedSize < 0) {
      throw std::runtime_error("Error during LZ4 decompression.");
    }
    return static_cast<size_t>(decompressedSize);
  }
};

#endif  // QLEVER_HAS_LZ4

#endif  // QLEVER_SRC_UTIL_COMPRESSION_LZ4WRAPPER_H
