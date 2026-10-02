// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDWRAPPER_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDWRAPPER_H

#include <absl/strings/str_cat.h>
#include <zstd.h>

#include <optional>
#include <stdexcept>
#include <string_view>
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

  // The result of the non-throwing functions below. On success, `size_`
  // contains the requested size. On failure, `size_` is `nullopt`, and
  // `errorMessage_` describes the error (it always refers to a string with
  // static storage duration).
  struct SizeOrError {
    std::optional<size_t> size_;
    std::string_view errorMessage_;
  };

  // Return the size of the uncompressed data of the ZSTD frame that starts at
  // `src` and consists of `numBytes` bytes, as stored in the header of that
  // frame. Report an error if `src` does not point to the beginning of a valid
  // ZSTD frame (in particular, if `numBytes` is too small to even hold a frame
  // header), or if the frame does not store the size of its uncompressed data
  // (which is the case for frames written by a streaming compressor, but never
  // for frames written by `compress` above). Note that only the frame header is
  // inspected, so this is cheap, but it does not detect corruption of the
  // compressed data itself.
  static SizeOrError tryToGetUncompressedSize(const void* src,
                                              size_t numBytes) noexcept {
    auto uncompressedSize = ZSTD_getFrameContentSize(src, numBytes);
    if (uncompressedSize == ZSTD_CONTENTSIZE_ERROR) {
      return {std::nullopt,
              "Could not determine the size of the uncompressed data: the "
              "given data does not start with a valid ZSTD frame header"};
    }
    if (uncompressedSize == ZSTD_CONTENTSIZE_UNKNOWN) {
      return {std::nullopt,
              "Could not determine the size of the uncompressed data: the "
              "given ZSTD frame does not store that size in its header"};
    }
    return {static_cast<size_t>(uncompressedSize), {}};
  }

  // Same as `tryToGetUncompressedSize`, but throw a descriptive exception
  // instead of reporting an error.
  static size_t getUncompressedSize(const void* src, size_t numBytes) {
    return valueOrThrow(tryToGetUncompressedSize(src, numBytes), "");
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
  // and return the number of bytes of the decompressed data. Report an error if
  // the decompression fails (e.g. because the compressed data is corrupted, or
  // because the buffer is too small).
  CPP_template(typename T)(
      requires(std::is_trivially_copyable_v<T>)) static SizeOrError
      tryToDecompressToBuffer(const char* src, size_t numBytes, T* buffer,
                              size_t bufferCapacity) noexcept {
    auto decompressedSize =
        ZSTD_decompress(buffer, bufferCapacity, src, numBytes);
    if (ZSTD_isError(decompressedSize)) {
      return {std::nullopt, ZSTD_getErrorName(decompressedSize)};
    }
    return {decompressedSize, {}};
  }

  // Same as `tryToDecompressToBuffer`, but throw a descriptive exception
  // instead of reporting an error.
  CPP_template(typename T)(
      requires(std::is_trivially_copyable_v<T>)) static size_t
      decompressToBuffer(const char* src, size_t numBytes, T* buffer,
                         size_t bufferCapacity) {
    return valueOrThrow(
        tryToDecompressToBuffer(src, numBytes, buffer, bufferCapacity),
        "error during decompression : ");
  }

 private:
  // Return the size of the `result`, or throw a `std::runtime_error` with the
  // `messagePrefix` followed by the error message of the `result`.
  static size_t valueOrThrow(const SizeOrError& result,
                             std::string_view messagePrefix) {
    if (!result.size_.has_value()) {
      throw std::runtime_error{
          absl::StrCat(messagePrefix, result.errorMessage_)};
    }
    return result.size_.value();
  }
};

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDWRAPPER_H
