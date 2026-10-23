// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDWRAPPER_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDWRAPPER_H

#include <absl/numeric/bits.h>
#include <absl/strings/str_cat.h>
#include <zstd.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <variant>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"

class ZstdWrapper {
 public:
  // Compress the given byte array and return the result;
  static std::vector<char> compress(const void* src, size_t numBytes,
                                    int compressionLevel = 3) {
    // NOTE: Compress into a buffer that the thread reuses, and copy only the
    // compressed bytes into the result. Creating the result with the size of
    // `ZSTD_compressBound` would zero-initialize it, which costs more than the
    // copy, because the compressed data is typically much smaller.
    thread_local std::vector<char> buffer;
    const size_t bound = ZSTD_compressBound(numBytes);
    if (buffer.size() < bound) {
      buffer.resize(bound);
    }
    auto compressedSize = checkError(
        ZSTD_compressCCtx(compressionContext(), buffer.data(), buffer.size(),
                          src, numBytes, compressionLevel));
    return std::vector<char>(buffer.begin(), buffer.begin() + compressedSize);
  }

  // The error that is reported by the non-throwing functions below. The
  // `message_` always refers to a string with static storage duration.
  struct Error {
    std::string_view message_;
  };

  // The result of the non-throwing functions below: the requested size on
  // success, and the `Error` otherwise.
  using SizeOrError = std::variant<size_t, Error>;

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
      return Error{
          "Could not determine the size of the uncompressed data: the given "
          "data does not start with a valid ZSTD frame header"};
    }
    if (uncompressedSize == ZSTD_CONTENTSIZE_UNKNOWN) {
      return Error{
          "Could not determine the size of the uncompressed data: the given "
          "ZSTD frame does not store that size in its header"};
    }
    return static_cast<size_t>(uncompressedSize);
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
        ZSTD_decompressDCtx(decompressionContext(), result.data(),
                            knownOriginalSize, src, numBytes);
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
    auto decompressedSize = ZSTD_decompressDCtx(decompressionContext(), buffer,
                                                bufferCapacity, src, numBytes);
    if (auto error = toError(decompressedSize)) {
      return error.value();
    }
    return decompressedSize;
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

  // Compress `data` with `reference` as a "prefix dictionary": the compressor
  // may refer to any byte sequence of the `reference`, so the result is small
  // if `data` mostly consists of pieces of the `reference` (this is the same
  // as `zstd --patch-from`). Decompression requires the identical `reference`
  // (see `tryToDecompressWithReference`).
  //
  // Long distance matching is enabled and the window covers the `reference`
  // and `data`, so references into the whole `reference` are possible. The
  // frame stores the size and a checksum of `data`, which the decompression
  // verifies.
  //
  // The `compressionLevel` is the usual ZSTD level (1 to 22). The high levels
  // (19 and above) give noticeably smaller results for this use case and are
  // still fast enough for data in the range of a few hundred megabytes.
  static std::vector<char> compressWithReference(ql::span<const char> reference,
                                                 ql::span<const char> data,
                                                 int compressionLevel) {
    // NOTE: ZSTD silently ignores a reference that overlaps with the data in
    // memory (it then compresses without the reference), so in that case
    // compress a copy of the data.
    if (!reference.empty() && !data.empty() &&
        reference.data() < data.data() + data.size() &&
        data.data() < reference.data() + reference.size()) {
      std::vector<char> copy{data.begin(), data.end()};
      return compressWithReference(reference, copy, compressionLevel);
    }
    // The compression context holds the parameters and the reference for the
    // `ZSTD_compress2` call below. Its creation only fails if no memory can be
    // allocated.
    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> context{
        ZSTD_createCCtx(), &ZSTD_freeCCtx};
    AD_CORRECTNESS_CHECK(context != nullptr);
    // Set a single compression parameter of the `context`, and throw if ZSTD
    // rejects it.
    auto setParameter = [&context](ZSTD_cParameter parameter, int value) {
      checkError(ZSTD_CCtx_setParameter(context.get(), parameter, value));
    };
    // The usual ZSTD compression level.
    setParameter(ZSTD_c_compressionLevel, compressionLevel);
    // The compressor can only refer to the last `2^windowLog` bytes before the
    // current position. The reference is logically placed directly before the
    // data, so the window has to cover both of them (see `windowLogFor`).
    setParameter(ZSTD_c_windowLog,
                 windowLogFor(reference.size() + data.size()));
    // Long distance matching efficiently finds long matches that are far back
    // in the window, which is the typical case when the data is a modified
    // version of the reference.
    setParameter(ZSTD_c_enableLongDistanceMatching, 1);
    // Store the size of the data in the frame header (see
    // `getUncompressedSize`).
    setParameter(ZSTD_c_contentSizeFlag, 1);
    // Append a checksum of the data to the frame (4 bytes), which the
    // decompression verifies. This in particular detects a wrong `reference`
    // during the decompression, which ZSTD otherwise could not detect.
    setParameter(ZSTD_c_checksumFlag, 1);
    // Use the `reference` as the prefix dictionary for the next frame only. The
    // `reference` is not copied, so it has to stay alive until the end of the
    // `ZSTD_compress2` call below.
    checkError(
        ZSTD_CCtx_refPrefix(context.get(), reference.data(), reference.size()));
    // Compress into a buffer of the worst-case size, and shrink it to the
    // actual size afterwards.
    std::vector<char> result(ZSTD_compressBound(data.size()));
    auto compressedSize = checkError(ZSTD_compress2(
        context.get(), result.data(), result.size(), data.data(), data.size()));
    result.resize(compressedSize);
    return result;
  }

  // Decompress data that was compressed by `compressWithReference` with the
  // same `reference` into `buffer` (which has to be large enough for the
  // decompressed data), and return the number of decompressed bytes. Report an
  // error if the decompression fails, for example because the compressed data
  // is corrupted, the buffer is too small, or the `reference` is not the one
  // that was used for the compression.
  //
  // NOTE: A wrong `reference` is detected via the checksum of the decompressed
  // data in the frame. That checksum is only 32 bits long, so in very rare
  // cases a wrong `reference` remains undetected.
  static SizeOrError tryToDecompressWithReference(
      ql::span<const char> reference, ql::span<const char> compressed,
      char* buffer, size_t bufferCapacity) noexcept {
    // The decompression context holds the parameters and the reference for the
    // `ZSTD_decompressDCtx` call below. Its creation only fails if no memory
    // can be allocated.
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> context{
        ZSTD_createDCtx(), &ZSTD_freeDCtx};
    if (context == nullptr) {
      return Error{"Could not allocate a ZSTD decompression context"};
    }
    // By default, the decompressor rejects frames with a window larger than
    // `2^27` bytes to limit its memory usage. Allow the largest window, because
    // the compressor chose the window such that it covers the reference (see
    // `compressWithReference`).
    if (auto error = toError(ZSTD_DCtx_setParameter(
            context.get(), ZSTD_d_windowLogMax,
            ZSTD_dParam_getBounds(ZSTD_d_windowLogMax).upperBound))) {
      return error.value();
    }
    // Use the `reference` as the prefix dictionary for the next frame only (it
    // is not copied, and has to be the same as during compression).
    if (auto error = toError(ZSTD_DCtx_refPrefix(
            context.get(), reference.data(), reference.size()))) {
      return error.value();
    }
    // Decompress the frame into the `buffer`, and get the number of
    // decompressed bytes (or an error code).
    auto decompressedSize =
        ZSTD_decompressDCtx(context.get(), buffer, bufferCapacity,
                            compressed.data(), compressed.size());
    if (auto error = toError(decompressedSize)) {
      return error.value();
    }
    return decompressedSize;
  }

  // Throw a `std::runtime_error` if `result` is a ZSTD error code, else return
  // it unchanged. The `result` is the return value of a ZSTD function that
  // returns either a size or an error code.
  static size_t checkError(size_t result) {
    if (auto error = toError(result)) {
      throw std::runtime_error{absl::StrCat("ZSTD error: ", error->message_)};
    }
    return result;
  }

 private:
  // Return the `Error` if `result` is a ZSTD error code, else `std::nullopt`.
  static std::optional<Error> toError(size_t result) noexcept {
    if (ZSTD_isError(result)) {
      return Error{ZSTD_getErrorName(result)};
    }
    return std::nullopt;
  }

  // The smallest `ZSTD_c_windowLog` such that the window covers `numBytes`
  // bytes (clamped to the range that ZSTD allows). The window is the range of
  // the last `2^windowLog` bytes before the current position, to which the
  // compressor can refer; a larger window requires more memory for the
  // compression and the decompression.
  static int windowLogFor(size_t numBytes) {
    int log = numBytes <= 1 ? 1 : absl::bit_width(numBytes - 1);
    auto bounds = ZSTD_cParam_getBounds(ZSTD_c_windowLog);
    return std::clamp(log, bounds.lowerBound, bounds.upperBound);
  }

  // The (de)compression context of the calling thread. The one-shot
  // `ZSTD_compress`/`ZSTD_decompress` functions create, initialize, and free a
  // context on every call, which is a noticeable cost when many small blocks
  // are compressed; a context that the thread reuses avoids that.
  static ZSTD_CCtx* compressionContext() {
    thread_local std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> context{
        ZSTD_createCCtx(), &ZSTD_freeCCtx};
    AD_CORRECTNESS_CHECK(context != nullptr);
    return context.get();
  }
  static ZSTD_DCtx* decompressionContext() {
    thread_local std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> context{
        ZSTD_createDCtx(), &ZSTD_freeDCtx};
    AD_CORRECTNESS_CHECK(context != nullptr);
    return context.get();
  }

  // Return the size of the `result`, or throw a `std::runtime_error` with the
  // `messagePrefix` followed by the message of the error of the `result`.
  static size_t valueOrThrow(const SizeOrError& result,
                             std::string_view messagePrefix) {
    if (const auto* error = std::get_if<Error>(&result)) {
      throw std::runtime_error{absl::StrCat(messagePrefix, error->message_)};
    }
    return std::get<size_t>(result);
  }
};

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDWRAPPER_H
