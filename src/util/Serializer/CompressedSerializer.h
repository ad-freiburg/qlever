// Copyright 2021 - 2025 The QLever Authors, in particular:
//
// 2021 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SERIALIZER_COMPRESSEDSERIALIZER_H
#define QLEVER_SRC_UTIL_SERIALIZER_COMPRESSEDSERIALIZER_H

#include <functional>
#include <optional>
#include <vector>

#include "backports/span.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/ExceptionHandling.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"
#include "util/UninitializedAllocator.h"

namespace ad_utility::serialization {

// A `vector<char>` where a `resize` doesn't zero-initialize the new bytes
// (for efficiency reasons).
using UninitializedBuffer =
    std::vector<char, ad_utility::default_init_allocator<char>>;

// A write serializer that writes the data in compressed blocks to the
// `UnderlyingSerializer` (which must be a `WriteSerializer') using the
// provided `CompressionFunction` (which must be callable that takes a
// span of chars and returns a vector of chars).
//
// Specifically, the data is buffered until the buffer
// exceeds the block size. Then the full block is compressed and serialized as a
// vector to the underlying serializer. The last, possibly incomplete block is
// written on destruction or when `close()` is called.
CPP_template(typename UnderlyingSerializer, typename CompressionFunction)(
    requires WriteSerializer<UnderlyingSerializer> CPP_and ql::concepts::
        invocable<CompressionFunction, ql::span<const char>,
                  UninitializedBuffer&>) class CompressedWriteSerializer {
 public:
  using SerializerType = WriteSerializerTag;

 private:
  std::optional<UnderlyingSerializer> underlyingSerializer_;
  CompressionFunction compressionFunction_;
  size_t blocksize_;
  // A buffer for the uncompressed data.
  UninitializedBuffer buffer_;
  // We need to temporarily store a single compressed block before flushing it.
  // Using a member variable for this purpose avoids reallocations.
  UninitializedBuffer compressedBuffer_;

 public:
  // Create from the underlying serializer, the function used for compression,
  // the `blocksize` for the compression.
  //
  // NOTE: We deliberately have no default value for the `blocksize`, as good
  // values depend on the nature of the compression function.
  CompressedWriteSerializer(UnderlyingSerializer underlyingSerializer,
                            CompressionFunction compressionFunction,
                            ad_utility::MemorySize blocksize)
      : underlyingSerializer_{std::in_place, std::move(underlyingSerializer)},
        compressionFunction_{std::move(compressionFunction)},
        blocksize_{blocksize.getBytes()} {
    buffer_.reserve(blocksize_);
  }

  // This is a move-only class.
  CompressedWriteSerializer(const CompressedWriteSerializer&) = delete;
  CompressedWriteSerializer& operator=(const CompressedWriteSerializer&) =
      delete;
  CompressedWriteSerializer(CompressedWriteSerializer&&) = default;
  CompressedWriteSerializer& operator=(CompressedWriteSerializer&&) = default;

  ~CompressedWriteSerializer() {
    ad_utility::terminateIfThrows(
        [this]() { close(); },
        "The closing of a `CompressedWriteSerializer` failed");
  }

  // Main serialization function.
  void serializeBytes(const char* bytePointer, size_t numBytes) {
    while (numBytes > 0) {
      size_t capacity = buffer_.capacity() - buffer_.size();
      size_t bytesToCopy = std::min(capacity, numBytes);
      buffer_.insert(buffer_.end(), bytePointer, bytePointer + bytesToCopy);
      if (bytesToCopy < capacity) {
        return;
      }
      flushBlock();
      numBytes -= bytesToCopy;
      bytePointer += bytesToCopy;
    }
  }

  // After a call to `close` no more calls to `serializeBytes` are allowed.
  void close() {
    if (underlyingSerializer_.has_value()) {
      flushBlock();
      underlyingSerializer_.reset();
    }
  }

  // Flush the temporary buffer, and then move out the underlying serializer.
  UnderlyingSerializer underlyingSerializer() && {
    AD_CORRECTNESS_CHECK(underlyingSerializer_.has_value());
    flushBlock();
    return std::move(*underlyingSerializer_);
  }

 private:
  // Flush the `buffer_` by compressing it and writing to the
  // `underlyingSerializer_`.
  //
  // NOTE: By the logic of `serializeBytes`, it is enforced that `buffer_` is
  // never larger than `blocksize_`. This lets us avoid unnecessary memory
  // allocations.
  void flushBlock() {
    if (buffer_.empty()) {
      return;
    }
    AD_CORRECTNESS_CHECK(buffer_.size() <= blocksize_);
    size_t uncompressedSize = buffer_.size();
    *underlyingSerializer_ << uncompressedSize;
    compressionFunction_(buffer_, compressedBuffer_);
    *underlyingSerializer_ << compressedBuffer_;
    buffer_.clear();
  }
};

// A read serializer that decompresses data in blocks from the
// `UnderlyingSerializer` (which must be a `ReadSerializer`) using the provided
// `DecompressionFunction` (which must be callable that takes a span of chars
// and returns the decompressed data in a preallocated target buffer).
//
// Reads compressed blocks (as vectors) from the underlying serializer,
// decompresses them, and provides the decompressed data to the caller.
// This is the counterpart to `CompressedWriteSerializer` above.
CPP_template(typename UnderlyingSerializer, typename DecompressionFunction)(
    requires ReadSerializer<UnderlyingSerializer> CPP_and ql::concepts::
        invocable<DecompressionFunction, ql::span<const char>,
                  ql::span<char>>) class CompressedReadSerializer {
 public:
  using SerializerType = ReadSerializerTag;

 private:
  UnderlyingSerializer underlyingSerializer_;
  DecompressionFunction decompressionFunction_;
  UninitializedBuffer buffer_;
  UninitializedBuffer compressedBuffer_;
  size_t bufferPos_ = 0;

 public:
  // Compress from the underlying serializer, as well as the decompression
  // function.
  CompressedReadSerializer(UnderlyingSerializer underlyingSerializer,
                           DecompressionFunction decompressionFunction)
      : underlyingSerializer_{std::move(underlyingSerializer)},
        decompressionFunction_{std::move(decompressionFunction)} {}

  // The serializer is move-only.
  CompressedReadSerializer(const CompressedReadSerializer&) = delete;
  CompressedReadSerializer& operator=(const CompressedReadSerializer&) = delete;
  CompressedReadSerializer(CompressedReadSerializer&&) = default;
  CompressedReadSerializer& operator=(CompressedReadSerializer&&) = default;

  // The main serialization function. If the `buffer_` doesn't contain enough
  // bytes, then additional blocks are read and decompressed from the underlying
  // buffer.
  void serializeBytes(char* bytePointer, size_t numBytes) {
    while (numBytes > 0) {
      // If buffer is empty, read and decompress the next block
      if (bufferPos_ >= buffer_.size()) {
        readNextBlock();
      }
      size_t bytesAvailable = buffer_.size() - bufferPos_;
      size_t bytesToCopy = std::min(bytesAvailable, numBytes);
      std::copy(buffer_.begin() + bufferPos_,
                buffer_.begin() + bufferPos_ + bytesToCopy, bytePointer);
      bytePointer += bytesToCopy;
      bufferPos_ += bytesToCopy;
      numBytes -= bytesToCopy;
    }
  }

 private:
  // Read the next block of data from the underlying serializer, decompress it
  // and store it in the `buffer_`. The `buffer_` will be overwritten by this
  // function, so it is important that the contents of the `buffer_` have
  // previously been fully consumed by the `serializeBytes` function.
  void readNextBlock() {
    size_t uncompressedSize;
    underlyingSerializer_ | uncompressedSize;
    underlyingSerializer_ | compressedBuffer_;
    buffer_.resize(uncompressedSize);
    decompressionFunction_(ql::span<const char>{compressedBuffer_},
                           ql::span<char>{buffer_});
    bufferPos_ = 0;
  }
};

// Zstd compression/decompression functions for use with CompressedSerializer
namespace detail {
struct ZstdCompress {
  void operator()(ql::span<const char> uncompressedInput,
                  UninitializedBuffer& target) const {
    size_t numBytes = uncompressedInput.size();
    target.resize(ZSTD_compressBound(numBytes));
    auto compressedSize = ZSTD_compress(target.data(), target.size(),
                                        uncompressedInput.data(), numBytes, 3);
    target.resize(compressedSize);
  }
};

struct ZstdDecompress {
  void operator()(ql::span<const char> compressedInput,
                  ql::span<char> target) const {
    auto uncompressedSize =
        ZSTD_decompress(target.data(), target.size(), compressedInput.data(),
                        compressedInput.size());
    AD_CONTRACT_CHECK(uncompressedSize == target.size());
  }
};

inline const ad_utility::MemorySize defaultZstdBlockSize =
    ad_utility::MemorySize::megabytes(8);
}  // namespace detail

// A write serializer that compresses data using `Zstd` before writing to an
// underlying serializer.
template <typename UnderlyingSerializer>
class ZstdWriteSerializer
    : public CompressedWriteSerializer<UnderlyingSerializer,
                                       detail::ZstdCompress> {
  using Base =
      CompressedWriteSerializer<UnderlyingSerializer, detail::ZstdCompress>;

 public:
  explicit ZstdWriteSerializer(
      UnderlyingSerializer underlyingSerializer,
      ad_utility::MemorySize blocksize = detail::defaultZstdBlockSize)
      : Base{std::move(underlyingSerializer), detail::ZstdCompress{},
             blocksize} {}
};

// A read serializer that decompresses data that was compressed using `Zstd`
// (with the `ZstdWriteSerializer` above) from an underlying serializer.
template <typename UnderlyingSerializer>
class ZstdReadSerializer
    : public CompressedReadSerializer<UnderlyingSerializer,
                                      detail::ZstdDecompress> {
  using Base =
      CompressedReadSerializer<UnderlyingSerializer, detail::ZstdDecompress>;

 public:
  explicit ZstdReadSerializer(UnderlyingSerializer underlyingSerializer)
      : Base{std::move(underlyingSerializer), detail::ZstdDecompress{}} {}
};

}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZER_COMPRESSEDSERIALIZER_H
