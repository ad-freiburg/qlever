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

namespace ad_utility::serialization {

/**
 * A write serializer that compresses data in blocks before writing to an
 * underlying serializer.
 *
 * Data is buffered until the buffer exceeds the block size, then the full
 * block is compressed and serialized as a vector to the underlying serializer.
 * The last (possibly incomplete) block is written on destruction or when
 * close() is called.
 *
 * @tparam UnderlyingSerializer The underlying serializer type (must be a
 * WriteSerializer)
 * @tparam CompressionFunction A callable that takes a span of chars and returns
 * a vector of chars
 */
CPP_template(typename UnderlyingSerializer, typename CompressionFunction)(
    requires WriteSerializer<UnderlyingSerializer> CPP_and
        ad_utility::InvocableWithConvertibleReturnType<
            CompressionFunction, ql::span<const char>,
            ql::span<const char>>) class CompressedWriteSerializer {
 public:
  using SerializerType = WriteSerializerTag;

 private:
  std::optional<UnderlyingSerializer> underlyingSerializer_;
  CompressionFunction compressionFunction_;
  size_t blocksize_;
  std::vector<char> buffer_;

 public:
  // Create from the underlying serialzier, the function used for compression,
  // the `blocksize` for the compression. Note: We deliberately have no default
  // value for the `blocksize`, as good values depend on the nature of the
  // compression function.
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
      flushBlocks(false);
      numBytes -= bytesToCopy;
      bytePointer += bytesToCopy;
    }
  }

  // After a call to `close` no more calls to `serializeBytes` are allowed.
  void close() {
    if (underlyingSerializer_.has_value()) {
      flushBlocks(true);
      underlyingSerializer_.reset();
    }
  }

  // Flush the temporary buffer, and then move out the underlying serializer.
  UnderlyingSerializer underlyingSerializer() && {
    AD_CORRECTNESS_CHECK(underlyingSerializer_.has_value());
    flushBlocks(true);
    return std::move(*underlyingSerializer_);
  }

 private:
  // Flush all complete blocks, and if `flushIncomplete` is true, also flush
  // the last incomplete block.
  void flushBlocks(bool flushIncomplete) {
    size_t offset = 0;
    // Flush all complete blocks
    while (offset + blocksize_ <= buffer_.size()) {
      flushSingleBlock(
          ql::span<const char>(buffer_.data() + offset, blocksize_));
      offset += blocksize_;
    }
    // Flush incomplete block at the end if requested
    if (flushIncomplete && offset < buffer_.size()) {
      flushSingleBlock(ql::span<const char>(buffer_.data() + offset,
                                            buffer_.size() - offset));
      offset = buffer_.size();
    }
    // Erase all flushed data
    if (offset > 0) {
      buffer_.erase(buffer_.begin(), buffer_.begin() + offset);
    }
  }

  void flushSingleBlock(ql::span<const char> blockData) {
    size_t uncompressedSize = blockData.size();
    *underlyingSerializer_ << uncompressedSize;
    *underlyingSerializer_ << compressionFunction_(blockData);
  }
};

/**
 * A read serializer that decompresses data in blocks from an underlying
 * serializer.
 *
 * Reads compressed blocks (as vectors) from the underlying serializer,
 * decompresses them, and provides the decompressed data to the caller.
 *
 * @tparam UnderlyingSerializer The underlying serializer type (must be a
 * ReadSerializer)
 * @tparam DecompressionFunction A callable that takes a span of chars
 * (compressed) and a size_t (uncompressed size) and returns a vector of chars
 */
CPP_template(typename UnderlyingSerializer, typename DecompressionFunction)(
    requires ReadSerializer<UnderlyingSerializer> CPP_and
        ad_utility::InvocableWithConvertibleReturnType<
            DecompressionFunction, ql::span<const char>, ql::span<const char>,
            size_t>) class CompressedReadSerializer {
 public:
  using SerializerType = ReadSerializerTag;

 private:
  UnderlyingSerializer underlyingSerializer_;
  DecompressionFunction decompressionFunction_;
  std::vector<char> buffer_;
  std::vector<char> compressedBuffer_;
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
  // and store it in the `buffer`. The `buffer` will be overwritten, so it is
  // important, to read in advance.
  void readNextBlock() {
    size_t uncompressedSize;
    underlyingSerializer_ | uncompressedSize;
    underlyingSerializer_ | compressedBuffer_;
    buffer_ = decompressionFunction_(ql::span<const char>(compressedBuffer_),
                                     uncompressedSize);
    bufferPos_ = 0;
  }
};

// Zstd compression/decompression functions for use with CompressedSerializer
namespace detail {
struct ZstdCompress {
  std::vector<char> operator()(ql::span<const char> data) const {
    return ZstdWrapper::compress(data.data(), data.size());
  }
};

struct ZstdDecompress {
  std::vector<char> operator()(ql::span<const char> data,
                               size_t uncompressedSize) const {
    return ZstdWrapper::decompress<char>(const_cast<char*>(data.data()),
                                         data.size(), uncompressedSize);
  }
};

inline const ad_utility::MemorySize defaultZstdBlockSize =
    ad_utility::MemorySize::megabytes(8);
}  // namespace detail

/**
 * A write serializer that compresses data using Zstd before writing to an
 * underlying serializer.
 */
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

/**
 * A read serializer that decompresses Zstd-compressed data from an underlying
 * serializer. The data must have been serialized using the
 * `ZstdWriteSerializer` above.
 */
template <typename UnderlyingSerializer>
class ZstdReadSerializer
    : public CompressedReadSerializer<UnderlyingSerializer,
                                      detail::ZstdDecompress> {
  using Base =
      CompressedReadSerializer<UnderlyingSerializer, detail::ZstdDecompress>;

 public:
  // ___________________________________________________________________________
  explicit ZstdReadSerializer(UnderlyingSerializer underlyingSerializer)
      : Base{std::move(underlyingSerializer), detail::ZstdDecompress{}} {}
};

}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZER_COMPRESSEDSERIALIZER_H
