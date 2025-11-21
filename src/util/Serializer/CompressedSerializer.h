//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMPRESSEDSERIALIZER_H
#define QLEVER_COMPRESSEDSERIALIZER_H

#include <functional>
#include <optional>
#include <vector>

#include "./SerializeVector.h"
#include "./Serializer.h"
#include "backports/span.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/MemorySize/MemorySize.h"

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
template <typename UnderlyingSerializer, typename CompressionFunction>
class CompressedWriteSerializer {
 public:
  using SerializerType = WriteSerializerTag;

  static_assert(WriteSerializer<UnderlyingSerializer>,
                "UnderlyingSerializer must be a WriteSerializer");

  CompressedWriteSerializer(UnderlyingSerializer underlyingSerializer,
                            CompressionFunction compressionFunction,
                            ad_utility::MemorySize blockSize)
      : underlyingSerializer_{std::in_place, std::move(underlyingSerializer)},
        compressionFunction_{std::move(compressionFunction)},
        blockSize_{blockSize.getBytes()} {
    buffer_.reserve(blockSize_);
  }

  CompressedWriteSerializer(const CompressedWriteSerializer&) = delete;
  CompressedWriteSerializer& operator=(const CompressedWriteSerializer&) =
      delete;
  CompressedWriteSerializer(CompressedWriteSerializer&&) = default;
  CompressedWriteSerializer& operator=(CompressedWriteSerializer&&) = default;

  ~CompressedWriteSerializer() { close(); }

  void serializeBytes(const char* bytePointer, size_t numBytes) {
    buffer_.insert(buffer_.end(), bytePointer, bytePointer + numBytes);
    flushBlocks(false);
  }

  void close() {
    if (underlyingSerializer_.has_value()) {
      flushBlocks(true);
      underlyingSerializer_.reset();
    }
  }

  UnderlyingSerializer underlyingSerializer() && {
    AD_CONTRACT_CHECK(underlyingSerializer_.has_value());
    flushBlocks(true);
    return std::move(*underlyingSerializer_);
  }

 private:
  // Flush all complete blocks, and if `flushIncomplete` is true, also flush
  // the last incomplete block.
  void flushBlocks(bool flushIncomplete) {
    size_t offset = 0;
    // Flush all complete blocks
    while (offset + blockSize_ <= buffer_.size()) {
      flushSingleBlock(
          ql::span<const char>(buffer_.data() + offset, blockSize_));
      offset += blockSize_;
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

  std::optional<UnderlyingSerializer> underlyingSerializer_;
  CompressionFunction compressionFunction_;
  size_t blockSize_;
  std::vector<char> buffer_;
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
template <typename UnderlyingSerializer, typename DecompressionFunction>
class CompressedReadSerializer {
 public:
  using SerializerType = ReadSerializerTag;

  static_assert(ReadSerializer<UnderlyingSerializer>,
                "UnderlyingSerializer must be a ReadSerializer");

  CompressedReadSerializer(UnderlyingSerializer underlyingSerializer,
                           DecompressionFunction decompressionFunction,
                           ad_utility::MemorySize blockSize)
      : underlyingSerializer_{std::move(underlyingSerializer)},
        decompressionFunction_{std::move(decompressionFunction)},
        blockSize_{blockSize.getBytes()} {}

  CompressedReadSerializer(const CompressedReadSerializer&) = delete;
  CompressedReadSerializer& operator=(const CompressedReadSerializer&) = delete;
  CompressedReadSerializer(CompressedReadSerializer&&) = default;
  CompressedReadSerializer& operator=(CompressedReadSerializer&&) = default;

  void serializeBytes(char* bytePointer, size_t numBytes) {
    size_t bytesRead = 0;
    while (bytesRead < numBytes) {
      // If buffer is empty, read and decompress the next block
      if (bufferPos_ >= buffer_.size()) {
        readNextBlock();
      }
      size_t bytesAvailable = buffer_.size() - bufferPos_;
      size_t bytesToCopy = std::min(bytesAvailable, numBytes - bytesRead);
      std::copy(buffer_.begin() + bufferPos_,
                buffer_.begin() + bufferPos_ + bytesToCopy,
                bytePointer + bytesRead);
      bufferPos_ += bytesToCopy;
      bytesRead += bytesToCopy;
    }
  }

  UnderlyingSerializer&& underlyingSerializer() && {
    return std::move(underlyingSerializer_);
  }

 private:
  void readNextBlock() {
    size_t uncompressedSize;
    underlyingSerializer_ | uncompressedSize;
    underlyingSerializer_ | compressedBuffer_;
    buffer_ = decompressionFunction_(ql::span<const char>(compressedBuffer_),
                                     uncompressedSize);
    bufferPos_ = 0;
  }

  UnderlyingSerializer underlyingSerializer_;
  DecompressionFunction decompressionFunction_;
  size_t blockSize_;
  std::vector<char> buffer_;
  std::vector<char> compressedBuffer_;
  size_t bufferPos_ = 0;
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
      ad_utility::MemorySize blockSize = detail::defaultZstdBlockSize)
      : Base{std::move(underlyingSerializer), detail::ZstdCompress{},
             blockSize} {}
};

/**
 * A read serializer that decompresses Zstd-compressed data from an underlying
 * serializer.
 */
template <typename UnderlyingSerializer>
class ZstdReadSerializer
    : public CompressedReadSerializer<UnderlyingSerializer,
                                      detail::ZstdDecompress> {
  using Base =
      CompressedReadSerializer<UnderlyingSerializer, detail::ZstdDecompress>;

 public:
  explicit ZstdReadSerializer(
      UnderlyingSerializer underlyingSerializer,
      ad_utility::MemorySize blockSize = detail::defaultZstdBlockSize)
      : Base{std::move(underlyingSerializer), detail::ZstdDecompress{},
             blockSize} {}
};

}  // namespace ad_utility::serialization

#endif  // QLEVER_COMPRESSEDSERIALIZER_H
