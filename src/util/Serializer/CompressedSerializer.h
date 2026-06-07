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
#include "util/Serializer/BufferedSerializer.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"
#include "util/UninitializedAllocator.h"

namespace ad_utility::serialization {

// A `BlockProcessor` (see `BufferedWriteSerializer`) that compresses each block
// using the provided `CompressionFunction` (which must be a callable that takes
// a span of chars and a target buffer to store the compressed result in) and
// writes it (prefixed with the uncompressed size) to the underlying serializer.
CPP_template(typename CompressionFunction)(
    requires ql::concepts::invocable<
        CompressionFunction, ql::span<const char>,
        UninitializedBuffer&>) class CompressingBlockProcessor {
 private:
  CompressionFunction compressionFunction_;
  // We need to temporarily store a single compressed block before flushing it.
  // Using a member variable for this purpose avoids reallocations.
  UninitializedBuffer compressedBuffer_;

 public:
  explicit CompressingBlockProcessor(CompressionFunction compressionFunction)
      : compressionFunction_{std::move(compressionFunction)} {}

  CPP_template(typename UnderlyingSerializer)(
      requires WriteSerializer<UnderlyingSerializer>) void
  operator()(ql::span<const char> block,
             UnderlyingSerializer& underlyingSerializer) {
    size_t uncompressedSize = block.size();
    underlyingSerializer << uncompressedSize;
    std::invoke(compressionFunction_, block, compressedBuffer_);
    underlyingSerializer << compressedBuffer_;
  }
};

// A `WriteSerializer` that writes the data in compressed blocks to the
// `UnderlyingSerializer` (which must be a `WriteSerializer`) using the
// provided `CompressionFunction` (which must be callable that takes a
// span of chars and returns a vector of chars).
//
// Specifically, the data is buffered until the buffer
// exceeds the block size. Then the full block is compressed and serialized as a
// vector to the underlying serializer. The last, possibly incomplete block is
// written on destruction or when `close()` is called.
//
// The buffering itself is performed by the `BufferedWriteSerializer`; the
// `CompressedWriteSerializer` merely plugs in a `CompressingBlockProcessor`
// that transforms (compresses) each block before it is passed to the next
// layer.
CPP_template(typename UnderlyingSerializer, typename CompressionFunction)(
    requires WriteSerializer<UnderlyingSerializer> CPP_and ql::concepts::
        invocable<CompressionFunction, ql::span<const char>,
                  UninitializedBuffer&>) class CompressedWriteSerializer
    : public BufferedWriteSerializer<
          UnderlyingSerializer,
          CompressingBlockProcessor<CompressionFunction>> {
  using Base =
      BufferedWriteSerializer<UnderlyingSerializer,
                              CompressingBlockProcessor<CompressionFunction>>;

 public:
  // Create from the underlying serializer, the function used for compression,
  // the `blocksize` for the compression.
  //
  // NOTE: We deliberately have no default value for the `blocksize`, as good
  // values depend on the nature of the compression function.
  CompressedWriteSerializer(UnderlyingSerializer underlyingSerializer,
                            CompressionFunction compressionFunction,
                            ad_utility::MemorySize blocksize)
      : Base{std::move(underlyingSerializer), blocksize,
             CompressingBlockProcessor<CompressionFunction>{
                 std::move(compressionFunction)}} {}
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
