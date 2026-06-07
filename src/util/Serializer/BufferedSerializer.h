//  Copyright 2026 The QLever Authors, in particular:
//
//  2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SERIALIZER_BUFFEREDSERIALIZER_H
#define QLEVER_SRC_UTIL_SERIALIZER_BUFFEREDSERIALIZER_H

#include <optional>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Serializer/Serializer.h"
#include "util/UninitializedAllocator.h"

namespace ad_utility::serialization {

// A `vector<char>` where a `resize`/`insert` doesn't zero-initialize the new
// bytes (for efficiency reasons).
using UninitializedBuffer = std::vector<char, default_init_allocator<char>>;

// The default `BlockProcessor` for the `BufferedWriteSerializer` below. It
// writes each buffered block verbatim to the underlying serializer, making the
// `BufferedWriteSerializer` completely transparent: the bytes arrive at the
// underlying serializer in exactly the same order, just batched into larger
// chunks.
struct PassthroughBlockProcessor {
  CPP_template(typename UnderlyingSerializer)(
      requires WriteSerializer<UnderlyingSerializer>) void
  operator()(ql::span<const char> block,
             UnderlyingSerializer& underlyingSerializer) const {
    underlyingSerializer.serializeBytes(block.data(), block.size());
  }
};

// A `WriteSerializer` that buffers the incoming bytes and only forwards them to
// the `UnderlyingSerializer` (which must be a `WriteSerializer`) once a full
// block (of the size passed to the constructor) has accumulated. This reduces
// the number of (potentially expensive) calls to the underlying serializer.
//
// The optional `BlockProcessor` can be used to transform each full block before
// it is written to the underlying serializer (see e.g. the
// `CompressedWriteSerializer`, which uses this to compress each block). By
// default, the block is written as-is (see `PassthroughBlockProcessor`).
CPP_template(typename UnderlyingSerializer,
             typename BlockProcessor = PassthroughBlockProcessor)(
    requires WriteSerializer<
        UnderlyingSerializer>) class BufferedWriteSerializer {
 public:
  using SerializerType = WriteSerializerTag;

 private:
  std::optional<UnderlyingSerializer> underlyingSerializer_;
  BlockProcessor blockProcessor_;
  size_t blocksize_;
  // The buffer for the not-yet-forwarded data. Its capacity is never exceeded,
  // so it is never reallocated after the initial `reserve`.
  UninitializedBuffer buffer_;

 public:
  // Create from the underlying serializer and the `blocksize` (the amount of
  // data that is buffered before a block is forwarded). The optional
  // `blockProcessor` transforms each block before forwarding it (see above).
  //
  // NOTE: We deliberately have no default value for the `blocksize`, as good
  // values depend on the use case.
  BufferedWriteSerializer(UnderlyingSerializer underlyingSerializer,
                          MemorySize blocksize,
                          BlockProcessor blockProcessor = {})
      : underlyingSerializer_{std::in_place, std::move(underlyingSerializer)},
        blockProcessor_{std::move(blockProcessor)},
        blocksize_{blocksize.getBytes()} {
    buffer_.reserve(blocksize_);
  }

  // This is a move-only class.
  BufferedWriteSerializer(const BufferedWriteSerializer&) = delete;
  BufferedWriteSerializer& operator=(const BufferedWriteSerializer&) = delete;
  BufferedWriteSerializer(BufferedWriteSerializer&&) = default;
  BufferedWriteSerializer& operator=(BufferedWriteSerializer&&) = default;

  ~BufferedWriteSerializer() {
    ad_utility::terminateIfThrows(
        [this]() { close(); },
        "The closing of a `BufferedWriteSerializer` failed");
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

  // Flush the remaining buffered data, and then move out the underlying
  // serializer.
  UnderlyingSerializer underlyingSerializer() && {
    AD_CORRECTNESS_CHECK(underlyingSerializer_.has_value());
    flushBlock();
    return std::move(*underlyingSerializer_);
  }

 private:
  // Forward the contents of the `buffer_` to the `blockProcessor_` (which
  // writes them to the underlying serializer) and clear it.
  void flushBlock() {
    if (buffer_.empty()) {
      return;
    }
    std::invoke(blockProcessor_, ql::span<const char>{buffer_},
                *underlyingSerializer_);
    buffer_.clear();
  }
};

}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZER_BUFFEREDSERIALIZER_H
