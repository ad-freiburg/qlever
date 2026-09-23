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

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

#include "backports/memory.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Serializer/Serializer.h"
#include "util/UninitializedAllocator.h"
#include "util/UniqueCleanup.h"

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
  struct State {
    UnderlyingSerializer underlyingSerializer_;
    BlockProcessor blockProcessor_;
    size_t blocksize_;
    // The buffer for the not-yet-forwarded data, allocated once with exactly
    // `blocksize_` bytes and never reallocated.
    //
    // NOTE: Both a `std::vector<char, default_init_allocator<char>>` and a
    // plain `std::vector<char>` are slower here: the custom allocator loses the
    // `memcpy` fast path for bulk copies, which is only taken for exactly
    // `std::allocator`, and `insert` also handles reallocation and insertion in
    // the middle, neither of which can happen here.
    std::unique_ptr<char[]> buffer_;
    // The number of bytes currently in the `buffer_`.
    size_t bufferSize_ = 0;

    // Forward the contents of the `buffer_` to the `blockProcessor_` (which
    // writes them to the underlying serializer) and clear it.
    void flushBlock() {
      if (bufferSize_ == 0) {
        return;
      }
      std::invoke(blockProcessor_,
                  ql::span<const char>{buffer_.get(), bufferSize_},
                  underlyingSerializer_);
      bufferSize_ = 0;
    }
  };

  // Flush the remaining buffered data and move out the underlying serializer.
  // Runs on destruction and when the serializer is overwritten, unless `close`
  // or `underlyingSerializer` was called before.
  struct Closer {
    UnderlyingSerializer operator()(State&& state) const {
      state.flushBlock();
      return std::move(state.underlyingSerializer_);
    }
  };
  unique_cleanup::UniqueCleanup<State, Closer> state_;

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
      : state_{
            State{std::move(underlyingSerializer), std::move(blockProcessor),
                  blocksize.getBytes(),
                  // NOTE: `make_unique_for_overwrite` (as opposed to
                  // `make_unique`) doesn't zero-initialize the buffer.
                  ql::make_unique_for_overwrite<char[]>(blocksize.getBytes())},
            Closer{}} {
    // A blocksize of zero would make `serializeBytes` below loop forever.
    AD_CONTRACT_CHECK(state_->blocksize_ > 0);
  }

  // This is a move-only class. The `UniqueCleanup` closes a serializer that is
  // overwritten, and never a moved-from one.
  BufferedWriteSerializer(const BufferedWriteSerializer&) = delete;
  BufferedWriteSerializer& operator=(const BufferedWriteSerializer&) = delete;
  BufferedWriteSerializer(BufferedWriteSerializer&&) = default;
  BufferedWriteSerializer& operator=(BufferedWriteSerializer&&) = default;

  // Main serialization function.
  void serializeBytes(const char* bytePointer, size_t numBytes) {
    State& state = *state_;
    while (numBytes > 0) {
      size_t capacity = state.blocksize_ - state.bufferSize_;
      size_t bytesToCopy = std::min(capacity, numBytes);
      std::memcpy(state.buffer_.get() + state.bufferSize_, bytePointer,
                  bytesToCopy);
      state.bufferSize_ += bytesToCopy;
      if (bytesToCopy < capacity) {
        return;
      }
      state.flushBlock();
      numBytes -= bytesToCopy;
      bytePointer += bytesToCopy;
    }
  }

  // Flush the remaining buffered data and destroy the underlying serializer.
  // After a call to `close` no more calls to `serializeBytes` are allowed.
  void close() {
    if (state_.isActive()) {
      std::move(state_).runNow();
    }
  }

  // Flush the remaining buffered data, and then move out the underlying
  // serializer.
  UnderlyingSerializer underlyingSerializer() && {
    AD_CORRECTNESS_CHECK(state_.isActive());
    return std::move(state_).runNow();
  }

  // Return the position at which the next serialized byte will end up in the
  // underlying serializer. This includes the bytes that are still sitting in
  // the buffer.
  //
  // NOTE: This is only meaningful if the blocks arrive at the underlying
  // serializer unchanged, which is the case for the
  // `PassthroughBlockProcessor`, but for example not for the
  // `CompressingBlockProcessor` (see `CompressedSerializer.h`), where a
  // position in the buffered stream bears no relation to a position in the
  // underlying serializer.
  [[nodiscard]] uint64_t getSerializationPosition() const {
    static_assert(
        std::is_same_v<BlockProcessor, PassthroughBlockProcessor>,
        "`getSerializationPosition` is only supported by a "
        "`BufferedWriteSerializer` that forwards its blocks unchanged");
    AD_CORRECTNESS_CHECK(state_.isActive());
    return state_->underlyingSerializer_.getSerializationPosition() +
           state_->bufferSize_;
  }

  // Overload of `serializeAtPosition` (see `Serializer.h`) for a
  // `BufferedWriteSerializer`. First flush the buffer, such that the
  // positions can then be handled entirely by the underlying serializer.
  //
  // NOTE: This is a hidden friend (and hence only found via ADL) because it
  // needs access to the buffer and to the underlying serializer, neither of
  // which is part of the public interface of this class. The same restriction
  // as for `getSerializationPosition` above applies.
  template <typename T>
  friend void serializeAtPosition(BufferedWriteSerializer& serializer,
                                  uint64_t position, const T& element) {
    static_assert(
        std::is_same_v<BlockProcessor, PassthroughBlockProcessor>,
        "`serializeAtPosition` is only supported by a "
        "`BufferedWriteSerializer` that forwards its blocks unchanged");
    AD_CORRECTNESS_CHECK(serializer.state_.isActive());
    serializer.state_->flushBlock();
    serializeAtPosition(serializer.state_->underlyingSerializer_, position,
                        element);
  }
};

}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZER_BUFFEREDSERIALIZER_H
