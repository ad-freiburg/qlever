// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDCONTEXT_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDCONTEXT_H

#include <zstd.h>

#include <array>
#include <cstddef>
#include <memory>
#include <vector>

#include "backports/span.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Exception.h"
#include "util/NoCopyNoMove.h"
#include "util/ThreadLocalMember.h"

namespace ad_utility {

// A ZSTD compression and decompression context that is reused for many calls.
// The one-shot functions of `ZstdWrapper` (`ZSTD_compress`, `ZSTD_decompress`)
// create, initialize, and free a context on every call, which is a noticeable
// cost when many small blocks are (de)compressed. The contexts are created
// lazily on first use, so a default-constructed `ZstdContext` is cheap. An
// object must not be used by several threads at the same time.
class ZstdContext {
 private:
  std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> compressionContext_{
      nullptr, &ZSTD_freeCCtx};
  std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> decompressionContext_{
      nullptr, &ZSTD_freeDCtx};

 public:
  // Compress the `numBytes` bytes at `src` into the `buffer` and return the
  // compressed bytes, which are a prefix of the `buffer`. The `buffer` is only
  // grown, but never shrunk, s.t. the caller can reuse it for many calls
  // without zero-initializing it each time. The result is the same as that of
  // `ZstdWrapper::compress`.
  ql::span<const char> compress(const void* src, size_t numBytes,
                                std::vector<char>& buffer,
                                int compressionLevel = 3) {
    std::array chunks{ql::span<const std::byte>{
        static_cast<const std::byte*>(src), numBytes}};
    return compressChunks(chunks, buffer, compressionLevel);
  }

  // Compress each of the `chunks` as a ZSTD frame of its own into the
  // `buffer`, and return the concatenation of the compressed frames, which is
  // a prefix of the `buffer` (see `compress` above for the reuse of the
  // `buffer`). For a single chunk, this is the same as `compress`.
  ql::span<const char> compressChunks(
      ql::span<const ql::span<const std::byte>> chunks,
      std::vector<char>& buffer, int compressionLevel = 3) {
    size_t bound = 0;
    for (auto chunk : chunks) {
      bound += ZSTD_compressBound(chunk.size());
    }
    if (buffer.size() < bound) {
      buffer.resize(bound);
    }
    size_t size = 0;
    for (auto chunk : chunks) {
      // NOTE: `ZSTD_compressCCtx` ignores all the parameters that were
      // previously set on the context, like the one-shot `ZSTD_compress`.
      size += ZstdWrapper::checkError(ZSTD_compressCCtx(
          getCompressionContext(), buffer.data() + size, buffer.size() - size,
          chunk.data(), chunk.size(), compressionLevel));
    }
    return {buffer.data(), size};
  }

  // Decompress the single ZSTD frame `compressed` into the `target` and return
  // the number of decompressed bytes. Throw if the decompression fails, e.g.
  // because the data is corrupted or the `target` is too small.
  size_t decompressToBuffer(ql::span<const char> compressed,
                            ql::span<std::byte> target) {
    return ZstdWrapper::checkError(ZSTD_decompressDCtx(
        getDecompressionContext(), target.data(), target.size(),
        compressed.data(), compressed.size()));
  }

  // Decompress the frames that `compressChunks` above has written into the
  // `targets`, one frame per target, which have to be filled completely.
  void decompressChunks(ql::span<const char> compressed,
                        ql::span<const ql::span<std::byte>> targets) {
    for (auto target : targets) {
      // A block for which no bytes were ever written (for example a block of
      // the permutations that only exists in the metadata) is empty, like for
      // `ZSTD_decompress`.
      if (compressed.empty()) {
        AD_CORRECTNESS_CHECK(target.empty());
        continue;
      }
      const size_t frameSize = ZstdWrapper::checkError(
          ZSTD_findFrameCompressedSize(compressed.data(), compressed.size()));
      auto numBytes =
          decompressToBuffer(compressed.subspan(0, frameSize), target);
      AD_CORRECTNESS_CHECK(numBytes == target.size());
      compressed = compressed.subspan(frameSize);
    }
    AD_CORRECTNESS_CHECK(compressed.empty());
  }

 private:
  // Return the compression context, and create it on the first call.
  ZSTD_CCtx* getCompressionContext() {
    if (!compressionContext_) {
      compressionContext_.reset(ZSTD_createCCtx());
      AD_CORRECTNESS_CHECK(compressionContext_ != nullptr);
    }
    return compressionContext_.get();
  }

  // Return the decompression context, and create it on the first call.
  ZSTD_DCtx* getDecompressionContext() {
    if (!decompressionContext_) {
      decompressionContext_.reset(ZSTD_createDCtx());
      AD_CORRECTNESS_CHECK(decompressionContext_ != nullptr);
    }
    return decompressionContext_.get();
  }
};

// A `ZstdContext` and a compression buffer for each thread, owned by an object
// (e.g. a writer or reader of compressed blocks) whose member functions
// (de)compress concurrently from several threads. Each thread uses the
// context and buffer of its own, s.t. no synchronization is needed, and they
// are freed when the owning object is destroyed (see `ThreadLocalMember`).
//
// Like `ThreadLocalMember`, this class can be neither copied nor moved. An
// owner that has to be movable can hold it via a `std::unique_ptr`. Using it
// from a thread that is already exiting (in the destructor of another
// `thread_local` object, after the values of the thread were destroyed) is not
// supported.
class ThreadLocalZstdContext : public NoCopyNoMove {
 private:
  struct PerThread {
    ZstdContext context_;
    std::vector<char> buffer_;
  };
  mutable ThreadLocalMember<PerThread> perThread_;

 public:
  ThreadLocalZstdContext() = default;

  // Compress like `ZstdContext::compress`, with the context and into the
  // buffer of the calling thread. The result is valid until the calling thread
  // compresses again with this object.
  ql::span<const char> compress(const void* src, size_t numBytes,
                                int compressionLevel = 3) const {
    auto& perThread = getPerThread();
    return perThread.context_.compress(src, numBytes, perThread.buffer_,
                                       compressionLevel);
  }

  // Compress like `ZstdContext::compressChunks`, with the context and into the
  // buffer of the calling thread. The result is valid until the calling thread
  // compresses again with this object.
  ql::span<const char> compressChunks(
      ql::span<const ql::span<const std::byte>> chunks,
      int compressionLevel = 3) const {
    auto& perThread = getPerThread();
    return perThread.context_.compressChunks(chunks, perThread.buffer_,
                                             compressionLevel);
  }

  // Decompress like `ZstdContext::decompressToBuffer`, with the context of the
  // calling thread.
  size_t decompressToBuffer(ql::span<const char> compressed,
                            ql::span<std::byte> target) const {
    return getPerThread().context_.decompressToBuffer(compressed, target);
  }

  // Decompress like `ZstdContext::decompressChunks`, with the context of the
  // calling thread.
  void decompressChunks(ql::span<const char> compressed,
                        ql::span<const ql::span<std::byte>> targets) const {
    getPerThread().context_.decompressChunks(compressed, targets);
  }

 private:
  // Return the context and buffer of the calling thread (see the class comment
  // for threads that are already exiting).
  PerThread& getPerThread() const {
    PerThread* perThread = perThread_.getOrCreate();
    AD_CORRECTNESS_CHECK(perThread != nullptr);
    return *perThread;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDCONTEXT_H
