// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_PARALLELBUFFER_H
#define QLEVER_SRC_PARSER_PARALLELBUFFER_H

#include <re2/re2.h>

#include <future>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "util/File.h"
#include "util/ThreadSafeQueue.h"
#include "util/UninitializedAllocator.h"

/**
 * @brief Abstract base class for certain input buffers.
 *
 * Is able to return a whole block of bytes from some kind of
 * input file/stream via the call to getNextBlock(). If it is
 * computationally expensive to get the next bytes (e.g. because
 * they have to be decompressed) this can be done asynchronously
 * between two calls to getNextBlock.
 */
class ParallelBuffer {
 public:
  using BufferType = ad_utility::UninitializedVector<char>;
  /**
   * @brief Specify the size of the blocks that are to be retrieved.
   * @param blocksize  the blocksize (in bytes)
   */
  explicit ParallelBuffer(size_t blocksize) : blocksize_(blocksize) {}
  virtual ~ParallelBuffer() = default;

  /**
   * @brief Get (approximately) the next blocksize_ bytes from the input stream.
   *
   * @return The next bytes or std::nullopt to signal EOF.
   */
  virtual std::optional<BufferType> getNextBlock() = 0;

  // Get the blocksize of this buffer.
  size_t getBlocksize() const { return blocksize_; }

 protected:
  size_t blocksize_ = 100 * (2 << 20);
};

/**
 * @brief Read the bytes from a file/stream and just pass them directly without
 * any modification.
 *
 * The next block is also read in parallel in case we have a
 * really slow filesystem etc.
 */
class ParallelFileBuffer : public ParallelBuffer {
 public:
  using BufferType = ad_utility::UninitializedVector<char>;

  // Construct a buffer that reads from `filename` with the given blocksize.
  // The file is opened immediately upon construction.
  ParallelFileBuffer(size_t blocksize, const std::string& filename);

  // _____________________________________________________
  std::optional<BufferType> getNextBlock() override;

 private:
  ad_utility::File file_;
  bool eof_ = true;
  BufferType buf_;
  std::future<size_t> fut_;
};

// A parallel buffer that reads input in blocks, where each block, except
// possibly the last, ends with `endRegex`. It wraps any `ParallelBuffer` as
// the underlying byte source.
class ParallelBufferWithEndRegex : public ParallelBuffer {
 public:
  // Construct from an already-opened `rawBuffer` and a regex that marks the
  // end of a statement. The blocksize is derived from `rawBuffer`.
  ParallelBufferWithEndRegex(std::unique_ptr<ParallelBuffer> rawBuffer,
                             std::string endRegex)
      : ParallelBuffer{rawBuffer->getBlocksize()},
        rawBuffer_{std::move(rawBuffer)},
        endRegex_{endRegex},
        endRegexAsString_{std::move(endRegex)} {}

  // Get the data that was read asynchronously after the previous call to this
  // function. Returns the part of the data until `endRegex` is found, with the
  // part after `endRegex` from the previous call prepended. If `endRegex` is
  // not found, simply return the rest of the data.
  std::optional<BufferType> getNextBlock() override;

 private:
  // Find `regex` near the end of `vec` by searching in blocks of 1000, 2000,
  // 4000... bytes. We have to do this, because "reverse" regex matching is not
  // trivial. Returns the number of bytes in `vec` until the end of the regex
  // match, or std::nullopt if the regex was not found at all.
  static std::optional<size_t> findRegexNearEnd(const BufferType& vec,
                                                const re2::RE2& regex);
  std::unique_ptr<ParallelBuffer> rawBuffer_;
  BufferType remainder_;
  re2::RE2 endRegex_;
  std::string endRegexAsString_;
  bool exhausted_ = false;
};

// A ParallelBuffer that serves a pre-loaded string in chunks of `blocksize`.
class StringParallelBuffer : public ParallelBuffer {
  std::string content_;
  size_t offset_ = 0;

 public:
  explicit StringParallelBuffer(std::string content, size_t blocksize)
      : ParallelBuffer{blocksize}, content_{std::move(content)} {}

  std::optional<BufferType> getNextBlock() override;
};

// A ParallelBuffer that streams its data from an HTTP request body delivered
// asynchronously by the HTTP session thread via a thread-safe queue. The
// HTTP session pushes chunks to the queue; getNextBlock() pops them (blocking
// until a chunk is available). The queue's finish() or pushException() signals
// EOF or an error to the parser thread.
class HttpBodyParallelBuffer : public ParallelBuffer {
  using ChunkQueue =
      ad_utility::data_structures::ThreadSafeQueue<std::vector<char>>;
  std::shared_ptr<ChunkQueue> queue_;

 public:
  explicit HttpBodyParallelBuffer(std::shared_ptr<ChunkQueue> queue,
                                  size_t blocksize)
      : ParallelBuffer{blocksize}, queue_{std::move(queue)} {}

  std::optional<BufferType> getNextBlock() override;
};

#endif  // QLEVER_SRC_PARSER_PARALLELBUFFER_H
