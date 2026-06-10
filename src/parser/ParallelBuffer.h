// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_PARALLELBUFFER_H
#define QLEVER_SRC_PARSER_PARALLELBUFFER_H

#include <absl/functional/any_invocable.h>

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "util/Iterators.h"
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
  // The blocks of the file, read ahead by a background thread.
  ad_utility::InputRangeTypeErased<BufferType> stream_;
};

// A parallel buffer that reads input in blocks, where each block, except
// possibly the last, ends at a position determined by `findEndPosition`. It
// wraps any `ParallelBuffer` as the underlying byte source.
class ParallelBufferWithEndRegex : public ParallelBuffer {
 public:
  // A function that, given a block, returns the number of bytes until the end
  // of the last statement in the block (i.e. the position at which the block
  // should be split), or `std::nullopt` if there is no such position. It is
  // expected to scan the block from the back.
  using EndPositionFinder =
      absl::AnyInvocable<std::optional<size_t>(std::string_view)>;

  // Construct from an already-opened `rawBuffer` and a function that finds the
  // end of a statement near the end of a block. `description` is used in error
  // messages to describe what marks the end of a statement. The blocksize is
  // derived from `rawBuffer`.
  ParallelBufferWithEndRegex(std::unique_ptr<ParallelBuffer> rawBuffer,
                             EndPositionFinder findEndPosition,
                             std::string description)
      : ParallelBuffer{rawBuffer->getBlocksize()},
        rawBuffer_{std::move(rawBuffer)},
        findEndPosition_{std::move(findEndPosition)},
        description_{std::move(description)} {}

  // Get the data that was read asynchronously after the previous call to this
  // function. Returns the part of the data until the end of the last statement
  // (as determined by `findEndPosition`), with the part after the end of a
  // statement from the previous call prepended. If no end of a statement is
  // found, simply return the rest of the data.
  std::optional<BufferType> getNextBlock() override;

 private:
  std::unique_ptr<ParallelBuffer> rawBuffer_;
  BufferType remainder_;
  EndPositionFinder findEndPosition_;
  std::string description_;
  bool exhausted_ = false;
};

#endif  // QLEVER_SRC_PARSER_PARALLELBUFFER_H
