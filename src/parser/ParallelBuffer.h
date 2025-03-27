// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#ifndef QLEVER_SRC_PARSER_PARALLELBUFFER_H
#define QLEVER_SRC_PARSER_PARALLELBUFFER_H

#include <re2/re2.h>

#include <future>
#include <optional>
#include <string>
#include <vector>

#include "../util/File.h"
#include "../util/UninitializedAllocator.h"

/**
 * @brief Abstract base class for certain input buffers.
 *
 * Is able to return a whole block of bytes from some kind of
 * input file/stream via the call to getNextBlock()
 * If It is computationally expensive to get the next bytes (e.g.
 * because they have to be decompressed) this can be done asynchronously
 * between two calls to getNextBlock.
 */
class ParallelBuffer {
 public:
  using BufferType = ad_utility::UninitializedVector<char>;
  /**
   * @brief Specify the size of the blocks that are to be retrieved
   * @param blocksize  the blocksize (in bytes)
   */
  explicit ParallelBuffer(size_t blocksize) : blocksize_(blocksize) {}
  virtual ~ParallelBuffer() = default;
  /**
   * @brief Open a file (in the sense of unix files, can also be a
   * (network/pipe) stream.
   * @param filename Name of the file which is to be opened.
   */
  virtual void open(const string& filename) = 0;
  /**
   * @brief Get (approximately) the next blocksize_ bytes from the input stream.
   *
   * Only valid after a call to open().
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
  explicit ParallelFileBuffer(size_t blocksize) : ParallelBuffer(blocksize) {}

  // _________________________________________________________________________
  void open(const string& filename) override;

  // _____________________________________________________
  std::optional<BufferType> getNextBlock() override;

 private:
  ad_utility::File file_;
  bool eof_ = true;
  BufferType buf_;
  std::future<size_t> fut_;
};

// A parallel buffer that reads input from the file in blocks, where each block,
// except possibly the last, ends with `endRegex`.
class ParallelBufferWithEndRegex : public ParallelBuffer {
 public:
  ParallelBufferWithEndRegex(size_t blocksize, std::string endRegex)
      : ParallelBuffer{blocksize},
        endRegex_{endRegex},
        endRegexAsString_{std::move(endRegex)} {}

  // Get the data that was read asynchronously after the previous call to this
  // function. Returns the part of the data until `endRegex` is found, with the
  // part after `endRegex` from the previous call prepended. If `endRegex` is
  // not found, simply return the rest of the data.
  std::optional<BufferType> getNextBlock() override;

  // Open the file from which the blocks are read.
  void open(const string& filename) override { rawBuffer_.open(filename); }

 private:
  // Find `regex` near the end of `vec` by searching in blocks of 1000, 2000,
  // 4000... bytes. We have to do this, because "reverse" regex matching is not
  // trivial. Returns the st Returns the number of bytes in `vec` until the end
  // of the regex match, or std::nullopt if the regex was not found at all.
  static std::optional<size_t> findRegexNearEnd(const BufferType& vec,
                                                const re2::RE2& regex);
  ParallelFileBuffer rawBuffer_{blocksize_};
  BufferType remainder_;
  re2::RE2 endRegex_;
  std::string endRegexAsString_;
  bool exhausted_ = false;
};

#endif  // QLEVER_SRC_PARSER_PARALLELBUFFER_H
