// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_ASYNCBLOCKSOURCE_H
#define QLEVER_SRC_PARSER_ASYNCBLOCKSOURCE_H

#include <re2/re2.h>

#include <boost/asio/any_io_executor.hpp>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "util/File.h"
#include "util/UninitializedAllocator.h"

namespace qlever::parser {

// A block of raw bytes read from an input source. The same byte buffer type
// is reused by the in-memory string parser, so callers can move buffers from
// here directly into `RdfStringParser::setInputStream`.
using ByteBlock = ad_utility::UninitializedVector<char>;

// Abstract async byte source. Replaces the old `ParallelBuffer` family. Each
// implementation is single-flight: callers must not call `asyncGetNextBlock`
// again before the previous handler has fired. The handler is always invoked
// via `exec`. Result encoding:
//   - `error == nullptr`, `block == nullopt`  → EOF (no more data).
//   - `error == nullptr`, `block` set          → next chunk of bytes.
//   - `error != nullptr`                       → fatal error; rethrow to
//                                                surface to the user.
class AsyncBlockSource {
 public:
  using Block = ByteBlock;
  using Handler = std::function<void(std::exception_ptr, std::optional<Block>)>;

  virtual void asyncGetNextBlock(boost::asio::any_io_executor exec,
                                 Handler handler) = 0;
  virtual size_t getBlocksize() const = 0;
  virtual ~AsyncBlockSource() = default;
};

// Read bytes from a file on disk. Replaces `ParallelFileBuffer`. There is no
// internal prefetching thread; the "next block read in parallel with parsing"
// overlap is achieved by the caller (typically a parser) arming the next
// `asyncGetNextBlock` request immediately, on the same executor.
class AsyncFileBlockSource : public AsyncBlockSource {
 public:
  // Opens `filename` immediately and prepares to deliver `blocksize`-sized
  // blocks. Throws if the file cannot be opened.
  AsyncFileBlockSource(size_t blocksize, const std::string& filename);

  void asyncGetNextBlock(boost::asio::any_io_executor exec,
                         Handler handler) override;
  size_t getBlocksize() const override { return blocksize_; }

 private:
  ad_utility::File file_;
  size_t blocksize_;
  bool eof_ = false;
};

// Cut blocks at statement boundaries. Replaces `ParallelBufferWithEndRegex`.
// Wraps any `AsyncBlockSource` as the underlying byte source; for each
// produced block, searches for the last match of `endRegex` (typically the
// `.` that ends a Turtle statement) and returns the part of the block up to
// and including that match, prepending any tail carried over from the
// previous block. Single-flight: the consumer must wait for the handler
// before calling `asyncGetNextBlock` again.
class AsyncEndRegexBlockSource : public AsyncBlockSource {
 public:
  AsyncEndRegexBlockSource(std::unique_ptr<AsyncBlockSource> inner,
                           std::string endRegex);

  void asyncGetNextBlock(boost::asio::any_io_executor exec,
                         Handler handler) override;
  size_t getBlocksize() const override { return inner_->getBlocksize(); }

  // Search for `regex` near the end of `vec` in exponentially-growing chunks
  // from the back. Returns the number of bytes in `vec` up to the end of the
  // match, or `nullopt` if no match was found.
  static std::optional<size_t> findRegexNearEnd(const Block& vec,
                                                const re2::RE2& regex);

 private:
  // After receiving a block whose regex match is missing, peek at the inner
  // source to decide whether this is the last block (treat as terminator) or
  // an oversized statement (raise). Continuation of `asyncGetNextBlock`.
  void handleNoMatch(boost::asio::any_io_executor exec, Block rawInput,
                     Handler handler);

  std::unique_ptr<AsyncBlockSource> inner_;
  Block remainder_;
  re2::RE2 endRegex_;
  std::string endRegexAsString_;
  bool exhausted_ = false;
};

}  // namespace qlever::parser

#endif  // QLEVER_SRC_PARSER_ASYNCBLOCKSOURCE_H
