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
#include <boost/asio/strand.hpp>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "util/File.h"
#include "util/MemorySize/MemorySize.h"
#include "util/UninitializedAllocator.h"

namespace qlever::parser {

// A block of raw bytes read from an input source. The same byte buffer type
// is reused by the in-memory string parser, so callers can move buffers from
// here directly into `RdfStringParser::setInputStream`.
using ByteBlock = ad_utility::UninitializedVector<char>;

// Abstract async byte source. Replaces the old `ParallelBuffer` family. The
// executor passed at construction owns an internal strand that serializes all
// `getNextBlockImpl()` calls. Callers must not invoke `asyncGetNextBlock`
// again before the previous handler has fired (single-flight contract).
// Result encoding of the handler:
//   - `error == nullptr`, `block == nullopt`  → EOF (no more data).
//   - `error == nullptr`, `block` set          → next chunk of bytes.
//   - `error != nullptr`                       → fatal error; rethrow to
//                                                surface to the user.
class AsyncBlockSource {
 public:
  using Block = ByteBlock;
  using Handler = std::function<void(std::exception_ptr, std::optional<Block>)>;

  explicit AsyncBlockSource(const boost::asio::any_io_executor& exec,
                            ad_utility::MemorySize blocksize);
  virtual ~AsyncBlockSource() = default;

  // Asynchronously deliver the next block of bytes. The `handler` is invoked
  // on the internal strand, meaning that handlers are never called
  // concurrently.
  void asyncGetNextBlock(Handler handler);

  ad_utility::MemorySize getBlocksize() const { return blocksize_; }

 protected:
  // Synchronously produce the next block of bytes. Called from within the
  // internal strand — implementers do not need extra synchronization.
  // Return `nullopt` to signal EOF.
  virtual std::optional<ByteBlock> getNextBlockImpl() = 0;

  // Helper for `AsyncEndRegexBlockSource`: call `getNextBlockImpl()` on a
  // different `AsyncBlockSource` instance. C++ protected-access rules prevent
  // calling a protected method on a sibling object, so this static trampoline
  // is provided in the base.
  static std::optional<ByteBlock> nextBlockFrom(AsyncBlockSource& src) {
    return src.getNextBlockImpl();
  }

 private:
  boost::asio::strand<boost::asio::any_io_executor> strand_;
  ad_utility::MemorySize blocksize_;
};

// An `AsyncBlockSource` (see above) that reads blocks sequentially from a
// given file.
class AsyncFileBlockSource : public AsyncBlockSource {
 public:
  // Open `filename` immediately and prepare to deliver `blocksize`-sized
  // blocks. Throws if the file cannot be opened.
  AsyncFileBlockSource(const boost::asio::any_io_executor& exec,
                       ad_utility::MemorySize blocksize,
                       const std::string& filename);

 protected:
  std::optional<ByteBlock> getNextBlockImpl() override;

 private:
  ad_utility::File file_;
  bool eof_ = false;
};

// Wrap an `AsyncBlockSource` and cut blocks at statement boundaries. For each
// block produced by the inner source, search for the last match of `endRegex`
// and return the part of the block up to and including that match, prepending
// any tail carried over from the previous block. Single-flight: the consumer
// must wait for the handler before calling `asyncGetNextBlock` again.
class AsyncEndRegexBlockSource : public AsyncBlockSource {
 public:
  // Wrap `inner` and cut its blocks at matches of `endRegex`.
  AsyncEndRegexBlockSource(const boost::asio::any_io_executor& exec,
                           std::unique_ptr<AsyncBlockSource> inner,
                           std::string endRegex);

  // Search for `regex` near the end of `vec` in exponentially-growing chunks
  // from the back. Returns the number of bytes in `vec` up to the end of the
  // match, or `nullopt` if no match was found.
  static std::optional<size_t> findRegexNearEnd(const Block& vec,
                                                const re2::RE2& regex);

 protected:
  std::optional<ByteBlock> getNextBlockImpl() override;

 private:
  std::unique_ptr<AsyncBlockSource> inner_;
  Block remainder_;
  re2::RE2 endRegex_;
  std::string endRegexAsString_;
  bool exhausted_ = false;
};

}  // namespace qlever::parser

#endif  // QLEVER_SRC_PARSER_ASYNCBLOCKSOURCE_H
