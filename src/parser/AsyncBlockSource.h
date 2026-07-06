// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_ASYNCBLOCKSOURCE_H
#define QLEVER_SRC_PARSER_ASYNCBLOCKSOURCE_H

#include <absl/functional/any_invocable.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "util/File.h"
#include "util/Forward.h"
#include "util/MemorySize/MemorySize.h"
#include "util/UninitializedAllocator.h"

namespace qlever::parser {

// A block of raw bytes read from an input source. The same byte buffer type
// is reused by the in-memory string parser, so callers can move buffers from
// here directly into `RdfStringParser::setInputStream`.
using ByteBlock = ad_utility::UninitializedVector<char>;

// Abstract base class for a source that produces `ByteBlock`s asynchronously.
// The base class provides the async interface (which internally synchronizes
// via a `strand`, see `asyncGetNextBlock` for details).
class AsyncBlockSource {
 public:
  using Block = ByteBlock;

 private:
  boost::asio::strand<boost::asio::any_io_executor> strand_;
  ad_utility::MemorySize blocksize_;

 public:
  // Construct from an executor (on which a strand will be created to
  // synchronize the calls to `asyncGetNextBlock`, and which will also be the
  // default executor for the provided completion tokens), and a `blocksize` for
  // the blocks to be received (which is a common implementation detail of all
  // the derived classes, and thus lives in the base class).
  explicit AsyncBlockSource(const boost::asio::any_io_executor& exec,
                            ad_utility::MemorySize blocksize);
  virtual ~AsyncBlockSource() = default;

  // Asynchronously deliver the next block of bytes. Accept any Asio
  // completion token (e.g. `boost::asio::use_future`). The completion fires
  // on the handler's associated executor, or on the `strand` managed by this
  // `AsyncBlockSource`, if no executor is associated with the token. The
  // completion signature is `void(std::exception_ptr, std::optional<Block>)`: a
  // null `exception_ptr` signals success, a non-null one signals an exception
  // that was thrown while retrieving the next block. A successful result with
  // `std::nullopt` means EOF (no more blocks available in this source).
  // Note: the actual reading of blocks is always done single-threaded (via a
  // `strand`) and blocking (by calling the non-async impl function on the
  // child classes). This is deliberate, because we have never seen the reading
  // to be a bottleneck and it decreases complexity significantly. Additionally,
  // asynchronous file IO using `Boost::Asio` requires `io_uring` support on
  // Linux, and we don't have a good fallback for systems without `io_uring`
  // that is worth the complexity.
  template <typename CompletionToken>
  auto asyncGetNextBlock(CompletionToken&& token) {
    namespace net = boost::asio;
    return net::async_initiate<CompletionToken,
                               void(std::exception_ptr, std::optional<Block>)>(
        [this](auto handler) mutable {
          // Fall back to `strand_` if the token/handler has no associated
          // executor.
          auto ex = net::get_associated_executor(handler, strand_);
          net::post(strand_, [this, h = std::move(handler), ex]() mutable {
            try {
              auto block = getNextBlockImpl();
              net::dispatch(
                  ex, [h = std::move(h), block = std::move(block)]() mutable {
                    // `std::move(h)` treats the handler as a one-shot,
                    // move-only callable, which is the required Asio
                    // convention for completion handlers.
                    std::move(h)(nullptr, std::move(block));
                  });
            } catch (...) {
              net::dispatch(ex, [h = std::move(h),
                                 ep = std::current_exception()]() mutable {
                std::move(h)(ep, std::nullopt);  // See comment above.
              });
            }
          });
        },
        AD_FWD(token));
  }

  ad_utility::MemorySize getBlocksize() const { return blocksize_; }

 protected:
  // Synchronously produce the next block of bytes. Called from within the
  // internal strand — implementers do not need extra synchronization.
  // Return `nullopt` to signal EOF, throw exceptions on errors.
  virtual std::optional<ByteBlock> getNextBlockImpl() = 0;

  // Helper for `AsyncStatementBoundaryBlockSource`: call `getNextBlockImpl()`
  // on a different `AsyncBlockSource` instance. C++ protected-access rules
  // prevent calling a protected method on a sibling object, so this static
  // trampoline is provided in the base.
  static std::optional<ByteBlock> nextBlockFrom(AsyncBlockSource& src) {
    return src.getNextBlockImpl();
  }
};

// An `AsyncBlockSource` (see above) that reads blocks sequentially from a
// given file.
class AsyncFileBlockSource : public AsyncBlockSource {
 private:
  ad_utility::File file_;
  bool eof_ = false;

 public:
  // Open `filename` immediately and prepare to deliver `blocksize`-sized
  // blocks. Throw if the file cannot be opened.
  AsyncFileBlockSource(const boost::asio::any_io_executor& exec,
                       ad_utility::MemorySize blocksize,
                       const std::string& filename);

 protected:
  std::optional<ByteBlock> getNextBlockImpl() override;
};

// Wrap an `AsyncBlockSource` and cut blocks at statement boundaries. For each
// block produced by the inner source, `findEndPosition` determines the number
// of bytes until the end of the last statement in the block (it is expected to
// scan the block from the back); the block is returned up to that position,
// with the tail carried over from the previous block prepended. If no statement
// boundary can be found in a complete block, an exception is thrown with a
// message that indicates possible mitigations for this error.
class AsyncStatementBoundaryBlockSource : public AsyncBlockSource {
 public:
  // A function that, given a block, returns the number of bytes until the end
  // of the last statement in the block (i.e. the position at which the block
  // should be split), or `std::nullopt` if there is no such position. It is
  // expected to scan the block from the back.
  using EndPositionFinder =
      absl::AnyInvocable<std::optional<size_t>(std::string_view)>;

 private:
  std::unique_ptr<AsyncBlockSource> inner_;
  Block remainder_;
  EndPositionFinder findEndPosition_;
  std::string description_;
  bool exhausted_ = false;

 public:
  // Wrap `inner` and cut its blocks at the positions determined by
  // `findEndPosition`. `description` is used in error messages to describe what
  // marks the end of a statement.
  AsyncStatementBoundaryBlockSource(const boost::asio::any_io_executor& exec,
                                    std::unique_ptr<AsyncBlockSource> inner,
                                    EndPositionFinder findEndPosition,
                                    std::string description);

 protected:
  std::optional<ByteBlock> getNextBlockImpl() override;
};

}  // namespace qlever::parser

#endif  // QLEVER_SRC_PARSER_ASYNCBLOCKSOURCE_H
