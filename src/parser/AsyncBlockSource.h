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
#include <boost/asio/strand.hpp>
#include <exception>
#include <functional>
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
class AsyncBlockSource {
 public:
  using Block = ByteBlock;

  // Completion handler signature for `asyncGetNextBlockImpl`. Called exactly
  // once, from any thread. A null `exception_ptr` together with `nullopt`
  // signals EOF; a non-null `exception_ptr` signals an error.
  using Handler = std::function<void(std::exception_ptr, std::optional<Block>)>;

 private:
  boost::asio::any_io_executor executor_;
  ad_utility::MemorySize blocksize_;

 public:
  // `exec` is the default executor onto which completions are dispatched if
  // the completion token passed to `asyncGetNextBlock` has no executor of its
  // own associated with it. `blocksize` is the preferred size for the blocks
  // to be received (a common implementation detail of all derived classes,
  // hence lives in the base class).
  AsyncBlockSource(const boost::asio::any_io_executor& exec,
                   ad_utility::MemorySize blocksize)
      : executor_{exec}, blocksize_{blocksize} {}
  virtual ~AsyncBlockSource() = default;

  // Asynchronously deliver the next block of bytes. Accept any Asio
  // completion token (e.g. `boost::asio::use_future`). The completion
  // signature is `void(std::exception_ptr, std::optional<Block>)`: a null
  // `exception_ptr` signals success, a non-null one signals an exception that
  // was thrown while retrieving the next block. A successful result with
  // `std::nullopt` means EOF (no more blocks available in this source).
  // The handler is dispatched onto the executor associated with `token`, or
  // onto the executor passed to the constructor if `token` has none of its
  // own.
  template <typename CompletionToken>
  auto asyncGetNextBlock(CompletionToken&& token) {
    namespace net = boost::asio;
    return net::async_initiate<CompletionToken,
                               void(std::exception_ptr, std::optional<Block>)>(
        [this](auto handler) mutable {
          auto ex = net::get_associated_executor(handler, executor_);
          asyncGetNextBlockImpl([h = std::move(handler), ex](
                                    std::exception_ptr ep,
                                    std::optional<Block> block) mutable {
            net::dispatch(
                ex, [h = std::move(h), ep, block = std::move(block)]() mutable {
                  std::move(h)(ep, std::move(block));
                });
          });
        },
        AD_FWD(token));
  }

  ad_utility::MemorySize getBlocksize() const { return blocksize_; }

 protected:
  // The single extension point required from every block source. Must invoke
  // `handler` exactly once (synchronously or asynchronously, from any
  // thread). Implementations are responsible for their own synchronization.
  virtual void asyncGetNextBlockImpl(Handler handler) = 0;

  // Helper for wrapper sources like `AsyncStatementBoundaryBlockSource`: call
  // `asyncGetNextBlockImpl` on a different `AsyncBlockSource` instance. C++
  // protected-access rules prevent calling a protected method on a sibling
  // object directly, so this static trampoline is provided in the base.
  static void callAsyncGetNextBlockImpl(AsyncBlockSource& src,
                                        Handler handler) {
    src.asyncGetNextBlockImpl(std::move(handler));
  }
};

// An `AsyncBlockSource` whose actual byte-fetching is synchronous/blocking.
// This is deliberate for the leaf sources derived from this class: the
// underlying I/O (currently: file reads) is cheap enough that running it on a
// single dedicated strand is sufficient, and this decreases complexity
// significantly. Additionally, asynchronous file IO using `Boost::Asio`
// requires `io_uring` support on Linux, and we don't have a good fallback for
// systems without `io_uring` that is worth the complexity.
//
// Wrapper/combinator sources that themselves call into another
// `AsyncBlockSource` (which might not be blocking, e.g. `HttpBodyBlockSource`)
// must NOT derive from this class: blocking on the inner source's result from
// within a task already running on this class's own (possibly single-thread)
// strand can deadlock if the inner source's completion is itself scheduled on
// that very same strand/executor. See `AsyncStatementBoundaryBlockSource`,
// which derives from `AsyncBlockSource` directly and chains onto its inner
// source via callbacks instead of blocking on it.
class BlockingAsyncBlockSource : public AsyncBlockSource {
 private:
  boost::asio::strand<boost::asio::any_io_executor> strand_;

 public:
  // Construct from an executor (on which a strand is created to serialize the
  // calls to `getNextBlockImpl`) and a blocksize.
  BlockingAsyncBlockSource(const boost::asio::any_io_executor& exec,
                           ad_utility::MemorySize blocksize);

 protected:
  // Synchronously produce the next block of bytes. Called from within the
  // internal strand -- implementers do not need extra synchronization.
  // Return `nullopt` to signal EOF, throw exceptions on errors.
  virtual std::optional<ByteBlock> getNextBlockImpl() = 0;

 private:
  void asyncGetNextBlockImpl(Handler handler) override;
};

// An `AsyncBlockSource` (see above) that reads blocks sequentially from a
// given file.
class AsyncFileBlockSource : public BlockingAsyncBlockSource {
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
  // marks the end of a statement. `exec` is only used as the default executor
  // for dispatching completions (see `AsyncBlockSource`'s constructor).
  AsyncStatementBoundaryBlockSource(const boost::asio::any_io_executor& exec,
                                    std::unique_ptr<AsyncBlockSource> inner,
                                    EndPositionFinder findEndPosition,
                                    std::string description);

 protected:
  void asyncGetNextBlockImpl(Handler handler) override;

 private:
  // Assemble the result block from `remainder_` and `rawInput[0,
  // endPosition)`, update `remainder_` to `rawInput[endPosition, end)`, and
  // pass the result to `handler`.
  void assembleAndDeliver(const Handler& handler, Block& rawInput,
                          size_t endPosition);

  // Mark this source exhausted and pass whatever is left in `remainder_` to
  // `handler` (`nullopt` if empty).
  void deliverRemainder(const Handler& handler);

  // Called when `findEndPosition_` found no boundary in `rawInput`. Peeks at
  // the next block from `inner_` to decide whether `rawInput` is simply the
  // last block (delivered as-is via `assembleAndDeliver`) or the search
  // failed because the batch was too small (in which case `handler` receives
  // a "statement too large" error).
  void handleMissingBoundary(Handler handler, Block rawInput);
};

}  // namespace qlever::parser

#endif  // QLEVER_SRC_PARSER_ASYNCBLOCKSOURCE_H
