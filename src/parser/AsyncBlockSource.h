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
#include <boost/asio/async_result.hpp>
#include <boost/asio/strand.hpp>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <string>

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
//
// Deliberately free of coroutines/`net::awaitable`: only a handler-based
// completion interface is used, so that this header -- and the block sources
// shared with the RDF parser (`AsyncFileBlockSource`,
// `AsyncEndRegexBlockSource`) -- keep compiling in the
// `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` build, which lacks coroutine support.
// Implementations that need genuine async I/O and have coroutines available
// (e.g. `HttpBodyBlockSource` in `index/InputFileServer.cpp`) may still use
// them internally; they just have to bridge to the handler-based
// `asyncGetNextBlockImpl` contract below.
class AsyncBlockSource {
 public:
  using Block = ByteBlock;

  // Completion handler signature for `asyncGetNextBlockImpl`. Called exactly
  // once, from any thread. A null `exception_ptr` together with `nullopt`
  // signals EOF; a non-null `exception_ptr` signals an error.
  using Handler = std::function<void(std::exception_ptr, std::optional<Block>)>;

 private:
  ad_utility::MemorySize blocksize_;

 public:
  // `blocksize` is the preferred size for the blocks to be received (a common
  // implementation detail of all derived classes, hence lives in the base
  // class).
  explicit AsyncBlockSource(ad_utility::MemorySize blocksize)
      : blocksize_{blocksize} {}
  virtual ~AsyncBlockSource() = default;

  // Asynchronously deliver the next block of bytes. Accepts any Asio
  // completion token (e.g. `boost::asio::use_future`). The completion
  // signature is `void(std::exception_ptr, std::optional<Block>)`: a null
  // `exception_ptr` signals success, a non-null one signals an exception that
  // was thrown while retrieving the next block. A successful result with
  // `std::nullopt` means EOF (no more blocks available in this source).
  // Note: the handler is invoked from whatever thread/executor
  // `asyncGetNextBlockImpl` happens to complete on; it is not automatically
  // redispatched onto the completion token's associated executor. Block
  // sources are responsible for their own execution context (e.g. via a
  // strand, see `SyncAsyncBlockSource`); no caller in this codebase currently
  // relies on redispatching, and always doing so would risk an unwanted extra
  // thread hop (e.g. onto Asio's default `system_executor`) for callers (like
  // `boost::asio::use_future`) that don't care.
  template <typename CompletionToken>
  auto asyncGetNextBlock(CompletionToken&& token) {
    namespace net = boost::asio;
    return net::async_initiate<CompletionToken,
                               void(std::exception_ptr, std::optional<Block>)>(
        [this](auto handler) mutable {
          asyncGetNextBlockImpl(
              [h = std::move(handler)](std::exception_ptr ep,
                                       std::optional<Block> block) mutable {
                // `std::move(h)` treats the handler as a one-shot, move-only
                // callable, which is the required Asio convention for
                // completion handlers.
                std::move(h)(ep, std::move(block));
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

  // Helper for wrapper sources like `AsyncEndRegexBlockSource`: call
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
// that very same strand/executor. See `AsyncEndRegexBlockSource`, which
// derives from `AsyncBlockSource` directly and chains onto its inner source
// via callbacks instead of blocking on it.
class SyncAsyncBlockSource : public AsyncBlockSource {
 private:
  boost::asio::strand<boost::asio::any_io_executor> strand_;

 public:
  // Construct from an executor (on which a strand is created to serialize the
  // calls to `getNextBlockImpl`) and a blocksize.
  SyncAsyncBlockSource(const boost::asio::any_io_executor& exec,
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
class AsyncFileBlockSource : public SyncAsyncBlockSource {
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
// block produced by the inner source, search for the last match of `endRegex`
// and return the part of the block up to and including the first capture group
// in that regex match, prepending any tail carried over from the previous
// block. If no statement boundary can be found in a complete block, an
// exception is thrown with a message that indicates possible mitigations for
// this error.
//
// This is a wrapper, not a leaf I/O source, and its inner source might be a
// genuinely asynchronous one (e.g. an HTTP-body-backed source, which is
// wrapped by this class just like a file-based source, to also cut it at
// statement boundaries). It therefore derives from `AsyncBlockSource`
// directly and chains onto the inner source's `asyncGetNextBlockImpl` purely
// via callbacks, never blocking on it -- see the `SyncAsyncBlockSource` class
// comment for why blocking here would be unsafe (it can deadlock if the inner
// source's completion is scheduled on the same single-threaded executor that
// would be blocking on it).
class AsyncEndRegexBlockSource : public AsyncBlockSource {
 private:
  std::unique_ptr<AsyncBlockSource> inner_;
  Block remainder_;
  re2::RE2 endRegex_;
  std::string endRegexAsString_;
  bool exhausted_ = false;

 public:
  // Wrap `inner` and cut its blocks at matches of `endRegex`.
  AsyncEndRegexBlockSource(std::unique_ptr<AsyncBlockSource> inner,
                           std::string endRegex);

  // Search for `regex` near the end of `vec` in exponentially-growing chunks
  // from the back. Return the number of bytes in `vec` up to the end of the
  // match, or `nullopt` if no match was found.
  static std::optional<size_t> findRegexNearEnd(const Block& vec,
                                                const re2::RE2& regex);

 protected:
  void asyncGetNextBlockImpl(Handler handler) override;
};

}  // namespace qlever::parser

#endif  // QLEVER_SRC_PARSER_ASYNCBLOCKSOURCE_H
