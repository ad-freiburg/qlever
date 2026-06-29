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
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/system_executor.hpp>
#include <boost/asio/use_future.hpp>
#include <exception>
#include <future>
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
// Completion signatures:
//   - `void(std::optional<Block>)` → delivers the next block, or `nullopt`
//     at EOF.
//   - `void(std::exception_ptr)` → fatal error; rethrow to surface to the
//     user.
class AsyncBlockSource {
 public:
  using Block = ByteBlock;

  explicit AsyncBlockSource(const boost::asio::any_io_executor& exec,
                            ad_utility::MemorySize blocksize);
  virtual ~AsyncBlockSource() = default;

  // Asynchronously deliver the next block of bytes. Accepts any Asio
  // completion token (e.g. `boost::asio::use_future`). The completion
  // fires on the handler's associated executor.
  //
  // When Boost.Asio supports multiple completion signatures
  // (`BOOST_ASIO_HAS_VARIADIC_TEMPLATES`), two separate signatures are used:
  // `void(std::optional<Block>)` for the success case and
  // `void(std::exception_ptr)` for errors. Older Boost.Asio versions fall back
  // to a single `void(std::exception_ptr, std::optional<Block>)` signature,
  // where a null `exception_ptr` indicates success.
  template <typename CompletionToken>
  auto asyncGetNextBlock(CompletionToken&& token) {
    namespace net = boost::asio;
    auto initiator = [this](auto handler) mutable {
      auto ex = net::get_associated_executor(handler, strand_);
      net::post(strand_, [this, h = std::move(handler), ex]() mutable {
        try {
          auto block = getNextBlockImpl();
          net::dispatch(ex,
                        [h = std::move(h), block = std::move(block)]() mutable {
#if defined(BOOST_ASIO_HAS_VARIADIC_TEMPLATES)
                          std::move(h)(std::move(block));
#else
                std::move(h)(nullptr, std::move(block));
#endif
                        });
        } catch (...) {
          net::dispatch(
              ex, [h = std::move(h), ep = std::current_exception()]() mutable {
#if defined(BOOST_ASIO_HAS_VARIADIC_TEMPLATES)
                std::move(h)(ep);
#else
                std::move(h)(ep, std::nullopt);
#endif
              });
        }
      });
    };
#if defined(BOOST_ASIO_HAS_VARIADIC_TEMPLATES)
    return net::async_initiate<CompletionToken, void(std::optional<Block>),
                               void(std::exception_ptr)>(
        std::move(initiator), std::forward<CompletionToken>(token));
#else
    return net::async_initiate<CompletionToken,
                               void(std::exception_ptr, std::optional<Block>)>(
        std::move(initiator), std::forward<CompletionToken>(token));
#endif
  }

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

// Specialization of `async_result` so that `boost::asio::use_future` works
// with the two-signature `asyncGetNextBlock`. This is only needed (and only
// valid) when Boost.Asio supports multiple completion signatures; older
// versions use the single-signature fallback which `use_future` handles
// natively. The success overload `void(std::optional<T>)` stores the value
// in a `std::promise`, and the error overload `void(std::exception_ptr)`
// stores the exception. The result is always a `std::future<std::optional<T>>`.
#if defined(BOOST_ASIO_HAS_VARIADIC_TEMPLATES)
namespace boost::asio {
template <typename Allocator, typename T>
class async_result<use_future_t<Allocator>, void(std::optional<T>),
                   void(std::exception_ptr)> {
 public:
  struct completion_handler_type {
    std::shared_ptr<std::promise<std::optional<T>>> p_ =
        std::make_shared<std::promise<std::optional<T>>>();
    using executor_type = boost::asio::system_executor;
    executor_type get_executor() const { return {}; }
    void operator()(std::optional<T> opt) { p_->set_value(std::move(opt)); }
    void operator()(std::exception_ptr ep) { p_->set_exception(std::move(ep)); }
  };
  using return_type = std::future<std::optional<T>>;
  explicit async_result(completion_handler_type& h)
      : future_{h.p_->get_future()} {}
  return_type get() { return std::move(future_); }
  template <typename Initiation, typename RawToken, typename... Args>
  static return_type initiate(Initiation&& init, RawToken&&, Args&&... args) {
    completion_handler_type handler;
    async_result result(handler);
    std::forward<Initiation>(init)(std::move(handler),
                                   std::forward<Args>(args)...);
    return result.get();
  }

 private:
  std::future<std::optional<T>> future_;
};
}  // namespace boost::asio
#endif  // defined(BOOST_ASIO_HAS_VARIADIC_TEMPLATES)

#endif  // QLEVER_SRC_PARSER_ASYNCBLOCKSOURCE_H
