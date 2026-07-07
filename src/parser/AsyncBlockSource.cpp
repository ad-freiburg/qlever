// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/AsyncBlockSource.h"

#include <absl/strings/str_cat.h>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "util/Exception.h"
#include "util/StringUtils.h"

namespace qlever::parser {

namespace net = boost::asio;

// ____________________________________________________________________________
SyncAsyncBlockSource::SyncAsyncBlockSource(const net::any_io_executor& exec,
                                           ad_utility::MemorySize blocksize)
    : AsyncBlockSource{blocksize}, strand_{net::make_strand(exec)} {}

// ____________________________________________________________________________
void SyncAsyncBlockSource::asyncGetNextBlockImpl(Handler handler) {
  net::post(strand_, [this, h = std::move(handler)]() mutable {
    try {
      h(nullptr, getNextBlockImpl());
    } catch (...) {
      h(std::current_exception(), std::nullopt);
    }
  });
}

// ____________________________________________________________________________
AsyncFileBlockSource::AsyncFileBlockSource(const net::any_io_executor& exec,
                                           ad_utility::MemorySize blocksize,
                                           const std::string& filename)
    : SyncAsyncBlockSource{exec, blocksize} {
  file_.open(filename, "r");
}

// ____________________________________________________________________________
std::optional<ByteBlock> AsyncFileBlockSource::getNextBlockImpl() {
  AD_CORRECTNESS_CHECK(file_.isOpen());
  if (eof_) {
    return std::nullopt;
  }
  Block buf;
  buf.resize(getBlocksize().getBytes());
  size_t n = file_.read(buf.data(), buf.size());
  if (n == 0) {
    eof_ = true;
    return std::nullopt;
  }
  buf.resize(n);
  return buf;
}

// ____________________________________________________________________________
AsyncStatementBoundaryBlockSource::AsyncStatementBoundaryBlockSource(
    std::unique_ptr<AsyncBlockSource> inner, EndPositionFinder findEndPosition,
    std::string description)
    : AsyncBlockSource{inner->getBlocksize()},
      inner_{std::move(inner)},
      findEndPosition_{std::move(findEndPosition)},
      description_{std::move(description)} {}

// ____________________________________________________________________________
void AsyncStatementBoundaryBlockSource::asyncGetNextBlockImpl(Handler handler) {
  // Assemble the result block from `remainder_` and `rawInput[0,
  // endPosition)`, update `remainder_` to `rawInput[endPosition, end)`, and
  // pass the result to `handler`.
  auto assembleResult = [this](const Handler& handler, Block& rawInput,
                               size_t endPosition) {
    Block result;
    result.reserve(remainder_.size() + endPosition);
    result.insert(result.end(), remainder_.begin(), remainder_.end());
    result.insert(result.end(), rawInput.begin(),
                  rawInput.begin() + endPosition);
    remainder_.clear();
    remainder_.insert(remainder_.end(), rawInput.begin() + endPosition,
                      rawInput.end());
    handler(nullptr, std::move(result));
  };

  // Mark this source exhausted and pass whatever is left in `remainder_` to
  // `handler` (`nullopt` if empty).
  auto returnRemainder = [this](const Handler& handler) {
    exhausted_ = true;
    if (remainder_.empty()) {
      handler(nullptr, std::nullopt);
    } else {
      handler(nullptr, std::exchange(remainder_, Block{}));
    }
  };

  if (exhausted_) {
    returnRemainder(handler);
    return;
  }

  // Fetch the next raw block from the inner source. This is chained via a
  // callback (never blocked upon) because `inner_` might itself be a
  // genuinely asynchronous source (e.g. an HTTP-body-backed one); blocking
  // here could deadlock if the inner completion happens to be scheduled on
  // the very executor we would be blocking (see the `SyncAsyncBlockSource`
  // class comment).
  AsyncBlockSource::callAsyncGetNextBlockImpl(
      *inner_,
      [this, handler = std::move(handler), assembleResult, returnRemainder](
          std::exception_ptr ep, std::optional<Block> rawOpt) mutable {
        if (ep) {
          handler(ep, std::nullopt);
          return;
        }
        if (!rawOpt.has_value()) {
          returnRemainder(handler);
          return;
        }
        Block rawInput = std::move(*rawOpt);

        // Search for the end of the last statement near the end of the raw
        // block.
        auto endPosition = findEndPosition_(
            std::string_view{rawInput.data(), rawInput.size()});
        if (endPosition.has_value()) {
          assembleResult(handler, rawInput, endPosition.value());
          return;
        }

        // No boundary found. Peek at the next raw block to decide how to
        // handle this: if the inner source has more data, the current block
        // is too short for a full statement and parsing must fail. If the
        // inner source is exhausted, the current block is the last one;
        // return it without requiring a match.
        AsyncBlockSource::callAsyncGetNextBlockImpl(
            *inner_,
            [this, handler = std::move(handler), rawInput = std::move(rawInput),
             assembleResult](std::exception_ptr ep2,
                             std::optional<Block> peek) mutable {
              if (ep2) {
                handler(ep2, std::nullopt);
                return;
              }
              if (peek.has_value()) {
                // Inner source has more data: this is a real "statement too
                // large" error.
                auto rawSize = rawInput.size();
                handler(std::make_exception_ptr(std::runtime_error{absl::StrCat(
                            "No statement boundary (", description_,
                            ") was found in the current input batch (which "
                            "was not the last one) of size ",
                            ad_utility::insertThousandSeparator(
                                std::to_string(rawSize), ','),
                            "; possible fixes are: "
                            "use `--parser-buffer-size` to increase the buffer "
                            "size or use `--parallel-parsing false` to disable "
                            "parallel parsing")}),
                        std::nullopt);
                return;
              }
              // The current block is the last one: return remainder_ +
              // rawInput.
              exhausted_ = true;
              assembleResult(handler, rawInput, rawInput.size());
            });
      });
}
}  // namespace qlever::parser
