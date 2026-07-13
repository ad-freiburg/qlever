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

namespace {
// Build the exception that signals that no statement boundary (as described
// by `description`) could be found in an input batch of `inputSize` bytes
// that was not the last one.
std::exception_ptr getNoStatementBoundaryError(std::string_view description,
                                               size_t inputSize) {
  return std::make_exception_ptr(std::runtime_error{absl::StrCat(
      "No statement boundary (", description,
      ") was found in the current input batch (which was not the last one) "
      "of size ",
      ad_utility::insertThousandSeparator(std::to_string(inputSize), ','),
      "; possible fixes are: "
      "use `--parser-buffer-size` to increase the buffer size or use "
      "`--parallel-parsing false` to disable parallel parsing")});
}
}  // namespace

// ____________________________________________________________________________
BlockingBlockSource::BlockingBlockSource(const net::any_io_executor& exec,
                                         ad_utility::MemorySize blocksize)
    : AsyncBlockSource{exec, blocksize}, strand_{net::make_strand(exec)} {}

// ____________________________________________________________________________
void BlockingBlockSource::asyncGetNextBlockImpl(Handler handler) {
  net::post(strand_, [this, h = std::move(handler)]() mutable {
    try {
      h(nullptr, getNextBlockImpl());
    } catch (...) {
      h(std::current_exception(), std::nullopt);
    }
  });
}

// ____________________________________________________________________________
FileBlockSource::FileBlockSource(const net::any_io_executor& exec,
                                 ad_utility::MemorySize blocksize,
                                 const std::string& filename)
    : BlockingBlockSource{exec, blocksize} {
  file_.open(filename, "r");
}

// ____________________________________________________________________________
std::optional<ByteBlock> FileBlockSource::getNextBlockImpl() {
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
    const net::any_io_executor& exec, std::unique_ptr<AsyncBlockSource> inner,
    EndPositionFinder findEndPosition, std::string description)
    : AsyncBlockSource{exec, inner->getBlocksize()},
      inner_{std::move(inner)},
      findEndPosition_{std::move(findEndPosition)},
      description_{std::move(description)} {}

// ____________________________________________________________________________
void AsyncStatementBoundaryBlockSource::assembleAndDeliver(Handler& handler,
                                                           Block& rawInput,
                                                           size_t endPosition) {
  Block result;
  result.reserve(remainder_.size() + endPosition);
  result.insert(result.end(), remainder_.begin(), remainder_.end());
  result.insert(result.end(), rawInput.begin(), rawInput.begin() + endPosition);
  remainder_.clear();
  remainder_.insert(remainder_.end(), rawInput.begin() + endPosition,
                    rawInput.end());
  handler(nullptr, std::move(result));
}

// ____________________________________________________________________________
void AsyncStatementBoundaryBlockSource::deliverRemainder(Handler& handler) {
  exhausted_ = true;
  if (remainder_.empty()) {
    handler(nullptr, std::nullopt);
  } else {
    handler(nullptr, std::exchange(remainder_, Block{}));
  }
}

// ____________________________________________________________________________
void AsyncStatementBoundaryBlockSource::handleMissingBoundary(Handler handler,
                                                              Block rawInput) {
  AsyncBlockSource::callAsyncGetNextBlockImpl(
      *inner_,
      AsyncBlockSource::forwardErrors(
          std::move(handler),
          [this, rawInput = std::move(rawInput)](
              Handler handler, std::optional<Block> peek) mutable {
            if (!peek.has_value()) {
              // `peek` is the result of fetching another block from
              // `inner_` right after `rawInput`, so `nullopt` here means
              // `inner_` is genuinely exhausted and `rawInput` is the last
              // block. It is thus correct to also mark this source
              // exhausted and return `remainder_ + rawInput` without
              // requiring a statement boundary in it.
              exhausted_ = true;
              return assembleAndDeliver(handler, rawInput, rawInput.size());
            }
            // Inner source has more data: this is a real "statement too
            // large" error.
            return handler(
                getNoStatementBoundaryError(description_, rawInput.size()),
                std::nullopt);
          }));
}

// ____________________________________________________________________________
void AsyncStatementBoundaryBlockSource::asyncGetNextBlockImpl(Handler handler) {
  if (exhausted_) {
    return deliverRemainder(handler);
  }

  // Fetch the next raw block from the inner source. This is chained via a
  // callback (never blocked upon) because `inner_` might itself be a
  // genuinely asynchronous source (e.g. an HTTP-body-backed one); blocking
  // here could deadlock if the inner completion happens to be scheduled on
  // the very executor we would be blocking (see the
  // `BlockingBlockSource` class comment).
  AsyncBlockSource::callAsyncGetNextBlockImpl(
      *inner_,
      AsyncBlockSource::forwardErrors(
          std::move(handler),
          [this](Handler handler, std::optional<Block> rawOpt) mutable {
            if (!rawOpt.has_value()) {
              return deliverRemainder(handler);
            }
            Block rawInput = std::move(*rawOpt);

            // Search for the end of the last statement near the end of the
            // raw block.
            auto endPosition = findEndPosition_(
                std::string_view{rawInput.data(), rawInput.size()});
            if (endPosition.has_value()) {
              return assembleAndDeliver(handler, rawInput, endPosition.value());
            }

            // No boundary found. Peek at the next raw block to decide how
            // to handle this: if the inner source has more data, the
            // current block is too short for a full statement and parsing
            // must fail. If the inner source is exhausted, the current
            // block is the last one; return it without requiring a match.
            handleMissingBoundary(std::move(handler), std::move(rawInput));
          }));
}
}  // namespace qlever::parser
