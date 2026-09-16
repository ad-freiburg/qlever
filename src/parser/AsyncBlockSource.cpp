// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/AsyncBlockSource.h"

#include <absl/strings/str_cat.h>

#include <boost/asio/post.hpp>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "util/Exception.h"
#include "util/StringUtils.h"

namespace qlever::parser {

namespace net = boost::asio;

namespace {
// Build the exception that signals that a block of `inputSize` bytes, which was
// not the last one, contains none of the positions (as described by
// `description`) at which a block may end. `inputName` names the affected
// input, so that the error can be attributed to one of the possibly many inputs
// of an index build. Suggest disabling parallel parsing only if the input is
// actually parsed in parallel (`isParsedInParallel`), because that suggestion
// is useless (and confusing) for an input that is already parsed serially.
std::exception_ptr getNoBlockBoundaryError(std::string_view description,
                                           size_t inputSize,
                                           std::string_view inputName,
                                           bool isParsedInParallel) {
  return std::make_exception_ptr(std::runtime_error{absl::StrCat(
      "Could not split the input \"", inputName,
      "\" into blocks: QLever ends a block at ", description,
      ", but the current block (which is not the last one) of size ",
      ad_utility::insertThousandSeparator(std::to_string(inputSize), ','),
      " bytes contains no such position. To fix this, use "
      "`--parser-buffer-size` to increase the buffer size",
      isParsedInParallel
          ? ", or use `--parallel-parsing false` to disable parallel parsing, "
            "which lets QLever end a block at any newline"
          : "",
      ".")});
}
}  // namespace

// ____________________________________________________________________________
BlockingBlockSource::BlockingBlockSource(const ql::any_io_executor& exec,
                                         ad_utility::MemorySize blocksize)
    : AsyncBlockSource{exec, blocksize}, strand_{net::make_strand(exec)} {}

// ____________________________________________________________________________
void BlockingBlockSource::asyncGetNextBlockImpl(Handler handler) {
  net::post(strand_, [this, h = std::move(handler)]() mutable {
    // NOTE: The handler is deliberately invoked *outside* the `try` block:
    // the `try` must only guard `getNextBlockImpl()`. Otherwise an exception
    // thrown from within the handler chain itself (which for wrapper sources
    // runs inline and contains fallible work) would be caught here and lead
    // to the handler being invoked a second time, which violates the
    // exactly-once contract and invokes a moved-from `Handler`.
    std::optional<Block> block;
    try {
      block = getNextBlockImpl();
    } catch (...) {
      return h(std::current_exception(), std::nullopt);
    }
    h(nullptr, std::move(block));
  });
}

// ____________________________________________________________________________
FileBlockSource::FileBlockSource(const ql::any_io_executor& exec,
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
    const ql::any_io_executor& exec, std::unique_ptr<AsyncBlockSource> inner,
    EndPositionFinder findEndPosition, std::string description,
    std::string inputName, bool isParsedInParallel)
    : AsyncBlockSource{exec, inner->getBlocksize()},
      inner_{std::move(inner)},
      findEndPosition_{std::move(findEndPosition)},
      description_{std::move(description)},
      inputName_{std::move(inputName)},
      isParsedInParallel_{isParsedInParallel} {}

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
            // Inner source has more data, so the block really cannot be
            // split.
            return handler(
                getNoBlockBoundaryError(description_, rawInput.size(),
                                        inputName_, isParsedInParallel_),
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
            // raw block. `findEndPosition_` is user-supplied code, so an
            // exception from it is delivered via the handler like any other
            // error (and must not escape into the code that invoked this
            // callback, see `BlockingBlockSource::asyncGetNextBlockImpl`).
            std::optional<size_t> endPosition;
            try {
              endPosition = findEndPosition_(
                  std::string_view{rawInput.data(), rawInput.size()});
            } catch (...) {
              return handler(std::current_exception(), std::nullopt);
            }
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
