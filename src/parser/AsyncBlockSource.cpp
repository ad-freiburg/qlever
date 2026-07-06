// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/AsyncBlockSource.h"

#include <absl/strings/str_cat.h>

#include <stdexcept>
#include <string_view>
#include <utility>

#include "util/Exception.h"
#include "util/StringUtils.h"

namespace qlever::parser {

namespace net = boost::asio;

// ____________________________________________________________________________
AsyncBlockSource::AsyncBlockSource(const net::any_io_executor& exec,
                                   ad_utility::MemorySize blocksize)
    : strand_{net::make_strand(exec)}, blocksize_{blocksize} {}

// ____________________________________________________________________________
AsyncFileBlockSource::AsyncFileBlockSource(const net::any_io_executor& exec,
                                           ad_utility::MemorySize blocksize,
                                           const std::string& filename)
    : AsyncBlockSource{exec, blocksize} {
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
AsyncEndRegexBlockSource::AsyncEndRegexBlockSource(
    const net::any_io_executor& exec, std::unique_ptr<AsyncBlockSource> inner,
    EndPositionFinder findEndPosition, std::string description)
    : AsyncBlockSource{exec, inner->getBlocksize()},
      inner_{std::move(inner)},
      findEndPosition_{std::move(findEndPosition)},
      description_{std::move(description)} {}

// ____________________________________________________________________________
std::optional<ByteBlock> AsyncEndRegexBlockSource::getNextBlockImpl() {
  // Mark the source exhausted and return whatever is left in `remainder_`.
  auto returnRemainder = [this]() -> std::optional<ByteBlock> {
    exhausted_ = true;
    if (remainder_.empty()) {
      return std::nullopt;
    }
    return std::exchange(remainder_, Block{});
  };

  if (exhausted_) {
    return returnRemainder();
  }

  // Fetch the next raw block from the inner source.
  auto rawOpt = nextBlockFrom(*inner_);
  if (!rawOpt) {
    return returnRemainder();
  }
  Block rawInput = std::move(*rawOpt);

  // Return the next block which is assembled by concatenating the
  // `remainder_` with `rawInput[0..endPosition]`. The rest of the
  // `rawInput` becomes the new `remainder_` for the next iteration.
  auto assembleResult = [this, &rawInput](auto endPosition) {
    Block result;
    result.reserve(remainder_.size() + endPosition);
    result.insert(result.end(), remainder_.begin(), remainder_.end());
    result.insert(result.end(), rawInput.begin(),
                  rawInput.begin() + endPosition);
    remainder_.clear();
    remainder_.insert(remainder_.end(), rawInput.begin() + endPosition,
                      rawInput.end());
    return result;
  };

  // Search for the end of the last statement near the end of the raw block.
  auto endPosition =
      findEndPosition_(std::string_view{rawInput.data(), rawInput.size()});
  if (endPosition.has_value()) {
    // End found: return remainder_ + rawInput[0..endPosition).
    return assembleResult(endPosition.value());
  }

  // No boundary found. Peek at the next raw block to decide how to handle this:
  // if the inner source has more data, the current block is too short for a
  // full statement and parsing must fail. If the inner source is exhausted,
  // the current block is the last one; return it without requiring a match.
  auto peek = nextBlockFrom(*inner_);
  if (peek) {
    // Inner source has more data: this is a real "statement too large" error.
    auto rawSize = rawInput.size();
    throw std::runtime_error{absl::StrCat(
        "The pattern ", description_,
        " which marks the end of a statement was not found in the "
        "current input batch (that was not the last one) of size ",
        ad_utility::insertThousandSeparator(std::to_string(rawSize), ','),
        "; possible fixes are: "
        "use `--parser-buffer-size` to increase the buffer size or "
        "use `--parallel-parsing false` to disable parallel parsing")};
  }
  // The current block is the last one: return remainder_ + rawInput.
  exhausted_ = true;
  return assembleResult(rawInput.size());
}
}  // namespace qlever::parser
