// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/AsyncBlockSource.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <stdexcept>
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
  size_t n = file_.read(buf.data(), getBlocksize().getBytes());
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
    std::string endRegex)
    : AsyncBlockSource{exec, inner->getBlocksize()},
      inner_{std::move(inner)},
      endRegex_{endRegex},
      endRegexAsString_{std::move(endRegex)} {}

// ____________________________________________________________________________
std::optional<size_t> AsyncEndRegexBlockSource::findRegexNearEnd(
    const Block& vec, const re2::RE2& regex) {
  size_t inputSize = vec.size();
  AD_CORRECTNESS_CHECK(inputSize > 0);
  size_t chunkSize = std::min<size_t>(1000, inputSize);
  re2::StringPiece regexResult;
  bool match = false;
  while (true) {
    auto startIdx = inputSize - chunkSize;
    auto regexInput = re2::StringPiece{vec.data() + startIdx, chunkSize};

    match = RE2::PartialMatch(regexInput, regex, &regexResult);
    if (match || chunkSize == inputSize) {
      break;
    }
    chunkSize = std::min(chunkSize * 2, inputSize);
  }
  if (!match) return std::nullopt;
  return regexResult.data() + regexResult.size() - vec.data();
}

// ____________________________________________________________________________
std::optional<ByteBlock> AsyncEndRegexBlockSource::getNextBlockImpl() {
  // Mark the source exhausted and return whatever is left in `remainder_`.
  auto returnRemainder = [this]() -> std::optional<ByteBlock> {
    exhausted_ = true;
    if (remainder_.empty()) return std::nullopt;
    Block result = std::move(remainder_);
    remainder_.clear();
    return result;
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

  // Search for the regex near the end of the raw block.
  auto endPosition = findRegexNearEnd(rawInput, endRegex_);
  if (endPosition.has_value()) {
    // Regex found: return remainder_ + rawInput[0..endPosition).
    return assembleResult(endPosition.value());
  }

  // No regex match. Peek at the next raw block to decide how to handle this:
  // if the inner source has more data, the current block is too short for a
  // full statement and parsing must fail. If the inner source is exhausted,
  // the current block is the last one; return it without requiring a match.
  auto peek = nextBlockFrom(*inner_);
  if (peek) {
    // Inner source has more data: this is a real "statement too large" error.
    auto rawSize = rawInput.size();
    throw std::runtime_error{absl::StrCat(
        "The regex ", endRegexAsString_,
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
