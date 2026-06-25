// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/AsyncBlockSource.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <boost/asio/post.hpp>
#include <stdexcept>
#include <utility>

#include "util/Exception.h"
#include "util/StringUtils.h"

namespace qlever::parser {

namespace net = boost::asio;

// ____________________________________________________________________________
AsyncFileBlockSource::AsyncFileBlockSource(size_t blocksize,
                                           const std::string& filename)
    : blocksize_{blocksize} {
  file_.open(filename, "r");
}

// ____________________________________________________________________________
void AsyncFileBlockSource::asyncGetNextBlock(net::any_io_executor exec,
                                             Handler handler) {
  if (eof_ || !file_.isOpen()) {
    net::post(exec,
              [h = std::move(handler)]() mutable { h(nullptr, std::nullopt); });
    return;
  }
  net::post(exec, [this, h = std::move(handler)]() mutable {
    try {
      Block buf;
      buf.resize(blocksize_);
      size_t n = file_.read(buf.data(), blocksize_);
      if (n == 0) {
        eof_ = true;
        h(nullptr, std::nullopt);
        return;
      }
      buf.resize(n);
      h(nullptr, std::move(buf));
    } catch (...) {
      h(std::current_exception(), std::nullopt);
    }
  });
}

// ____________________________________________________________________________
AsyncEndRegexBlockSource::AsyncEndRegexBlockSource(
    std::unique_ptr<AsyncBlockSource> inner, std::string endRegex)
    : inner_{std::move(inner)},
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
    if (match) break;
    if (chunkSize == inputSize) break;
    chunkSize = std::min(chunkSize * 2, inputSize);
  }
  if (!match) return std::nullopt;
  return regexResult.data() + regexResult.size() - vec.data();
}

// ____________________________________________________________________________
void AsyncEndRegexBlockSource::asyncGetNextBlock(net::any_io_executor exec,
                                                 Handler handler) {
  if (exhausted_) {
    if (remainder_.empty()) {
      net::post(exec, [h = std::move(handler)]() mutable {
        h(nullptr, std::nullopt);
      });
      return;
    }
    Block copy = std::move(remainder_);
    remainder_.clear();
    net::post(exec, [h = std::move(handler), copy = std::move(copy)]() mutable {
      h(nullptr, std::move(copy));
    });
    return;
  }
  inner_->asyncGetNextBlock(
      exec, [this, exec, h = std::move(handler)](
                std::exception_ptr ep, std::optional<Block> opt) mutable {
        if (ep) {
          h(ep, std::nullopt);
          return;
        }
        if (!opt) {
          exhausted_ = true;
          if (remainder_.empty()) {
            h(nullptr, std::nullopt);
            return;
          }
          Block copy = std::move(remainder_);
          remainder_.clear();
          h(nullptr, std::move(copy));
          return;
        }
        Block rawInput = std::move(*opt);
        auto endPosition = findRegexNearEnd(rawInput, endRegex_);
        if (!endPosition) {
          handleNoMatch(exec, std::move(rawInput), std::move(h));
          return;
        }
        Block result;
        result.reserve(remainder_.size() + *endPosition);
        result.insert(result.end(), remainder_.begin(), remainder_.end());
        result.insert(result.end(), rawInput.begin(),
                      rawInput.begin() + *endPosition);
        remainder_.clear();
        remainder_.insert(remainder_.end(), rawInput.begin() + *endPosition,
                          rawInput.end());
        h(nullptr, std::move(result));
      });
}

// ____________________________________________________________________________
void AsyncEndRegexBlockSource::handleNoMatch(net::any_io_executor exec,
                                             Block rawInput, Handler handler) {
  // Peek at the inner source: if there is more data, this is a real
  // "block too small for one statement" error; if it returns EOF, this is the
  // last block and we treat its end as the implicit terminator.
  inner_->asyncGetNextBlock(exec, [this, raw = std::move(rawInput),
                                   h = std::move(handler)](
                                      std::exception_ptr ep,
                                      std::optional<Block> peek) mutable {
    if (ep) {
      h(ep, std::nullopt);
      return;
    }
    if (peek) {
      auto rawSize = raw.size();
      h(std::make_exception_ptr(std::runtime_error{absl::StrCat(
            "The regex ", endRegexAsString_,
            " which marks the end of a statement was not found in the "
            "current input batch (that was not the last one) of size ",
            ad_utility::insertThousandSeparator(std::to_string(rawSize), ','),
            "; possible fixes are: "
            "use `--parser-buffer-size` to increase the buffer size or "
            "use `--parallel-parsing false` to disable parallel parsing")}),
        std::nullopt);
      return;
    }
    exhausted_ = true;
    Block result;
    result.reserve(remainder_.size() + raw.size());
    result.insert(result.end(), remainder_.begin(), remainder_.end());
    result.insert(result.end(), raw.begin(), raw.end());
    remainder_.clear();
    h(nullptr, std::move(result));
  });
}

}  // namespace qlever::parser
