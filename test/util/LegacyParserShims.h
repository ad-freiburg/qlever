// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Synchronous, blocking shims around the asynchronous RDF parser classes,
// used by `test/RdfParserTest.cpp`. They preserve the legacy
// `RdfStreamParser`, `RdfParallelParser`, and `RdfMultifileParser`
// constructor + `getLine(triple)` + `getBatch()` API on top of the new
// `AsyncBlockSource` / `AsyncSingleFileParser` / `AsyncMultifileParser`
// types. New production code should call the async API directly.

#ifndef QLEVER_TEST_UTIL_LEGACYPARSERSHIMS_H
#define QLEVER_TEST_UTIL_LEGACYPARSERSHIMS_H

#include <boost/asio/thread_pool.hpp>
#include <future>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "index/InputFileSpecification.h"
#include "parser/AsyncBlockSource.h"
#include "parser/AsyncMultifileParser.h"
#include "parser/AsyncSingleFileParser.h"
#include "parser/RdfParser.h"

namespace qlever::test {

// `ParallelFileBuffer` survives only as an alias for the async block source
// so old test code can still write `std::make_unique<ParallelFileBuffer>(N,
// path)` to construct a byte source for the parser shims below.
using LegacyParallelFileBuffer = qlever::parser::AsyncFileBlockSource;

// Helper: synchronously drain one `asyncGetNextBatch` call.
template <typename AsyncParser>
inline std::optional<std::vector<TurtleTriple>> drainOneBatch(
    AsyncParser& parser) {
  std::promise<
      std::pair<std::exception_ptr, std::optional<std::vector<TurtleTriple>>>>
      promise;
  auto future = promise.get_future();
  parser.asyncGetNextBatch(
      [&promise](std::exception_ptr ep,
                 std::optional<std::vector<TurtleTriple>> opt) {
        promise.set_value({ep, std::move(opt)});
      });
  auto [ep, opt] = future.get();
  if (ep) std::rethrow_exception(ep);
  return opt;
}

// Generic synchronous wrapper over an `AsyncSingleFileParser`. Owns a small
// `thread_pool` used to drive the parser to completion.
template <typename InnerParser>
class LegacyStreamParser {
 public:
  LegacyStreamParser(
      std::unique_ptr<qlever::parser::AsyncBlockSource> buffer,
      const EncodedIriManager* ev,
      TripleComponent defaultGraph = qlever::specialIds().at(DEFAULT_GRAPH_IRI))
      : pool_{2} {
    parser_ = qlever::parser::makeStreamingParser<InnerParser>(
        std::move(buffer), ev, std::move(defaultGraph), pool_.get_executor());
  }

  ~LegacyStreamParser() {
    parser_->cancel();
    while (!eof_) {
      try {
        auto batch = drainOneBatch(*parser_);
        if (!batch) eof_ = true;
      } catch (...) {
      }
    }
  }

  bool getLine(TurtleTriple& triple) { return getOne(triple); }

  std::optional<std::vector<TurtleTriple>> getBatch() {
    return drainOneBatch(*parser_);
  }

  void printAndResetQueueStatistics() {}

  TurtleParserIntegerOverflowBehavior& integerOverflowBehavior() {
    return parser_->integerOverflowBehavior();
  }
  bool& invalidLiteralsAreSkipped() {
    return parser_->invalidLiteralsAreSkipped();
  }
  size_t getParsePosition() const { return 0; }

 private:
  bool getOne(TurtleTriple& triple) {
    if (cursor_ < buffer_.size()) {
      triple = std::move(buffer_[cursor_++]);
      return true;
    }
    if (eof_) return false;
    auto batch = drainOneBatch(*parser_);
    if (!batch) {
      eof_ = true;
      return false;
    }
    buffer_ = std::move(*batch);
    cursor_ = 0;
    if (buffer_.empty()) {
      // The async parser may legitimately emit empty batches; treat that as
      // "no more triples right now, ask again".
      return getOne(triple);
    }
    triple = std::move(buffer_[cursor_++]);
    return true;
  }

  // NOTE: the field declaration order is load-bearing. `parser_` must outlive
  // any handlers running on `pool_`, so `pool_` must be destroyed (which
  // joins all threads) before `parser_` goes away. Put `parser_` first.
  std::unique_ptr<qlever::parser::AsyncSingleFileParser> parser_;
  boost::asio::thread_pool pool_;
  std::vector<TurtleTriple> buffer_;
  size_t cursor_ = 0;
  bool eof_ = false;
};

// Same as `LegacyStreamParser`, but uses the parallel async parser inside.
template <typename InnerParser>
class LegacyParallelParser {
 public:
  LegacyParallelParser(std::unique_ptr<qlever::parser::AsyncBlockSource> buffer,
                       const EncodedIriManager* ev,
                       const TripleComponent& defaultGraph =
                           qlever::specialIds().at(DEFAULT_GRAPH_IRI))
      : pool_{2} {
    parser_ = qlever::parser::makeParallelFileParser<InnerParser>(
        std::move(buffer), ev, defaultGraph, pool_.get_executor());
  }

  // Overload that accepts a `sleepTimeForTesting` argument; ignored by the
  // async parser, which no longer exposes that knob. Kept so that tests of
  // destructor cancellation can keep their original signature.
  template <typename Duration>
  LegacyParallelParser(std::unique_ptr<qlever::parser::AsyncBlockSource> buffer,
                       const EncodedIriManager* ev,
                       const TripleComponent& defaultGraph,
                       Duration /*sleepTimeForTesting*/)
      : LegacyParallelParser{std::move(buffer), ev, defaultGraph} {}

  ~LegacyParallelParser() {
    // Cancel cooperatively so pending parse tasks unblock quickly, then
    // drain whatever remains in the output channel so the pool can join
    // without dangling references.
    parser_->cancel();
    while (!eof_) {
      try {
        auto batch = drainOneBatch(*parser_);
        if (!batch) eof_ = true;
      } catch (...) {
      }
    }
  }

  bool getLine(TurtleTriple& triple) {
    if (cursor_ < buffer_.size()) {
      triple = std::move(buffer_[cursor_++]);
      return true;
    }
    if (eof_) return false;
    auto batch = drainOneBatch(*parser_);
    if (!batch) {
      eof_ = true;
      return false;
    }
    buffer_ = std::move(*batch);
    cursor_ = 0;
    if (buffer_.empty()) return getLine(triple);
    triple = std::move(buffer_[cursor_++]);
    return true;
  }
  std::optional<std::vector<TurtleTriple>> getBatch() {
    return drainOneBatch(*parser_);
  }
  void printAndResetQueueStatistics() {}
  TurtleParserIntegerOverflowBehavior& integerOverflowBehavior() {
    return parser_->integerOverflowBehavior();
  }
  bool& invalidLiteralsAreSkipped() {
    return parser_->invalidLiteralsAreSkipped();
  }
  size_t getParsePosition() const { return 0; }

 private:
  std::unique_ptr<qlever::parser::AsyncSingleFileParser> parser_;
  boost::asio::thread_pool pool_;
  std::vector<TurtleTriple> buffer_;
  size_t cursor_ = 0;
  bool eof_ = false;
};

// Sync shim over `AsyncMultifileParser`.
class LegacyMultifileParser {
 public:
  LegacyMultifileParser(
      const std::vector<qlever::InputFileSpecification>& files,
      const EncodedIriManager* ev,
      ad_utility::MemorySize bufferSize = ad_utility::MemorySize::megabytes(10))
      : pool_{4} {
    parser_ = std::make_unique<qlever::parser::AsyncMultifileParser>(
        files, ev, bufferSize, pool_.get_executor());
  }

  ~LegacyMultifileParser() {
    parser_->cancel();
    while (!eof_) {
      try {
        auto batch = drainOneBatch(*parser_);
        if (!batch) eof_ = true;
      } catch (...) {
      }
    }
  }

  std::optional<std::vector<TurtleTriple>> getBatch() {
    return drainOneBatch(*parser_);
  }

  bool getLine(TurtleTriple& triple) {
    if (cursor_ < buffer_.size()) {
      triple = std::move(buffer_[cursor_++]);
      return true;
    }
    if (eof_) return false;
    auto batch = getBatch();
    if (!batch) {
      eof_ = true;
      return false;
    }
    buffer_ = std::move(*batch);
    cursor_ = 0;
    if (buffer_.empty()) return getLine(triple);
    triple = std::move(buffer_[cursor_++]);
    return true;
  }
  void printAndResetQueueStatistics() {}
  TurtleParserIntegerOverflowBehavior& integerOverflowBehavior() {
    return parser_->integerOverflowBehavior();
  }
  bool& invalidLiteralsAreSkipped() {
    return parser_->invalidLiteralsAreSkipped();
  }
  size_t getParsePosition() const { return 0; }

 private:
  std::unique_ptr<qlever::parser::AsyncMultifileParser> parser_;
  boost::asio::thread_pool pool_;
  std::vector<TurtleTriple> buffer_;
  size_t cursor_ = 0;
  bool eof_ = false;
};

}  // namespace qlever::test

#endif  // QLEVER_TEST_UTIL_LEGACYPARSERSHIMS_H
