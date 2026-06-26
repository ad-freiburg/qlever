// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_ASYNCSINGLEFILEPARSER_H
#define QLEVER_SRC_PARSER_ASYNCSINGLEFILEPARSER_H

#include <boost/asio/any_io_executor.hpp>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "index/InputFileSpecification.h"
#include "parser/AsyncBlockSource.h"
#include "parser/RdfParser.h"  // for TurtleTriple, TurtleParser, NQuadParser

namespace qlever::parser {

// Abstract async single-file RDF parser. Replaces the synchronous
// `RdfStreamParser<T>` and `RdfParallelParser<T>`. The handler is invoked
// with one batch of triples per call; `nullopt` (and no error) signals EOF.
// Single-flight: do not call `asyncGetNextBatch` again before the previous
// handler has fired.
class AsyncSingleFileParser {
 public:
  using TripleBatch = std::vector<TurtleTriple>;
  using Handler =
      std::function<void(std::exception_ptr, std::optional<TripleBatch>)>;

  virtual void asyncGetNextBatch(Handler handler) = 0;

  // Parser-wide options. Mirror the ones on `RdfParserBase`; each concrete
  // class forwards to its inner parser instance.
  virtual TurtleParserIntegerOverflowBehavior& integerOverflowBehavior() = 0;
  virtual bool& invalidLiteralsAreSkipped() = 0;

  // Cooperatively cancel any in-flight work. After `cancel()`, subsequent
  // `asyncGetNextBatch` calls (and any already-pending one) resolve with
  // EOF or an `operation_aborted` exception as soon as the cancellation
  // propagates. Safe to call from any thread; idempotent. Default: no-op.
  virtual void cancel() {}

  virtual ~AsyncSingleFileParser() = default;
};

// Create an `AsyncSingleFileParser` for one input file specification. The
// concrete type is `AsyncStreamingParser<T>` for `parseInParallel_ == false`
// or `AsyncParallelFileParser<T>` otherwise. Templated over the tokenizer.
// The `exec` drives both the block source and the parse work.
std::unique_ptr<AsyncSingleFileParser> makeAsyncSingleFileParser(
    const qlever::InputFileSpecification& input, const EncodedIriManager* ev,
    ad_utility::MemorySize bufferSize, boost::asio::any_io_executor exec);

// Direct constructors for the two concrete async parser types. Used by
// tests; production code uses `makeAsyncSingleFileParser`.
template <typename InnerParser>
std::unique_ptr<AsyncSingleFileParser> makeStreamingParser(
    std::unique_ptr<AsyncBlockSource> rawBuffer, const EncodedIriManager* ev,
    TripleComponent defaultGraph, boost::asio::any_io_executor exec);

template <typename InnerParser>
std::unique_ptr<AsyncSingleFileParser> makeParallelFileParser(
    std::unique_ptr<AsyncBlockSource> rawBuffer, const EncodedIriManager* ev,
    const TripleComponent& defaultGraph, boost::asio::any_io_executor exec);

}  // namespace qlever::parser

#endif  // QLEVER_SRC_PARSER_ASYNCSINGLEFILEPARSER_H
