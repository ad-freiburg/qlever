// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_ASYNCMULTIFILEPARSER_H
#define QLEVER_SRC_PARSER_ASYNCMULTIFILEPARSER_H

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/strand.hpp>
#include <memory>
#include <mutex>
#include <vector>

#include "index/InputFileSpecification.h"
#include "parser/AsyncSingleFileParser.h"
#include "util/MemorySize/MemorySize.h"

namespace qlever::parser {

// Asynchronous orchestrator over multiple input files. Replaces the
// synchronous `RdfMultifileParser`. Up to `NUM_PARALLEL_PARSER_THREADS` files
// are parsed concurrently; each file's batches are pushed into a single
// output channel and consumed in arbitrary inter-file order via
// `asyncGetNextBatch`.
class AsyncMultifileParser {
 public:
  using TripleBatch = std::vector<TurtleTriple>;
  using Handler = AsyncSingleFileParser::Handler;

  AsyncMultifileParser(std::vector<qlever::InputFileSpecification> files,
                       const EncodedIriManager* encodedIriManager,
                       ad_utility::MemorySize bufferSize);

  ~AsyncMultifileParser();

  // Same single-flight contract as `AsyncSingleFileParser`. EOF is signaled
  // by `(nullptr, nullopt)`; an exception by `(exception_ptr, nullopt)`.
  void asyncGetNextBatch(boost::asio::any_io_executor exec, Handler handler);

  // Parser-wide settings; applied to every per-file parser when it is
  // constructed (so call these before the first `asyncGetNextBatch`).
  TurtleParserIntegerOverflowBehavior& integerOverflowBehavior();
  bool& invalidLiteralsAreSkipped();

  // Cooperatively cancel parsing. After `cancel()`, in-flight per-file
  // parsers are cancelled and the next `asyncGetNextBatch` resolves to EOF
  // (or to a previously-captured error). Idempotent.
  void cancel();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace qlever::parser

#endif  // QLEVER_SRC_PARSER_ASYNCMULTIFILEPARSER_H
