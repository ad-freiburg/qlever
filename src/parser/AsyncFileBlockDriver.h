// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_ASYNCFILEBLOCKDRIVER_H
#define QLEVER_SRC_PARSER_ASYNCFILEBLOCKDRIVER_H

#include <boost/asio/thread_pool.hpp>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "index/InputFileSpecification.h"
#include "parser/AsyncBlockSource.h"
#include "util/MemorySize/MemorySize.h"

namespace qlever::parser {

// Encapsulate the I/O thread pool, `AsyncBlockSource`, and in-flight future
// that both `RdfStreamParser` and `RdfParallelParser` previously duplicated.
// Construct with an `InputFileSpecification`, a blocksize, and an end-regex;
// then call `getNextBlock()` to retrieve blocks one at a time.
class AsyncFileBlockDriver {
 public:
  // Open the file described by `spec`, wrap it in an `AsyncEndRegexBlockSource`
  // with the given `endRegex`, and arm the first async read.
  AsyncFileBlockDriver(const qlever::InputFileSpecification& spec,
                       ad_utility::MemorySize blocksize, std::string endRegex);

  // Wait for the in-flight read, arm the next one if more data follows, and
  // return the block. Returns `nullopt` at EOF or if no read is in flight;
  // rethrows on error.
  std::optional<ByteBlock> getNextBlock();

  // Wait for any in-flight handler to prevent use-after-free of `fileBuffer_`.
  ~AsyncFileBlockDriver();

 private:
  // Declared before `fileBuffer_` so that the I/O thread outlives the source
  // it drives.
  boost::asio::thread_pool ioPool_{1};
  std::unique_ptr<AsyncBlockSource> fileBuffer_;
  std::future<std::optional<ByteBlock>> pendingBlock_;
};

}  // namespace qlever::parser

#endif  // QLEVER_SRC_PARSER_ASYNCFILEBLOCKDRIVER_H
