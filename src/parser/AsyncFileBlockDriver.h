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

// A small wrapper around an `AsyncBlockSource` that is required temporarily
// while the rest of the index builder pipeline is not yet migrated to
// Boost::Asio. It internally holds a `unique_ptr<AsyncEndRegexBlockSource>` and
// schedules it on a thread pool with a single thread. The public interface is
// a synchronous `getNextBlock()` function, the asynchronous prefetching of the
// next block is purely internal.
class AsyncFileBlockDriver {
 public:
  // Open the file described by `spec`, wrap it in an `AsyncEndRegexBlockSource`
  // that cuts blocks at the positions determined by `findEndPosition`
  // (`description` is used in error messages, see `AsyncEndRegexBlockSource`),
  // and immediately start prefetching the first block.
  AsyncFileBlockDriver(
      const qlever::InputFileSpecification& spec,
      ad_utility::MemorySize blocksize,
      AsyncEndRegexBlockSource::EndPositionFinder findEndPosition,
      std::string description);

  // Synchronously obtain the next block. This waits for the next block to
  // become available and then immediately starts prefetching the next one.
  std::optional<ByteBlock> getNextBlock();

  ~AsyncFileBlockDriver();

 private:
  // `ioPool_` is declared before `fileBuffer_` so that the I/O thread outlives
  // the source it drives.
  boost::asio::thread_pool ioPool_{1};
  std::unique_ptr<AsyncBlockSource> fileBuffer_;
  std::future<std::optional<ByteBlock>> pendingBlock_;
};

}  // namespace qlever::parser

#endif  // QLEVER_SRC_PARSER_ASYNCFILEBLOCKDRIVER_H
