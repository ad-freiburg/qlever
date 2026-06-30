// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/AsyncFileBlockDriver.h"

#include <boost/asio/use_future.hpp>
#include <memory>
#include <utility>

namespace qlever::parser {

// ____________________________________________________________________________
AsyncFileBlockDriver::AsyncFileBlockDriver(
    const qlever::InputFileSpecification& spec,
    ad_utility::MemorySize blocksize, std::string endRegex) {
  fileBuffer_ = std::make_unique<AsyncEndRegexBlockSource>(
      ioPool_.get_executor(),
      spec.makeAsyncBlockSource(ioPool_.get_executor(), blocksize),
      std::move(endRegex));
  pendingBlock_ = fileBuffer_->asyncGetNextBlock(boost::asio::use_future);
}

// ____________________________________________________________________________
AsyncFileBlockDriver::~AsyncFileBlockDriver() {
  // Wait for any in-flight handler to prevent use-after-free of `fileBuffer_`.
  if (pendingBlock_.valid()) {
    pendingBlock_.wait();
  }
  // To additionally be safe that the `fileBuffer_` is unused before it gets
  // destroyed.
  ioPool_.join();
}

// ____________________________________________________________________________
std::optional<ByteBlock> AsyncFileBlockDriver::getNextBlock() {
  if (!pendingBlock_.valid()) return std::nullopt;
  auto opt = pendingBlock_.get();
  if (opt.has_value()) {
    pendingBlock_ = fileBuffer_->asyncGetNextBlock(boost::asio::use_future);
  }
  return opt;
}

}  // namespace qlever::parser
