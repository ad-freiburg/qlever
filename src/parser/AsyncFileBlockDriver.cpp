// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/AsyncFileBlockDriver.h"

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
  armNextBlock();
}

// ____________________________________________________________________________
AsyncFileBlockDriver::~AsyncFileBlockDriver() {
  // Wait for any in-flight handler to prevent use-after-free of `fileBuffer_`.
  if (pendingBlock_.valid()) {
    pendingBlock_.wait();
  }
}

// ____________________________________________________________________________
void AsyncFileBlockDriver::armNextBlock() {
  auto promise = std::make_shared<std::promise<BlockResult>>();
  pendingBlock_ = promise->get_future();
  fileBuffer_->asyncGetNextBlock(
      [p = std::move(promise)](std::exception_ptr ep,
                               std::optional<ByteBlock> opt) mutable {
        p->set_value({std::move(ep), std::move(opt)});
      });
}

// ____________________________________________________________________________
std::optional<ByteBlock> AsyncFileBlockDriver::getNextBlockSync() {
  auto [ep, opt] = pendingBlock_.get();
  if (ep) std::rethrow_exception(ep);
  if (opt.has_value()) armNextBlock();
  return opt;
}

}  // namespace qlever::parser
