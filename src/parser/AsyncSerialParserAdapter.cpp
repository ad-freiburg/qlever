// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/AsyncSerialParserAdapter.h"

#include <boost/asio/post.hpp>
#include <exception>
#include <utility>

#include "util/Exception.h"

namespace net = boost::asio;

// _____________________________________________________________________________
AsyncSerialParserAdapter::AsyncSerialParserAdapter(
    const ql::any_io_executor& executor, ParserFactory parserFactory)
    : AsyncRdfParserBase{executor},
      strand_{net::make_strand(executor)},
      parserFactory_{std::move(parserFactory)} {
  AD_CONTRACT_CHECK(parserFactory_ != nullptr);
}

// _____________________________________________________________________________
void AsyncSerialParserAdapter::asyncGetBatchImpl(Handler handler) {
  net::post(strand_, [this, h = std::move(handler)]() mutable {
    std::exception_ptr exception;
    OptionalTriples batch;
    if (!finished_) {
      try {
        // The first call also has to create the parser, see the constructor.
        if (parser_ == nullptr) {
          parser_ = std::move(parserFactory_)();
          AD_CORRECTNESS_CHECK(parser_ != nullptr);
        }
        batch = parser_->getBatch();
      } catch (...) {
        exception = std::current_exception();
      }
      finished_ = !batch.has_value();
    }
    // NOTE: `h` may be invoked directly from within the strand, see the
    // comment on `AsyncRdfParserBase::Handler`.
    std::move(h)(std::move(exception), std::move(batch));
  });
}
