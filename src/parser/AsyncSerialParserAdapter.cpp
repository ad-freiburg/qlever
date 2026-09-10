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
    const ql::any_io_executor& executor, std::unique_ptr<RdfParserBase> parser)
    : AsyncRdfParserBase{executor},
      strand_{net::make_strand(executor)},
      parser_{std::move(parser)} {
  AD_CONTRACT_CHECK(parser_ != nullptr);
}

// _____________________________________________________________________________
void AsyncSerialParserAdapter::asyncGetBatchImpl(Handler handler) {
  net::post(strand_, [this, h = std::move(handler)]() mutable {
    std::exception_ptr exception;
    OptionalTriples batch;
    if (!finished_) {
      try {
        batch = parser_->getBatch();
      } catch (...) {
        exception = std::current_exception();
      }
      finished_ = !batch.has_value();
    }
    // NOTE: `h` may be invoked directly from within the strand, because
    // `AsyncRdfParserBase::asyncGetBatch` does nothing but `post` the actual
    // completion handler onto its associated executor (see the comment on
    // `asyncGetBatchImpl` there). The caller's processing of the batch, which
    // typically runs inline in that completion handler and is expensive, hence
    // does not block the strand and thus not the next `getBatch()` call.
    std::move(h)(exception, std::move(batch));
  });
}
