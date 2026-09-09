// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/RdfAsyncParallelParser.h"

#include <boost/asio/use_awaitable.hpp>
#include <string>

#include "parser/Tokenizer.h"
#include "parser/TokenizerCtre.h"
#include "util/Exception.h"

namespace net = boost::asio;

// ____________________________________________________________________________
template <typename Parser>
RdfAsyncParallelParser<Parser>::RdfAsyncParallelParser(
    const ql::any_io_executor& executor,
    const qlever::InputFileSpecification& spec,
    ad_utility::MemorySize blocksize,
    const EncodedIriManager* encodedIriManager,
    const TripleComponent& defaultGraphIri)
    : executor_{executor},
      state_{encodedIriManager, defaultGraphIri},
      blockSource_{executor, spec.makeAsyncBlockSource(executor, blocksize),
                   detail::findEndOfLastStatement,
                   std::string{detail::statementBoundaryDescription}},
      blockFetchPermit_{executor, 1} {}

// ____________________________________________________________________________
template <typename Parser>
net::awaitable<typename RdfAsyncParallelParser<Parser>::OptionalTriples>
RdfAsyncParallelParser<Parser>::getBatchCoroutine() {
  // A previous batch failed, so signal a clean end of the input to stop the
  // caller's pipeline without reporting yet another error.
  if (errorWasEncountered_.load()) {
    co_return std::nullopt;
  }
  try {
    // Acquire the single permit before anything else. This serializes the
    // fetching of the blocks and at the same time waits for the parsing of the
    // header, see the class comment. The permit is released by its destructor
    // if anything below throws.
    Permit permit = co_await blockFetchPermit_.asyncAcquire(net::use_awaitable);
    AD_CORRECTNESS_CHECK(permit.isValid());
    // The first call parses the header. Because it holds the permit while
    // doing so, no other call can interleave with (or overtake) it.
    if (!headerWasParsed_) {
      headerWasParsed_ = true;
      try {
        // Feed the blocks of the input to the header parser one by one, until
        // it reports that the header is complete.
        while (state_.parseHeaderStep(
            co_await blockSource_.asyncGetNextBlock(net::use_awaitable))) {
        }
      } catch (...) {
        // Store the error while the permit is still held, so that the calls
        // that are waiting for it also see it below.
        initializationError_ = std::current_exception();
      }
    }
    // The parsing of the header failed (possibly in another call), so not a
    // single batch can be parsed; report that error.
    if (initializationError_) {
      std::rethrow_exception(initializationError_);
    }
    // The first caller gets to parse the remainder that was left over by the
    // parsing of the header, all others fetch a fresh block.
    auto block = state_.takeRemainderFromInitialization();
    if (!block.has_value()) {
      block = co_await blockSource_.asyncGetNextBlock(net::use_awaitable);
    }
    // Let the next waiting call fetch its block while this call parses the
    // block it just got. This is safe: `blockSource_` has already updated all
    // of its state by the time the `co_await` above has resumed us.
    permit.release();
    if (!block.has_value()) {
      co_return std::nullopt;
    }
    co_return state_.parseBatch(std::move(block).value());
  } catch (...) {
    // Only the first error is propagated to its caller, all subsequent calls
    // get a clean end of the input instead, see the class comment.
    if (!errorWasEncountered_.exchange(true)) {
      throw;
    }
    co_return std::nullopt;
  }
}

template class RdfAsyncParallelParser<TurtleParser<Tokenizer>>;
template class RdfAsyncParallelParser<TurtleParser<TokenizerCtre>>;
template class RdfAsyncParallelParser<NQuadParser<Tokenizer>>;
template class RdfAsyncParallelParser<NQuadParser<TokenizerCtre>>;
