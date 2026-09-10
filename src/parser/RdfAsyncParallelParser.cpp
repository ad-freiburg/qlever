// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/RdfAsyncParallelParser.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <string>
#include <utility>

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
    : AsyncRdfParserBase{executor},
      state_{encodedIriManager, defaultGraphIri},
      blockSource_{executor, spec.makeAsyncBlockSource(executor, blocksize),
                   detail::findEndOfLastStatement,
                   std::string{detail::statementBoundaryDescription}},
      blockFetchPermit_{executor, 1} {}

// _____________________________________________________________________________
template <typename Parser>
void RdfAsyncParallelParser<Parser>::asyncGetBatchImpl(Handler handler) {
  boost::asio::co_spawn(executor(), getBatchCoroutine(), std::move(handler));
}

// ____________________________________________________________________________
template <typename Parser>
net::awaitable<void> RdfAsyncParallelParser<Parser>::parseHeader() {
  while (state_.parseHeaderStep(
      co_await blockSource_.asyncGetNextBlock(net::use_awaitable))) {
    // Nothing to do, all the work happens inside `parseHeaderStep`.
  }
}

// ____________________________________________________________________________
template <typename Parser>
net::awaitable<typename RdfAsyncParallelParser<Parser>::OptionalTriples>
RdfAsyncParallelParser<Parser>::getBatchCoroutine() {
  // A previous batch failed, so signal a clean end of the input to stop the
  // caller's pipeline without reporting yet another error.
  if (errorWasEncountered_.load()) {
    co_return std::nullopt;
  }
  // Declared before the `try` block below and hence destroyed only after it,
  // in particular only after `errorWasEncountered_` has been set. Were the
  // permit released during the unwinding (that is, before the `catch` block
  // runs), then a call that is waiting for the permit could acquire it and
  // continue although the header of this parser is broken.
  Permit permit;
  try {
    // Acquire the single permit before anything else. This serializes the
    // fetching of the blocks and at the same time waits for the parsing of the
    // header, see the class comment.
    permit = co_await blockFetchPermit_.asyncAcquire(net::use_awaitable);
    AD_CORRECTNESS_CHECK(permit.isValid());
    // Another call has failed while this call was waiting for the permit, so
    // there is nothing left to parse.
    if (errorWasEncountered_.load()) {
      co_return std::nullopt;
    }
    // The first call parses the header. Because it holds the permit while
    // doing so, no other call can interleave with (or overtake) it. An error
    // is propagated to this very call, exactly like an error during the
    // parsing of a batch below.
    if (!std::exchange(headerWasParsed_, true)) {
      co_await parseHeader();
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
    // get a clean end of the input instead, see the class comment. The permit
    // (if it is still held) is only released after this handler has run, see
    // its declaration above.
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
