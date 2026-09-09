// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_ASYNCPARSERDRIVER_H
#define QLEVER_SRC_PARSER_ASYNCPARSERDRIVER_H

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cstddef>
#include <exception>
#include <optional>
#include <utility>
#include <vector>

#include "index/ConstantsIndexBuilding.h"
#include "parser/RdfParser.h"
#include "util/Forward.h"
#include "util/ThreadSafeQueue.h"

// Drive an asynchronous RDF parser from its own thread pool and expose it
// through the synchronous `RdfParserBase` interface, so that it can be used
// wherever `RdfParallelParser` can.
//
// `AsyncParser` may be any type with a
// `asyncGetBatch(CompletionToken)` operation whose completion signature is
// `void(std::exception_ptr, std::optional<std::vector<TurtleTriple>>)`, see
// `RdfAsyncParallelParser` for the (currently only) such parser.
//
// `NUM_PARALLEL_PARSER_THREADS` task chains each keep one `asyncGetBatch()`
// call in flight concurrently and push the parsed batches into a bounded
// `ThreadSafeQueue`. A task chain is not a thread of its own: it is the
// coroutine `runTaskChain()`, which suspends (without blocking a thread) while
// it waits for its batch. `getBatch()` pops from that queue, propagating
// errors via exception and signalling EOF via `nullopt`. When any task chain
// encounters an error, `ThreadSafeQueue::pushException` forwards it to the
// next `getBatch()` call; batches from still-in-flight sibling chains are
// silently discarded once the queue is finished.
template <typename AsyncParser>
class AsyncParserDriver : public RdfParserBase {
 private:
  boost::asio::thread_pool pool_{NUM_PARALLEL_PARSER_THREADS};
  AsyncParser asyncParser_;
  // Batches pushed by the task chains. `pop()` returns `nullopt` (EOF) or
  // throws (error) when all of them have stopped.
  ad_utility::data_structures::ThreadSafeQueue<std::vector<TurtleTriple>>
      queue_{NUM_PARALLEL_PARSER_THREADS};
  std::atomic<size_t> numActiveTaskChains_{0};
  // Guards the lazy startup of the task chains: true once they have been
  // started by the first call to `getBatch()`.
  std::atomic<bool> taskChainsStarted_{false};

 public:
  // Construct the `AsyncParser` on this driver's own thread pool. The `args`
  // are forwarded to its constructor after the executor of that pool, so a
  // derived class only has to supply whatever the concrete parser needs (see
  // `RdfParallelParserViaAsync`).
  template <typename... Args>
  explicit AsyncParserDriver(const EncodedIriManager* encodedIriManager,
                             Args&&... args)
      : RdfParserBase{encodedIriManager},
        asyncParser_{pool_.get_executor(), AD_FWD(args)...} {}

  // Finish the queue and join the pool, so that no pool thread accesses
  // `queue_` or `asyncParser_` after those are destroyed (the members are
  // destroyed after this body has run, and `pool_` is declared first and hence
  // destroyed last).
  ~AsyncParserDriver() override {
    queue_.finish();
    pool_.join();
  }

  // Pop and return the next batch from the queue. Throw if any task chain
  // encountered a parse error; return `nullopt` on EOF. The task chains are
  // started lazily on the first call so that immediately-destroyed parsers
  // (created but never consumed, e.g. in tests) do not leave in-flight
  // asynchronous operations that would delay the destructor.
  std::optional<std::vector<TurtleTriple>> getBatch() override {
    if (!taskChainsStarted_.exchange(true)) {
      numActiveTaskChains_ = NUM_PARALLEL_PARSER_THREADS;
      for (size_t i = 0; i < NUM_PARALLEL_PARSER_THREADS; ++i) {
        startTaskChain();
      }
    }
    return queue_.pop();
  }

  size_t getParsePosition() const override { return 0; }

 private:
  // One task chain: repeatedly get the next batch and push it into `queue_`,
  // until the input is exhausted or `queue_` no longer accepts batches. A parse
  // error propagates out of this coroutine and is handled by the completion
  // handler in `startTaskChain` below.
  boost::asio::awaitable<void> runTaskChain() {
    namespace net = boost::asio;
    while (auto batch =
               co_await asyncParser_.asyncGetBatch(net::use_awaitable)) {
      if (!queue_.push(std::move(batch).value())) {
        break;
      }
    }
  }

  // Start one task chain on `pool_`, and when it has stopped, forward a
  // possible parse error to `queue_` and signal EOF via `queue_.finish()` if
  // this was the last active chain.
  void startTaskChain() {
    namespace net = boost::asio;
    net::co_spawn(pool_.get_executor(), runTaskChain(),
                  net::bind_executor(pool_.get_executor(),
                                     [this](std::exception_ptr eptr) {
                                       if (eptr) {
                                         queue_.pushException(std::move(eptr));
                                       }
                                       if (--numActiveTaskChains_ == 0) {
                                         queue_.finish();
                                       }
                                     }));
  }
};

#endif  // QLEVER_SRC_PARSER_ASYNCPARSERDRIVER_H
