// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_ASYNCPARSERDRIVER_H
#define QLEVER_SRC_PARSER_ASYNCPARSERDRIVER_H

#include <atomic>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/thread_pool.hpp>
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
// `ThreadSafeQueue`. A task chain is not a thread of its own: it is a sequence
// of asynchronous operations, each of which schedules its successor onto the
// thread pool once it has completed. `getBatch()` pops from that queue,
// propagating errors via exception and signalling EOF via `nullopt`. When any
// task chain encounters an error, `ThreadSafeQueue::pushException` forwards it
// to the next `getBatch()` call; batches from still-in-flight sibling chains
// are silently discarded once the queue is finished.
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
  // Decrement the number of active task chains and signal EOF via
  // `queue_.finish()` when the last one has stopped.
  void taskChainFinished() {
    if (--numActiveTaskChains_ == 0) {
      queue_.finish();
    }
  }

  // Schedule one link of a task chain: call `asyncGetBatch` and, on completion,
  // push the result into `queue_` and schedule the next link, or stop the
  // chain via `taskChainFinished()` on EOF or queue rejection.
  void startTaskChain() {
    namespace net = boost::asio;
    asyncParser_.asyncGetBatch(net::bind_executor(
        pool_.get_executor(),
        [this](std::exception_ptr eptr,
               std::optional<std::vector<TurtleTriple>> batch) mutable {
          if (eptr) {
            queue_.pushException(eptr);
            taskChainFinished();
            return;
          }
          if (batch.has_value()) {
            if (queue_.push(std::move(*batch))) {
              startTaskChain();
            } else {
              taskChainFinished();
            }
          } else {
            taskChainFinished();
          }
        }));
  }
};

#endif  // QLEVER_SRC_PARSER_ASYNCPARSERDRIVER_H
