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
// `NUM_PARALLEL_PARSER_THREADS` workers each keep one `asyncGetBatch()` call
// in flight concurrently and push the parsed batches into a bounded
// `ThreadSafeQueue`. `getBatch()` pops from that queue, propagating errors via
// exception and signalling EOF via `nullopt`. When any worker encounters an
// error, `ThreadSafeQueue::pushException` forwards it to the next `getBatch()`
// call; batches from still-in-flight sibling workers are silently discarded
// once the queue is finished.
template <typename AsyncParser>
class AsyncParserDriver : public RdfParserBase {
 private:
  std::optional<boost::asio::thread_pool> pool_;
  std::optional<AsyncParser> asyncParser_;
  // Batches pushed by workers. `pop()` returns `nullopt` (EOF) or throws
  // (error) when all workers have stopped.
  ad_utility::data_structures::ThreadSafeQueue<std::vector<TurtleTriple>>
      queue_{NUM_PARALLEL_PARSER_THREADS};
  std::atomic<size_t> activeWorkers_{0};
  // Guards the lazy startup of workers: true once the workers have been
  // started by the first call to `getBatch()`.
  std::atomic<bool> workersStarted_{false};

 public:
  // Construct the `AsyncParser` on this driver's own thread pool. The `args`
  // are forwarded to its constructor after the executor of that pool, so a
  // derived class only has to supply whatever the concrete parser needs (see
  // `RdfParallelParserViaAsync`).
  template <typename... Args>
  AsyncParserDriver(const EncodedIriManager* encodedIriManager, Args&&... args)
      : RdfParserBase{encodedIriManager},
        pool_{std::in_place, NUM_PARALLEL_PARSER_THREADS} {
    asyncParser_.emplace(pool_->get_executor(), AD_FWD(args)...);
  }

  // Finish the queue and join the pool so that no pool thread accesses
  // `queue_` or `asyncParser_` after those are destroyed. This destructor also
  // handles early destruction on exception.
  ~AsyncParserDriver() override {
    if (pool_.has_value()) {
      queue_.finish();
      pool_->join();
      asyncParser_.reset();
    }
  }

  // Pop and return the next batch from the worker queue. Throw if any worker
  // encountered a parse error; return `nullopt` on EOF. Workers are started
  // lazily on the first call so that immediately-destroyed parsers (created
  // but never consumed, e.g. in tests) do not leave in-flight asynchronous
  // operations that would delay the destructor.
  std::optional<std::vector<TurtleTriple>> getBatch() override {
    if (!asyncParser_.has_value()) {
      return std::nullopt;
    }
    if (!workersStarted_.exchange(true)) {
      activeWorkers_ = NUM_PARALLEL_PARSER_THREADS;
      for (size_t i = 0; i < NUM_PARALLEL_PARSER_THREADS; ++i) {
        startWorker();
      }
    }
    return queue_.pop();
  }

  size_t getParsePosition() const override { return 0; }

 private:
  // Decrement the active worker count and signal EOF via `queue_.finish()`
  // when the last worker stops.
  void workerCompleted() {
    if (--activeWorkers_ == 0) {
      queue_.finish();
    }
  }

  // Initiate one async cycle: call `asyncGetBatch` and, on completion, push
  // the result into `queue_` and start another cycle, or stop the worker via
  // `workerCompleted()` on EOF or queue rejection.
  void startWorker() {
    namespace net = boost::asio;
    asyncParser_->asyncGetBatch(net::bind_executor(
        pool_->get_executor(),
        [this](std::exception_ptr eptr,
               std::optional<std::vector<TurtleTriple>> batch) mutable {
          if (eptr) {
            queue_.pushException(eptr);
            workerCompleted();
            return;
          }
          if (batch.has_value()) {
            if (queue_.push(std::move(*batch))) {
              startWorker();
            } else {
              workerCompleted();
            }
          } else {
            workerCompleted();
          }
        }));
  }
};

#endif  // QLEVER_SRC_PARSER_ASYNCPARSERDRIVER_H
